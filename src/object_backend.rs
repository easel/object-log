use crate::model::LogBackend;
use crate::segment::{Segment, decode_segment, encode_segment};
use crate::util::sha256_hex;
use crate::{
    AckMode, AppendBatch, AppendResult, AppendedRecord, ObjectKey, ObjectLogError, ObjectStore,
    ObjectVersion, ProducerState, ReadBatch, ReadRequest, TimestampPolicy, TopicPartition,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// External epoch validation hook. Callers own the control plane.
#[async_trait]
pub trait EpochGuard: Send + Sync {
    async fn check(
        &self,
        topic_partition: &TopicPartition,
        expected_epoch: u64,
    ) -> Result<(), ObjectLogError>;
}

#[derive(Clone)]
pub struct ObjectLogBackendConfig {
    pub min_records_per_segment: usize,
}

impl Default for ObjectLogBackendConfig {
    fn default() -> Self {
        Self {
            min_records_per_segment: 1,
        }
    }
}

pub struct ObjectLogBackend {
    store: Arc<dyn ObjectStore>,
    epoch_guard: Option<Arc<dyn EpochGuard>>,
    config: ObjectLogBackendConfig,
}

impl ObjectLogBackend {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            epoch_guard: None,
            config: ObjectLogBackendConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ObjectLogBackendConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_epoch_guard(mut self, epoch_guard: Arc<dyn EpochGuard>) -> Self {
        self.epoch_guard = Some(epoch_guard);
        self
    }

    async fn load_manifest(
        &self,
        topic_partition: &TopicPartition,
    ) -> Result<(Manifest, Option<ObjectVersion>), ObjectLogError> {
        let key = manifest_key(topic_partition)?;
        match self.store.get(&key).await? {
            Some(object) => {
                let manifest = serde_json::from_slice(&object.value)?;
                Ok((manifest, Some(object.version)))
            }
            None => Ok((Manifest::default(), None)),
        }
    }

    fn validate_batch(&self, batch: &AppendBatch) -> Result<(), ObjectLogError> {
        if batch.records.is_empty() {
            return Err(ObjectLogError::InvalidBatch);
        }
        if batch.records.len() < self.config.min_records_per_segment {
            return Err(ObjectLogError::InvalidBatch);
        }
        Ok(())
    }
}

#[async_trait]
impl LogBackend for ObjectLogBackend {
    async fn append(&self, batch: AppendBatch) -> Result<AppendResult, ObjectLogError> {
        self.validate_batch(&batch)?;
        if !self.store.capabilities().compare_and_set {
            return Err(ObjectLogError::UnsupportedCapability("compare_and_set"));
        }

        if let Some(expected_epoch) = batch.expected_epoch
            && let Some(guard) = &self.epoch_guard
        {
            guard.check(&batch.topic_partition, expected_epoch).await?;
        }

        let (mut manifest, expected_version) = self.load_manifest(&batch.topic_partition).await?;
        if let Some(producer) = &batch.producer
            && let Some(existing) = manifest.producers.get(&producer_key(producer))
        {
            if existing.record_count == batch.records.len() {
                return Ok(AppendResult {
                    topic_partition: batch.topic_partition,
                    base_offset: Some(existing.base_offset),
                    last_offset: Some(existing.last_offset),
                    record_count: existing.record_count,
                    acked: batch.acks != AckMode::None,
                    commit_ref: Some(existing.segment_key.clone()),
                });
            }
            return Err(ObjectLogError::SequenceConflict);
        }

        let base_offset = manifest.next_offset;
        let append_timestamp = now_ms();
        let records = batch
            .records
            .into_iter()
            .enumerate()
            .map(|(index, record)| AppendedRecord {
                offset: base_offset + index as u64,
                key: record.key,
                value: record.value,
                headers: record.headers,
                timestamp_ms: match batch.timestamp_policy {
                    TimestampPolicy::CreateTime => record.timestamp_ms.unwrap_or(append_timestamp),
                    TimestampPolicy::LogAppendTime => append_timestamp,
                },
            })
            .collect::<Vec<_>>();
        let epoch = batch.expected_epoch.unwrap_or(0);
        let segment = Segment {
            topic_partition: batch.topic_partition.clone(),
            base_offset,
            epoch,
            records,
        };
        let encoded = encode_segment(&segment)?;
        let checksum = sha256_hex(&encoded);
        let segment_key = segment_key(&batch.topic_partition, base_offset)?;
        self.store
            .put_if_absent(&segment_key, encoded.clone())
            .await?;

        let entry = ManifestEntry {
            segment_key: segment_key.as_str().to_string(),
            base_offset,
            last_offset: segment.last_offset(),
            record_count: segment.records.len(),
            epoch,
            checksum,
        };
        manifest.next_offset = entry.last_offset + 1;
        manifest.entries.push(entry.clone());
        if let Some(producer) = &batch.producer {
            manifest.producers.insert(
                producer_key(producer),
                ProducerCommit {
                    base_offset: entry.base_offset,
                    last_offset: entry.last_offset,
                    record_count: entry.record_count,
                    segment_key: entry.segment_key.clone(),
                },
            );
        }
        let manifest_bytes = Bytes::from(serde_json::to_vec(&manifest)?);
        self.store
            .compare_and_set(
                &manifest_key(&batch.topic_partition)?,
                expected_version,
                manifest_bytes,
            )
            .await?;

        Ok(AppendResult {
            topic_partition: batch.topic_partition,
            base_offset: (batch.acks != AckMode::None).then_some(entry.base_offset),
            last_offset: (batch.acks != AckMode::None).then_some(entry.last_offset),
            record_count: entry.record_count,
            acked: batch.acks != AckMode::None,
            commit_ref: (batch.acks != AckMode::None).then_some(entry.segment_key),
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadBatch, ObjectLogError> {
        let (manifest, _) = self.load_manifest(&request.topic_partition).await?;
        let mut records = Vec::new();
        let mut next_offset = request.start_offset;
        for entry in manifest.entries {
            if entry.last_offset < request.start_offset {
                continue;
            }
            if records.len() >= request.max_records {
                break;
            }
            let key = ObjectKey::new(entry.segment_key.clone())?;
            let object = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| ObjectLogError::MissingObject(entry.segment_key.clone()))?;
            if sha256_hex(&object.value) != entry.checksum {
                return Err(ObjectLogError::CorruptSegment(
                    "manifest checksum mismatch".to_string(),
                ));
            }
            let segment = decode_segment(&object.value)?;
            if segment.topic_partition != request.topic_partition {
                return Err(ObjectLogError::CorruptSegment(
                    "segment topic partition mismatch".to_string(),
                ));
            }
            if segment.base_offset != entry.base_offset
                || segment.last_offset() != entry.last_offset
            {
                return Err(ObjectLogError::OffsetDiscontinuity {
                    expected: entry.base_offset,
                    actual: segment.base_offset,
                });
            }
            for record in segment.records {
                if record.offset < request.start_offset {
                    continue;
                }
                if record.offset != next_offset {
                    return Err(ObjectLogError::OffsetDiscontinuity {
                        expected: next_offset,
                        actual: record.offset,
                    });
                }
                if records.len() == request.max_records {
                    break;
                }
                next_offset = record.offset + 1;
                records.push(record);
            }
        }
        Ok(ReadBatch {
            records,
            next_offset,
            high_watermark: Some(manifest.next_offset),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Manifest {
    #[serde(default = "manifest_version")]
    version: u16,
    #[serde(default)]
    next_offset: u64,
    #[serde(default)]
    entries: Vec<ManifestEntry>,
    #[serde(default)]
    producers: BTreeMap<String, ProducerCommit>,
}

fn manifest_version() -> u16 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ManifestEntry {
    segment_key: String,
    base_offset: u64,
    last_offset: u64,
    record_count: usize,
    epoch: u64,
    checksum: String,
}

fn producer_key(value: &ProducerState) -> String {
    format!(
        "{}:{}:{}",
        value.producer_id, value.producer_epoch, value.base_sequence
    )
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProducerCommit {
    base_offset: u64,
    last_offset: u64,
    record_count: usize,
    segment_key: String,
}

fn manifest_key(topic_partition: &TopicPartition) -> Result<ObjectKey, ObjectLogError> {
    ObjectKey::new(format!(
        "topics/{}/partitions/{:010}/manifest",
        topic_partition.topic.as_str(),
        topic_partition.partition.0
    ))
}

fn segment_key(
    topic_partition: &TopicPartition,
    base_offset: u64,
) -> Result<ObjectKey, ObjectLogError> {
    ObjectKey::new(format!(
        "topics/{}/partitions/{:010}/segments/{base_offset:020}.olseg",
        topic_partition.topic.as_str(),
        topic_partition.partition.0
    ))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}
