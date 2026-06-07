use async_trait::async_trait;
use bytes::Bytes;
use object_log::{
    AckMode, AppendBatch, AppendRecord, LocalObjectStore, LogBackend, MemoryObjectStore, ObjectKey,
    ObjectLogBackend, ObjectLogBackendConfig, ObjectLogError, ObjectStore, ObjectVersion,
    PartitionId, ProducerState, PutOutcome, ReadRequest, RecordHeader, StoreCapabilities,
    StoredObject, TimestampPolicy, TopicName, TopicPartition,
};
use std::sync::Arc;

fn topic_partition(name: &str) -> TopicPartition {
    TopicPartition::new(TopicName::new(name).expect("valid topic"), PartitionId(0))
}

fn test_backend(store: Arc<dyn ObjectStore>) -> ObjectLogBackend {
    ObjectLogBackend::new(store).with_config(ObjectLogBackendConfig {
        min_records_per_segment: 2,
        allow_tiny_segments_for_tests: true,
    })
}

fn records(values: &[&'static [u8]]) -> Vec<AppendRecord> {
    values
        .iter()
        .map(|value| AppendRecord::new(Bytes::from_static(value)))
        .collect()
}

#[tokio::test]
async fn append_and_read_committed_offsets() {
    let backend = test_backend(Arc::new(MemoryObjectStore::default()));
    let tp = topic_partition("events");

    let result = backend
        .append(AppendBatch::new(tp.clone(), records(&[b"a", b"b", b"c"])))
        .await
        .expect("append");

    assert_eq!(result.base_offset, Some(0));
    assert_eq!(result.last_offset, Some(2));
    assert!(result.acked);

    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await
        .expect("read");
    assert_eq!(read.next_offset, 3);
    assert_eq!(read.high_watermark, Some(3));
    assert_eq!(read.records.len(), 3);
    assert_eq!(read.records[1].offset, 1);
    assert_eq!(read.records[1].value, Bytes::from_static(b"b"));
}

#[tokio::test]
async fn read_from_middle_returns_contiguous_subset() {
    let backend = test_backend(Arc::new(MemoryObjectStore::default()));
    let tp = topic_partition("partial");
    backend
        .append(AppendBatch::new(tp.clone(), records(&[b"a", b"b", b"c"])))
        .await
        .expect("append");

    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 1,
            max_records: 1,
        })
        .await
        .expect("read");

    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].offset, 1);
    assert_eq!(read.records[0].value, Bytes::from_static(b"b"));
    assert_eq!(read.next_offset, 2);
}

#[tokio::test]
async fn acks_none_returns_no_committed_offset_claim() {
    let backend = test_backend(Arc::new(MemoryObjectStore::default()));
    let tp = topic_partition("fire_and_forget");
    let mut batch = AppendBatch::new(tp, records(&[b"a"]));
    batch.acks = AckMode::None;

    let result = backend.append(batch).await.expect("append");

    assert_eq!(result.base_offset, None);
    assert_eq!(result.last_offset, None);
    assert!(!result.acked);
    assert_eq!(result.record_count, 1);
}

#[tokio::test]
async fn idempotent_producer_retry_returns_original_offsets() {
    let backend = test_backend(Arc::new(MemoryObjectStore::default()));
    let tp = topic_partition("idempotent");
    let producer = ProducerState {
        producer_id: 7,
        producer_epoch: 1,
        base_sequence: 100,
    };
    let mut batch = AppendBatch::new(tp.clone(), records(&[b"a", b"b"]));
    batch.producer = Some(producer.clone());

    let first = backend.append(batch.clone()).await.expect("first append");
    let second = backend.append(batch).await.expect("retry append");

    assert_eq!(first.base_offset, Some(0));
    assert_eq!(second.base_offset, Some(0));
    assert_eq!(second.last_offset, Some(1));

    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await
        .expect("read");
    assert_eq!(read.records.len(), 2);
}

#[tokio::test]
async fn stale_epoch_is_fenced_before_visibility() {
    struct Guard;

    #[async_trait]
    impl object_log::EpochGuard for Guard {
        async fn check(
            &self,
            _topic_partition: &TopicPartition,
            expected_epoch: u64,
        ) -> Result<(), ObjectLogError> {
            if expected_epoch == 2 {
                Ok(())
            } else {
                Err(ObjectLogError::Fenced)
            }
        }
    }

    let backend =
        test_backend(Arc::new(MemoryObjectStore::default())).with_epoch_guard(Arc::new(Guard));
    let tp = topic_partition("fenced");
    let mut batch = AppendBatch::new(tp.clone(), records(&[b"a"]));
    batch.expected_epoch = Some(1);

    let err = backend.append(batch).await.expect_err("fenced");
    assert_eq!(err, ObjectLogError::Fenced);

    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await
        .expect("read");
    assert!(read.records.is_empty());
}

#[tokio::test]
async fn local_store_recovers_across_backend_instances() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(LocalObjectStore::new(dir.path()));
    let backend = test_backend(store.clone());
    let tp = topic_partition("local_recovery");
    backend
        .append(AppendBatch::new(tp.clone(), records(&[b"a", b"b"])))
        .await
        .expect("append");

    let recovered = test_backend(Arc::new(LocalObjectStore::new(dir.path())));
    let read = recovered
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 1,
            max_records: 10,
        })
        .await
        .expect("read");

    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].value, Bytes::from_static(b"b"));
}

#[tokio::test]
async fn pqueue_and_niflheim_payloads_round_trip_as_opaque_bytes() {
    let backend = test_backend(Arc::new(MemoryObjectStore::default()));
    let tp = topic_partition("opaque_payloads");
    let mut pqueue = AppendRecord::new(Bytes::from_static(b"{\"cmd\":\"BatchClaim\"}"));
    pqueue.key = Some(Bytes::from_static(b"tenant/queue/shard"));
    pqueue
        .headers
        .push(RecordHeader::new("source", Bytes::from_static(b"pqueue")));
    let mut niflheim = AppendRecord::new(Bytes::from_static(b"\x00row-payload"));
    niflheim.key = Some(Bytes::from_static(b"tenant/collection/partition"));
    niflheim
        .headers
        .push(RecordHeader::new("source", Bytes::from_static(b"niflheim")));

    backend
        .append(AppendBatch::new(tp.clone(), vec![pqueue, niflheim]))
        .await
        .expect("append");

    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await
        .expect("read");

    assert_eq!(
        read.records[0].value,
        Bytes::from_static(b"{\"cmd\":\"BatchClaim\"}")
    );
    assert_eq!(
        read.records[0].headers[0].value,
        Bytes::from_static(b"pqueue")
    );
    assert_eq!(
        read.records[1].value,
        Bytes::from_static(b"\x00row-payload")
    );
    assert_eq!(
        read.records[1].headers[0].value,
        Bytes::from_static(b"niflheim")
    );
}

#[tokio::test]
async fn log_append_time_overwrites_record_timestamp() {
    let backend = test_backend(Arc::new(MemoryObjectStore::default()));
    let tp = topic_partition("timestamps");
    let mut record = AppendRecord::new(Bytes::from_static(b"a"));
    record.timestamp_ms = Some(1);
    let mut batch = AppendBatch::new(tp.clone(), vec![record]);
    batch.timestamp_policy = TimestampPolicy::LogAppendTime;

    backend.append(batch).await.expect("append");
    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 1,
        })
        .await
        .expect("read");

    assert!(read.records[0].timestamp_ms > 1);
}

#[tokio::test]
async fn corrupt_segment_is_rejected_on_read() {
    let store = Arc::new(MemoryObjectStore::default());
    let backend = test_backend(store.clone());
    let tp = topic_partition("corrupt");
    backend
        .append(AppendBatch::new(tp.clone(), records(&[b"a", b"b"])))
        .await
        .expect("append");

    let key =
        ObjectKey::new("topics/corrupt/partitions/0000000000/segments/00000000000000000000.olseg")
            .expect("key");
    let object = store.get(&key).await.expect("get").expect("object");
    let mut bytes = object.value.to_vec();
    bytes[16] ^= 0xff;
    store
        .compare_and_set(&key, Some(object.version), Bytes::from(bytes))
        .await
        .expect("overwrite");

    let err = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await
        .expect_err("corruption");

    assert!(matches!(err, ObjectLogError::CorruptSegment(_)));
}

#[tokio::test]
async fn production_config_rejects_tiny_segments() {
    let backend = ObjectLogBackend::new(Arc::new(MemoryObjectStore::default()));
    let err = backend
        .append(AppendBatch::new(topic_partition("tiny"), records(&[b"a"])))
        .await
        .expect_err("tiny segment rejected");

    assert_eq!(err, ObjectLogError::InvalidBatch);
}

#[tokio::test]
async fn manifest_cas_conflict_prevents_visibility() {
    struct ConflictStore {
        inner: MemoryObjectStore,
    }

    #[async_trait]
    impl ObjectStore for ConflictStore {
        async fn get(&self, key: &ObjectKey) -> Result<Option<StoredObject>, ObjectLogError> {
            self.inner.get(key).await
        }

        async fn put_if_absent(
            &self,
            key: &ObjectKey,
            value: Bytes,
        ) -> Result<PutOutcome, ObjectLogError> {
            self.inner.put_if_absent(key, value).await
        }

        async fn compare_and_set(
            &self,
            _key: &ObjectKey,
            _expected: Option<ObjectVersion>,
            _value: Bytes,
        ) -> Result<StoredObject, ObjectLogError> {
            Err(ObjectLogError::Conflict)
        }

        async fn list(&self, prefix: &str) -> Result<Vec<ObjectKey>, ObjectLogError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectLogError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> StoreCapabilities {
            self.inner.capabilities()
        }
    }

    let backend = test_backend(Arc::new(ConflictStore {
        inner: MemoryObjectStore::default(),
    }));
    let tp = topic_partition("conflict");
    let err = backend
        .append(AppendBatch::new(tp.clone(), records(&[b"a", b"b"])))
        .await
        .expect_err("conflict");

    assert_eq!(err, ObjectLogError::Conflict);
    let read = backend
        .read(ReadRequest {
            topic_partition: tp,
            start_offset: 0,
            max_records: 10,
        })
        .await
        .expect("read");
    assert!(read.records.is_empty());
}

#[test]
fn invalid_names_and_keys_are_rejected() {
    assert!(matches!(
        TopicName::new("tenant/topic"),
        Err(ObjectLogError::InvalidTopic(_))
    ));
    assert!(matches!(
        TopicName::new(".."),
        Err(ObjectLogError::InvalidTopic(_))
    ));
    assert!(matches!(
        ObjectKey::new("topics/../escape"),
        Err(ObjectLogError::InvalidObjectKey(_))
    ));
}
