use crate::ObjectLogError;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;

/// Kafka-compatible topic name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicName(String);

impl TopicName {
    /// Create a topic name that is safe for Kafka-style use and object prefixes.
    pub fn new(value: impl Into<String>) -> Result<Self, ObjectLogError> {
        let value = value.into();
        if value.is_empty()
            || value.contains('\0')
            || value.contains('/')
            || value.split('.').any(|part| part == "..")
            || value == "."
            || value == ".."
        {
            return Err(ObjectLogError::InvalidTopic(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Zero-based partition identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(pub u32);

/// One ordered partition log.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicPartition {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl TopicPartition {
    pub fn new(topic: TopicName, partition: PartitionId) -> Self {
        Self { topic, partition }
    }
}

/// Ordered Kafka-compatible record header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub name: String,
    pub value: Bytes,
}

impl RecordHeader {
    pub fn new(name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Record supplied by a producer before offsets are assigned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<RecordHeader>,
    pub timestamp_ms: Option<i64>,
}

impl AppendRecord {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: value.into(),
            headers: Vec::new(),
            timestamp_ms: None,
        }
    }
}

/// Record returned by replay after an offset and append timestamp are assigned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedRecord {
    pub offset: u64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<RecordHeader>,
    pub timestamp_ms: i64,
}

/// Kafka producer acknowledgement compatibility mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckMode {
    None,
    Leader,
    All,
}

/// Timestamp behavior compatible with Kafka create-time/log-append-time policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampPolicy {
    CreateTime,
    LogAppendTime,
}

/// Metadata needed for Kafka-style idempotent producer duplicate suppression.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProducerState {
    pub producer_id: u64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

/// Append request for one topic partition.
#[derive(Clone, Debug)]
pub struct AppendBatch {
    pub topic_partition: TopicPartition,
    pub records: Vec<AppendRecord>,
    pub acks: AckMode,
    pub expected_epoch: Option<u64>,
    pub producer: Option<ProducerState>,
    pub timestamp_policy: TimestampPolicy,
}

impl AppendBatch {
    pub fn new(topic_partition: TopicPartition, records: Vec<AppendRecord>) -> Self {
        Self {
            topic_partition,
            records,
            acks: AckMode::All,
            expected_epoch: None,
            producer: None,
            timestamp_policy: TimestampPolicy::CreateTime,
        }
    }
}

/// Append outcome for one topic partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub topic_partition: TopicPartition,
    pub base_offset: Option<u64>,
    pub last_offset: Option<u64>,
    pub record_count: usize,
    pub acked: bool,
    pub commit_ref: Option<String>,
}

/// Read request for one topic partition.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub topic_partition: TopicPartition,
    pub start_offset: u64,
    pub max_records: usize,
}

/// Contiguous replay batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadBatch {
    pub records: Vec<AppendedRecord>,
    pub next_offset: u64,
    pub high_watermark: Option<u64>,
}

#[async_trait]
pub trait LogBackend: Send + Sync {
    async fn append(&self, batch: AppendBatch) -> Result<AppendResult, ObjectLogError>;
    async fn read(&self, request: ReadRequest) -> Result<ReadBatch, ObjectLogError>;
}
