use crate::ObjectLogError;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;

/// Kafka-compatible topic name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicName(String);

impl TopicName {
    /// Create a topic name that is safe for Kafka-style use and object prefixes.
    ///
    /// Rejects empty names and names containing `\0`, `/`, or a `..` path
    /// component (returning [`ObjectLogError::InvalidTopic`]).
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

    /// Borrow the validated topic name as a string slice.
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

/// One ordered partition log, identified by its topic and partition.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicPartition {
    /// The owning topic.
    pub topic: TopicName,
    /// The partition within the topic.
    pub partition: PartitionId,
}

impl TopicPartition {
    /// Create a topic-partition handle.
    pub fn new(topic: TopicName, partition: PartitionId) -> Self {
        Self { topic, partition }
    }
}

/// Ordered Kafka-compatible record header (key/value metadata on a record).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    /// Header name.
    pub name: String,
    /// Header value (opaque bytes).
    pub value: Bytes,
}

impl RecordHeader {
    /// Create a record header from a name and value.
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
    /// Optional partition key (opaque bytes).
    pub key: Option<Bytes>,
    /// Record payload (opaque bytes).
    pub value: Bytes,
    /// Ordered record headers.
    pub headers: Vec<RecordHeader>,
    /// Producer-supplied create-time in epoch milliseconds, if any. Honored only
    /// under [`TimestampPolicy::CreateTime`]; otherwise the append time is used.
    pub timestamp_ms: Option<i64>,
}

impl AppendRecord {
    /// Create a record with the given value and no key, headers, or timestamp.
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
    /// Dense, monotonically assigned offset within the partition.
    pub offset: u64,
    /// Optional partition key (opaque bytes).
    pub key: Option<Bytes>,
    /// Record payload (opaque bytes).
    pub value: Bytes,
    /// Ordered record headers.
    pub headers: Vec<RecordHeader>,
    /// Resolved record timestamp in epoch milliseconds.
    pub timestamp_ms: i64,
}

/// Kafka producer acknowledgement compatibility mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckMode {
    /// Fire-and-forget: the durable write still happens, but the result reports
    /// no committed offsets and `acked = false`.
    None,
    /// Acknowledge once the leader has the write durably.
    Leader,
    /// Acknowledge once the write is durable (the default for this core).
    All,
}

/// Timestamp behavior compatible with Kafka create-time / log-append-time policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampPolicy {
    /// Use the producer-supplied [`AppendRecord::timestamp_ms`], falling back to
    /// the append time when absent.
    CreateTime,
    /// Overwrite every record timestamp with the broker's append time.
    LogAppendTime,
}

/// Metadata needed for Kafka-style idempotent producer duplicate suppression.
///
/// A retried batch with the same `(producer_id, producer_epoch, base_sequence)`
/// and record count returns the originally committed offsets instead of
/// appending again; a mismatch yields [`ObjectLogError::SequenceConflict`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProducerState {
    /// Stable producer identifier.
    pub producer_id: u64,
    /// Producer epoch, bumped to fence prior incarnations.
    pub producer_epoch: i16,
    /// First record sequence number in the batch.
    pub base_sequence: i32,
}

/// Append request for one topic partition.
#[derive(Clone, Debug)]
pub struct AppendBatch {
    /// Target partition.
    pub topic_partition: TopicPartition,
    /// Records to append, in order.
    pub records: Vec<AppendRecord>,
    /// Acknowledgement mode (affects only the returned offsets, not durability).
    pub acks: AckMode,
    /// Optional writer epoch, validated by an [`EpochGuard`](crate::EpochGuard)
    /// before the write becomes visible.
    pub expected_epoch: Option<u64>,
    /// Optional idempotent-producer metadata for duplicate suppression.
    pub producer: Option<ProducerState>,
    /// How record timestamps are resolved.
    pub timestamp_policy: TimestampPolicy,
}

impl AppendBatch {
    /// Create a batch with default settings: [`AckMode::All`], no epoch, no
    /// producer metadata, and [`TimestampPolicy::CreateTime`].
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
    /// The partition the batch was appended to.
    pub topic_partition: TopicPartition,
    /// First assigned offset, or `None` under [`AckMode::None`].
    pub base_offset: Option<u64>,
    /// Last assigned offset, or `None` under [`AckMode::None`].
    pub last_offset: Option<u64>,
    /// Number of records committed.
    pub record_count: usize,
    /// Whether the write was acknowledged (false only under [`AckMode::None`]).
    pub acked: bool,
    /// Backend-specific reference to the committed segment, when acknowledged.
    pub commit_ref: Option<String>,
}

/// Read request for one topic partition.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    /// Partition to read from.
    pub topic_partition: TopicPartition,
    /// First offset to return (inclusive).
    pub start_offset: u64,
    /// Maximum number of records to return.
    pub max_records: usize,
}

/// Contiguous replay batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadBatch {
    /// Records read, in contiguous offset order starting at the request offset.
    pub records: Vec<AppendedRecord>,
    /// The offset immediately after the last returned record (resume point).
    pub next_offset: u64,
    /// The partition high-watermark (next offset to be assigned), if known.
    pub high_watermark: Option<u64>,
}

/// Kafka-style append/read log over a topic partition.
#[async_trait]
pub trait LogBackend: Send + Sync {
    /// Append a batch of records to its partition, assigning dense offsets.
    async fn append(&self, batch: AppendBatch) -> Result<AppendResult, ObjectLogError>;
    /// Read a contiguous run of records starting at [`ReadRequest::start_offset`].
    async fn read(&self, request: ReadRequest) -> Result<ReadBatch, ObjectLogError>;
}
