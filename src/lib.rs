//! Kafka-compatible core log semantics with an object-storage backend.

mod error;
mod model;
mod object_backend;
mod segment;
mod store;
mod util;

pub use error::ObjectLogError;
pub use model::{
    AckMode, AppendBatch, AppendRecord, AppendResult, AppendedRecord, LogBackend, PartitionId,
    ProducerState, ReadBatch, ReadRequest, RecordHeader, TimestampPolicy, TopicName,
    TopicPartition,
};
pub use object_backend::{EpochGuard, ObjectLogBackend, ObjectLogBackendConfig};
pub use store::{
    LocalObjectStore, MemoryObjectStore, ObjectKey, ObjectStore, ObjectVersion, PutOutcome,
    StoreCapabilities, StoredObject,
};
