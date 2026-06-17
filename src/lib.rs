//! An embeddable, append-only log core with a pluggable object-storage backend.
//!
//! `object-log` implements Kafka-style log semantics — topics, partitions, dense
//! monotonic offsets, idempotent producers, and batched append/read — on top of a
//! small [`ObjectStore`] trait, so the durable bytes can live in memory, on a
//! local filesystem, or (via your own adapter) in S3-compatible object storage.
//!
//! # Architecture
//!
//! - [`ObjectStore`] is the storage *port*: a minimal async key/value trait with
//!   conditional writes ([`ObjectStore::put_if_absent`],
//!   [`ObjectStore::compare_and_set`]). Two adapters ship in-crate:
//!   [`MemoryObjectStore`] (tests/dev) and [`LocalObjectStore`] (filesystem).
//! - [`LogBackend`] is the log API: [`LogBackend::append`] / [`LogBackend::read`]
//!   over [`AppendBatch`] / [`ReadBatch`].
//! - [`ObjectLogBackend`] implements [`LogBackend`] over any [`ObjectStore`]: it
//!   writes immutable, content-checksummed *segments* and a per-partition
//!   manifest, assigning dense offsets via an optimistic compare-and-set on the
//!   manifest. An optional [`EpochGuard`] fences concurrent writers.
//!
//! # Concurrency model
//!
//! [`ObjectLogBackend::append`] is *optimistic*: it reads the current manifest,
//! writes the new segment, then commits the manifest with a
//! [`ObjectStore::compare_and_set`] guarded by the version it read. If a
//! concurrent writer committed in between, the commit fails with
//! [`ObjectLogError::Conflict`] and the caller should retry (re-reading the
//! manifest). There is no internal retry loop — single-writer-per-partition, or
//! caller-side retry/serialization, is expected. The losing writer's orphaned
//! segment is harmless: it is content-addressed and simply unreferenced.
//!
//! # Example
//!
//! ```
//! use object_log::{
//!     AppendBatch, AppendRecord, LogBackend, MemoryObjectStore, ObjectLogBackend,
//!     PartitionId, ReadRequest, TopicName, TopicPartition,
//! };
//! use std::sync::Arc;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let store = Arc::new(MemoryObjectStore::default());
//! let backend = ObjectLogBackend::new(store);
//!
//! let tp = TopicPartition::new(TopicName::new("events").unwrap(), PartitionId(0));
//! let appended = backend
//!     .append(AppendBatch::new(tp.clone(), vec![AppendRecord::new("hello")]))
//!     .await
//!     .unwrap();
//! assert_eq!(appended.base_offset, Some(0));
//!
//! let read = backend
//!     .read(ReadRequest { topic_partition: tp, start_offset: 0, max_records: 10 })
//!     .await
//!     .unwrap();
//! assert_eq!(read.records.len(), 1);
//! assert_eq!(read.records[0].value, "hello");
//! # });
//! ```
#![deny(missing_docs)]

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
