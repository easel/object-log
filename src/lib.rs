//! A buffered, multiplexing append log over pluggable object storage.
//!
//! object-log stores an ordered, offset-addressed log as immutable objects in any
//! [`BlobStore`] (memory, local filesystem, or — behind a feature — S3). It deals
//! only in **opaque payload bytes**: it knows nothing about record formats, Kafka,
//! or brokers. Two seams keep it generic:
//!
//! - [`BlobStore`] — the storage *port*: durable-on-return `put`, `get`,
//!   `get_range`, `list`, `delete`. Adapters [`MemoryBlobStore`] and
//!   [`LocalBlobStore`] ship in-crate.
//! - [`Sequencer`] — the *sequencing seam*: assigns offsets to durably-stored
//!   batches and owns the offset→location index. The engine (added on top of these
//!   types) buffers and group-commits many batches into one object, PUTs it
//!   durably, then calls the sequencer — so PUT count is decoupled from produce
//!   count. A consumer plugs its own sequencer (e.g. a Kafka coordinator); the
//!   engine forwards each sequencer's [`Sequencer::Meta`] uninterpreted.
//!
//! # Example — the storage port
//!
//! ```
//! use object_log::{BlobStore, MemoryBlobStore};
//! use bytes::Bytes;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let store = MemoryBlobStore::new();
//! store.put("logs/0", Bytes::from_static(b"hello world")).await.unwrap();
//! assert_eq!(store.get("logs/0").await.unwrap().unwrap(), "hello world");
//! // Read just a slice — e.g. one batch out of a multiplexed object.
//! assert_eq!(store.get_range("logs/0", 0..5).await.unwrap().unwrap(), "hello");
//! # });
//! ```
#![deny(missing_docs)]

mod blob;
mod error;
mod sequencer;

pub use blob::{BlobStore, LocalBlobStore, MemoryBlobStore};
pub use error::ObjectLogError;
pub use sequencer::{
    BatchLocation, CommitBatch, CommitOutcome, IndexEntry, PartitionKey, Sequencer,
};
