use thiserror::Error;

/// Errors returned by object-log core and storage backends.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ObjectLogError {
    /// The topic name is empty or contains a reserved character (`\0`, `/`, `..`).
    #[error("invalid topic: {0}")]
    InvalidTopic(String),
    /// The object key is empty or contains a reserved character (`\0`, `..`).
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),
    /// The append batch was empty or smaller than the configured minimum segment.
    #[error("append batch must contain at least one record")]
    InvalidBatch,
    /// A retried idempotent producer batch does not match the committed one.
    #[error("producer sequence conflicts with committed batch")]
    SequenceConflict,
    /// The writer's epoch is stale; a newer epoch has fenced it off (see
    /// [`EpochGuard`](crate::EpochGuard)).
    #[error("writer is fenced by a newer epoch")]
    Fenced,
    /// An optimistic compare-and-set lost a race; re-read and retry.
    #[error("manifest or object compare-and-set conflict")]
    Conflict,
    /// `put_if_absent` found the key already present with different bytes.
    #[error("object already exists with different bytes")]
    ObjectConflict,
    /// The backing [`ObjectStore`](crate::ObjectStore) lacks a capability the
    /// backend requires (e.g. compare-and-set).
    #[error("required store capability is not available: {0}")]
    UnsupportedCapability(&'static str),
    /// The storage layer was unreachable or returned an I/O error.
    #[error("storage unavailable: {0}")]
    StorageUnavailable(String),
    /// The manifest referenced a segment object that is no longer present.
    #[error("missing object: {0}")]
    MissingObject(String),
    /// A segment failed checksum, magic, version, or structural validation.
    #[error("corrupt segment: {0}")]
    CorruptSegment(String),
    /// A read observed a non-contiguous offset, indicating a gap or reordering.
    #[error("offset discontinuity: expected {expected}, actual {actual}")]
    OffsetDiscontinuity {
        /// The next offset the reader expected.
        expected: u64,
        /// The offset actually encountered.
        actual: u64,
    },
    /// The segment was written by an incompatible (future) format version.
    #[error("unsupported segment version: {0}")]
    UnsupportedSegmentVersion(u16),
    /// A value could not be encoded/decoded (e.g. a field exceeded its width).
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<std::io::Error> for ObjectLogError {
    fn from(value: std::io::Error) -> Self {
        Self::StorageUnavailable(value.to_string())
    }
}

impl From<serde_json::Error> for ObjectLogError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}
