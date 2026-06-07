use thiserror::Error;

/// Errors returned by object-log core and storage backends.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ObjectLogError {
    #[error("invalid topic: {0}")]
    InvalidTopic(String),
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),
    #[error("append batch must contain at least one record")]
    InvalidBatch,
    #[error("producer sequence conflicts with committed batch")]
    SequenceConflict,
    #[error("backend does not support idempotent producer metadata")]
    UnsupportedIdempotence,
    #[error("writer is fenced by a newer epoch")]
    Fenced,
    #[error("manifest or object compare-and-set conflict")]
    Conflict,
    #[error("object already exists with different bytes")]
    ObjectConflict,
    #[error("required store capability is not available: {0}")]
    UnsupportedCapability(&'static str),
    #[error("storage unavailable: {0}")]
    StorageUnavailable(String),
    #[error("missing object: {0}")]
    MissingObject(String),
    #[error("corrupt segment: {0}")]
    CorruptSegment(String),
    #[error("offset discontinuity: expected {expected}, actual {actual}")]
    OffsetDiscontinuity { expected: u64, actual: u64 },
    #[error("unsupported segment version: {0}")]
    UnsupportedSegmentVersion(u16),
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
