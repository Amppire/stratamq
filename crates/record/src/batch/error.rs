use crate::compression::CompressionError;
use crate::header::HeaderError;

/// Errors produced while encoding or decoding a
/// [`RecordBatchV0`](super::RecordBatchV0).
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    /// The header could not be decoded or violated a defensive bound.
    #[error("header error: {0}")]
    Header(#[from] HeaderError),
    /// The stored CRC32C did not match the computed checksum of the records.
    #[error("CRC32C checksum mismatch")]
    ChecksumMismatch,
    /// A compression codec failed.
    #[error("compression error: {0}")]
    Compression(#[from] CompressionError),
    /// The record bytes were truncated or otherwise malformed.
    #[error("malformed record framing: {0}")]
    MalformedRecord(String),
}

impl From<crate::cursor::CursorError> for BatchError {
    fn from(err: crate::cursor::CursorError) -> Self {
        BatchError::MalformedRecord(err.to_string())
    }
}
