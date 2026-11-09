use super::{HEADER_LEN, MAX_RECORD_COUNT, MAX_TOTAL_LEN};

/// Errors produced while decoding a [`BatchHeader`](super::BatchHeader).
#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    /// Fewer than [`HEADER_LEN`] bytes were available.
    #[error("header truncated: need {HEADER_LEN} bytes, got {0}")]
    Truncated(usize),
    /// The `format_version` byte is not a supported version.
    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u8),
    /// `record_count` exceeded [`MAX_RECORD_COUNT`].
    #[error("record_count {0} exceeds maximum {MAX_RECORD_COUNT}")]
    RecordCountTooLarge(u32),
    /// `total_len` exceeded [`MAX_TOTAL_LEN`].
    #[error("total_len {0} exceeds maximum {MAX_TOTAL_LEN}")]
    TotalLenTooLarge(u32),
}
