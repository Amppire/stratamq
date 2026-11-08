//! Fixed-width Record Batch v0 header.
//!
//! The header is a fixed 44-byte, big-endian structure that prefixes every
//! batch on the wire and on disk. All multi-byte integers are big-endian
//! (network byte order).
//!
//! Note: the listed fields sum to 44 bytes. The acceptance criteria text in
//! issue #10 says "48-byte struct", which is a spec arithmetic error being
//! corrected alongside this implementation — the canonical size is 44.
//!
//! `producer_id` here is a wire-level 64-bit transport producer handle used for
//! idempotency `(producer_id, producer_epoch, first_sequence)`. It is NOT the
//! UUIDv7 `strata_types::ProducerId`.

/// The exact encoded size of a [`BatchHeader`] in bytes.
pub const HEADER_LEN: usize = 44;

/// Maximum number of records a single batch may declare (defensive bound).
pub const MAX_RECORD_COUNT: u32 = 10_000;

/// Maximum total batch length in bytes (16 MiB; defensive bound).
pub const MAX_TOTAL_LEN: u32 = 16 * 1024 * 1024;

/// Current format version byte.
pub const FORMAT_VERSION_V0: u8 = 0x00;

/// Fixed-width, big-endian Record Batch v0 header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    /// Total batch bytes including header and records.
    pub total_len: u32,
    /// Format version (`0x00` for Draft 0).
    pub format_version: u8,
    /// Bit flags: bit 0 `has_subject`, bit 1 `is_transactional`.
    pub flags: u8,
    /// Compression codec byte (0 None, 1 LZ4, 2 Zstd).
    pub compression: u8,
    /// Reserved, always `0x00`.
    pub reserved: u8,
    /// Number of individual records in the batch.
    pub record_count: u32,
    /// Wire-level producer handle (not the UUID `ProducerId`).
    pub producer_id: u64,
    /// Epoch counter for fencing stale producers.
    pub producer_epoch: u32,
    /// Monotonic producer sequence for the first record.
    pub first_sequence: u64,
    /// Commit/publish timestamp (ms) for the first record.
    pub first_timestamp: u64,
    /// Castagnoli CRC32C over the record bytes following the header.
    pub crc32c: u32,
}

impl BatchHeader {
    /// Encode this header to exactly [`HEADER_LEN`] big-endian bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        unimplemented!("BatchHeader::encode")
    }

    /// Decode a header from the front of `bytes`, validating defensive bounds.
    ///
    /// # Errors
    /// Returns [`HeaderError`] if the slice is too short, the version is
    /// unsupported, or `record_count` / `total_len` exceed their caps.
    pub fn decode(_bytes: &[u8]) -> Result<Self, HeaderError> {
        unimplemented!("BatchHeader::decode")
    }
}

/// Errors produced while decoding a [`BatchHeader`].
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
