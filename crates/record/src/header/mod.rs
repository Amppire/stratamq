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

mod error;
#[cfg(test)]
mod tests;

pub use error::HeaderError;

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
        let mut out = [0u8; HEADER_LEN];
        out[0..4].copy_from_slice(&self.total_len.to_be_bytes());
        out[4] = self.format_version;
        out[5] = self.flags;
        out[6] = self.compression;
        out[7] = self.reserved;
        out[8..12].copy_from_slice(&self.record_count.to_be_bytes());
        out[12..20].copy_from_slice(&self.producer_id.to_be_bytes());
        out[20..24].copy_from_slice(&self.producer_epoch.to_be_bytes());
        out[24..32].copy_from_slice(&self.first_sequence.to_be_bytes());
        out[32..40].copy_from_slice(&self.first_timestamp.to_be_bytes());
        out[40..44].copy_from_slice(&self.crc32c.to_be_bytes());
        out
    }

    /// Decode a header from the front of `bytes`, validating defensive bounds.
    ///
    /// # Errors
    /// Returns [`HeaderError`] if the slice is too short, the version is
    /// unsupported, or `record_count` / `total_len` exceed their caps.
    pub fn decode(bytes: &[u8]) -> Result<Self, HeaderError> {
        if bytes.len() < HEADER_LEN {
            return Err(HeaderError::Truncated(bytes.len()));
        }

        let format_version = bytes[4];
        if format_version != FORMAT_VERSION_V0 {
            return Err(HeaderError::UnsupportedVersion(format_version));
        }

        let total_len = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        if total_len > MAX_TOTAL_LEN {
            return Err(HeaderError::TotalLenTooLarge(total_len));
        }

        let record_count = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        if record_count > MAX_RECORD_COUNT {
            return Err(HeaderError::RecordCountTooLarge(record_count));
        }

        Ok(Self {
            total_len,
            format_version,
            flags: bytes[5],
            compression: bytes[6],
            reserved: bytes[7],
            record_count,
            producer_id: u64::from_be_bytes(bytes[12..20].try_into().unwrap()),
            producer_epoch: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
            first_sequence: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
            first_timestamp: u64::from_be_bytes(bytes[32..40].try_into().unwrap()),
            crc32c: u32::from_be_bytes(bytes[40..44].try_into().unwrap()),
        })
    }
}
