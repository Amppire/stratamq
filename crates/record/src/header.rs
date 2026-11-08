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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> BatchHeader {
        BatchHeader {
            total_len: 128,
            format_version: FORMAT_VERSION_V0,
            flags: 0b11,
            compression: 2,
            reserved: 0,
            record_count: 3,
            producer_id: 0x0102_0304_0506_0708,
            producer_epoch: 0x0A0B_0C0D,
            first_sequence: 0x1112_1314_1516_1718,
            first_timestamp: 0x2122_2324_2526_2728,
            crc32c: 0xDEAD_BEEF,
        }
    }

    #[test]
    fn header_encodes_to_exactly_44_bytes() {
        let encoded = sample_header().encode();
        assert_eq!(encoded.len(), 44);
        assert_eq!(HEADER_LEN, 44);
    }

    #[test]
    fn header_fields_are_big_endian_at_fixed_offsets() {
        let encoded = sample_header().encode();
        // total_len @ 0
        assert_eq!(&encoded[0..4], &128u32.to_be_bytes());
        // format_version @ 4, flags @ 5, compression @ 6, reserved @ 7
        assert_eq!(encoded[4], FORMAT_VERSION_V0);
        assert_eq!(encoded[5], 0b11);
        assert_eq!(encoded[6], 2);
        assert_eq!(encoded[7], 0);
        // record_count @ 8
        assert_eq!(&encoded[8..12], &3u32.to_be_bytes());
        // producer_id @ 12
        assert_eq!(&encoded[12..20], &0x0102_0304_0506_0708u64.to_be_bytes());
        // producer_epoch @ 20
        assert_eq!(&encoded[20..24], &0x0A0B_0C0Du32.to_be_bytes());
        // first_sequence @ 24
        assert_eq!(&encoded[24..32], &0x1112_1314_1516_1718u64.to_be_bytes());
        // first_timestamp @ 32
        assert_eq!(&encoded[32..40], &0x2122_2324_2526_2728u64.to_be_bytes());
        // crc32c @ 40
        assert_eq!(&encoded[40..44], &0xDEAD_BEEFu32.to_be_bytes());
    }

    #[test]
    fn header_round_trips_through_encode_decode() {
        let original = sample_header();
        let decoded = BatchHeader::decode(&original.encode()).expect("decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let err = BatchHeader::decode(&[0u8; HEADER_LEN - 1]).unwrap_err();
        assert!(matches!(err, HeaderError::Truncated(n) if n == HEADER_LEN - 1));
    }

    #[test]
    fn decode_rejects_record_count_over_cap() {
        let mut h = sample_header();
        h.record_count = MAX_RECORD_COUNT + 1;
        let err = BatchHeader::decode(&h.encode()).unwrap_err();
        assert!(matches!(err, HeaderError::RecordCountTooLarge(n) if n == MAX_RECORD_COUNT + 1));
    }

    #[test]
    fn decode_rejects_total_len_over_cap() {
        let mut h = sample_header();
        h.total_len = MAX_TOTAL_LEN + 1;
        let err = BatchHeader::decode(&h.encode()).unwrap_err();
        assert!(matches!(err, HeaderError::TotalLenTooLarge(n) if n == MAX_TOTAL_LEN + 1));
    }

    #[test]
    fn decode_rejects_unsupported_version() {
        let mut h = sample_header();
        h.format_version = 0xFF;
        let err = BatchHeader::decode(&h.encode()).unwrap_err();
        assert!(matches!(err, HeaderError::UnsupportedVersion(0xFF)));
    }
}
