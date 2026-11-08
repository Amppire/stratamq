//! Record Batch v0 encoder and decoder.
//!
//! A [`RecordBatchV0`] is the fixed 44-byte [`crate::header::BatchHeader`]
//! followed by the (optionally compressed) concatenation of individually
//! framed records. Encoding flattens records into bytes; decoding rebuilds
//! records from bytes, validating integrity and defensive bounds first.

use crate::Record;
use crate::compression::CompressionError;
use crate::header::HeaderError;

/// The decoded content of a single record entry.
///
/// A record's `RecordId` is assigned by the broker at commit time (NMP §8.1)
/// and is NOT carried on the wire, so decoding yields the record's content
/// plus its committed `timestamp` and optional `revision` rather than a
/// fully-formed [`Record`] with an id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedEntry {
    /// Absolute commit timestamp (ms), reconstructed from the batch
    /// `first_timestamp` plus this entry's `timestamp_delta`.
    pub timestamp: u64,
    /// Committed subject revision, if present.
    pub revision: Option<u64>,
    /// The subject, if present.
    pub subject: Option<crate::Subject>,
    /// The partition/ordering key, if present.
    pub key: Option<bytes::Bytes>,
    /// Record headers (sorted by key).
    pub headers: std::collections::BTreeMap<String, bytes::Bytes>,
    /// The record payload.
    pub payload: bytes::Bytes,
}

/// An encoded/decoded Record Batch v0: producer metadata plus records.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatchV0 {
    /// Wire-level producer handle (not the UUID `ProducerId`).
    pub producer_id: u64,
    /// Epoch counter for fencing stale producers.
    pub producer_epoch: u32,
    /// Monotonic producer sequence for the first record.
    pub first_sequence: u64,
    /// Commit/publish timestamp (ms) for the first record.
    pub first_timestamp: u64,
    /// Whether the batch is marked transactional (header flag bit 1).
    pub is_transactional: bool,
    /// The records carried by this batch.
    pub records: Vec<Record>,
}

impl RecordBatchV0 {
    /// Encode the batch to its on-wire/on-disk byte representation using the
    /// given compression codec for the record bytes.
    ///
    /// # Errors
    /// Returns [`BatchError`] if the batch violates a defensive bound or the
    /// codec fails.
    pub fn encode(
        &self,
        _codec: crate::compression::CompressionCodec,
    ) -> Result<Vec<u8>, BatchError> {
        unimplemented!("RecordBatchV0::encode")
    }

    /// Decode a batch from `bytes`, validating the header, defensive bounds,
    /// and the CRC32C checksum before allocating record storage.
    ///
    /// # Errors
    /// Returns [`BatchError`] on truncation, bound violation, checksum
    /// mismatch, or malformed record framing.
    pub fn decode(_bytes: &[u8]) -> Result<Self, BatchError> {
        unimplemented!("RecordBatchV0::decode")
    }
}

/// Errors produced while encoding or decoding a [`RecordBatchV0`].
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

/// Per-record entry presence flags.
const ENTRY_FLAG_HAS_SUBJECT: u8 = 0b001;
const ENTRY_FLAG_HAS_REVISION: u8 = 0b010;
const ENTRY_FLAG_HAS_KEY: u8 = 0b100;

/// Encode a single [`Record`] as a length-prefixed entry, relative to the
/// batch's `first_timestamp`.
///
/// Layout (big-endian):
/// `length:u32 | timestamp_delta:u32 | flags:u8 | [subject_len:u16 + bytes] |
///  [subject_revision:u64] | [key_len:u32 + bytes] | headers_count:u16 +
///  [key_len:u16 + key | val_len:u32 + val]* | payload_len:u32 + bytes`
///
/// `length` counts the bytes that follow it (the entry body).
///
/// # Errors
/// Returns [`BatchError`] if the record's timestamp precedes `first_timestamp`.
pub(crate) fn encode_entry(_record: &Record, _first_timestamp: u64) -> Result<Vec<u8>, BatchError> {
    unimplemented!("encode_entry")
}

/// Decode a single record entry from the front of `bytes`, returning the
/// decoded content and the number of bytes consumed.
///
/// # Errors
/// Returns [`BatchError::MalformedRecord`] on truncation or inconsistent
/// length fields.
pub(crate) fn decode_entry(
    _bytes: &[u8],
    _first_timestamp: u64,
) -> Result<(DecodedEntry, usize), BatchError> {
    unimplemented!("decode_entry")
}

#[cfg(test)]
mod entry_tests {
    use super::*;
    use crate::{ProposedRecord, Subject};
    use bytes::Bytes;

    const FIRST_TS: u64 = 1_000_000;

    fn entry_of(record: &Record) -> DecodedEntry {
        let encoded = encode_entry(record, FIRST_TS).expect("encode");
        let (decoded, consumed) = decode_entry(&encoded, FIRST_TS).expect("decode");
        assert_eq!(consumed, encoded.len(), "decode must consume whole entry");
        decoded
    }

    fn assert_content_matches(record: &Record, decoded: &DecodedEntry) {
        assert_eq!(decoded.timestamp, record.timestamp());
        assert_eq!(decoded.revision, record.revision());
        assert_eq!(decoded.subject.as_ref(), record.subject());
        assert_eq!(decoded.key.as_ref(), record.key());
        assert_eq!(&decoded.headers, record.headers());
        assert_eq!(&decoded.payload, record.payload());
    }

    #[test]
    fn full_record_round_trips() {
        let record = ProposedRecord::new("hello payload")
            .with_subject(Subject::new("orders.us"))
            .with_key("customer-42")
            .with_header("content-type", "application/json")
            .with_header("trace-id", "abc123")
            .commit(FIRST_TS + 250, Some(7));
        let decoded = entry_of(&record);
        assert_content_matches(&record, &decoded);
    }

    #[test]
    fn bare_payload_only_round_trips() {
        let record = ProposedRecord::new("just a payload").commit(FIRST_TS, None);
        let decoded = entry_of(&record);
        assert_content_matches(&record, &decoded);
        assert!(decoded.subject.is_none());
        assert!(decoded.revision.is_none());
        assert!(decoded.key.is_none());
        assert!(decoded.headers.is_empty());
    }

    #[test]
    fn subject_without_revision_round_trips() {
        let record = ProposedRecord::new("p")
            .with_subject(Subject::new("s"))
            .commit(FIRST_TS + 1, None);
        let decoded = entry_of(&record);
        assert_content_matches(&record, &decoded);
        assert!(decoded.subject.is_some());
        assert!(decoded.revision.is_none());
    }

    #[test]
    fn empty_payload_is_distinct_from_absent_key() {
        // Empty payload, present-but-empty key: presence flags must survive.
        let record = ProposedRecord::new(Bytes::new())
            .with_key(Bytes::new())
            .commit(FIRST_TS, None);
        let decoded = entry_of(&record);
        assert_eq!(decoded.key, Some(Bytes::new()));
        assert_eq!(decoded.payload, Bytes::new());
    }

    #[test]
    fn timestamp_delta_is_relative_to_first_timestamp() {
        let record = ProposedRecord::new("p").commit(FIRST_TS + 5000, None);
        let encoded = encode_entry(&record, FIRST_TS).unwrap();
        // delta is at offset 4 (after the u32 length prefix)
        let delta = u32::from_be_bytes(encoded[4..8].try_into().unwrap());
        assert_eq!(delta, 5000);
    }

    #[test]
    fn decode_rejects_truncated_entry() {
        let record = ProposedRecord::new("payload").commit(FIRST_TS, None);
        let encoded = encode_entry(&record, FIRST_TS).unwrap();
        let err = decode_entry(&encoded[..encoded.len() - 2], FIRST_TS).unwrap_err();
        assert!(matches!(err, BatchError::MalformedRecord(_)));
    }
}
