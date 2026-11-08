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

impl DecodedEntry {
    /// Rebuild a [`Record`] from this decoded content, minting a fresh
    /// `RecordId`. The id is broker-assigned at commit (NMP §8.1) and is not
    /// carried on the wire, so a decoded batch's records carry new ids; equality
    /// against the source batch is therefore content-only.
    #[must_use]
    pub fn into_record(self) -> Record {
        let mut proposed = crate::ProposedRecord::new(self.payload);
        if let Some(subject) = self.subject {
            proposed = proposed.with_subject(subject);
        }
        if let Some(key) = self.key {
            proposed = proposed.with_key(key);
        }
        for (name, value) in self.headers {
            proposed = proposed.with_header(name, value);
        }
        proposed.commit(self.timestamp, self.revision)
    }
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
        codec: crate::compression::CompressionCodec,
    ) -> Result<Vec<u8>, BatchError> {
        use crate::header::{
            BatchHeader, FORMAT_VERSION_V0, HEADER_LEN, MAX_RECORD_COUNT, MAX_TOTAL_LEN,
        };

        let record_count = u32::try_from(self.records.len())
            .map_err(|_| BatchError::MalformedRecord("record_count exceeds u32".into()))?;
        if record_count > MAX_RECORD_COUNT {
            return Err(BatchError::Header(
                crate::header::HeaderError::RecordCountTooLarge(record_count),
            ));
        }

        // 1. Flatten records into the uncompressed blob.
        let mut raw = Vec::new();
        let mut has_subject = false;
        for record in &self.records {
            if record.subject().is_some() {
                has_subject = true;
            }
            raw.extend_from_slice(&encode_entry(record, self.first_timestamp)?);
        }

        // 2. Compress the blob as a unit.
        let stored = codec.compress(&raw)?;

        // 3. Castagnoli CRC32C over the STORED (compressed) bytes —
        //    hardware-accelerated; integrity is checked before decompression
        //    on the read path.
        let crc32c = crc32c::crc32c(&stored);

        // 4. Defensive total_len bound.
        let total_len = u32::try_from(HEADER_LEN + stored.len())
            .map_err(|_| BatchError::MalformedRecord("batch exceeds u32 total_len".into()))?;
        if total_len > MAX_TOTAL_LEN {
            return Err(BatchError::Header(
                crate::header::HeaderError::TotalLenTooLarge(total_len),
            ));
        }

        // 5. Build the header flags: bit0 has_subject, bit1 is_transactional.
        let mut flags = 0u8;
        if has_subject {
            flags |= 0b01;
        }
        if self.is_transactional {
            flags |= 0b10;
        }

        let header = BatchHeader {
            total_len,
            format_version: FORMAT_VERSION_V0,
            flags,
            compression: codec.as_u8(),
            reserved: 0,
            record_count,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            first_sequence: self.first_sequence,
            first_timestamp: self.first_timestamp,
            crc32c,
        };

        // 6. Prepend header to stored records.
        let mut out = Vec::with_capacity(HEADER_LEN + stored.len());
        out.extend_from_slice(&header.encode());
        out.extend_from_slice(&stored);
        Ok(out)
    }

    /// Decode a batch from `bytes`, validating the header, defensive bounds,
    /// and the CRC32C checksum before allocating record storage.
    ///
    /// # Errors
    /// Returns [`BatchError`] on truncation, bound violation, checksum
    /// mismatch, or malformed record framing.
    pub fn decode(bytes: &[u8]) -> Result<Self, BatchError> {
        use crate::compression::CompressionCodec;
        use crate::header::{BatchHeader, HEADER_LEN};

        // 1. Header (validates version + record_count/total_len caps).
        let header = BatchHeader::decode(bytes)?;

        // 2. The stored record region is everything after the header, bounded
        //    by the header's total_len.
        let total_len = header.total_len as usize;
        if total_len < HEADER_LEN || total_len > bytes.len() {
            return Err(BatchError::MalformedRecord(format!(
                "total_len {total_len} inconsistent with buffer length {}",
                bytes.len()
            )));
        }
        let stored = &bytes[HEADER_LEN..total_len];

        // 3. Verify CRC32C over the stored bytes BEFORE decompressing.
        if crc32c::crc32c(stored) != header.crc32c {
            return Err(BatchError::ChecksumMismatch);
        }

        // 4. Decompress.
        let codec = CompressionCodec::from_u8(header.compression)
            .map_err(|_| BatchError::MalformedRecord("unknown compression codec".into()))?;
        let raw = codec.decompress(stored)?;

        // 5. Walk the blob, decoding exactly record_count entries.
        let mut records = Vec::with_capacity(header.record_count as usize);
        let mut offset = 0usize;
        for _ in 0..header.record_count {
            let (entry, consumed) = decode_entry(&raw[offset..], header.first_timestamp)?;
            offset += consumed;
            records.push(entry.into_record());
        }
        if offset != raw.len() {
            return Err(BatchError::MalformedRecord(format!(
                "{} trailing record bytes after {} entries",
                raw.len() - offset,
                header.record_count
            )));
        }

        Ok(Self {
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            first_sequence: header.first_sequence,
            first_timestamp: header.first_timestamp,
            is_transactional: header.flags & 0b10 != 0,
            records,
        })
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

impl From<crate::cursor::CursorError> for BatchError {
    fn from(err: crate::cursor::CursorError) -> Self {
        BatchError::MalformedRecord(err.to_string())
    }
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
pub(crate) fn encode_entry(record: &Record, first_timestamp: u64) -> Result<Vec<u8>, BatchError> {
    let delta = record
        .timestamp()
        .checked_sub(first_timestamp)
        .ok_or_else(|| {
            BatchError::MalformedRecord(format!(
                "record timestamp {} precedes batch first_timestamp {first_timestamp}",
                record.timestamp()
            ))
        })?;
    let delta = u32::try_from(delta)
        .map_err(|_| BatchError::MalformedRecord(format!("timestamp_delta {delta} exceeds u32")))?;

    let mut flags = 0u8;
    if record.subject().is_some() {
        flags |= ENTRY_FLAG_HAS_SUBJECT;
    }
    if record.revision().is_some() {
        flags |= ENTRY_FLAG_HAS_REVISION;
    }
    if record.key().is_some() {
        flags |= ENTRY_FLAG_HAS_KEY;
    }

    // Build the entry body, then prefix it with its u32 length.
    let mut body = Vec::new();
    body.extend_from_slice(&delta.to_be_bytes());
    body.push(flags);

    if let Some(subject) = record.subject() {
        let s = subject.as_str().as_bytes();
        let len = u16::try_from(s.len())
            .map_err(|_| BatchError::MalformedRecord("subject exceeds u16 length".into()))?;
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(s);
    }

    if let Some(revision) = record.revision() {
        body.extend_from_slice(&revision.to_be_bytes());
    }

    if let Some(key) = record.key() {
        let len = u32::try_from(key.len())
            .map_err(|_| BatchError::MalformedRecord("key exceeds u32 length".into()))?;
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(key);
    }

    let headers = record.headers();
    let header_count = u16::try_from(headers.len())
        .map_err(|_| BatchError::MalformedRecord("too many headers (exceeds u16)".into()))?;
    body.extend_from_slice(&header_count.to_be_bytes());
    for (name, value) in headers {
        let name_bytes = name.as_bytes();
        let name_len = u16::try_from(name_bytes.len())
            .map_err(|_| BatchError::MalformedRecord("header name exceeds u16 length".into()))?;
        body.extend_from_slice(&name_len.to_be_bytes());
        body.extend_from_slice(name_bytes);
        let val_len = u32::try_from(value.len())
            .map_err(|_| BatchError::MalformedRecord("header value exceeds u32 length".into()))?;
        body.extend_from_slice(&val_len.to_be_bytes());
        body.extend_from_slice(value);
    }

    let payload = record.payload();
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| BatchError::MalformedRecord("payload exceeds u32 length".into()))?;
    body.extend_from_slice(&payload_len.to_be_bytes());
    body.extend_from_slice(payload);

    let body_len = u32::try_from(body.len())
        .map_err(|_| BatchError::MalformedRecord("entry body exceeds u32 length".into()))?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&body_len.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// A bounded forward cursor over an entry's bytes. Every read is length-checked
/// so a truncated or inconsistent entry yields [`BatchError::MalformedRecord`]
/// rather than panicking.
use crate::cursor::Cursor;

/// Decode a single record entry from the front of `bytes`, returning the
/// decoded content and the number of bytes consumed.
///
/// # Errors
/// Returns [`BatchError::MalformedRecord`] on truncation or inconsistent
/// length fields.
pub(crate) fn decode_entry(
    bytes: &[u8],
    first_timestamp: u64,
) -> Result<(DecodedEntry, usize), BatchError> {
    // Read the u32 length prefix, then bound the body to exactly that many bytes
    // so an over-long length field cannot read past the entry.
    let mut head = Cursor::new(bytes);
    let body_len = head.u32()? as usize;
    let body_end = 4usize
        .checked_add(body_len)
        .ok_or_else(|| BatchError::MalformedRecord("entry length arithmetic overflow".into()))?;
    if body_end > bytes.len() {
        return Err(BatchError::MalformedRecord(format!(
            "entry truncated: declared body {body_len}, have {} after prefix",
            bytes.len().saturating_sub(4)
        )));
    }
    let mut c = Cursor::new(&bytes[4..body_end]);

    let delta = c.u32()?;
    let timestamp = first_timestamp
        .checked_add(u64::from(delta))
        .ok_or_else(|| BatchError::MalformedRecord("timestamp overflow".into()))?;
    let flags = c.u8()?;

    let subject = if flags & ENTRY_FLAG_HAS_SUBJECT != 0 {
        let len = c.u16()? as usize;
        let raw = c.take(len)?;
        let s = std::str::from_utf8(raw)
            .map_err(|_| BatchError::MalformedRecord("subject is not valid UTF-8".into()))?;
        Some(crate::Subject::new(s))
    } else {
        None
    };

    let revision = if flags & ENTRY_FLAG_HAS_REVISION != 0 {
        Some(c.u64()?)
    } else {
        None
    };

    let key = if flags & ENTRY_FLAG_HAS_KEY != 0 {
        let len = c.u32()? as usize;
        Some(bytes::Bytes::copy_from_slice(c.take(len)?))
    } else {
        None
    };

    let header_count = c.u16()? as usize;
    let mut headers = std::collections::BTreeMap::new();
    for _ in 0..header_count {
        let name_len = c.u16()? as usize;
        let name_raw = c.take(name_len)?;
        let name = std::str::from_utf8(name_raw)
            .map_err(|_| BatchError::MalformedRecord("header name is not valid UTF-8".into()))?
            .to_owned();
        let val_len = c.u32()? as usize;
        let value = bytes::Bytes::copy_from_slice(c.take(val_len)?);
        headers.insert(name, value);
    }

    let payload_len = c.u32()? as usize;
    let payload = bytes::Bytes::copy_from_slice(c.take(payload_len)?);

    if !c.is_exhausted() {
        return Err(BatchError::MalformedRecord(format!(
            "entry has {} trailing bytes after payload",
            c.remaining()
        )));
    }

    let entry = DecodedEntry {
        timestamp,
        revision,
        subject,
        key,
        headers,
        payload,
    };
    Ok((entry, body_end))
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

#[cfg(test)]
mod batch_tests {
    use super::*;
    use crate::compression::CompressionCodec;
    use crate::header::{BatchHeader, HEADER_LEN, MAX_RECORD_COUNT, MAX_TOTAL_LEN};
    use crate::{ProposedRecord, Subject};
    use bytes::Bytes;

    const FIRST_TS: u64 = 2_000_000;

    fn sample_batch() -> RecordBatchV0 {
        let records = vec![
            ProposedRecord::new("first payload")
                .with_subject(Subject::new("orders.us"))
                .with_key("k1")
                .with_header("content-type", "application/json")
                .commit(FIRST_TS, Some(1)),
            ProposedRecord::new("second payload").commit(FIRST_TS + 100, None),
            ProposedRecord::new(Bytes::from_static(b"third"))
                .with_subject(Subject::new("orders.eu"))
                .commit(FIRST_TS + 250, Some(2)),
        ];
        RecordBatchV0 {
            producer_id: 0xDEAD_BEEF_0000_0001,
            producer_epoch: 7,
            first_sequence: 5000,
            first_timestamp: FIRST_TS,
            is_transactional: false,
            records,
        }
    }

    fn assert_records_equal(a: &RecordBatchV0, b: &RecordBatchV0) {
        assert_eq!(a.records.len(), b.records.len(), "record count");
        for (x, y) in a.records.iter().zip(&b.records) {
            // Content equality: id is NOT on the wire, so compare content only.
            assert_eq!(x.timestamp(), y.timestamp());
            assert_eq!(x.subject(), y.subject());
            assert_eq!(x.revision(), y.revision());
            assert_eq!(x.key(), y.key());
            assert_eq!(x.headers(), y.headers());
            assert_eq!(x.payload(), y.payload());
        }
    }

    fn assert_meta_equal(a: &RecordBatchV0, b: &RecordBatchV0) {
        assert_eq!(a.producer_id, b.producer_id);
        assert_eq!(a.producer_epoch, b.producer_epoch);
        assert_eq!(a.first_sequence, b.first_sequence);
        assert_eq!(a.first_timestamp, b.first_timestamp);
        assert_eq!(a.is_transactional, b.is_transactional);
    }

    // AC #3: None/LZ4/Zstd all round-trip to identical records after decode.
    #[test]
    fn round_trips_uncompressed() {
        let batch = sample_batch();
        let bytes = batch.encode(CompressionCodec::None).expect("encode");
        let decoded = RecordBatchV0::decode(&bytes).expect("decode");
        assert_meta_equal(&batch, &decoded);
        assert_records_equal(&batch, &decoded);
    }

    #[test]
    fn round_trips_lz4() {
        let batch = sample_batch();
        let bytes = batch.encode(CompressionCodec::Lz4).expect("encode");
        let decoded = RecordBatchV0::decode(&bytes).expect("decode");
        assert_records_equal(&batch, &decoded);
    }

    #[test]
    fn round_trips_zstd() {
        let batch = sample_batch();
        let bytes = batch.encode(CompressionCodec::Zstd).expect("encode");
        let decoded = RecordBatchV0::decode(&bytes).expect("decode");
        assert_records_equal(&batch, &decoded);
    }

    #[test]
    fn all_codecs_decode_to_identical_records() {
        let batch = sample_batch();
        let none = RecordBatchV0::decode(&batch.encode(CompressionCodec::None).unwrap()).unwrap();
        let lz4 = RecordBatchV0::decode(&batch.encode(CompressionCodec::Lz4).unwrap()).unwrap();
        let zstd = RecordBatchV0::decode(&batch.encode(CompressionCodec::Zstd).unwrap()).unwrap();
        assert_records_equal(&none, &lz4);
        assert_records_equal(&none, &zstd);
    }

    #[test]
    fn empty_batch_round_trips() {
        let batch = RecordBatchV0 {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: 0,
            first_timestamp: FIRST_TS,
            is_transactional: false,
            records: vec![],
        };
        let bytes = batch.encode(CompressionCodec::None).expect("encode");
        let decoded = RecordBatchV0::decode(&bytes).expect("decode");
        assert_eq!(decoded.records.len(), 0);
        assert_meta_equal(&batch, &decoded);
    }

    // The header's compression byte and flags must reflect the encode request.
    #[test]
    fn header_records_codec_and_flags() {
        let mut batch = sample_batch();
        batch.is_transactional = true;
        let bytes = batch.encode(CompressionCodec::Zstd).expect("encode");
        let header = BatchHeader::decode(&bytes).expect("header");
        assert_eq!(header.compression, 2, "zstd codec byte");
        assert_eq!(header.record_count, 3);
        assert_eq!(header.producer_id, batch.producer_id);
        assert_eq!(header.first_sequence, batch.first_sequence);
        assert_eq!(header.flags & 0b10, 0b10, "is_transactional bit");
        assert_eq!(
            header.total_len as usize,
            bytes.len(),
            "total_len covers whole batch"
        );
    }

    // AC #2: corrupting one payload byte fails decode with ChecksumMismatch.
    #[test]
    fn corrupting_one_payload_byte_fails_checksum() {
        let batch = sample_batch();
        let mut bytes = batch.encode(CompressionCodec::None).expect("encode");
        // Flip a byte in the record region (past the 44-byte header).
        let idx = HEADER_LEN + 5;
        bytes[idx] ^= 0xFF;
        let err = RecordBatchV0::decode(&bytes).unwrap_err();
        assert!(
            matches!(err, BatchError::ChecksumMismatch),
            "expected ChecksumMismatch, got {err:?}"
        );
    }

    // AC #4: bounded allocation — reject record_count over the cap.
    #[test]
    fn decode_rejects_record_count_over_cap() {
        let batch = sample_batch();
        let mut bytes = batch.encode(CompressionCodec::None).expect("encode");
        // Overwrite record_count (offset 8..12) with cap+1.
        let bad = MAX_RECORD_COUNT + 1;
        bytes[8..12].copy_from_slice(&bad.to_be_bytes());
        let err = RecordBatchV0::decode(&bytes).unwrap_err();
        assert!(matches!(err, BatchError::Header(_)), "got {err:?}");
    }

    // AC #4: reject total_len over the 16 MiB cap.
    #[test]
    fn decode_rejects_total_len_over_cap() {
        let batch = sample_batch();
        let mut bytes = batch.encode(CompressionCodec::None).expect("encode");
        let bad = MAX_TOTAL_LEN + 1;
        bytes[0..4].copy_from_slice(&bad.to_be_bytes());
        let err = RecordBatchV0::decode(&bytes).unwrap_err();
        assert!(matches!(err, BatchError::Header(_)), "got {err:?}");
    }

    #[test]
    fn decode_rejects_truncated_batch() {
        let batch = sample_batch();
        let bytes = batch.encode(CompressionCodec::None).expect("encode");
        let err = RecordBatchV0::decode(&bytes[..HEADER_LEN - 1]).unwrap_err();
        assert!(matches!(err, BatchError::Header(_)), "got {err:?}");
    }
}
