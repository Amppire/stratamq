mod entry {
    use super::super::*;
    use crate::record::Record;
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

mod batch {
    use super::super::*;
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
