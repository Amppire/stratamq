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
