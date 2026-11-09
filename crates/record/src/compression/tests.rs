use super::*;

const SAMPLE: &[u8] = b"the quick brown fox jumps over the lazy dog, the quick brown fox again";

#[test]
fn as_u8_matches_discriminant() {
    assert_eq!(CompressionCodec::None.as_u8(), 0);
    assert_eq!(CompressionCodec::Lz4.as_u8(), 1);
    assert_eq!(CompressionCodec::Zstd.as_u8(), 2);
}

#[test]
fn from_u8_accepts_known_codecs() {
    assert_eq!(
        CompressionCodec::from_u8(0).unwrap(),
        CompressionCodec::None
    );
    assert_eq!(CompressionCodec::from_u8(1).unwrap(), CompressionCodec::Lz4);
    assert_eq!(
        CompressionCodec::from_u8(2).unwrap(),
        CompressionCodec::Zstd
    );
}

#[test]
fn from_u8_rejects_unknown_codec() {
    let err = CompressionCodec::from_u8(3).unwrap_err();
    assert!(matches!(err, CompressionError::UnknownCodec(3)));
}

#[test]
fn none_round_trips_identically() {
    let codec = CompressionCodec::None;
    let compressed = codec.compress(SAMPLE).unwrap();
    assert_eq!(compressed, SAMPLE, "None must store bytes verbatim");
    assert_eq!(codec.decompress(&compressed).unwrap(), SAMPLE);
}

#[test]
fn lz4_round_trips() {
    let codec = CompressionCodec::Lz4;
    let compressed = codec.compress(SAMPLE).unwrap();
    assert_eq!(codec.decompress(&compressed).unwrap(), SAMPLE);
}

#[test]
fn zstd_round_trips() {
    let codec = CompressionCodec::Zstd;
    let compressed = codec.compress(SAMPLE).unwrap();
    assert_eq!(codec.decompress(&compressed).unwrap(), SAMPLE);
}

#[test]
fn empty_input_round_trips_for_all_codecs() {
    for codec in [
        CompressionCodec::None,
        CompressionCodec::Lz4,
        CompressionCodec::Zstd,
    ] {
        let compressed = codec.compress(&[]).unwrap();
        assert_eq!(codec.decompress(&compressed).unwrap(), Vec::<u8>::new());
    }
}

#[test]
fn decompress_rejects_corrupt_zstd_input() {
    // Zstd frames carry a magic number (and optional checksums), so random
    // bytes are rejected at the codec layer.
    //
    // LZ4 *block* format (what `lz4_flex` size-prepend uses) has no magic
    // number or integrity check: many random byte sequences decode to some
    // output without error. Detecting corrupted LZ4 is therefore NOT the
    // codec's job -- the batch-level CRC32C over the stored (compressed)
    // bytes is what catches it, before decompression ever runs.
    let garbage = [0xFFu8, 0x00, 0xAB, 0x13, 0x37, 0x42, 0x99, 0x01];
    let err = CompressionCodec::Zstd.decompress(&garbage).unwrap_err();
    assert!(matches!(err, CompressionError::Codec(_)));
}
