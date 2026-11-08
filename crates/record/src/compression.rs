//! Batch payload compression codecs for Record Batch v0.
//!
//! The `compression` byte in the batch header selects one of these codecs; the
//! codec is applied to the concatenated record bytes as a single unit.

/// Compression codec applied to a batch's record bytes.
///
/// The discriminant is the on-wire byte value stored in the batch header's
/// `compression` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    /// No compression; record bytes are stored verbatim.
    #[default]
    None = 0,
    /// LZ4 block compression.
    Lz4 = 1,
    /// Zstandard compression.
    Zstd = 2,
}

impl CompressionCodec {
    /// The on-wire byte value for this codec.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse a codec from its on-wire byte value.
    ///
    /// # Errors
    /// Returns [`CompressionError::UnknownCodec`] for any value other than 0, 1, 2.
    pub fn from_u8(value: u8) -> Result<Self, CompressionError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            other => Err(CompressionError::UnknownCodec(other)),
        }
    }

    /// Compress `input` with this codec, returning the compressed bytes.
    ///
    /// For LZ4 and Zstd the original length is embedded so [`decompress`] needs
    /// no external size hint. `None` returns the bytes verbatim (zero overhead).
    ///
    /// [`decompress`]: Self::decompress
    ///
    /// # Errors
    /// Returns [`CompressionError`] if the underlying codec fails.
    pub fn compress(self, input: &[u8]) -> Result<Vec<u8>, CompressionError> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Lz4 => Ok(lz4_flex::compress_prepend_size(input)),
            Self::Zstd => {
                zstd::encode_all(input, 0).map_err(|e| CompressionError::Codec(e.to_string()))
            }
        }
    }

    /// Decompress `input` with this codec, returning the original bytes.
    ///
    /// # Errors
    /// Returns [`CompressionError`] if the input is malformed for this codec.
    pub fn decompress(self, input: &[u8]) -> Result<Vec<u8>, CompressionError> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Lz4 => lz4_flex::decompress_size_prepended(input)
                .map_err(|e| CompressionError::Codec(e.to_string())),
            Self::Zstd => {
                zstd::decode_all(input).map_err(|e| CompressionError::Codec(e.to_string()))
            }
        }
    }
}

/// Errors produced while compressing or decompressing batch record bytes.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    /// The `compression` header byte did not map to a known codec.
    #[error("unknown compression codec byte: {0}")]
    UnknownCodec(u8),
    /// The underlying codec rejected the input (e.g. corrupt compressed data).
    #[error("codec failure: {0}")]
    Codec(String),
}

#[cfg(test)]
mod tests {
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
}
