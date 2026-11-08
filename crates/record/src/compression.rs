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
    pub fn from_u8(_value: u8) -> Result<Self, CompressionError> {
        unimplemented!("CompressionCodec::from_u8")
    }

    /// Compress `input` with this codec, returning the compressed bytes.
    ///
    /// # Errors
    /// Returns [`CompressionError`] if the underlying codec fails.
    pub fn compress(self, _input: &[u8]) -> Result<Vec<u8>, CompressionError> {
        unimplemented!("CompressionCodec::compress")
    }

    /// Decompress `input` with this codec, returning the original bytes.
    ///
    /// # Errors
    /// Returns [`CompressionError`] if the input is malformed for this codec.
    pub fn decompress(self, _input: &[u8]) -> Result<Vec<u8>, CompressionError> {
        unimplemented!("CompressionCodec::decompress")
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
