//! Batch payload compression codecs for Record Batch v0.
//!
//! The `compression` byte in the batch header selects one of these codecs; the
//! codec is applied to the concatenated record bytes as a single unit.

mod error;
#[cfg(test)]
mod tests;

pub use error::CompressionError;

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
