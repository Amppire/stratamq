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
