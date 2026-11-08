//! Record Batch v0 encoder and decoder.
//!
//! A [`RecordBatchV0`] is the fixed 44-byte [`crate::header::BatchHeader`]
//! followed by the (optionally compressed) concatenation of individually
//! framed records. Encoding flattens records into bytes; decoding rebuilds
//! records from bytes, validating integrity and defensive bounds first.

use crate::Record;
use crate::compression::CompressionError;
use crate::header::HeaderError;

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
