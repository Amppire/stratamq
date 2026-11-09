#![forbid(unsafe_code)]

//! `strata-record`: the Record Batch v0 binary codec.
//!
//! `lib.rs` is a thin wrapper: each concept lives in its own module and is
//! re-exported here for a flat public surface.

pub mod batch;
pub mod compression;
pub mod cursor;
pub mod header;
pub mod proposed_record;
pub mod record;
pub mod subject;

pub use proposed_record::ProposedRecord;
pub use record::Record;
pub use subject::Subject;
