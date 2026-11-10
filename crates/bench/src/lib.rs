#![forbid(unsafe_code)]

//! `strata-bench`: the P0 benchmark harness (`bench-core`).
//!
//! `lib.rs` is a thin wrapper: each concept lives in its own module.

pub mod generator;
pub mod metrics;
pub mod schema;
pub mod traits;
