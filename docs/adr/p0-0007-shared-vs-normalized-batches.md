# ADR-P0-007: Shared Wire/Storage Batch Format vs. Normalized Translation

## Status
Experimental (Pending Codec Benchmark)

## Context
High-throughput event streaming spends significant CPU time serializing, deserializing, and copying message batches. A shared binary layout allows validated wire batches to be written directly to disk segments without re-encoding, but couples the network protocol to disk storage.

## Candidates Considered
1. **Shared Batch Representation**: Direct zero-copy passthrough from NMP wire frame into WAL and segment files.
2. **Normalized Internal Representation**: Decode incoming frames into Rust domain structs, then encode into a separate storage format.

## Evidence Required & Decision Threshold
- Measure CPU cycles/GB, allocation count per record, and memory copies under 100B, 1KiB, and 10KiB payload sizes.
- **Threshold**: Shared format wins if it delivers $> 30\%$ throughput/CPU improvement without hindering protocol version evolution.
