# ADR-P0-003: Default Storage Commit Architecture

## Status
Experimental (Pending Storage Tournament)

## Context
Messaging workloads have divergent storage demands: interactive work queues require single-digit millisecond commit latencies, while high-volume event streaming demands low-cost long-term retention.

## Candidates Considered
1. **Candidate A: Local-First WAL**: Batches append to local NVMe WAL, replicate to quorum, fsync, and roll to local segments. Object store is strictly cold archival.
2. **Candidate B: Hybrid WAL + Object Storage**: Batches commit to low-latency local WAL/cache; immutable segments are compacted and uploaded asynchronously to object storage (Default hypothesis).
3. **Candidate C: Object-Primary**: Batches aggregate and commit directly to S3-compatible storage before client ACK.

## Evidence Required & Decision Threshold
- Run benchmarks `STORE-W1` (interactive work latency), `STORE-S1` (streaming throughput), and `STORE-R1` (cold replay).
- **Threshold**: Hybrid WAL + Object wins as default if it provides within $2\times$ the commit latency of Local-First while enabling elastic storage and fast node recovery.
