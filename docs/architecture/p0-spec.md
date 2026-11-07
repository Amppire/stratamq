# P0 Technical Specification

**Architecture Validation and Experimental Implementation**  
*Engineering Source of Truth — Baseline: November 2026*

---

## Contents
1. [Purpose and Exit Criteria](#1-purpose-and-exit-criteria)
2. [P0 Repository and Harness Architecture](#2-p0-repository-and-harness-architecture)
3. [Common Semantic Prototype](#3-common-semantic-prototype)
   - 3.1 [Subject and Revision Prototype](#31-subject-and-revision-prototype)
   - 3.2 [Subject-Scale Experiment](#32-subject-scale-experiment)
4. [Record Batch v0](#4-record-batch-v0)
5. [Sharding Tournament](#5-sharding-tournament)
6. [Storage Tournament](#6-storage-tournament)
7. [Transport Tournament](#7-transport-tournament)
8. [Delivery Tournament](#8-delivery-tournament)
9. [Wire/Storage Representation Tournament](#9-wirestorage-representation-tournament)
10. [Mixed-Workload Isolation Experiment](#10-mixed-workload-isolation-experiment)
11. [Correctness and Failure Injection](#11-correctness-and-failure-injection)
12. [Performance Instrumentation](#12-performance-instrumentation)
13. [Decision Thresholds](#13-decision-thresholds)
14. [Deliverables and ADRs](#14-deliverables-and-adrs)
15. [P0 Execution Sequence](#15-p0-execution-sequence)
- [Appendix A. Experiment Scenario Definitions](#appendix-a-experiment-scenario-definitions)
- [Appendix B. Common Result Schema](#appendix-b-common-result-schema)
- [Appendix C. Experiment Implementation Rules](#appendix-c-experiment-implementation-rules)
- [Appendix D. P0 Completion Checklist](#appendix-d-p0-completion-checklist)
- [Appendix E. Optimistic Concurrency Correctness Scenarios](#appendix-e-optimistic-concurrency-correctness-scenarios)

---

## 1. Purpose and Exit Criteria

P0 exists to validate physical architecture choices using reproducible benchmark evidence before production code freezes assumptions.

### Exit Criteria:
1. Confirm one immutable record model supports stream, work, broadcast, and keyed subscriptions.
2. Select between fixed/dynamic partitions and virtual sharding.
3. Select default storage commit architecture and supported durability profiles.
4. Define NMP Draft 0 framing and Record Batch v0 format.
5. Determine TCP vs QUIC transport positioning.
6. Validate receiver-authorized demand windows against dedicated push/pull models.
7. Select shared vs translated wire/storage representations.
8. Demonstrate a viable mixed-workload isolation strategy.

---

## 2. P0 Repository and Harness Architecture

All candidate implementations conform to common experiment traits in [`crates/bench`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/bench):

```rust
pub trait StorageExperiment {
    async fn append(&self, batch: RecordBatch, durability: Durability) -> CommitReceipt;
    async fn read(&self, query: ReadRange) -> RecordBatchSet;
    async fn recover(&self) -> RecoveryReport;
}

pub trait ShardingExperiment {
    fn locate(&self, key: &[u8]) -> Placement;
    fn rebalance(&mut self, topology: Topology) -> MovementPlan;
}

pub trait DeliveryExperiment {
    async fn grant(&self, demand: DemandWindow);
    async fn recv(&self) -> DeliveryBatch;
}
```

---

## 3. Common Semantic Prototype

The semantic prototype maintains in-memory state machines for journals and subscriptions to prove correctness invariants before introducing physical storage.

| Mode | State Maintained | Operations |
|---|---|---|
| **stream** | Cursor & committed checkpoint | `subscribe`, `fetch`/`demand`, `seek`, `commit` |
| **work** | Ready, Leased, RetryWait, DeadLettered, generations | `acquire`, `ack`, `nack`, `extend_lease`, `expire_leases`, `promote_retries` |
| **broadcast**| Per-subscription cursor / session | `subscribe`, `deliver`, `commit`, `close` |
| **keyed** | Per-key owner + pending queue | `register(record_id, key)`, deliver in key order, promote pending on completion |

### 3.1. Subject and Revision Prototype
- Maintain latest revision per `(journal, subject)`.
- Conditional append with `ExpectedRevision::Exact(n)`, `ExpectedRevision::NoSubject`, and `ExpectedRevision::Any`.
- Rejection with deterministic `AppendError::RevisionMismatch`.

### 3.2. Subject-Scale Experiment
Workload evaluation across 100k, 1M, and 10M logical subjects measuring memory footprint of the head index, append lookup latency, and recovery checkpoint time. Target: Subject lookup overhead must be $< 10\%$ CPU on keyed append workloads.

---

## 4. Record Batch v0

Binary layout for publication and storage batches:

```
RecordBatchV0
  u32 total_len
  u8  format_version
  u8  flags
  u8  compression (None = 0, LZ4 = 1, Zstd = 2)
  u8  reserved
  u32 record_count
  u64 producer_id
  u32 producer_epoch
  u64 first_sequence
  u64 first_timestamp
  u32 crc32c
  bytes records[]
```

---

## 5. Sharding Tournament

- **Candidate A: Fixed / Dynamic Partitions**: Traditional coarse partitions owning ordering, storage, and consensus.
- **Candidate B: Virtual Sharding**: 65,536 or 262,144 logical shards mapped to fewer physical Storage Groups via rendezvous hashing.
- **Candidate C: Range Placement**: Key ranges mapping to groups with split/merge capabilities.
- **Decision Threshold**: Virtual sharding is accepted if routing overhead is $\le 5\text{--}10\%$ CPU and metadata memory is bounded while reducing data churn during node addition/removal.

---

## 6. Storage Tournament

- **Candidate A: Local-First**: Local WAL + local segments + optional object archival.
- **Candidate B: Hybrid WAL + Object Storage**: Low-latency commit layer + background segment compaction and upload to S3 (Default hypothesis).
- **Candidate C: Object-Primary**: Direct durable commit to object storage before ACK.

---

## 7. Transport Tournament

Evaluate identical NMP semantic frames over:
1. **TCP**: Single connection with application-layer logical stream IDs.
2. **QUIC**: Mapping logical streams to QUIC streams under packet loss and WAN latency.

---

## 8. Delivery Tournament

Evaluate:
1. **Classic Pull**: Request/response polling with long-poll batching.
2. **Push with Fixed Credit**: Continuous push bounded by static window.
3. **Demand-Window Delivery**: Dynamic client-authorized budget (`records`, `bytes`, `deadline`, `continuous`).

---

## 9. Wire/Storage Representation Tournament

Compare **Shared RecordBatch Encoding** (zero-copy from socket to WAL/segment) vs. **Normalized Internal Representation** (parse on ingress, serialize on append).

---

## 10. Mixed-Workload Isolation Experiment

Run a mixed baseline (40% streaming, 30% work queue, 20% replay, 10% retries) while background compaction and object uploads are active. Measure interactive work p99 latency degradation.

---

## 11. Correctness and Failure Injection

- Crash injection during WAL append, fsync, segment roll, and metadata commit.
- Truncated tail recovery and corruption checksum failure (fail closed).
- Work lease expiration races (stale generation rejection).
- Shard movement during active keyed processing.

---

## 12. Decision Thresholds

| Decision | Threshold / Criterion |
|---|---|
| **Virtual Sharding** | Adopt if rebalancing churn is dramatically lower and routing CPU penalty is $\le 10\%$. |
| **Hybrid Storage** | Adopt as default if durable commit latency is within 2x of local WAL while slashing storage cost. |
| **Demand Windows** | Adopt if performance equals push/pull while simplifying protocol to a single primitive. |
| **QUIC Default** | Adopt only if WAN/multiplexing benefits outweigh CPU overhead; otherwise TCP default. |
| **Resource Scheduler** | Mandatory if mixed workload degrades foreground p99 by $> 2\times$. |
