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
9. Freeze committed record identity across publication receipts, durable storage, recovery, replay, and redelivery.
10. Define the durability and recovery contract for subject heads, cursor commits, work acknowledgments, retries, leases, and keyed state.
11. Complete the minimum interoperable NMP Draft 0 contract: exact frame layout, error scope, retryability, disconnect, and reconnect behavior.
12. Define retention safety invariants and assign production implementation to P1, replay-specific behavior to P3, and object-tier behavior to P4.

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

The header above is a candidate common envelope, not proof that publication and committed storage entries are identical. P0 must resolve the committed identity contract in [GitHub issue #32](https://github.com/Amppire/stratamq/issues/32): a record assigned in `PUBLISH_RESULT` must retain the same `RecordId` after WAL recovery, segment roll, replay, tier movement, and redelivery. ADR-P0-007 cannot be accepted using a representation that recreates identity during decode.

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

All candidates must include the metadata cost and recovery path for stable record identity, atomic subject-revision heads, durable subscription checkpoints, and earliest-available retention positions. Throughput comparisons that omit required correctness metadata are not semantically equivalent.

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

Both candidates must preserve identical committed `RecordId`, subject revision, ordering, checksum, and recovery semantics. Publication input may omit broker-assigned fields, but committed storage and subsequent delivery must reproduce the original commit receipt. The tournament report must state whether publication and committed batches are the same representation, versioned variants, or separate formats.

---

## 10. Mixed-Workload Isolation Experiment

Run a mixed baseline (40% streaming, 30% work queue, 20% replay, 10% retries) while background compaction and object uploads are active. Measure interactive work p99 latency degradation.

---

## 11. Correctness and Failure Injection

- Crash injection during WAL append, fsync, segment roll, and metadata commit.
- Truncated tail recovery and corruption checksum failure (fail closed).
- Work lease expiration races (stale generation rejection).
- Shard movement during active keyed processing.
- Stable record identity across append, restart, replay, and redelivery.
- Atomic recovery of a record and its per-subject revision head.
- Cursor, ACK/NACK, retry, dead-letter, generation, and keyed-state recovery according to the durable-state contract.
- Monotonic lease/retry deadline reconstruction or conservative invalidation after restart.
- Retention-boundary errors when a cursor or replay position is older than the earliest available record.

---

## 12. Performance Instrumentation

Every tournament result must record scenario ID, implementation candidate, code revision, hardware and operating-system details, payload and batch distribution, concurrency, warm-up and sample duration, throughput, p50/p95/p99/p99.9 latency, CPU time, peak RSS, allocation count where available, and disk/network bytes. Results must be machine-readable and accompanied by the semantic-equivalence statement required by the ADR process.

Correctness metadata is part of the measured implementation. A candidate may not omit identity indexes, revision heads, subscription checkpoints, checksums, fsync, or recovery work to improve benchmark results.

---

## 13. Decision Thresholds

| Decision | Threshold / Criterion |
|---|---|
| **Virtual Sharding** | Adopt if rebalancing churn is dramatically lower and routing CPU penalty is $\le 10\%$. |
| **Hybrid Storage** | Adopt as default if durable commit latency is within 2x of local WAL while slashing storage cost. |
| **Demand Windows** | Adopt if performance equals push/pull while simplifying protocol to a single primitive. |
| **QUIC Default** | Adopt only if WAN/multiplexing benefits outweigh CPU overhead; otherwise TCP default. |
| **Resource Scheduler** | Mandatory if mixed workload degrades foreground p99 by $> 2\times$. |
| **Committed Identity** | No candidate is acceptable unless `RecordId` remains stable across receipt, storage, recovery, replay, and redelivery. |
| **Durable State** | No acknowledged operation may recover outside the documented operation-specific recovery outcomes. |
| **NMP Draft 0** | The encoded frame layout and error/reconnect behavior must be unambiguous enough for an independent client implementation. |
| **Retention Safety** | The chosen storage design must support deterministic earliest-position errors and safe payload reclamation without dangling state references. |

---

## 14. Deliverables and ADRs

P0 produces reproducible reports for each tournament, updated Accepted or rejected ADRs, the stabilized RecordBatch and NMP Draft 0 contracts, and a semantic-closure report covering:

- [GitHub issue #32](https://github.com/Amppire/stratamq/issues/32): committed record identity and wire/storage representation;
- [GitHub issue #33](https://github.com/Amppire/stratamq/issues/33): retention safety and phase boundary;
- [GitHub issue #34](https://github.com/Amppire/stratamq/issues/34): NMP frame, error, retry, disconnect, and reconnect contract;
- [GitHub issue #35](https://github.com/Amppire/stratamq/issues/35): durable subscription state and recovery semantics.

P0 freezes these contracts and implements only the prototype depth needed to validate them. Production persistence and operational tooling remain assigned to their later phases.

---

## 15. P0 Execution Sequence

1. Complete semantic state machines, deterministic clocks, RecordBatch codec hardening, and the benchmark harness.
2. Freeze identity, durable-state, NMP, and retention contracts before accepting representation or storage results.
3. Execute codec, sharding, storage, delivery, transport, mixed-workload, and failure-injection scenarios under equivalent semantics.
4. Integrate NMP ingress through durable append and verify receipt, identity, OCC, and recovery behavior end to end.
5. Package evidence, resolve failed thresholds, update ADR statuses, and conduct the P0 exit review.

---

## Appendix A. Experiment Scenario Definitions

The canonical P0 scenario IDs are `SHARD-U1`, `SHARD-H1`, `SHARD-R1`, `STORE-W1`, `STORE-S1`, `STORE-R1`, `NET-L1`, `NET-W1`, `DEL-S1`, `MIX-1`, `MIX-2`, `FAIL-1`, `FAIL-2`, and `FAIL-3`. Each scenario must publish its exact workload parameters with the result artifact.

## Appendix B. Common Result Schema

Results must include the instrumentation fields from §12 plus candidate-specific configuration, correctness outcome, failure-injection outcome, and links to raw artifacts. Comparing aggregate throughput alone is insufficient.

## Appendix C. Experiment Implementation Rules

Candidates must provide equivalent durability, checksums, identity preservation, state metadata, and backpressure. Experimental shortcuts must be declared and cannot be used to accept an ADR without a semantic-equivalence justification.

## Appendix D. P0 Completion Checklist

- [ ] Common semantic prototype invariants pass deterministic and property tests.
- [ ] RecordBatch decoding is bounded and fuzzed.
- [ ] Committed identity, durable state, NMP, and retention contracts are frozen.
- [ ] Every tournament has reproducible evidence and an ADR disposition.
- [ ] Failure injection proves the documented receipt and recovery outcomes.
- [ ] End-to-end ingress preserves record identity and subject OCC across restart.
- [ ] Later-phase implementation work is tracked without leaving P0 semantics ambiguous.

## Appendix E. Optimistic Concurrency Correctness Scenarios

- Concurrent `ExpectedRevision::Exact(k)` writers produce exactly one success for a single subject head.
- `ExpectedRevision::NoSubject` succeeds only when no committed subject head exists.
- A successful subject append and its head revision recover atomically.
- Replaying a pre-crash expected revision produces the same deterministic conflict after recovery.
- Distinct subjects remain independently appendable without a global journal lock in the candidate design.
