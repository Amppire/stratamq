# Implementation Plan

**First-Principles Event Streaming and Reliable Messaging**  
*Engineering Source of Truth — Baseline: November 2026*

---

## Contents
1. [Executive Summary](#1-executive-summary)
2. [Product Vision and Differentiation](#2-product-vision-and-differentiation)
3. [Functional Scope](#3-functional-scope)
4. [Core Semantic Architecture](#4-core-semantic-architecture)
5. [Subscription and Ordering Semantics](#5-subscription-and-ordering-semantics)
6. [Native Protocol Architecture](#6-native-protocol-architecture)
7. [Storage Architecture](#7-storage-architecture)
8. [Sharding and Placement Architecture](#8-sharding-and-placement-architecture)
9. [Execution, Leases, Retries, and Idempotency](#9-execution-leases-retries-and-idempotency)
10. [Mixed-Workload Resource Isolation](#10-mixed-workload-resource-isolation)
11. [Embedded, Standalone, and Clustered Runtimes](#11-embedded-standalone-and-clustered-runtimes)
12. [Security Architecture](#12-security-architecture)
13. [Observability and Diagnostics](#13-observability-and-diagnostics)
14. [SDK and Compatibility Strategy](#14-sdk-and-compatibility-strategy)
15. [Testing and Verification Strategy](#15-testing-and-verification-strategy)
16. [Benchmark Program](#16-benchmark-program)
17. [Delivery Roadmap P0–v1](#17-delivery-roadmap-p0v1)
18. [Repository and Module Structure](#18-repository-and-module-structure)
19. [CI/CD and Release Engineering](#19-cicd-and-release-engineering)
20. [Risks and Mitigations](#20-risks-and-mitigations)
21. [v1 Acceptance Criteria](#21-v1-acceptance-criteria)
- [Appendix A. Initial Configuration Model](#appendix-a-initial-configuration-model)
- [Appendix B. API Examples](#appendix-b-api-examples)
- [Appendix C. Research References](#appendix-c-research-references)
- [Appendix D. Detailed Subsystem Requirements](#appendix-d-detailed-subsystem-requirements)
- [Appendix E. Detailed Phase Work Breakdown](#appendix-e-detailed-phase-work-breakdown)
- [Appendix F. Operational Behavior and Failure Semantics](#appendix-f-operational-behavior-and-failure-semantics)
- [Appendix G. Performance and Capacity Targets](#appendix-g-performance-and-capacity-targets)
- [Appendix H. Market Positioning and Adoption Strategy](#appendix-h-market-positioning-and-adoption-strategy)
- [Appendix I. Event Database Capability Integration](#appendix-i-event-database-capability-integration)

---

## 1. Executive Summary

The Unified Durable Messaging Platform (**StrataMQ**) is an open-source Rust-based system intended to provide high-throughput event streaming and reliable work-queue messaging from one first-principles architecture. Its purpose is not to recreate Kafka, RabbitMQ, or NATS behind a different implementation language. Instead, the project separates application semantics from physical architecture so that ordering, delivery mode, consumer concurrency, storage placement, replication, retention, and durability can evolve independently.

The platform exposes **Journals**, immutable **Records**, and **Subscriptions**. A subscription selects stream, work, broadcast, keyed, or ephemeral semantics. Physical partition counts are not part of the application contract. The system uses virtual shards and internal storage groups, benchmarked during P0 against simpler fixed/dynamic partitions.

The native binary protocol (**NMP**) is designed for record batches, multiplexing, capability negotiation, demand windows, work leases, cursor operations, replay branches, and broker-local atomic operations.

---

## 2. Product Vision and Differentiation

| Area | Conventional Constraint | Project Direction |
|---|---|---|
| **Scaling** | Partition counts constrain ordering and consumer parallelism | Logical sharding and physical storage ownership separated |
| **Queue vs Stream** | Different products/modes with different storage engines | One immutable record model, multiple subscription semantics |
| **Storage** | Broker local disks or object-primary lock-in | Storage/durability profile independent by journal |
| **Delivery** | Push and pull appear as separate consumer models | One receiver-authorized demand-window primitive supports both |
| **Replay** | Offset reset or republishing affects production state | Isolated replay branches as native first-class state |
| **Keyed Processing**| Tied directly to physical partitions | Logical key ownership independent of storage group count |
| **Protocol** | Client API reflects broker internal topology | Native protocol reflects semantic operations |

---

## 3. Functional Scope

### 3.1. Event-Streaming Capabilities
- Durable batch publication and retrieval.
- Independent durable subscriptions with cursors.
- Retention by age, size, or policy.
- Seek by record ID, timestamp, or cursor checkpoint.
- Historical replay without disturbing production position.
- Optional keyed ordering; batch and continuous consumption; tiered local/object storage.

### 3.2. Work-Queue Capabilities
- Competing consumers.
- Visibility leases and fencing generations.
- ACK, NACK, release, and lease extension.
- Fixed, linear, exponential, and explicit retry schedules.
- Dead-letter terminal state without mandatory payload duplication.
- Delayed and scheduled eligibility; priority and workload-aware scheduling.

### 3.3. Cross-Cutting Capabilities
- Transport-level producer idempotency.
- Application-level idempotency keys.
- Broker-local atomic consume/publish/ACK operations.
- Broadcast and keyed subscription modes.
- Mixed-workload resource isolation.
- Embedded library, standalone server, and clustered modes.

---

## 4. Core Semantic Architecture

The core architecture consists of two planes:
- **Record Plane**: Stores immutable publication data (`Journal → immutable Record Batches → Segments → Retention/Tiering`).
- **State Plane**: Stores subscription-specific cursor, delivery, retry, ownership, replay, and scheduler metadata (`Subscription → Cursor | Lease | Retry | Key Ownership | Replay | Scheduler State`).

### 4.1. Journal
Logical namespace and policy boundary defining retention, default ordering, metadata policies, and storage class.

### 4.2. Record
Globally unique sortable identifier (UUIDv7), journal ID, commit timestamp, optional key, headers, payload bytes, producer sequence metadata, optional idempotency key, optional schema reference, and causal metadata.

### 4.3. Subscription
Central delivery abstraction identifying source journal, mode, filter, ordering requirement, durability, retry and lease policies, concurrency settings, and replay behavior.

### 4.4. Subject / Logical Substream
Optional logical substream within a Journal representing an independently ordered entity/aggregate (e.g., `orders.1234`). Each subject has a monotonically increasing revision.

### 4.5. Optimistic Concurrency Control
Publish requests may specify `expected_revision` (`Any`, `Exact(u64)`, `NoSubject`). Commit succeeds only when the current subject revision matches the expected revision.

---

## 5. Subscription and Ordering Semantics

- **Stream Mode**: Linear cursor, commit, seek, replay.
- **Work Mode**: Leases with generation fencing. Transitions:
  ```
  READY -> LEASED -> ACKED (Complete)
             |-> NACK / EXPIRE -> RETRY_WAIT -> READY
             `-> max attempts -> DEAD_LETTER
  ```
- **Broadcast Mode**: Independent cursor observation for each subscriber without payload duplication.
- **Keyed Mode**: Single active logical owner per key with strict sequential delivery for that key while distinct keys execute concurrently.
- **Ordering Modes**: `none`, `key`, `journal`, and future `causal`.

---

## 6. Native Protocol Architecture (NMP)

Canonical binary multiplexed protocol over TCP (first) and QUIC (evaluated).
- **Demand-Window Flow Control**: Consumer grants bounded credits (`max_records`, `max_bytes`, `deadline`, `continuous`). Replenished via `WINDOW_UPDATE`.
- **Key Opcode Families**: Session, Metadata, Publish, Subscribe, Demand, Work, Stream, Replay, Atomic operations, Errors.

---

## 7. Storage Architecture

Three durability candidates evaluated in P0:
- **Candidate A (Local-First)**: Append batch to WAL, fsync/quorum, local segments, optional tiering.
- **Candidate B (Hybrid WAL + Object)**: Low-latency WAL/cache layer + background immutable segments to S3/object store (Default hypothesis).
- **Candidate C (Object-Primary)**: Durably commit to object storage before ACK.

---

## 8. Sharding and Placement Architecture

- No user-selected partition count.
- Virtual shards (65,536 or 262,144) mapped to Storage Groups via rendezvous hashing.
- Storage Group is the unit of physical replication, placement, and rebalancing.

---

## 9. Execution, Leases, Retries, and Idempotency

- **Lease Fencing**: Each redelivery increments `generation`; stale generation ACKs are rejected deterministically.
- **Timing Engine**: Hierarchical timer wheel for near-term retries and persistent index for long-duration delays.
- **Idempotency**: Transport deduplication via `(producer_id, epoch, sequence)` plus business deduplication via application `idempotency_key`.

---

## 10. Mixed-Workload Resource Isolation

Unified workloads require an internal resource scheduler allocating CPU, I/O, network, and memory across priority classes:
1. `critical` (commit path, consensus heartbeats)
2. `interactive` (work acquisition, live delivery)
3. `normal` (stream consumption, normal replay)
4. `background` (compaction, cold object upload)
5. `recovery` (snapshot restore, replica catch-up)

---

## 11. Embedded, Standalone, and Clustered Runtimes

- **Embedded**: Crate-level in-process engine.
- **Standalone**: Single-process server daemon with NMP/TCP and local storage.
- **Clustered**: Multi-node consensus, replication, and storage group rebalancing.

---

## 12. Security Architecture

- TLS via `rustls`, mTLS for cluster and client identity.
- SASL/OAuth2/JWT authentication.
- Journal and Subscription RBAC/ACL.
- Strict defensive parsing with length/count bounds before allocation.
- `#![forbid(unsafe_code)]` in all core crates.

---

## 13. Observability and Diagnostics

Authoritative metrics (OpenMetrics/Prometheus), tracing (OpenTelemetry), and state inspection tooling.

---

## 14. SDK and Compatibility Strategy

- Native SDKs (Rust, Go, TypeScript, Java, Python, .NET).
- Kafka and AMQP adapters operating as protocol translation layers without distorting core engine internals.

---

## 15. Testing and Verification Strategy

- Deterministic property tests (`proptest`).
- Model-style concurrency testing (`loom`).
- Continuous fuzzing of NMP frame decoder and batch parser (`cargo-fuzz`).
- Crash-injection test suite (kill -9, truncated WAL, disk full).

---

## 16. Benchmark Program

Tournaments for Sharding, Storage, Transport, Delivery, Batch format, and Mixed-Workload Isolation.

---

## 17. Delivery Roadmap P0–v1

| Phase | Description |
|---|---|
| **P0** | Architecture Validation: Semantic prototype, tournaments, RecordBatch v0, NMP Draft 0, ADRs. |
| **P1** | Single-Node Production Foundation: Chosen storage/sharding, durable WAL, NMP/TCP, CLI. |
| **P2** | Reliability & Recovery: Producer/business idempotency, timer wheels, crash recovery, DLQ. |
| **P3** | Advanced Messaging: Replay branches, atomic local ops, causal metadata, work scheduler. |
| **P4** | Object Storage & Durability Classes: S3 tiering, storage class profiles, cache/prefetch. |
| **P5** | Resource Isolation: Multi-class resource budget scheduler. |
| **P6** | Security: TLS, mTLS, JWT/OIDC, ACL/RBAC, audit logging. |
| **P7** | Cluster Metadata: Metadata consensus, placement epochs, routing. |
| **P8** | Replication & Failover: Storage-group replication, leader election, catch-up. |
| **P9** | Rebalancing: Node drain/expansion, storage group migration. |
| **P10**| QUIC Productionization: QUIC transport promote if benchmark justified. |
| **P11**| Compatibility: Kafka migration adapter, AMQP adapter. |
| **P12**| SDK Ecosystem & v1 Hardening: Multi-language SDKs, runbooks, v1 GA. |

---

## 18. Repository and Module Structure

```
crates/
  types/              # Strongly-typed UUIDv7 IDs
  record/             # Record and RecordBatch models
  journal/            # Journal engine with OCC
  subscription/       # Stream, Work, Keyed, Broadcast engines
  storage-core/       # Abstract storage traits & common engine
  sharding/           # Virtual sharding & placement
  protocol-core/      # NMP frame codecs & batch parser
  bench/              # Benchmark harness & tournament runners
  server/             # Standalone & clustered broker daemon
  cli/                # Operator command-line interface
```

---

## 19. CI/CD and Release Engineering

Multi-platform builds (Linux x86_64/arm64, macOS arm64, Windows x86_64), minimal non-root OCI images, SBOM generation, signed artifacts, nightly regression and fuzz runs.

---

## 20. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Scope creep into workflow engine | High | Enforce non-goals; keep execution state messaging-oriented. |
| Virtual sharding overhead | High | Benchmark gate; fall back to dynamic coarse partitions if penalty > 10%. |
| Mixed-workload p99 latency degradation | High | Strict internal resource scheduler and priority budgets. |
| Protocol complexity | Medium | Clean NMP Draft 0 specification and capability negotiation. |

---

## 21. v1 Acceptance Criteria

1. Stream, work, broadcast, and keyed subscriptions over unified immutable record store.
2. Verified durability classes with zero acknowledged loss under crash testing.
3. Cluster replication and transparent rebalancing.
4. NMP protocol with demand-window flow control.
5. Production telemetry, diagnostics, and CLI tooling.
