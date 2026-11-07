# Architecture Decision Register (ADR)

**StrataMQ Engineering Decision History and Evidence Requirements**  
*Architecture Baseline: November 2026*

---

## Overview

This register tracks all architectural decisions for StrataMQ to prevent research hypotheses from becoming accidental permanent architecture. Decisions follow an explicit lifecycle:
- **Accepted**: Verified foundational architecture, backed by proof of correctness or design requirements.
- **Experimental**: P0 tournament candidates requiring empirical benchmark/failure evidence before graduation.
- **Planned / Proposed**: Formulated semantic features planned for subsequent releases.
- **Deferred**: Explicitly post-v1 or deferred until prior milestones are proven.
- **Superseded**: Decisions replaced by later evidence-backed findings.

---

## Governance & Evaluation Process

1. State the decision and competing alternatives.
2. Define correctness constraints before performance measurements.
3. Create a reproducible benchmark or failure test.
4. Record measured results, operational complexity, and implementation cost.
5. Choose the simplest approach that meets the product thesis and quality targets.
6. Record reversibility and migration strategy.

---

## 1. Foundational Architecture Decisions

| ID | Title | Status | Scope |
|---|---|---|---|
| [ADR-001](adr-001-rust-for-core-broker.md) | Rust for Core Broker Engine | Accepted | Language & Systems |
| [ADR-002](adr-002-immutable-record-plane.md) | Immutable Record Plane & Separate State Plane | Accepted | Storage & Semantics |
| [ADR-003](adr-003-subscription-defined-semantics.md) | Subscription-Defined Consumption Semantics | Accepted | Consumption Modes |
| [ADR-004](adr-004-native-protocol-required.md) | Native Messaging Protocol (NMP) Canonical Wire Protocol | Accepted | Network & Wire |
| [ADR-005](adr-005-compatibility-as-boundary-adapters.md) | Compatibility Protocols as Boundary Adapters | Accepted | Interoperability |
| [ADR-006](adr-006-bounded-backpressure.md) | Bounded Backpressure Across All Internal Pipelines | Accepted | Resource Isolation |
| [ADR-007](adr-007-physical-topology-hidden.md) | Hide Physical Sharding Topology from Client API | Accepted | Sharding & Topology |
| [ADR-008](adr-008-mixed-workload-isolation.md) | Mixed-Workload Resource Isolation as First-Class Invariant | Accepted | Schedulers & SLO |
| [ADR-009](adr-009-optional-subject-logical-substreams.md) | Optional Subject / Logical Substream Abstraction | Accepted | Ingestion Semantics |
| [ADR-010](adr-010-per-subject-revision-and-occ.md) | Per-Subject Monotonic Revision & Optimistic Concurrency | Accepted | Concurrency Model |
| [ADR-015](adr-015-uuidv7-for-entity-identifiers.md) | UUIDv7 K-Sortable Identifiers for Domain Entities | Accepted | Identity Model |
| [ADR-016](adr-016-fenced-leases-for-work-queues.md) | Fenced Leases and Heartbeat Extensions for Work Queues | Accepted | Work Queues |

---

## 2. P0 Experimental Decisions (Pending Benchmark Tournaments)

| ID | Title | Status | Evidence Required |
|---|---|---|---|
| [ADR-P0-001](p0-0001-sharding-tournament.md) | Fixed/Dynamic Partitions vs. Virtual Sharding | Experimental | Sharding Tournament (`SHARD-U1`, `SHARD-R1`) |
| [ADR-P0-002](p0-0002-placement-tournament.md) | Rendezvous vs. Ring vs. Range Placement | Experimental | Placement & movement benchmarks |
| [ADR-P0-003](p0-0003-storage-tournament.md) | Local-First vs. Hybrid WAL/Object vs. Object-Primary | Experimental | Storage Tournament (`STORE-W1`, `STORE-S1`) |
| [ADR-P0-004](p0-0004-storage-classes.md) | Multi-Tier Storage Classes (low-latency, balanced, economical) | Experimental | Operational complexity & SLA profiles |
| [ADR-P0-005](p0-0005-transport-tournament.md) | NMP/TCP vs. NMP/QUIC Default Transport | Experimental | Transport Tournament (`NET-L1`, `NET-W1`) |
| [ADR-P0-006](p0-0006-demand-window-delivery.md) | Receiver-Authorized Demand-Window Delivery Model | Experimental | Delivery Tournament (`DEL-S1`) |
| [ADR-P0-007](p0-0007-shared-vs-normalized-batches.md) | Shared Wire/Storage Batch Format vs. Normalized Translation | Experimental | Codec & zero-copy benchmarks |
| [ADR-P0-008](p0-0008-batch-codec-layout.md) | Record Batch v0 Fixed-Width vs. Varint Layout | Experimental | Serialization CPU & allocation tests |
| [ADR-P0-009](p0-0009-resource-isolation-policy.md) | Mixed-Workload Resource Scheduler & Priority Budgets | Experimental | Mixed-workload tests (`MIX-1`, `MIX-2`) |
| [ADR-P0-010](p0-0010-keyed-failure-policy.md) | Keyed Ordering Guarantees & Key Failure Policy | Experimental | Ordering & fault-injection tests |

---

## 3. Planned Semantic & Ecosystem Decisions

| ID | Title | Status | Scope |
|---|---|---|---|
| [ADR-011](adr-011-declared-secondary-indexes.md) | Declared Secondary Indexes Over Metadata | Planned / Deferred | Post-P1 Search |
| [ADR-012](adr-012-derived-materialized-subscriptions.md) | Derived & Materialized Subscriptions (WASM isolation) | Planned / Deferred | Stream Transforms |
| [ADR-013](adr-013-connector-workers-outside-core.md) | Out-of-Process Connector Workers | Planned | Integrations |
| [ADR-014](adr-014-atomic-multi-subject-append.md) | Atomic Multi-Subject Appends Within Single Commit Domain | Proposed | Transactions |

---

## 4. Explicitly Deferred Decisions

- **Final Raft/Consensus Implementation**: Abstracted until single-node foundation is proven (P7/P8).
- **Multi-Region Active-Active Consensus**: Deferred until single-region clustering is fully validated.
- **Full Schema Registry**: Defer mandatory schema registries; retain lightweight optional schema references.
- **Cross-Storage-Group Distributed Transactions (2PC)**: Deferred to prevent early distributed transaction complexity.
- **Global Causal Ordering**: Deferred beyond v1 core ordering models.
- **io_uring / Kernel-Bypass / Custom Allocators**: Deferred until profiling proves I/O or allocator bottlenecks.
