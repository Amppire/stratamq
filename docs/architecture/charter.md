# Product & Architecture Charter

**First-Principles Event Streaming and Reliable Messaging**  
*Engineering Source of Truth — Baseline: November 2026*

---

## Contents
1. [Purpose and Product Thesis](#1-purpose-and-product-thesis)
2. [Market Problem and Positioning](#2-market-problem-and-positioning)
3. [Architectural Invariants](#3-architectural-invariants)
4. [Public Conceptual Model](#4-public-conceptual-model)
   - 4.1 [Journal, Subject, Record, and Subscription](#41-journal-subject-record-and-subscription)
   - 4.2 [Event-Database Capabilities Without Changing Product Boundary](#42-event-database-capabilities-without-changing-the-product-boundary)
5. [Product Boundaries and Non-Goals](#5-product-boundaries-and-non-goals)
6. [Deployment Philosophy](#6-deployment-philosophy)
7. [Quality Attributes](#7-quality-attributes)
8. [Compatibility Philosophy](#8-compatibility-philosophy)
9. [Governance and Decision Process](#9-governance-and-decision-process)
10. [Success Criteria](#10-success-criteria)
- [Appendix A. Product Personas and Representative Workloads](#appendix-a-product-personas-and-representative-workloads)
- [Appendix B. Principles for Evaluating Innovation](#appendix-b-principles-for-evaluating-innovation)
- [Appendix C. Event-Sourcing Consistency Principles](#appendix-c-event-sourcing-consistency-principles)

---

## 1. Purpose and Product Thesis

This charter defines the stable product intent and architectural principles for the **StrataMQ** Unified Durable Messaging Platform. It exists so that benchmark-driven implementation decisions can evolve without changing the identity of the project.

The project is not “Kafka rewritten in Rust,” “RabbitMQ with streams,” or a feature checklist combining existing brokers. Existing systems are benchmark targets and sources of proven ideas, not architectural templates.

The product thesis is to build a distributed durable messaging platform in which application-visible semantics—**event streaming, reliable work delivery, broadcast delivery, keyed ordered processing, scheduling, replay, and retention**—are expressed over a **common immutable record substrate** while physical placement, storage, replication, consumer parallelism, and delivery mechanics remain independently optimizable.

---

## 2. Market Problem and Positioning

The messaging market has converged from both directions. Stream-oriented systems increasingly add queue-style work consumption, while queue-oriented systems increasingly add retained logs and replay. Cloud-native streaming systems are simultaneously separating compute from storage and moving long-term durability into object storage. These trends show demand for fewer specialized messaging systems, but current products remain constrained by historical assumptions about partitions, queues, local broker disks, or one dominant consumption model.

The project targets teams that otherwise operate multiple messaging layers: a streaming platform for durable event histories, a work queue for background jobs, ad hoc retry/idempotency infrastructure, and separate replay/diagnostic tooling. The platform aims to collapse a meaningful subset of that infrastructure without becoming a workflow engine.

| Problem | Observed Industry Response | Project Opportunity |
|---|---|---|
| Streams and queues require separate infrastructure | Kafka Share Groups; RabbitMQ Streams; JetStream work retention | Native multi-mode subscriptions over one record store |
| Physical partitions leak into application scaling | Manual partition sizing and rebalancing remain common | Separate logical ordering/sharding from physical storage groups |
| Long retention is expensive on broker disks | Tiered storage; object-primary streaming | Make durability/storage policy an independent dimension |
| Push/pull choices leak transport policy into applications | Kafka long-poll, NATS pull batches, Rabbit stream credits | Receiver-authorized demand windows |
| Replay and incident recovery are operationally awkward | Offset resets, DLQs, republishing, bespoke tooling | Branchable and selective replay as native state |

---

## 3. Architectural Invariants

1. **Committed record data is immutable.** Mutable execution state is maintained separately.
2. **Journal and Subscription are logical application abstractions;** storage groups, replication groups, segment files, and virtual shards are internal implementation concepts.
3. **Stream, work, broadcast, keyed, and ephemeral delivery modes operate over the same record model** unless P0 disproves the feasibility of this invariant.
4. **The native protocol (NMP) is canonical.** Compatibility protocols may map to it but cannot dictate the core model.
5. **All internal queues, buffers, windows, and work schedulers are bounded and participate in backpressure.**
6. **Correctness and explicit durability guarantees take precedence over headline throughput.**
7. **Mixed workloads must be isolated** so replay, compaction, recovery, or tier uploads cannot unpredictably starve interactive delivery.
8. **Single-node operation remains a first-class deployment** after clustering is introduced.
9. **No mandatory external database, coordination service, or JVM runtime** is required for the core broker.
10. **Exactly-once claims are scoped precisely;** the broker does not claim exactly-once effects in arbitrary external systems.

---

## 4. Public Conceptual Model

The user-facing conceptual model intentionally contains few objects. A **Journal** is a logical durable namespace; a **Record** is an immutable unit published into a journal; a **Subscription** defines how matching records are observed or executed; **Delivery** represents stream batches or leased work; **Replay** is isolated historical consumption state.

```
Journal -> immutable Records -> Subscription
                                  |-> stream cursor
                                  |-> work lease/ACK state
                                  |-> broadcast cursor
                                  |-> keyed ownership
                                  `-> replay branch
```

### 4.1. Journal, Subject, Record, and Subscription
- The public semantic hierarchy is **Journal → optional Subject → immutable Record**.
- A **Journal** is the durable namespace and policy boundary.
- A **Subject** is a logical substream, aggregate, entity, or independently ordered record sequence within that Journal (e.g., `orders.1234`).
- Subjects are **semantic, not physical**. A subject does not imply a dedicated partition, storage file, consensus group, or broker. The placement engine may map many subjects through virtual shards onto shared storage/replication groups while preserving ordering and revision guarantees.
- A subject maintains a **monotonically increasing revision**. Publication may optionally include an `expected_revision`. A mismatched expected revision is rejected atomically, providing optimistic concurrency control (OCC) without introducing locks into the application model.
- Records may also be published **without a subject** when the workload does not need entity-level ordering or optimistic concurrency.

### 4.2. Event-Database Capabilities Without Changing the Product Boundary
The platform may later provide declared secondary indexes, selective historical query, derived/materialized subscriptions, and connector workers. These capabilities exist to make retained records easier to use; they do not change the product into a general-purpose OLTP database, workflow engine, or arbitrary compute runtime.

---

## 5. Product Boundaries and Non-Goals

- **Do not become a workflow/DAG orchestration engine in v1.**
- **Do not require application code to run inside the broker runtime.**
- **Do not expose a general-purpose database query layer over payload bodies.**
- **Do not promise perfect Kafka or AMQP compatibility in early releases.**
- **Do not implement multi-region active-active consensus before the single-region architecture is proven.**
- **Do not use cross-storage-group distributed transactions as a prerequisite for the initial product.**
- **Do not optimize around unvalidated technologies such as io_uring, custom allocators, or kernel bypass before profiling.**

---

## 6. Deployment Philosophy

The same semantic engine must progress through embedded, standalone, and clustered deployment modes.
- **Embedded library**: for edge software, appliances, tests, and local applications.
- **Standalone mode**: production-capable single server for small to medium installations without external dependencies.
- **Clustered mode**: adds placement, replication, failover, and rebalancing without changing core application semantics.

```
embedded library -> single process/server -> multi-node cluster
       (same journal / record / subscription semantics)
```

---

## 7. Quality Attributes

| Attribute | Requirement |
|---|---|
| **Correctness** | No silent acknowledged-record loss under documented durability settings; stale leases cannot acknowledge newer deliveries. |
| **Latency** | Interactive work delivery must remain predictable under mixed load; p99 and p99.9 are first-class metrics. |
| **Throughput** | Batch-oriented paths should reach competitive streaming and queue throughput without relying on unsafe durability comparisons. |
| **Resource efficiency** | Idle footprint and per-connection/per-subscription overhead should remain suitable for lean deployments. |
| **Operability** | Topology and storage placement are system-managed by default; diagnostics explain blocked delivery and recovery state. |
| **Evolvability** | Protocol capability negotiation and modular crates allow features to evolve without rewriting the platform. |
| **Security** | Memory safety, bounded allocations, authentication negotiation, TLS, ACL/RBAC, fuzzing, and supply-chain checks are built into the roadmap. |

---

## 8. Compatibility Philosophy

Kafka and AMQP endpoints are **adoption tools, not internal APIs**. The compatibility layer should target migration-oriented support for common producer, consumer, acknowledgment, and group patterns. Native features such as replay branches, demand windows, keyed execution ownership, storage classes, and adaptive hints remain available through NMP even when no equivalent exists in compatibility protocols.

---

## 9. Governance and Decision Process

Architecture decisions are recorded in ADRs. P0 decisions require benchmark evidence, documented test conditions, and explicit pass/fail criteria. Novelty for novelty's sake is not sufficient justification; simpler alternatives win when they provide comparable value.

1. State the decision and competing alternatives.
2. Define correctness constraints before performance measurements.
3. Create a reproducible benchmark or failure test.
4. Record measured results, operational complexity, and implementation cost.
5. Choose the simplest approach that meets the product thesis and quality targets.
6. Record reversibility and migration strategy.

---

## 10. Success Criteria

1. One record store efficiently supports stream and work semantics.
2. Application-visible scaling does not require preselecting physical partition counts.
3. Low-latency queue workloads can coexist with replay and object-storage tiering.
4. Native protocol flow control prevents unbounded buffering and supports both fetch-style and continuous delivery APIs.
5. Replay, idempotency, diagnostics, and keyed ordering reduce application-side reliability plumbing.
6. Single-node deployment remains simple, while clustering adds capacity and availability rather than an entirely different operational model.

---

## Appendix A. Product Personas and Representative Workloads

| Persona | Representative Need | Why the Platform Should Fit |
|---|---|---|
| **SaaS Platform Team** | Events for analytics plus background work/retries | One record substrate and separate subscriptions reduce dual-broker infrastructure. |
| **Data/Event Platform Team** | High-throughput retained events and backfills | Stream mode, branchable replay, tiered/object storage. |
| **Application Platform Team** | Reliable async APIs/jobs with simple operations | Work leases, idempotency, retries, single binary/cluster growth. |
| **Edge/Self-Hosted Operator** | Durable messaging without a heavy control plane | Embedded/standalone mode and no mandatory external DB/coordinator. |
| **Infrastructure Engineer** | Predictable mixed workload and transparent scaling | Logical topology abstraction, resource isolation, benchmarkable semantics. |

---

## Appendix B. Principles for Evaluating Innovation

- Prefer compositional innovation over novelty for novelty’s sake.
- Any abstraction that hides topology must still expose enough diagnostics for operators to understand ownership, movement, latency, and failures.
- Automatic scaling/placement features must have deterministic safety boundaries and manual override/inspection paths.
- New protocol features must solve a semantic or efficiency problem that cannot be expressed cleanly through existing compatibility layers.
- Simpler implementations win when they meet the same product objective with fewer correctness and operational risks.

---

## Appendix C. Event-Sourcing Consistency Principles

Entity-oriented applications need stronger semantics than idempotent publication alone. The platform therefore treats per-subject revision and expected-revision checks as foundational consistency primitives.
1. Subject revisions are monotonically increasing within a subject.
2. Expected revision is checked at commit time in the same ownership/commit domain as the append.
3. A revision conflict is a deterministic application-visible result, not an implicit retry.
4. Subject ordering remains independent of physical shard placement.
5. Atomic multi-subject appends are a later capability and are initially constrained to one commit domain.
