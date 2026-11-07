# StrataMQ

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust Edition](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](rust-toolchain.toml)
[![Safety](https://img.shields.io/badge/unsafe-forbidden-success.svg)](crates/)

**StrataMQ** is a unified, high-performance distributed message broker and event-streaming engine engineered in Rust. It eliminates the dual-system operational overhead of running separate streaming platforms (e.g. Kafka) and task queues (e.g. RabbitMQ) by decoupling **immutable log storage** from **multi-modal subscription state**.

---

## Core Differentiators

- **Dual-Plane Architecture**:
  - **Record Plane**: High-throughput, append-only log batches with per-subject monotonic revisions and tiered storage.
  - **State Plane**: Subscription-specific execution state machines managing cursors, generation-fenced leases, retries, and dead-letter queues over shared records without payload duplication.
- **Optimistic Concurrency Control (OCC)**: Fine-grained aggregate consistency via `ExpectedRevision` (`Any`, `Exact(u64)`, `NoSubject`), enabling native event-sourcing without distributed locks.
- **Multi-Modal Subscriptions**:
  - **Stream**: Sequential cursor tracking, commits, rollbacks, and historical log replay.
  - **Work Queue**: Competing consumers with generation-fenced leases, heartbeat extensions, configurable retry schedules, and non-duplicating DLQ.
  - **Keyed Work Queue**: Strictly ordered, serial delivery per key with concurrent execution across distinct keys.
  - **Broadcast**: Independent pub/sub fanout cursors over shared records.
- **Native Messaging Protocol (NMP)**: Binary, multiplexed protocol driven by receiver-authorized **Demand Windows** (`max_records`, `max_bytes`, `continuous`), unifying push and pull semantics under bounded backpressure.
- **Virtual Sharding**: Application-visible partition counts are eliminated; keys hash into a stable virtual shard namespace mapped to physical storage groups.
- **Strict Memory Safety**: Core domain, journal, and subscription crates enforce `#![forbid(unsafe_code)]`.
- **K-Sortable UUIDv7 Domain Types**: Strongly-typed newtype wrappers (`RecordId`, `JournalId`, `SubscriptionId`, `ConsumerId`, `ProducerId`, `LeaseId`).

---

## Workspace Architecture

StrataMQ is structured as a decoupled Cargo workspace:

```
stratamq/
├── crates/
│   ├── types/           # Strongly-typed domain UUIDv7 identifiers
│   ├── record/          # Immutable Record, ProposedRecord, and RecordBatch models
│   ├── journal/         # Append log with subject-scoped monotonic revisions & OCC
│   ├── subscription/    # Stream, Work, Keyed, and Broadcast state engines
│   ├── storage-core/    # Storage engine abstractions, WAL, and S3 tiering (P0 Tournament)
│   ├── sharding/        # Virtual sharding, placement, and rendezvous hashing (P0 Tournament)
│   ├── protocol-core/   # Native Messaging Protocol (NMP Draft 0) codecs & frames
│   ├── bench/           # Benchmark scenarios, tournament harnesses, and HDR latency samplers
│   ├── server/          # Broker daemon runtime (TCP/QUIC)
│   └── cli/             # Operator CLI for inspection and management
└── docs/
    ├── architecture/    # Architecture Charter, Implementation Plan, NMP Spec, System Overview
    ├── adr/             # Architecture Decision Register (Foundational, P0, Planned)
    ├── DEVELOPMENT.md   # Developer standards, quality assurance, and verification guide
    └── roadmap.md       # Implementation status across phases P0 to v1
```

### Crate Overview

| Crate | Package Name | Status | Description |
|---|---|---|---|
| [`crates/types`](crates/types) | `strata-types` | Active | Strongly-typed domain IDs generated via macro (`RecordId`, `JournalId`, etc.) |
| [`crates/record`](crates/record) | `strata-record` | Active | `Record` data model, `Subject` wrapper, and builder-style `ProposedRecord` |
| [`crates/journal`](crates/journal) | `strata-journal` | Active | In-memory append journal with `ExpectedRevision` OCC validation |
| [`crates/subscription`](crates/subscription) | `strata-subscription` | Active | Subscription state machines: `StreamState`, `WorkState`, `KeyedWorkState` |
| [`crates/protocol-core`](crates/protocol-core) | `protocol-core` | In Progress | NMP Draft 0 binary framing, demand-window accounting, and batch codecs |
| [`crates/storage-core`](crates/storage-core) | `storage-core` | Planned | Storage traits, WAL append, segment compaction, and object tiering |
| [`crates/sharding`](crates/sharding) | `sharding` | Planned | Virtual shard rendezvous placement and dynamic rebalancing |
| [`crates/bench`](crates/bench) | `bench` | Planned | Architecture tournament harnesses and failure-injection tests |
| [`crates/server`](crates/server) | `server` | Planned | Standalone broker daemon and network listeners |
| [`crates/cli`](crates/cli) | `cli` | Planned | Operator CLI for inspection and cluster management |

---

## Documentation Index

Comprehensive engineering specifications and design records are located in [`docs/`](docs/):

- **[Product & Architecture Charter](docs/architecture/charter.md)**: Foundational thesis, market positioning, architectural invariants, non-goals, and quality attributes.
- **[Implementation Plan](docs/architecture/implementation-plan.md)**: Comprehensive roadmap from P0 research to v1 GA, subsystem specifications, operational semantics, and risk analysis.
- **[P0 Technical Specification](docs/architecture/p0-spec.md)**: Research tournament designs for sharding, storage, transport, delivery, and mixed-workload isolation.
- **[Native Messaging Protocol (NMP) — Draft 0](docs/architecture/nmp-draft-0.md)**: Canonical wire protocol specification, binary framing, opcode registry, and demand windows.
- **[Architecture Decision Register (ADR)](docs/adr/README.md)**: Authoritative index of foundational, experimental (P0), and planned architectural decisions.
- **[System Architecture Overview](docs/architecture/overview.md)**: Detailed technical dive into the dual-plane model, OCC sequence, and consumption engines.
- **[Development Guide](docs/DEVELOPMENT.md)**: Coding conventions, property testing guidelines, fuzzing, and verification standards.
- **[Delivery Roadmap & Status](docs/roadmap.md)**: Live progress tracking and checklist for P0 exit and P1 transition.
- **[Dual-Workstream Execution Plan](docs/roadmap.md#2-phase-p0-milestone-architecture-validation--dual-workstream-plan)**: Parallel work breakdown, shared integration contracts, ownership matrix, and task tracking.

---

## Getting Started

### Prerequisites

- **Rust**: Toolchain supporting **Rust 2024 Edition** (channel configured in [`rust-toolchain.toml`](rust-toolchain.toml)).
- Cargo components: `clippy` and `rustfmt`.

Verify your installation:
```bash
rustc --version
cargo --version
```

### Building the Project

Compile all workspace targets:
```bash
cargo build --workspace
```

---

## Testing & Quality Assurance

StrataMQ enforces a zero-warning policy on Clippy and requires all tests to pass.

### Run Lint Checks

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

### Run Tests

```bash
cargo test --workspace
```

### Code Formatting

```bash
cargo fmt --all -- --check
```

---

## Usage Examples

### 1. Appending Records with Subject Revision Checks

```rust
use strata_journal::{ExpectedRevision, Journal};
use strata_record::{ProposedRecord, Subject};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut journal = Journal::new();
    let orders_subject = Subject::new("orders.us-east");

    // Append initial record expecting no prior revisions
    let proposed = ProposedRecord::new("order-1001-created")
        .with_subject(orders_subject.clone())
        .with_header("content-type", "application/json");

    let first_append = journal.append(proposed, ExpectedRevision::NoSubject)?;
    println!(
        "Appended position: {}, revision: {:?}",
        first_append.position, first_append.revision // Revision is Some(0)
    );

    // Append subsequent update using optimistic locking (expecting revision 0)
    let update = ProposedRecord::new("order-1001-paid")
        .with_subject(orders_subject.clone());

    let second_append = journal.append(update, ExpectedRevision::Exact(0))?;
    println!("Updated revision: {:?}", second_append.revision); // Revision is Some(1)

    Ok(())
}
```

### 2. Stream Subscriptions (Cursor & Replay)

```rust
use strata_journal::{ExpectedRevision, Journal};
use strata_record::ProposedRecord;
use strata_subscription::StreamState;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut journal = Journal::new();
    journal.append(ProposedRecord::new("event-1"), ExpectedRevision::Any)?;
    journal.append(ProposedRecord::new("event-2"), ExpectedRevision::Any)?;

    let mut stream = StreamState::new();

    // Fetch batch of up to 10 records
    let batch = stream.fetch(&journal, 10);
    assert_eq!(batch.len(), 2);

    // Commit consumer position
    stream.commit();
    assert_eq!(stream.committed_position(), 2);

    // Seek back to replay from position 0
    stream.seek(0);
    let replayed = stream.fetch(&journal, 1);
    assert_eq!(replayed.len(), 1);

    Ok(())
}
```

### 3. Competing Worker Queue with Generation-Fenced Leases

```rust
use strata_subscription::{Lease, RetryDelay, RetryPolicy, WorkState};
use strata_types::{ConsumerId, RecordId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = RetryPolicy {
        max_attempts: 3,
        delay: RetryDelay::Fixed { delay: 1000 },
    };
    let mut work_state = WorkState::new(policy);

    let record_id = RecordId::new();
    work_state.register(record_id);

    let worker = ConsumerId::new();
    let current_time_ms = 1_000;
    let lease_duration_ms = 5_000;

    // Acquire work item
    let leases: Vec<Lease> = work_state.acquire(worker, current_time_ms, lease_duration_ms, 1);
    assert_eq!(leases.len(), 1);
    let lease = leases[0];

    // Worker extends lease during long-running processing
    let extended_lease = work_state.extend_lease(lease, 2_000, 3_000)?;

    // Acknowledge upon successful completion
    work_state.ack(extended_lease, 3_000)?;

    Ok(())
}
```

---

## Authors & License

Developed by Haris and Shaunak.

Licensed under the [Apache License, Version 2.0](LICENSE).
