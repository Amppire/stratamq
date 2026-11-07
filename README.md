# StrataMQ

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust Edition](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](rust-toolchain.toml)
[![Safety](https://img.shields.io/badge/unsafe-forbidden-success.svg)](crates/)

**StrataMQ** is a high-performance, modular message queuing and event-streaming engine engineered in Rust. It combines the predictable sequential access of append-only logs with rich subscription semantics—supporting streaming event logs, competing worker queues with fenced leases, keyed serial dispatch, and broadcast fanout within a unified architecture.

---

## Key Features

- **Strict Memory Safety**: Core domain and subscription logic enforce `#![forbid(unsafe_code)]`.
- **Append-Only Journaling**: Fast sequential appends with global log ordering and per-subject revision tracking.
- **Optimistic Concurrency Control (OCC)**: Conditional writes using `ExpectedRevision` (`Any`, `Exact(u64)`, `NoSubject`) to eliminate write anomalies and race conditions.
- **Multi-Modal Subscriptions**:
  - **Stream**: Linear cursor-based consumption, seek operations, commit markers, and historical log replay.
  - **Work Queue**: Competing consumer model with generation-fenced leases, heartbeat extensions, configurable retries (Immediate, Fixed, Exponential Backoff), and Dead-Letter Queue (DLQ) support.
  - **Keyed Work Queue**: Guaranteed per-key message sequencing that allows concurrent execution across distinct keys without head-of-line blocking.
  - **Broadcast**: Independent stream cursors for pub/sub fanout.
- **K-Sortable UUIDv7 Identifiers**: Strongly-typed newtypes (`RecordId`, `JournalId`, `SubscriptionId`, `ConsumerId`, `ProducerId`, `LeaseId`) providing time-ordered locality and zero collision probability.

---

## Workspace Architecture

StrataMQ is organized as a Cargo workspace with decoupled, single-responsibility crates:

```
stratamq/
├── crates/
│   ├── types/           # Core domain newtypes and UUIDv7 identifiers
│   ├── record/          # Immutable Record and ProposedRecord data models
│   ├── journal/         # Append-only journal with subject-scoped revision checks
│   ├── subscription/    # Stream, Work, Keyed, and Broadcast consumption engines
│   ├── storage-core/    # Storage engine abstractions and persistence interfaces (WIP)
│   ├── sharding/        # Partitioning and distributed clustering coordinator (WIP)
│   ├── protocol-core/   # Binary wire protocol framing and codec (WIP)
│   ├── server/          # Broker runtime and service daemon (WIP)
│   ├── cli/             # Operator CLI for cluster and topic management (WIP)
│   └── bench/           # Benchmark scenarios and performance harnesses
└── docs/
    ├── architecture/    # Detailed system design documents
    └── adr/             # Architecture Decision Records
```

### Crate Overview

| Crate | Package Name | Status | Description |
|---|---|---|---|
| [`crates/types`](crates/types) | `strata-types` | Active | Strongly-typed domain IDs generated via macro (`RecordId`, `JournalId`, etc.) |
| [`crates/record`](crates/record) | `strata-record` | Active | `Record` data model, `Subject` wrapper, and builder-style `ProposedRecord` |
| [`crates/journal`](crates/journal) | `strata-journal` | Active | In-memory append journal with `ExpectedRevision` OCC validation |
| [`crates/subscription`](crates/subscription) | `strata-subscription` | Active | Subscription state machines: `StreamState`, `WorkState`, `KeyedWorkState` |
| [`crates/storage-core`](crates/storage-core) | `storage-core` | Planned | Disk persistence layer, segment files, WAL, and index structures |
| [`crates/sharding`](crates/sharding) | `sharding` | Planned | Partition distribution, consistent hashing, and cluster coordination |
| [`crates/protocol-core`](crates/protocol-core) | `protocol-core` | Planned | Network frame serialization, deserialization, and protocol codecs |
| [`crates/server`](crates/server) | `server` | Planned | Network listener, connection lifecycle, and request routing |
| [`crates/cli`](crates/cli) | `cli` | Planned | Management CLI for managing subscriptions, topics, and metrics |
| [`crates/bench`](crates/bench) | `bench` | Planned | End-to-end and micro-benchmarks for latency and throughput |

---

## Getting Started

### Prerequisites

- **Rust**: Rust toolchain supporting the **2024 Edition** (channel specified in [`rust-toolchain.toml`](rust-toolchain.toml)).
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

StrataMQ maintains strict code quality and linting standards. Before submitting changes, verify that the workspace passes both Clippy with warnings denied and the full test suite.

### Run Lint Checks

Ensure there are zero warnings across all workspace crates and targets:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```

### Run Tests

Execute unit and integration tests across the entire workspace:
```bash
cargo test --workspace
```

### Code Formatting

Check formatting compliance:
```bash
cargo fmt --all -- --check
```

Format code automatically:
```bash
cargo fmt --all
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

    // Append the initial record expecting no prior revisions
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

### 3. Competing Worker Queue with Leases

```rust
use strata_subscription::{FifoReadyQueue, Lease, RetryDelay, RetryPolicy, WorkState};
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

    // Worker extends lease during long processing
    let extended_lease = work_state.extend_lease(lease, 2_000, 3_000)?;

    // Acknowledge upon successful completion
    work_state.ack(extended_lease, 3_000)?;

    Ok(())
}
```

---

## Documentation

Comprehensive design guides and architecture documentation are located in [`docs/`](docs/):

- **[System Architecture](docs/architecture/overview.md)**: Deep dive into the layered architecture, storage model, concurrency patterns, and subscription state machines.
- **[Development Guide](docs/DEVELOPMENT.md)**: Developer standards, coding conventions, testing guidelines, and workflow practices.
- **Architecture Decision Records (ADRs)**:
  - [ADR 0001: UUIDv7 for Domain Entity Identifiers](docs/adr/0001-uuidv7-for-entity-identifiers.md)
  - [ADR 0002: Optimistic Concurrency with Subject Revisions](docs/adr/0002-optimistic-concurrency-and-subject-revisions.md)
  - [ADR 0003: Fenced Leases and Heartbeats for Work Queues](docs/adr/0003-fenced-leases-for-work-queues.md)

---

## Authors & License

Developed by Haris and Shaunak.

Licensed under the [Apache License, Version 2.0](LICENSE).
