# ADR-010: Per-Subject Monotonic Revision & Optimistic Concurrency

## Status
Accepted

## Context
In event-sourcing and message streaming, multiple producers frequently attempt to mutate or append state concerning the same logical entity or domain aggregate (such as an order, user account, or sensor stream). Without concurrency validation, concurrent writers risk producing conflicting event logs or overwriting state changes without knowledge of prior transactions.

Pessimistic locking across distributed nodes creates high contention, latency spikes, and deadlocks.

## Decision
We implement **Optimistic Concurrency Control (OCC)** scoped at the `Subject` level within `strata-journal`:

1. Every `Record` can optionally be tagged with a `Subject`.
2. When tagged, the Journal assigns a sequential, monotonic `revision` number scoped strictly to that `Subject` (beginning at `0`).
3. Producers append records using an `ExpectedRevision` enum:
   - `ExpectedRevision::NoSubject`: Asserts that no record has ever been appended for this subject (ideal for aggregate initialization events).
   - `ExpectedRevision::Exact(u64)`: Asserts the latest subject revision matches the specified value.
   - `ExpectedRevision::Any`: Bypasses optimistic locking and accepts writes unconditionally.
4. If the actual revision does not match the expected revision, the journal immediately rejects the append with `AppendError::RevisionMismatch`.

## Consequences
- **Positive**:
  - Lock-free atomic conflict detection without holding exclusive locks during write preparation.
  - Distinct subjects append concurrently without cross-subject blocking or contention.
  - Native foundation for CQRS and event-sourcing aggregate consistency.
- **Negative**:
  - High-contention subjects encounter optimistic write rejections, requiring retry handling with backoff in client SDKs.
  - The journal and storage layers must index subject revisions efficiently in memory and on disk.
