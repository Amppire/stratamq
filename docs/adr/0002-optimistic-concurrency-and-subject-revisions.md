# ADR 0002: Optimistic Concurrency with Subject Revisions

## Status
Accepted

## Context
In event-sourcing and message streaming, multiple producers frequently attempt to mutate or append state concerning the same logical entity or domain aggregate (such as an order, user account, or sensor stream). Without concurrency validation, concurrent writers risk producing conflicting event logs or overwriting state changes without knowledge of prior transactions.

Pessimistic locking across distributed nodes creates high contention, latency spikes, and deadlocks.

## Decision
We implemented **Optimistic Concurrency Control (OCC)** scoped at the `Subject` level within `strata-journal`.

1. Every `Record` can optionally be tagged with a `Subject`.
2. When tagged, the Journal assigns a sequential, monotonic `revision` number scoped strictly to that `Subject` (beginning at 0).
3. Producers append records using an `ExpectedRevision` enum:
   - `ExpectedRevision::NoSubject`: Expects that no record has ever been appended for this subject (ideal for initialization events).
   - `ExpectedRevision::Exact(n)`: Expects the current latest revision for this subject to be exactly `n`.
   - `ExpectedRevision::Any`: Bypasses optimistic locking and accepts writes unconditionally.
4. If the actual revision does not match the expected revision, the journal immediately rejects the append with `AppendError::RevisionMismatch`.

## Consequences

### Positive
- **Lock-Free Concurrency**: Producers do not hold exclusive locks while preparing writes.
- **Atomic Conflict Detection**: Revisions are checked and updated atomically during the append operation.
- **Granular Scoping**: Revision tracking is isolated per subject rather than locking the entire journal or partition.
- **Support for Event-Driven Aggregates**: Enables out-of-the-box building blocks for CQRS / Event Sourcing patterns.

### Negative
- High-contention subjects may experience frequent optimistic rejection, requiring retry loops with exponential backoff on client producers.
