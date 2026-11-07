# ADR-014: Atomic Multi-Subject Appends Within Single Commit Domain

## Status
Proposed (P3)

## Context
Complex event-sourced domain models often need to commit updates across two related aggregates (e.g., updating an `Order` subject and a `CustomerBalance` subject) atomically. Implementing distributed two-phase commit (2PC) across arbitrary network nodes adds high operational complexity and latency.

## Decision
Atomic multi-subject appends will be supported in NMP (`BEGIN_OPERATION`, `APPEND`, `COMMIT_OPERATION`), but **strictly constrained to subjects within the same commit/ownership domain (Storage Group)**. Cross-storage-group distributed transactions remain explicitly deferred.

## Consequences
- **Positive**: High throughput and atomic guarantees for collocated aggregates without 2PC overhead.
- **Negative**: Clients attempting cross-domain atomic operations receive an explicit `UNSUPPORTED_SCOPE` error.
