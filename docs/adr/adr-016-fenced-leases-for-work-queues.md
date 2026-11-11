# ADR-016: Fenced Leases and Heartbeat Extensions for Work Queues

## Status
Accepted

## Context
When running competing workers consuming from a shared work queue (`WorkState`), consumers take exclusive temporary ownership of messages. In real-world distributed environments, worker failures, network partitions, long GC pauses, or crashed processes can cause workers to hang mid-execution.

If a hung worker awakens after its lease has expired and been reassigned to another consumer, any subsequent acknowledgment or state mutation could result in duplicate processing or silent data corruption (the classic "split-brain worker" problem).

## Decision
We implement **generation-fenced leases** with heartbeat extensions in `strata-subscription`:

1. **Leases (`Lease`)**:
   - Consumers acquire messages for a bounded duration (`expires_at = now.saturating_add(lease_duration)`).
   - Each lease is bound to a specific `ConsumerId`, `LeaseId`, and monotonically increasing `generation` counter.
2. **Generation Fencing**:
   - When a lease expires and another consumer acquires the record, the record's `generation` counter increments.
   - Any validation operation (`ack`, `nack`, `extend_lease`) from the stale consumer fails with `WorkError::StaleGeneration` or `WorkError::LeaseExpired`, preventing split-brain writes.
3. **Heartbeat Extensions (`extend_lease`)**:
   - Long-running workers can periodically extend their active lease before expiration without changing generations, provided their current lease remains valid.
   - The broker calculates the extension from its authoritative active lease state. Retried or reordered older receipts with the same lease ID and generation cannot shorten, resurrect, or move the current deadline into the past.
4. **Configurable Retry Policies (`RetryPolicy`)**:
   - Supports `Immediate`, `Fixed`, and `Exponential` delay schedules with a configurable `max_attempts` ceiling before automatically transitioning failed items to `WorkStatus::DeadLettered`.

## Consequences
- **Positive**:
  - Complete immunity to split-brain worker acknowledgments and race conditions.
  - Dynamic task duration support via heartbeat extensions (`extend_lease`).
  - Graceful recovery: unresponsive workers drop leases upon `expire_leases`, re-enqueuing items for healthy workers.
- **Negative**:
  - Long-running workers must run active background heartbeat renewal loops.
  - Time coordination relies on monotonic clock timestamps.
  - Process-local monotonic deadlines require an explicit reconstruction or conservative invalidation rule after restart; P0 freezes that semantic contract and P1/P2 implement durable recovery.
