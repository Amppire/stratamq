# ADR 0003: Fenced Leases and Heartbeats for Work Queues

## Status
Accepted

## Context
When running competing workers consuming from a shared work queue (`WorkState`), consumers take exclusive temporary ownership of messages. In real-world distributed environments, worker failures, network partitions, long GC pauses, or crashed processes can cause workers to hang mid-execution.

If a hung worker awakens after its lease has been reassigned to another consumer, any subsequent acknowledgment or state mutation could result in duplicate processing or silent data corruption (the classic "split-brain worker" problem).

## Decision
We implemented **generation-fenced leases** with heartbeat extensions in `strata-subscription`:

1. **Leases (`Lease`)**:
   - Consumers acquire messages for a bounded duration (`expires_at = now + lease_duration`).
   - Each lease is bound to a specific `ConsumerId`, `LeaseId`, and monotonically increasing `generation` counter.
2. **Generation Fencing**:
   - When a lease expires and another consumer acquires the record, the record's `generation` counter increments.
   - Any validation operation (`ack`, `nack`, `extend_lease`) from the stale consumer fails with `WorkError::StaleGeneration` or `WorkError::LeaseExpired`.
3. **Heartbeat Extensions (`extend_lease`)**:
   - Long-running workers can periodically extend their active lease before expiration without changing generations, provided their current lease remains valid.
4. **Configurable Retry Policies**:
   - Supports `Immediate`, `Fixed`, and `Exponential` delay schedules with a configurable `max_attempts` ceiling before automatically transitioning failed items to `WorkStatus::DeadLettered`.

## Consequences

### Positive
- **Split-Brain Immunity**: Stale consumers cannot complete or acknowledge work that has been reassigned.
- **Dynamic Task Length**: Long jobs can run safely by renewing leases through `extend_lease`.
- **Fault-Tolerant Retries**: Unresponsive or crashing workers naturally drop leases when `expire_leases` runs, making items available again to healthy workers.

### Negative
- Requires workers to have reasonably synchronized monotonic time (or server-driven time) to coordinate expiry accurately.
- Requires long-running workers to run periodic background heartbeat loops to extend leases.
