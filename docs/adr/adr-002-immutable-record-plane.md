# ADR-002: Immutable Record Plane & Separate State Plane

## Status
Accepted

## Context
Traditional brokers either treat messages as ephemeral items to be deleted upon acknowledgment (work queues like RabbitMQ), or as retained logs in which messages cannot be independently leased or acknowledged (event streams like Kafka). Supporting both patterns often results in dual-system infrastructure or clunky emulations that mutate payloads or duplicate data.

## Decision
We separate the platform into two distinct architectural planes:
1. **Record Plane**: Stores immutable published record batches (`Journal → Segments → Retention/Tiering`). Committed payloads and headers never mutate or get deleted on consumer acknowledgment.
2. **State Plane**: Stores subscription-specific execution state (`Subscription → Cursor | Lease | Retry | Key Ownership | Replay | Scheduler`).

## Consequences
- **Positive**: A single record can participate simultaneously in streaming consumption, competing worker queues, broadcast pub/sub, and historical replays without payload duplication.
- **Negative**: Storage reclamation cannot be driven simply by message ACK; retention policies (TTL, max size, or compact rules) manage Record Plane lifecycle independently of consumer acknowledgments.

Retention therefore requires an explicit safety contract: expired cursors fail with a deterministic earliest-available-position error, mutable state may not retain dangling references to reclaimed payloads, and tier movement preserves record identity and logical position. Replay-specific pinning and object-tier lifecycle remain later-phase implementations.
