# ADR-003: Subscription-Defined Consumption Semantics

## Status
Accepted

## Context
When designing a unified messaging engine, systems historically make an architectural bet: either build a streaming log and layer work-queue emulations on top (e.g., Kafka Share Groups), or build a queue engine and attempt to add stream offsets (e.g., RabbitMQ Streams).

## Decision
Subscriptions are the central delivery abstraction. In StrataMQ, consumption semantics are defined by the `Subscription` mode rather than the underlying `Journal`:
- **Stream**: Sequential cursor tracking, commits, and seek replays.
- **Work**: Competing consumers with generation-fenced leases, retries, and DLQ.
- **Broadcast**: Independent stream cursors for pub/sub fanout.
- **Keyed**: Single active worker per partition key with in-order processing.
- **Ephemeral**: Short-lived sessions without durable cursor checkpoints.

## Consequences
- **Positive**: Eliminates separate broker infrastructure for streams vs. queues. Applications consume identical events as analytics streams or background jobs.
- **Negative**: The subscription engine must manage diverse state machines and coordinate leasing vs. streaming read paths over the shared storage layer.
