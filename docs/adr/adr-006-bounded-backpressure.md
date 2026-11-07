# ADR-006: Bounded Backpressure Across All Internal Pipelines

## Status
Accepted

## Context
Under high ingest rates or during slow-consumer/slow-disk events, brokers frequently experience unbounded queue growth, memory fragmentation, and eventual Out-Of-Memory (OOM) crashes.

## Decision
All internal buffers, channels, queues, timer pools, demand windows, and staging areas are strictly bounded. Backpressure must propagate up to the network layer:
1. TCP/QUIC ingress is throttled when WAL staging or replication falls behind.
2. Delivery buffers are bounded by consumer-authorized credit windows.
3. Memory allocations for parsing frames and batches have strict advertised limits (`max_frame_bytes`, `max_batch_records`).

## Consequences
- **Positive**: Complete OOM immunity under sustained slow consumer and slow disk scenarios; predictable memory consumption.
- **Negative**: Overloaded producers will experience backpressure or explicit `SERVER_BUSY` errors during overload events.
