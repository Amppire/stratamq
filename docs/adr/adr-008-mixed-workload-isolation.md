# ADR-008: Mixed-Workload Resource Isolation as First-Class Invariant

## Status
Accepted

## Context
A major challenge of unifying event streaming and work queues on a single broker is that large historical log replays, segment compactions, or tiered object uploads can consume disk I/O and network bandwidth, spiking interactive work queue delivery latency ($p99$).

## Decision
StrataMQ includes a dedicated **Resource Scheduler** operating across five priority classes:
1. `critical`: Commit path, quorum heartbeats, durable metadata.
2. `interactive`: Low-latency work delivery and lease acknowledgments.
3. `normal`: Sequential streaming reads.
4. `background`: Segment compaction and object store uploads.
5. `recovery`: Catch-up replicas and snapshot restore.

Each class is assigned explicit budgets for CPU work queues, disk IOPS/bandwidth, and network rate.

## Consequences
- **Positive**: Guarantees interactive work latency ($p99 < 2\times$ baseline) even during heavy historical replays and maintenance tasks.
- **Negative**: Adds scheduling logic and accounting overhead inside the broker execution pipeline.
