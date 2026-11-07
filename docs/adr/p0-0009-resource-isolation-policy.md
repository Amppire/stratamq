# ADR-P0-009: Mixed-Workload Resource Isolation Policy

## Status
Experimental (Pending Mixed Workload Benchmark)

## Context
Running event stream replays and cold segment compactions concurrently with real-time work queue delivery causes severe head-of-line blocking and I/O starvation.

## Candidates Considered
1. **No Isolation**: Rely strictly on OS page cache and Tokio thread pool scheduling.
2. **Static Priority Class Budgets**: Hard allocations of CPU worker threads, disk bandwidth, and network credits for `critical`, `interactive`, `normal`, `background`, and `recovery`.
3. **Adaptive Dynamic Feedback Scheduler**: Dynamic PID-controller adjusting background limits based on live interactive p99 latency measurements.

## Evidence Required & Decision Threshold
- Run benchmark `MIX-2` (40% stream, 30% work, 20% replay, 10% retry + compaction/upload).
- **Threshold**: Static priority class budgets are preferred as the baseline. Adaptive scheduling is introduced only if static budgets fail to maintain interactive $p99 < 2\times$ baseline.
