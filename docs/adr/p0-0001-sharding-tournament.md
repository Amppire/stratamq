# ADR-P0-001: Fixed/Dynamic Partitions vs. Virtual Sharding

## Status
Experimental (Pending Sharding Tournament)

## Context
Traditional systems (e.g., Kafka) expose coarse partitions to the application layer. This couples physical storage count with consumer concurrency and creates significant data-rebalancing churn during cluster expansion. Virtual sharding decouples logical keys into a large, stable shard space mapped to fewer physical storage groups.

## Candidates Considered
1. **Candidate A: Coarse Physical Partitions**: Traditional design where partition is the unit of ordering, storage, and replication.
2. **Candidate B: Virtual Sharding**: 65,536 or 262,144 logical shards mapped to storage groups via rendezvous hashing.
3. **Candidate C: Dynamic Range Placement**: Ordered key ranges that dynamically split and merge.

## Evidence Required & Decision Threshold
- Run benchmarks `SHARD-U1` (uniform keys), `SHARD-H1` (hot keys), and `SHARD-R1` (node additions).
- **Threshold**: Virtual sharding is accepted if routing CPU overhead is $\le 5\text{--}10\%$ compared to Candidate A, metadata memory is bounded, and data movement during topology change is strictly minimal. If routing overhead exceeds $10\%$, dynamic coarse partitions will be retained.
