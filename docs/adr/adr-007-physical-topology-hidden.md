# ADR-007: Hide Physical Sharding Topology from Client API

## Status
Accepted

## Context
In Apache Kafka, producers and consumers explicitly bind to physical partition IDs (e.g., Partition 0, 1, 2). This leaks cluster capacity planning into application design, creates complex partition rebalancing, and makes dynamic scaling difficult.

## Decision
Physical partitions and storage groups are completely hidden from the user-facing application contract:
1. Applications interact only with **Journals**, optional **Subjects**, and **Subscriptions**.
2. Key-based affinity maps through a large, stable virtual shard namespace onto internal physical Storage Groups.
3. Storage groups and replicas can be rebalanced, merged, or split without altering record identifiers, cursors, or client code.

## Consequences
- **Positive**: Seamless scaling, transparent rebalancing, and zero client recompilation when cluster topology evolves.
- **Negative**: Internal routing layer must maintain placement tables and handle shard routing epochs.
