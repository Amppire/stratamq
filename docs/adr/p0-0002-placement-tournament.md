# ADR-P0-002: Placement Algorithm Tournament

## Status
Experimental (Pending Placement Benchmark)

## Context
Virtual shards must be mapped to physical Storage Groups and nodes in a deterministic, balanced manner that minimizes shard movement when nodes join or leave the cluster.

## Candidates Considered
1. **Rendezvous Hashing (Highest Random Weight)**: Deterministic hash weighting per node; minimum disruption on topology changes.
2. **Consistent Hash Ring with Virtual Nodes**: Traditional ring-based hashing.
3. **Explicit Mapping Tables**: Master-managed routing table with explicit administrative placement.

## Evidence Required & Decision Threshold
- Measure lookup throughput, memory consumption of placement state, and distribution variance across skewed and uniform node capacities.
- **Threshold**: Rendezvous hashing is the default candidate unless benchmark shows higher lookup latency than hash rings under large node counts.
