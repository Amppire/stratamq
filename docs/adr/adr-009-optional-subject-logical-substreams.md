# ADR-009: Optional Subject / Logical Substream Abstraction

## Status
Accepted

## Context
Event-sourcing, domain-driven design, and entity-centric workloads require maintaining strict ordering for specific aggregate roots (e.g. `order-1234`, `device-9`, `user-456`) without forcing each entity into its own heavy physical log file or dedicated consensus group.

## Decision
We introduce **Subject** as an optional logical substream within a Journal:
- A `Journal` acts as a namespace containing multiple `Subject` instances.
- Subjects are logical abstractions mapped across virtual shards; they do not dictate physical partition counts.
- Records can be published with or without a Subject.

## Consequences
- **Positive**: Enables entity-oriented event-sourcing and per-aggregate ordering while decoupling physical file count from entity cardinality.
- **Negative**: The storage and journal layers must maintain efficient subject indexes mapping subjects to their latest revisions and record ranges.
