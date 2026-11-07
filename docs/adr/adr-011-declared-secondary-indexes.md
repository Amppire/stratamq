# ADR-011: Declared Secondary Indexes Over Metadata

## Status
Planned / Deferred (Post-P1)

## Context
Operators and consumers frequently need to retrieve historical records matching specific metadata attributes (such as user IDs, correlation IDs, or event types) without scanning entire historical segment files. However, automatic full payload indexing turns the messaging engine into an OLTP database with severe write-amplification and compaction penalties.

## Decision
StrataMQ will support **opt-in declared secondary indexes** strictly over record metadata and schema-declared fields. Full-payload arbitrary indexing is explicitly rejected.

## Consequences
- **Positive**: Enables fast historical lookups and selective replays without unbounded write amplification.
- **Negative**: Secondary indexes require extra storage and add bookkeeping overhead during segment compaction.
