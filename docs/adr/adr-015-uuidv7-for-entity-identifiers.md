# ADR-015: UUIDv7 K-Sortable Identifiers for Domain Entities

## Status
Accepted

## Context
A distributed message broker and event stream requires robust, globally unique identifiers for records, journals, subscriptions, consumers, producers, and leases. Traditionally, systems use one of the following:
1. Sequential integer IDs (`u64`): Requires centralized coordination (coordination bottleneck), database auto-increment locks, or per-shard atomic counters, and exposes business transaction volumes.
2. Random UUIDv4: Lacks chronological ordering, destroying B-Tree and LSM storage cache locality due to random page fragmentation.
3. Custom Snowflake/Timestamp IDs: Requires dedicated machine ID allocation and NTP drift safeguards.

## Decision
We adopted **UUIDv7** (RFC 9562) as the foundational identifier format for all domain entities in `strata-types`:
- `RecordId`
- `JournalId`
- `SubscriptionId`
- `ConsumerId`
- `ProducerId`
- `LeaseId`

All identifiers are generated via the `define_uuid_id!` macro into strongly-typed newtypes with standard serialization (`serde`), equality, hashing, and conversions.

UUIDv7 embeds a 48-bit millisecond-precision Unix timestamp in the leading bits, providing k-sortable chronological ordering while maintaining 74 bits of cryptographic randomness.

## Consequences
- **Positive**:
  - **Decentralized Generation**: IDs are generated safely across independent threads or client SDK nodes without distributed locks or coordination.
  - **Natural Index Locality**: Monotonically increasing leading bits cluster records chronologically in disk segments, indexes, and LSM trees.
  - **Type Safety**: Newtype wrappers eliminate parameter-swapping bugs at compile time (e.g. passing a `ConsumerId` where a `RecordId` is expected).
  - **Interoperability**: Standard 128-bit format integrates seamlessly with external databases, JSON serialization, and binary network protocols.
- **Negative**:
  - Takes 16 bytes compared to 8-byte integers (`u64`). However, the locality and decentralization benefits far outweigh the minor overhead.
