# ADR 0001: UUIDv7 for Domain Entity Identifiers

## Status
Accepted

## Context
A distributed message broker and event stream requires robust identifiers for records, journals, subscriptions, consumers, producers, and leases. Traditionally, systems use one of the following:
1. Sequential integer IDs (auto-incrementing integers / sequence counters).
2. Random UUIDv4 identifiers.
3. Timestamp-prefixed strings or custom Snowflake-like IDs.

Sequential integers require a centralized coordinator or atomic counter per shard and expose business volume metrics. Random UUIDv4 values cause index fragmentation in B-Trees / LSM storage due to non-sequential keys and provide no temporal locality.

## Decision
We adopted **UUIDv7** (RFC 9562) as the underlying representation for all entity identifiers, packaged into strongly-typed newtype wrappers using the `define_uuid_id!` macro:
- `RecordId`
- `JournalId`
- `SubscriptionId`
- `ConsumerId`
- `ProducerId`
- `LeaseId`

## Consequences

### Positive
- **K-Sortable / Chronological Order**: UUIDv7 embeds a 48-bit millisecond timestamp in the leading bits, ensuring natural temporal ordering in indices and storage keys.
- **Decentralized Generation**: IDs can be generated safely across independent threads or client nodes without coordination or distributed locks.
- **Type Safety**: Newtype wrappers prevent parameter swapping bugs at compile time (e.g., passing a `ConsumerId` where a `RecordId` is expected).
- **Interoperability**: Standard 128-bit UUID format enables out-of-the-box serialization with Serde, database compatibility, and binary network serialization.

### Negative
- UUIDv7 takes 16 bytes compared to 8-byte integers (`u64`). However, the locality and decentralization benefits outweigh the extra 8 bytes of storage overhead.
