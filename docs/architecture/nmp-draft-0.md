# Native Messaging Protocol (NMP) — Draft 0

**Canonical Binary Protocol for Streaming and Reliable Work Delivery**  
*Engineering Source of Truth — Baseline: November 2026*

---

## Contents
1. [Scope and Goals](#1-scope-and-goals)
2. [Design Principles](#2-design-principles)
3. [Transport Binding](#3-transport-binding)
4. [Connection and Session Lifecycle](#4-connection-and-session-lifecycle)
5. [Frame Model](#5-frame-model)
6. [Capability Negotiation](#6-capability-negotiation)
7. [Record Batch v0](#7-record-batch-v0)
8. [Publishing Operations](#8-publishing-operations)
9. [Subscription Opening and Modes](#9-subscription-opening-and-modes)
10. [Demand Windows and Delivery](#10-demand-windows-and-delivery)
11. [Work Delivery and Leases](#11-work-delivery-and-leases)
12. [Stream Cursor Operations](#12-stream-cursor-operations)
13. [Replay Operations](#13-replay-operations)
14. [Atomic Messaging Operations](#14-atomic-messaging-operations)
15. [Flow Control and Throttling](#15-flow-control-and-throttling)
16. [Errors and Retries](#16-errors-and-retries)
17. [Security and Authentication](#17-security-and-authentication)
18. [Extensibility and Versioning](#18-extensibility-and-versioning)
19. [P0 Open Questions](#19-p0-open-questions)
- [Appendix A. Draft Opcode Registry](#appendix-a-draft-opcode-registry)
- [Appendix B. Limits and Defensive Parsing](#appendix-b-limits-and-defensive-parsing)
- [Appendix C. Subscription Delivery Sequences](#appendix-c-subscription-delivery-sequences)
- [Appendix D. Session Resumption and Reconnection Considerations](#appendix-d-session-resumption-and-reconnection-considerations)
- [Appendix E. Protocol Correctness Requirements](#appendix-e-protocol-correctness-requirements)
- [Appendix F. Error Codes and Receipt Fields](#appendix-f-error-codes-and-receipt-fields)

---

## 1. Scope and Goals

**NMP** is the canonical binary protocol for StrataMQ. It exposes logical journals, subscriptions, record batches, demand windows, cursors, leases, replay, and broker-local atomic operations while completely abstracting physical partition and storage-group concepts from the application layer.

---

## 2. Design Principles

- **Batch-Native**: Record batches are the fundamental unit of publication, delivery, and storage.
- **Multiplexed**: One transport connection hosts multiple independent producers and subscriptions.
- **Receiver-Controlled**: Consumers grant explicit record count and byte budgets via demand windows.
- **Capability-Negotiated**: Optional features are negotiated during handshake rather than requiring lockstep protocol upgrades.
- **Transport-Independent**: TCP and QUIC bindings implement the exact same semantic operations.
- **Defensive Parsing**: All frame lengths, counts, and metadata sizes are validated before memory allocation.

---

## 3. Transport Binding

### 3.1. TCP Binding
Uses a long-lived TLS connection. Multiplexing is handled at the application layer via a 32-bit `stream_id` in the frame header. Flow control is strictly enforced at the application level to bound memory usage.

### 3.2. QUIC Binding
Maps logical streams to native QUIC bidirectional/unidirectional streams while preserving frame-level semantics, request correlation, and flow control.

---

## 4. Connection and Session Lifecycle

```
Client                               Server
  │                                    │
  ├─────────── 0x0001 HELLO ──────────>│  (version, capabilities, auth choices)
  │<────────── 0x0002 WELCOME ─────────┤  (negotiated limits, session ID)
  │                                    │
  │[Optional]  0x0003 AUTH ───────────>│
  │<────────── 0x0003 AUTH (ok) ───────┤
  │                                    │
  ├─────────── OPEN Stream / Op ──────>│
  │<────────── DELIVER_BATCH / Result ─┤
  │                                    │
  ├─────────── 0x7FFF GOAWAY ─────────>│
```

---

## 5. Frame Model

```
FrameHeader Draft 0 (24 bytes)
┌───────────────┬───────────────┬───────────────────────────────┐
│ version (u8)  │  flags (u8)   │         opcode (u16)          │
├───────────────┴───────────────┴───────────────────────────────┤
│                         stream_id (u32)                       │
├───────────────────────────────────────────────────────────────┤
│                        request_id (u64)                       │
├───────────────────────────────────────────────────────────────┤
│                       payload_len (u32)                       │
├───────────────────────────────────────────────────────────────┤
│ payload bytes [payload_len]                                   │
└───────────────────────────────────────────────────────────────┘
```

- `stream_id`: Identifies the logical multiplexed session or subscription.
- `request_id`: Correlates asynchronous request/response operations.
- `payload_len`: Validated against `max_frame_bytes` before allocation.

---

## 6. Capability Negotiation

| Capability | Bit / Name | Meaning |
|---|---|---|
| `batch_publish` | `0x0001` | Server accepts `PUBLISH_BATCH` |
| `demand_window` | `0x0002` | Client authorizes bounded record/byte windows |
| `work_leases` | `0x0004` | Competing consumer lease management (`ACK`, `NACK`, `EXTEND_LEASE`) |
| `stream_cursor` | `0x0008` | Cursor checkpointing and seek operations |
| `replay_branch` | `0x0010` | Isolated historical replay sessions |
| `atomic_ops` | `0x0020` | Broker-local atomic consume/publish/ACK operations |
| `compression_lz4`| `0x0040` | LZ4 batch compression supported |
| `compression_zstd`| `0x0080`| Zstandard batch compression supported |

---

## 7. Record Batch v0

The binary format for data frames and disk segments:

```
RecordBatchV0
  u32 total_len
  u8  format_version (0x00)
  u8  flags
  u8  compression (0 = None, 1 = LZ4, 2 = Zstd)
  u8  reserved
  u32 record_count
  u64 producer_id
  u32 producer_epoch
  u64 first_sequence
  u64 first_timestamp
  u32 crc32c
  bytes records[]
```

---

## 8. Publishing Operations

### 8.1. `PUBLISH` / `PUBLISH_BATCH`
```
PublishTarget {
  journal_id: JournalId,
  subject: Option<Subject>,
  expected_revision: ExpectedRevision,
}

ExpectedRevision := Any | NoSubject | Exact(u64)
```

- **Receipt**: Returns `PUBLISH_RESULT` with `commit_ts`, assigned record IDs, and `committed_subject_revision`.
- **OCC Rejection**: Returns `REVISION_CONFLICT` when `expected_revision` does not match current subject revision.
- **Transport Idempotency**: Evaluates `(producer_id, producer_epoch, sequence)`. Duplicate retries return the prior commit receipt with `deduplicated = true`.

---

## 9. Subscription Opening and Modes

```
SUBSCRIBE (0x0200) {
  subscription_id: SubscriptionId,
  mode: Stream | Work | Broadcast | Keyed | Ephemeral,
  start_position: Begin | End | RecordId(id) | Timestamp(ts),
  client_limits: ClientLimits
}
```

---

## 10. Demand Windows and Delivery

Flow control unifies push and pull semantics through receiver-authorized demand credits:

```
DEMAND (0x0210) {
  max_records: u32,
  max_bytes: u32,
  wait_ms: u32,
  continuous: bool
}
```

- Server delivers batches (`DELIVER_BATCH`) decrementing available credits.
- Client replenishes credits via `WINDOW_UPDATE (0x0211)`.
- If credits reach zero, delivery halts; memory usage is bounded on both broker and client.

---

## 11. Work Delivery and Leases

In `Work` mode, `DELIVER_BATCH` includes per-record lease metadata:
- `lease_id`: UUIDv7 identifying the lease instance.
- `generation`: Monotonically increasing counter for this record.
- `expires_at`: Absolute timestamp when the lease expires.

### Operations:
- `ACK_BATCH (0x0230)`: `[{ record_id, lease_id, generation }]`
- `NACK (0x0231)`: `RecordId, lease_id, generation, reason`
- `EXTEND_LEASE (0x0232)`: `RecordId, lease_id, generation, extension_ms`

Stale generation attempts receive `STALE_LEASE` (error `0x0041`) and are deterministically rejected.

---

## 12. Stream Cursor Operations

- `SEEK (0x0240)`: Repositions cursor by logical record ID, timestamp, or offset.
- `COMMIT_CURSOR (0x0241)`: Durably persists the consumer offset for the subscription.

---

## 13. Replay Operations

- `CREATE_REPLAY (0x0300)`: Spawns an isolated replay branch without disturbing active production subscription cursors.

---

## 14. Atomic Messaging Operations

- `BEGIN_OPERATION (0x0310)`: Initializes a broker-local transaction within a single storage ownership domain.
- Groups publishes and ACKs atomically:
  ```
  BEGIN_OPERATION -> op_id
  PUBLISH_BATCH { op_id, ... }
  ACK_BATCH { op_id, ... }
  COMMIT_OPERATION { op_id }
  ```

---

## 15. Draft Opcode Registry

| Opcode | Name | Direction | Category |
|---|---|---|---|
| `0x0001` | `HELLO` | C → S | Session |
| `0x0002` | `WELCOME` | S → C | Session |
| `0x0003` | `AUTH` | Both | Security |
| `0x0004` | `PING` / `PONG` | Both | Heartbeat |
| `0x0010` | `METADATA` | Both | Discovery |
| `0x0100` | `PUBLISH` | C → S | Publishing |
| `0x0101` | `PUBLISH_BATCH` | C → S | Publishing |
| `0x0102` | `PUBLISH_RESULT` | S → C | Publishing |
| `0x0200` | `SUBSCRIBE` | C → S | Subscription |
| `0x0201` | `SUBSCRIBED` | S → C | Subscription |
| `0x0210` | `DEMAND` | C → S | Flow Control |
| `0x0211` | `WINDOW_UPDATE` | C → S | Flow Control |
| `0x0220` | `DELIVER_BATCH` | S → C | Delivery |
| `0x0230` | `ACK_BATCH` | C → S | Work Queue |
| `0x0231` | `NACK` | C → S | Work Queue |
| `0x0232` | `EXTEND_LEASE` | C → S | Work Queue |
| `0x0240` | `SEEK` | C → S | Stream |
| `0x0241` | `COMMIT_CURSOR` | C → S | Stream |
| `0x0300` | `CREATE_REPLAY` | C → S | Replay |
| `0x0310` | `BEGIN_OPERATION` | C → S | Atomic Ops |
| `0x0311` | `COMMIT_OPERATION` | C → S | Atomic Ops |
| `0x03F0` | `HINT` | S → C | Flow Control |
| `0x7FFE` | `ERROR` | Both | System |
| `0x7FFF` | `GOAWAY` | Both | Lifecycle |
