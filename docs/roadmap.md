# StrataMQ Delivery Roadmap & Implementation Status

**Tracking Progress from P0 Research to v1 General Availability**  
*Architecture Baseline: November 2026*

---

## 1. Roadmap Phase Overview

*For day-to-day parallel developer assignments, milestone sequencing, and the live task board, see the [Dual-Workstream Coordination Plan](COORDINATION.md).*

| Phase | Description | Status | Target Deliverables |
|---|---|---|---|
| **P0** | **Architecture Validation** | 🟡 **In Progress** | Semantic prototype, Sharding tournament, Storage tournament, Transport tournament, Delivery tournament, RecordBatch v0, NMP Draft 0 |
| **P1** | **Single-Node Production Foundation** | ⚪ Planned | Durable disk WAL, segment roll, sparse indexes, crash recovery, NMP/TCP server, CLI tool |
| **P2** | **Reliability & Recovery** | ⚪ Planned | Producer idempotency table, business idempotency keys, hierarchical timer wheels, DLQ replaying, crash-recovery suites |
| **P3** | **Advanced Messaging** | ⚪ Planned | Isolated replay branches, broker-local atomic operations, causal metadata, priority work schedulers |
| **P4** | **Object Storage & Durability Classes** | ⚪ Planned | S3-compatible segment tier, prefetch/caching, storage class profiles (`low-latency`, `balanced`, `economical`) |
| **P5** | **Mixed-Workload Resource Isolation** | ⚪ Planned | Multi-class resource budget scheduler (CPU, I/O, network, memory) |
| **P6** | **Security & Access Control** | ⚪ Planned | TLS/mTLS, SASL/OAuth2/JWT, Journal & Subscription ACL/RBAC, audit logging |
| **P7** | **Cluster Metadata & Placement** | ⚪ Planned | Metadata consensus, placement epochs, rendezvous shard routing |
| **P8** | **Replication & Failover** | ⚪ Planned | Storage-group replication, leader failover, snapshot transfer, catch-up |
| **P9** | **Cluster Rebalancing** | ⚪ Planned | Node drain/expansion, live shard movement, hot-shard detection |
| **P10**| **Transport Maturity** | ⚪ Planned | QUIC promotion if benchmark-justified; multi-stream multiplexing |
| **P11**| **Compatibility Adapters** | ⚪ Planned | Kafka boundary adapter, AMQP 0-9-1 boundary adapter |
| **P12**| **SDK Ecosystem & v1 GA** | ⚪ Planned | Multi-language SDKs (Go, TypeScript, Python), operational runbooks, backup/restore, v1 GA |

---

## 2. Detailed Phase P0 Status

Phase P0 is the architecture validation phase. It produces experimental implementations and benchmark tournament reports to resolve pending ADRs before freezing single-node production code.

### 2.1. Common Semantic Prototype
- [x] Strongly-typed domain IDs with UUIDv7 ([`strata-types`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/types/src/lib.rs))
- [x] Immutable record data model with headers & keys ([`strata-record`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/record/src/lib.rs))
- [x] In-memory Journal with per-subject revision tracking & OCC ([`strata-journal`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/journal/src/lib.rs))
- [x] Stream subscription state machine with cursor, commit, & seek ([`strata-subscription::StreamState`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/subscription/src/stream.rs))
- [x] Work queue state machine with generation-fenced leases, extensions, & retries ([`strata-subscription::WorkState`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/subscription/src/work.rs))
- [ ] Complete Keyed work queue lifecycle: promotion of pending items on completion ([`strata-subscription::KeyedWorkState`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/subscription/src/keyed.rs))
- [ ] Replace linear lease/retry scans with time-indexed queue or hierarchical timer wheel
- [ ] Property-based testing with `proptest` for concurrent OCC and lease expiration races

### 2.2. Record Batch v0 Specification
- [ ] Define `RecordBatchV0` binary layout with length prefixes, CRC32C, producer ID, epoch, sequence, and flags ([`crates/record`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/record))
- [ ] Implement LZ4 and Zstandard batch compression codecs
- [ ] Fuzz test decoder with `cargo-fuzz`

### 2.3. Native Messaging Protocol (NMP Draft 0)
- [ ] Binary frame header codec: `[version, flags, opcode, stream_id, request_id, payload_len]` ([`crates/protocol-core`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/protocol-core))
- [ ] Frame parser with defensive allocation bounds (`max_frame_bytes`, `max_batch_records`)
- [ ] Implement demand-window accounting (`DEMAND` and `WINDOW_UPDATE`)

### 2.4. Tournament Harnesses & Experiments ([`crates/bench`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/bench))
- [ ] **Sharding Tournament**: Compare Coarse Partitions vs. Virtual Sharding (65k/262k) via Rendezvous Hashing (`SHARD-U1`, `SHARD-H1`, `SHARD-R1`)
- [ ] **Storage Tournament**: Compare Local-First WAL vs. Hybrid WAL+Object vs. Object-Primary (`STORE-W1`, `STORE-S1`, `STORE-R1`)
- [ ] **Transport Tournament**: Compare NMP over TCP vs. NMP over QUIC under LAN and WAN packet loss (`NET-L1`, `NET-W1`)
- [ ] **Delivery Tournament**: Compare Request/Response Pull vs. Server Push vs. Demand-Window (`DEL-S1`)
- [ ] **Codec Tournament**: Compare Shared Wire/Storage Batch format vs. Normalized Internal Representation
- [ ] **Mixed-Workload Isolation**: Run mixed stream/work/replay workload (`MIX-1`, `MIX-2`) and evaluate priority class scheduler

---

## 3. P0 Exit Criteria Checklist

Before graduating to Phase P1 (Single-Node Production Foundation), the project must satisfy all criteria from Implementation Plan §17.1:

- [ ] Unified record/subscription model verified under property tests for all subscription modes.
- [ ] Sharding architecture selected with measured routing and rebalancing trade-offs (ADR-P0-001).
- [ ] Default storage commit architecture and durability profiles chosen with benchmark evidence (ADR-P0-003, ADR-P0-004).
- [ ] NMP Draft 0 framing and Record Batch v0 format validated and stabilized.
- [ ] TCP vs. QUIC positioning decided with empirical measurements (ADR-P0-005).
- [ ] Demand-window delivery validated against push/pull implementations (ADR-P0-006).
- [ ] Shared vs. normalized batch representation decided with CPU and copy metrics (ADR-P0-007).
- [ ] Mixed-workload isolation strategy proven to preserve interactive $p99 < 2\times$ baseline (ADR-P0-009).
- [ ] All tournament results captured in reproducible benchmark reports and accepted ADRs.
