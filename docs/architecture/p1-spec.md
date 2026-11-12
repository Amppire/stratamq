# P1 Technical Specification

**Single-Node Production Foundation**  
*Entry gate: Phase P0 Architecture Validation complete*

---

## 1. Purpose

P1 converts the evidence-backed P0 architecture and semantic contracts into a production-capable standalone broker. It delivers durable local storage, deterministic startup recovery, NMP/TCP publication and consumption, durable stream cursors, safe basic retention, and operator diagnostics without requiring clustering or an external coordination service.

P1 preserves the public `Journal`, optional `Subject`, immutable `Record`, and `Subscription` model. Internal storage, indexing, and runtime decisions must remain replaceable by later replication and placement layers.

## 2. Entry Criteria

P1 begins only after:

- required P0 tournament ADRs have accepted or explicitly deferred dispositions;
- the committed identity contract in [issue #32](https://github.com/Amppire/stratamq/issues/32) is frozen;
- retention safety in [issue #33](https://github.com/Amppire/stratamq/issues/33) is accepted;
- the NMP Draft 0 minimum interoperable contract in [issue #34](https://github.com/Amppire/stratamq/issues/34) is frozen;
- durable state and recovery outcomes in [issue #35](https://github.com/Amppire/stratamq/issues/35) are normative;
- the P0 exit review publishes reproducible evidence and the selected P1 architecture.

## 3. Scope and Non-Goals

### In Scope

- selected single-node WAL and segment architecture;
- stable `RecordId`, logical positions, timestamps, and subject revisions across restart;
- durable stream cursor checkpoints;
- age/size retention and earliest-available-position errors;
- NMP/TCP standalone runtime with bounded connections and graceful shutdown;
- publish, stream, work, keyed, and broadcast integration at the P1 durability depth;
- configuration, metrics, tracing, diagnostics, and operator CLI;
- deterministic crash, corruption, load, and restart acceptance tests.

### Explicitly Deferred

- multi-node metadata consensus, replication, failover, and rebalancing;
- durable producer/business idempotency tables beyond the P1 integration hooks;
- fully persistent lease/retry/DLQ/keyed work state;
- object-tier lifecycle and tier-aware retention;
- replay-branch lifecycle and broker-local atomic operations;
- authentication/RBAC and compatibility protocol endpoints.

### Dual-Workstream Division

P1 is planned for two developers working in parallel against shared contracts established by P0:

- **Haris — State & Networking:** owns durable subscription cursors, the standalone NMP/TCP runtime, protocol-to-broker integration, configuration, metrics, diagnostics, and operator CLI. Primary component ownership is `subscription`, `protocol-core`, `server`, and `cli`.
- **Shaunak — Record & Storage:** owns WAL durability, immutable segments and indexes, startup recovery, manifest and subject-head reconstruction, retention, and storage benchmarks. Primary component ownership is `record`, `journal`, `storage-core`, and `bench`.
- **Joint ownership:** both developers approve the materialized P1 architecture, integrate the record and state planes end to end, and execute the phase exit gate. Joint issues are not handoff points: both owners are accountable for compatible interfaces, test fixtures, and acceptance evidence.

Each developer should be able to advance their workstream independently after P1-01 freezes the cross-plane interfaces. Changes to shared record identity, commit receipts, recovery ordering, cursor durability, or protocol error semantics require review from both workstreams.

## 4. Runtime and Durability Model

The standalone process owns one recovery lifecycle and one local commit domain. Startup validates configuration, recovers authoritative record and metadata state, and only then advertises readiness. Shutdown stops admission, sends `GOAWAY`, drains accepted operations, syncs required state, and closes storage.

| Operation | P1 durable boundary |
|---|---|
| Publish receipt | Record bytes, stable IDs, logical positions, checksum, and subject-head advancement satisfy the selected durability profile |
| Stream cursor commit | Subscription checkpoint reaches the P1 durable state store before a durable-success response |
| Work ACK/NACK | State-machine semantics and fencing are enforced; full restart persistence is completed in P2 unless the P0 contract requires a P1 subset |
| Segment roll | Finalized segment and manifest update are crash-consistent |
| Retention deletion | Manifest/earliest-position update and deletion workflow recover to a valid state after interruption |

## 5. Work Breakdown

| Task | GitHub Issue | Owner | Depends On | Deliverable |
|---|---|---|---|---|
| P1-01 | [#42](https://github.com/Amppire/stratamq/issues/42) | Haris + Shaunak | P0 exit | Materialize accepted P0 decisions into the single-node architecture |
| P1-02 | [#38](https://github.com/Amppire/stratamq/issues/38) | Shaunak | P1-01 | Durable WAL append, sync, bounded backpressure, and commit receipts |
| P1-03 | [#36](https://github.com/Amppire/stratamq/issues/36) | Shaunak | P1-01, P1-02 | Immutable segments, checksums, and sparse indexes |
| P1-04 | [#37](https://github.com/Amppire/stratamq/issues/37) | Shaunak | P1-02, P1-03 | Startup recovery, manifest/index rebuild, and subject-head restoration |
| P1-05 | [#41](https://github.com/Amppire/stratamq/issues/41) | Haris | P1-01, P1-02 | Durable stream cursor checkpoints |
| P1-06 | [#43](https://github.com/Amppire/stratamq/issues/43) | Shaunak | P1-03, P1-04, P1-05 | Basic retention and deterministic boundary errors |
| P1-07 | [#40](https://github.com/Amppire/stratamq/issues/40) | Haris | P1-01, P1-02, NMP Draft 0 | Standalone server runtime and graceful shutdown |
| P1-08 | [#39](https://github.com/Amppire/stratamq/issues/39) | Haris + Shaunak | P1-04, P1-05, P1-07 | End-to-end publish, stream, and work delivery |
| P1-09 | [#44](https://github.com/Amppire/stratamq/issues/44) | Haris | All subsystems | Configuration, metrics, diagnostics, and CLI |
| P1-10 | [#45](https://github.com/Amppire/stratamq/issues/45) | Haris + Shaunak | P1-02 through P1-09 | Single-node acceptance, load, restart, and exit review |

GitHub milestone: [Phase P1: Single-Node Production Foundation](https://github.com/Amppire/stratamq/milestone/2).

## 6. Execution Sequence

1. Freeze the P1 architecture and interfaces from accepted P0 outcomes.
2. Implement WAL, segment, manifest, identity, revision, and recovery foundations.
3. Add durable cursor checkpoints and safe retention.
4. Assemble the standalone NMP/TCP runtime and integrate delivery modes.
5. Add bounded observability and operator workflows.
6. Execute the P1 crash/load/restart gate and publish evidence before tagging the phase complete.

## 7. Exit Criteria

- [ ] No acknowledged record is lost across the P1 crash matrix.
- [ ] `RecordId`, logical position, timestamp, and subject revision remain stable across restart and segment roll.
- [ ] OCC outcomes are deterministic before and after recovery.
- [ ] Durable cursor commits do not silently roll back.
- [ ] NMP/TCP publish and consumption paths are bounded and interoperable.
- [ ] Retention reclaims only eligible segments and returns explicit boundary errors.
- [ ] Startup, readiness, graceful shutdown, and fatal storage behavior are deterministic.
- [ ] Operators can inspect recovery, retention, lag, blocked delivery, and storage health.
- [ ] P1 acceptance, property, corruption, load, and repeated-restart tests pass.

