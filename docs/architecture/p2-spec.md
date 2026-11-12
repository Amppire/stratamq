# P2 Technical Specification

**Reliability and Recovery**  
*Entry gate: Phase P1 standalone broker accepted*

---

## 1. Purpose

P2 makes publication retries and work delivery deterministic under duplicate requests, crashes, broker downtime, delayed execution, clock adjustments, and hostile storage failures. It adds durable producer and application idempotency, persistent work/timing state, DLQ recovery operations, bounded recovery checkpoints, and optional negotiated session resumption.

## 2. Entry Criteria

P2 begins only after:

- the P1 storage, recovery, cursor, retention, and standalone server contracts are accepted;
- P1 crash testing proves no acknowledged record loss;
- stable record identity and subject OCC survive restart;
- the durable state outcomes from #35 have concrete P2 storage ownership;
- baseline recovery time, metadata growth, work latency, and storage failure behavior are measured.

## 3. Scope and Non-Goals

### In Scope

- durable producer epoch/sequence idempotency and prior-receipt replay;
- optional application idempotency keys with explicit scope and retention;
- persistent work, retry, DLQ, generation, keyed pending, and blocked state;
- restart-safe lease, retry, and delayed-delivery timing;
- DLQ inspection, release, and replay without payload duplication;
- state snapshots/checkpoints and bounded recovery time;
- disk-full, corruption, partial-write, repeated-crash, and stalled-I/O hardening;
- optional NMP session resumption behind capability negotiation;
- property, model, fuzz, and failure-injection verification.

### Explicitly Deferred

- replay branches and broker-local atomic consume/publish/ACK operations (P3);
- object-tier storage and tier-aware durability classes (P4);
- adaptive mixed-workload scheduling beyond the accepted baseline (P5);
- TLS identity, authentication, RBAC/ACL, and audit security program (P6);
- clustering and replication (P7–P9).

### Dual-Workstream Division

P2 retains the P1 component boundaries and divides reliability work between two parallel owners:

- **Haris — State & Networking:** owns persistent work/retry/DLQ/keyed state, the restart-safe timer engine, DLQ operator workflows, and optional negotiated session resumption. Primary component ownership remains `subscription`, `protocol-core`, `server`, and `cli`.
- **Shaunak — Record & Storage:** owns producer and application idempotency persistence, state snapshots and checkpoints, bounded recovery, and hostile-storage failure hardening. Primary component ownership remains `record`, `journal`, `storage-core`, and `bench`.
- **Joint ownership:** both developers execute the reliability exit gate and approve any cross-plane recovery contract. The joint gate verifies that storage restoration and subscription restoration compose into one deterministic broker outcome.

The durable-state schema, checkpoint generation, recovery ordering, and idempotency receipt format are shared contracts. Either workstream may implement internally in parallel, but incompatible changes to these contracts require review from both owners.

## 4. Reliability Contracts

| Concern | P2 requirement |
|---|---|
| Transport retry | The same producer epoch/sequence returns the original commit result without another append |
| Business duplicate | A live idempotency key returns the original result or a deterministic content conflict |
| Work transition | Durable ACK/NACK/retry/DLQ/keyed transitions recover only to outcomes allowed by #35 |
| Lease/retry timing | Process-local monotonic values are never reinterpreted after restart; recovery follows the accepted restart rule |
| Snapshot/checkpoint | Crash during publication leaves the previous authoritative checkpoint usable |
| Storage failure | No success is emitted when the required durability boundary fails; corruption fails closed |

## 5. Work Breakdown

| Task | GitHub Issue | Owner | Depends On | Deliverable |
|---|---|---|---|---|
| P2-01 | [#49](https://github.com/Amppire/stratamq/issues/49) | Shaunak | P1 WAL/recovery | Durable producer epoch and sequence idempotency |
| P2-02 | [#48](https://github.com/Amppire/stratamq/issues/48) | Shaunak | P2-01 storage patterns | Application idempotency keys and deduplication policy |
| P2-03 | [#53](https://github.com/Amppire/stratamq/issues/53) | Haris | P1 state hooks, #35 | Persistent work, retry, DLQ, and keyed subscription state |
| P2-04 | [#52](https://github.com/Amppire/stratamq/issues/52) | Haris | P2-03, #21 | Restart-safe timers for leases, retries, and delayed delivery |
| P2-05 | [#50](https://github.com/Amppire/stratamq/issues/50) | Haris | P2-03 | DLQ inspection, replay, and recovery operations |
| P2-06 | [#47](https://github.com/Amppire/stratamq/issues/47) | Shaunak | P2-01 through P2-04 | State snapshots, checkpoints, compaction, and bounded recovery |
| P2-07 | [#54](https://github.com/Amppire/stratamq/issues/54) | Shaunak | P1 recovery, P2-06 | Disk-full, corruption, stalled-I/O, and repeated-crash hardening |
| P2-08 | [#46](https://github.com/Amppire/stratamq/issues/46) | Haris | NMP reconnect baseline, P2-01 | Optional negotiated session resumption or explicit deferral |
| P2-09 | [#51](https://github.com/Amppire/stratamq/issues/51) | Haris + Shaunak | P2-01 through P2-08 | Reliability, idempotency, recovery, and exit review |

GitHub milestone: [Phase P2: Reliability & Recovery](https://github.com/Amppire/stratamq/milestone/3).

## 6. Execution Sequence

1. Implement durable transport and application idempotency on the P1 commit path.
2. Persist work state and establish restart-safe timing semantics.
3. Add DLQ operator workflows and bounded state snapshots/checkpoints.
4. Harden all durable paths against disk-full, corruption, stalls, and repeated crash.
5. Evaluate optional session resumption without coupling P2 completion to an unjustified feature.
6. Execute the P2 verification matrix and publish evidence before tagging the phase complete.

## 7. Exit Criteria

- [ ] Duplicate transport retries never append a second committed record.
- [ ] Business idempotency conflicts and expiry are deterministic.
- [ ] Work generations, terminal states, retries, DLQ state, and keyed ordering recover correctly.
- [ ] Lease and delayed-delivery timing remains correct across restart and wall-clock changes.
- [ ] DLQ replay is bounded, explicit, idempotent, and observable.
- [ ] Snapshot/checkpoint lifecycle bounds recovery time and metadata growth.
- [ ] Disk-full, corruption, partial-write, stall, and repeated-crash tests fail closed.
- [ ] Optional session resumption is either verified or explicitly deferred with rationale.
- [ ] P2 property, model, fuzz, failure-injection, and operational acceptance suites pass.

