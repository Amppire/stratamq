# ADR-P0-010: Keyed Ordering Guarantees and Failure Policy

## Status
Experimental Semantic Freeze

## Context
In Keyed Work subscriptions, messages with the same partition key must be processed sequentially by a single worker. If an earlier message for key $K$ fails (NACKed or reaches lease expiration), the system must have an explicit policy regarding whether subsequent messages for key $K$ are blocked or allowed to proceed.

## Candidates Considered
1. **Strict Block (Default for Event Sourcing)**: A failed or retrying record for key $K$ blocks all subsequent records for key $K$ until the blocking record succeeds or transitions to Dead-Letter.
2. **Relaxed / Skip**: Later records for key $K$ proceed immediately; the failed record retries independently in the background.

## Decision
StrataMQ exposes key failure policy explicitly in the `Subscription` definition (`KeyFailurePolicy::StrictBlock` vs. `KeyFailurePolicy::Relaxed`). `StrictBlock` is the default to maintain event-sourcing consistency invariants.
