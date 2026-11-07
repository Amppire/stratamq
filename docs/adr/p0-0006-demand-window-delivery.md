# ADR-P0-006: Receiver-Authorized Demand-Window Delivery

## Status
Experimental / High Confidence (Pending Delivery Tournament)

## Context
Message delivery models are traditionally split into consumer pull (Kafka poll) or broker push (RabbitMQ credits). Push can overwhelm slow consumers unless strictly throttled; pull introduces polling latency and higher CPU overhead.

## Decision
StrataMQ adopts **Demand Windows** as the single unified delivery primitive:
- Consumer issues `DEMAND { max_records, max_bytes, wait_ms, continuous }`.
- Broker delivers batches against the authorized credit window (`DELIVER_BATCH`).
- Consumer replenishes credits with `WINDOW_UPDATE`.

## Evidence Required & Decision Threshold
- Run benchmark `DEL-S1` (slow consumers) and bursty streaming workloads.
- **Threshold**: Demand windows win if they match specialized push/pull performance while simplifying protocol architecture and providing strict memory bounds on both sides.
