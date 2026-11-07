# ADR-005: Compatibility Protocols as Boundary Adapters

## Status
Accepted

## Context
Adoption is significantly accelerated if existing ecosystems can interact with StrataMQ. However, baking Kafka or AMQP protocol requirements directly into the storage or journal engine would compromise the core design invariants.

## Decision
Compatibility protocols (Kafka, AMQP 0-9-1) are treated strictly as **boundary adapters**, positioned outside the core engine:
```
Client (Kafka / AMQP) ──> Compatibility Adapter ──> NMP / Core Engine
```
The compatibility layer translates requests into native Journal and Subscription semantics. Features with no native mapping in older protocols are omitted or adapted rather than forcing internal architectural distortions.

## Consequences
- **Positive**: Core broker remains clean, modular, and unencumbered by legacy protocol quirks.
- **Negative**: Perfect 100% bug-for-bug behavioral parity with edge Kafka/RabbitMQ features is non-goal.
