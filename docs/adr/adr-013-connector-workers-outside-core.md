# ADR-013: Out-of-Process Connector Workers

## Status
Planned (P11+)

## Context
Messaging brokers commonly incorporate external connectors (e.g. S3 sinks, Postgres CDC, HTTP webhooks). Embedding external network connectors directly into the broker core couples protocol crash domains and memory management with unpredictable third-party endpoints.

## Decision
Connectors will run as **independent worker processes** consuming via native NMP subscriptions. They maintain checkpointed cursor state and scale separately from broker storage groups.

## Consequences
- **Positive**: Complete fault isolation; connector crashes cannot bring down the broker daemon.
- **Negative**: Adds deployment and orchestration overhead for operating connector worker fleets.
