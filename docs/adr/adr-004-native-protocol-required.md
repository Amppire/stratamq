# ADR-004: Native Messaging Protocol (NMP) as Canonical Wire Protocol

## Status
Accepted

## Context
Existing protocols (Kafka binary protocol, AMQP 0-9-1/1.0, MQTT, gRPC/HTTP2) were engineered around specific physical models (Kafka around partitions and consumer group heartbeats, AMQP around exchanges and channels). Adopting an existing protocol as canonical would inherently constrain the platform's ability to offer native demand windows, fenced leases, isolated replay branches, and broker-local atomic transactions.

## Decision
We define and implement a purpose-built **Native Messaging Protocol (NMP)** as canonical. NMP is batch-native, binary, multiplexed over one transport connection, and driven by receiver-authorized demand credits (`DEMAND` and `WINDOW_UPDATE`).

## Consequences
- **Positive**: Native expression of leases, cursors, replay branches, and backpressure without protocol compromises.
- **Negative**: Requires writing custom client SDKs across primary programming languages (Rust, Go, TypeScript, Java, Python).
