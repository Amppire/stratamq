# ADR-P0-004: Multi-Tier Storage Classes

## Status
Experimental (Pending Storage Tournament)

## Context
A single storage configuration cannot simultaneously optimize for sub-millisecond interactive queue latencies and ultra-cheap, petabyte-scale telemetry streams.

## Decision
StrataMQ evaluates three first-class **Storage Classes** configurable per Journal:
1. `low-latency`: Replicated NVMe WAL + local NVMe segments (for financial transactions, low-latency queues).
2. `balanced`: Durable WAL commit + object-primary segments (for general mixed queue and stream workloads).
3. `economical`: Object-primary with micro-batching (for audit logs, metrics, telemetry).

## Evidence Required
Evaluate whether supporting three storage classes adds unmanageable complexity to the storage abstraction layer or if a single trait interface can cleanly support all three profiles.
