# ADR-001: Rust for Core Broker Engine

## Status
Accepted

## Context
A high-throughput unified messaging engine demands deterministic low-latency execution, predictable memory utilization without garbage-collection pauses, custom binary framing, and direct asynchronous I/O primitives. At the same time, network-facing servers are vulnerable to buffer overflows, race conditions, and memory corruption bugs.

## Alternatives Considered
1. **Go**: Faster development velocity and simpler concurrency model (goroutines), but runtime garbage collection induces unpredictable tail latencies ($p99$ and $p99.9$), and memory footprints are higher under heavy allocation churn.
2. **C++ (e.g., Seastar / ASIO)**: Maximum hardware control, but lacks compile-time memory safety guarantees, resulting in higher maintenance and security auditing overhead.
3. **Rust**: Zero-cost abstractions, deterministic resource deallocation without GC, fearless concurrency, and compile-time memory safety.

## Decision
We chose **Rust** for the entire core StrataMQ broker engine. All core domain, storage, and subscription crates enforce `#![forbid(unsafe_code)]`. Go remains an option for client SDKs and external benchmark harnesses.

## Consequences
- **Positive**: Compile-time safety guarantees, zero GC pause spikes, compact memory layout, and single standalone static binary distribution.
- **Negative**: Steeper learning curve, slower compilation times, and strict lifetime management across asynchronous boundaries.
