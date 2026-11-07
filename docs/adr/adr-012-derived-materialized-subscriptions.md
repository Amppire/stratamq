# ADR-012: Derived & Materialized Subscriptions (WASM Isolation)

## Status
Planned / Deferred (Post-P1)

## Context
Aggregating, filtering, or projecting record streams into derived streams or materialized state is a common requirement in event-driven systems. Running arbitrary code inside the broker process risks crashing or destabilizing core storage.

## Decision
Derived subscriptions will initially support declarative transforms (projections and filters). If user-defined functions are introduced, they must execute inside sandboxed WebAssembly (WASM) runtimes with bounded execution time and memory limits, preserving core broker stability.

## Consequences
- **Positive**: In-broker transformations without memory corruption or denial-of-service risks.
- **Negative**: WASM runtime overhead; restricted capability set compared to full stream processing frameworks.
