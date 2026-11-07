# StrataMQ Development & Engineering Guide

**Guidelines, Standards, Verification Protocols, and Architectural Governance**  
*Architecture Baseline: November 2026*

---

## 1. Environment & Prerequisites

StrataMQ is engineered against the **Rust 2024 Edition**. Toolchain specifications are configured in [`rust-toolchain.toml`](../rust-toolchain.toml).

Install required toolchain components:
```bash
rustup component add clippy rustfmt
cargo install cargo-fuzz # For security and protocol parser fuzzing
```

Verify your environment:
```bash
rustc --version
cargo --version
```

---

## 2. Quality Assurance & Verification

Every pull request and commit must strictly satisfy both linting and testing checks without errors or warnings.

### 2.1. Clippy (Zero-Warning Policy)
Run Clippy across all workspace targets with warnings denied as hard errors:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```

### 2.2. Unit & Integration Testing
Execute the complete workspace test suite:
```bash
cargo test --workspace
```

### 2.3. Documentation Build
Verify that all rustdoc links and documentation build without warnings:
```bash
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

### 2.4. Code Formatting
Check formatting compliance:
```bash
cargo fmt --all -- --check
```

Format code automatically:
```bash
cargo fmt --all
```

### 2.5. Continuous Integration (GitHub Actions)
All checks are automated on GitHub Actions via [`.github/workflows/ci.yml`](file:///Users/harisrovca/Documents/Workspaces/stratamq/.github/workflows/ci.yml):
- **Format**: Validates `cargo fmt`.
- **Clippy**: Validates `cargo clippy --workspace --all-targets -- -D warnings`.
- **Test**: Matrix test suite across `ubuntu-latest` and `macos-latest`.
- **Doc**: Validates `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- **Security Audit**: Scans dependencies with `cargo audit`.

Release builds and cross-platform packaging are automated via [`.github/workflows/cd.yml`](file:///Users/harisrovca/Documents/Workspaces/stratamq/.github/workflows/cd.yml) on Git version tags (`v*`).

---

## 3. Architecture & Code Conventions

### 3.1. Zero Unsafe Code
All core domain, journal, subscription, and protocol crates must declare:
```rust
#![forbid(unsafe_code)]
```
Unsafe optimizations are forbidden during Phase P0. Any future introduction of `unsafe` (e.g. in SIMD parsing or zero-copy buffers) must be isolated in dedicated leaf crates, rigorously audited, and accompanied by formal justification.

### 3.2. Error Handling & Typing
- Use `thiserror` for all library error enums (e.g., `AppendError`, `WorkError`).
- Provide informative error variants carrying relevant identifiers (`RecordId`, `LeaseId`, `generation`).
- **Zero Panics in Production Code**: Never use `.unwrap()` or `.expect()` in library code paths; reserve them exclusively for `#[cfg(test)]` test cases.

### 3.3. Strong Typing & Domain Newtypes
- Never pass primitive types (`u64`, `Uuid`, `String`) for domain identifiers.
- Use the `define_uuid_id!` macro in [`crates/types`](file:///Users/harisrovca/Documents/Workspaces/stratamq/crates/types/src/lib.rs) to create strongly-typed newtypes (`RecordId`, `JournalId`, `SubscriptionId`, `ConsumerId`, `ProducerId`, `LeaseId`).

### 3.4. Overflow & Defensive Arithmetic
- Guard all timestamp, sequence, and duration arithmetic against overflow using `saturating_add`, `saturating_sub`, and `saturating_mul`.
- Validate all incoming wire frame lengths, batch counts, and header sizes *prior* to memory allocation.

---

## 4. Testing & Verification Strategies

### 4.1. Property-Based Testing
Critical state invariants must be verified using property-based testing (e.g., `proptest`):
- **Monotonicity**: Subject revisions strictly increase by 1 on each committed append.
- **Lease Exclusivity**: At no time can two distinct consumers hold active unexpired leases for the same record.
- **Generation Fencing**: An acknowledgment referencing a stale generation must fail deterministically under any interleaving.
- **OCC Invariants**: Given $N$ concurrent writers targeting `ExpectedRevision::Exact(k)`, exactly one writer must succeed and $N-1$ must receive `AppendError::RevisionMismatch`.

### 4.2. Concurrency Model Checking
Concurrency-critical data structures (e.g., lock-free queues or atomic lease tables) must be verified with `loom` before being merged into the codebase.

### 4.3. Protocol & Codec Fuzzing
All NMP frame parsers and RecordBatch v0 decoders must maintain continuous fuzz targets using `cargo-fuzz`. PRs touching codecs must pass a smoke corpus fuzz test.

---

## 5. Architectural Governance & ADRs

All architectural changes must follow the process laid out in the [Architecture Decision Register](file:///Users/harisrovca/Documents/Workspaces/stratamq/docs/adr/README.md):
1. **No Accidental Architecture**: Implementation choices must not bypass the ADR process.
2. **Evidence Packaging**: Experimental ADRs (such as P0 tournaments) require:
   - Benchmark scenario IDs and machine-readable JSON results.
   - Semantic equivalence statement.
   - Failure and crash-injection test results.
   - Resource metrics (CPU cycles, RSS, disk/network bandwidth).
   - Reversibility and migration analysis.
