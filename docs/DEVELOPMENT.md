# StrataMQ Development Guide

This guide outlines setup instructions, coding conventions, testing protocols, and workflow guidelines for contributors and developers working on StrataMQ.

---

## 1. Environment & Prerequisites

StrataMQ is developed against the **Rust 2024 Edition**. Toolchain specifications are configured in [`rust-toolchain.toml`](../rust-toolchain.toml).

Ensure you have the required components installed:
```bash
rustup component add clippy rustfmt
```

---

## 2. Quality Assurance & Verification

Every pull request and commit must satisfy both linting and testing checks without errors or warnings.

### Clippy (Zero-Warning Policy)

Run Clippy across all workspace targets with warnings treated as hard errors:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

### Running Tests

Execute the complete workspace test suite (unit tests, integration tests, doc-tests):

```bash
cargo test --workspace
```

### Code Formatting

Check formatting compliance:
```bash
cargo fmt --all -- --check
```

Format code:
```bash
cargo fmt --all
```

---

## 3. Architecture & Code Conventions

### 3.1. Zero Unsafe Code
All core crates must include the following attribute at the top of their `lib.rs`:
```rust
#![forbid(unsafe_code)]
```
Any introduction of `unsafe` in core crates will be rejected by the compiler.

### 3.2. Error Handling
- Use `thiserror` for library error definitions (`enum AppendError`, `enum WorkError`).
- Use descriptive error messages and structured error variants over generic string errors.
- Never use `.unwrap()` or `.expect()` in production library code paths; reserve them exclusively for test assertions (`#[cfg(test)]`).

### 3.3. Strong Typing & Newtypes
- Never use bare primitive types (`u64`, `Uuid`, `String`) for domain identifiers.
- Use the `define_uuid_id!` macro in `strata-types` to create strongly-typed newtypes for all identifiers.

### 3.4. Numeric Arithmetic
- Always guard timestamp and duration calculations against overflows using `saturating_add`, `saturating_sub`, and `saturating_mul`.

---

## 4. Workspace Crates & Dependencies

When adding dependencies:
1. Declare shared dependencies in the workspace root [`Cargo.toml`](../Cargo.toml) under `[workspace.dependencies]`.
2. Reference them in individual crate `Cargo.toml` files using `foo.workspace = true`.
3. Keep dependencies lean and minimal to preserve fast build times and minimal dependency footprints.
