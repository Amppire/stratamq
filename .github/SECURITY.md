# Security Policy

## Supported Versions

StrataMQ is currently in active development (Phase P0: Architecture Validation). Security updates and fixes are applied continuously to the `dev` and `main` branches.

| Version / Branch | Supported          | Status |
| ---------------- | ------------------ | ------ |
| `main`           | :white_check_mark: | Production Releases |
| `dev`            | :white_check_mark: | Active Development |
| `< 0.0.1`        | :x:                | Unsupported |

---

## Reporting a Vulnerability

We take the security of StrataMQ seriously. If you discover a security vulnerability or potential threat (e.g. memory safety risk, boundary check bypass, denial-of-service vector, or unauthenticated privilege escalation):

1. **Do NOT report security vulnerabilities via public GitHub issues.**
2. Report the vulnerability privately via one of the following channels:
   - **GitHub Private Vulnerability Reporting**: Open an advisory under the Security tab of this repository.
   - **Direct Security Contact**: Email **harisrovca@icloud.com** with the subject line `[SECURITY] StrataMQ Vulnerability Report`.

### What to Include in Your Report

To help us triage and resolve the issue quickly, please provide:
- A clear description of the vulnerability and potential impact.
- Affected components, crates, or protocol opcodes.
- Step-by-step reproduction instructions or a minimal proof-of-concept (PoC).
- Any proposed mitigations or patch suggestions if available.

### Response Timeline

- **Initial Acknowledgment**: Within 48 hours of report submission.
- **Triage & Assessment**: Within 5 business days.
- **Fix & Disclosure Coordination**: We will coordinate with the reporter on a timeline for releasing a patched version and public security advisory.

---

## Security Invariants

StrataMQ enforces several compile-time and protocol-level security invariants:
- **`#![forbid(unsafe_code)]`**: Zero unsafe code in core domain and state machine crates.
- **Defensive Frame Parsing**: Bounded allocation caps (`max_frame_bytes = 16MiB`, `max_batch_records = 10,000`) before buffer allocations.
- **Bounded Backpressure**: All internal channels and demand windows enforce strict memory limits to prevent denial-of-service and OOM failures.
