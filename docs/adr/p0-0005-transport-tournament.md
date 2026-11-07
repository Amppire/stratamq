# ADR-P0-005: Transport Tournament (NMP/TCP vs. NMP/QUIC)

## Status
Experimental (Pending Transport Tournament)

## Context
Transport protocols impact CPU efficiency, connection setup latency, and multiplexing head-of-line blocking under packet loss.

## Candidates Considered
1. **NMP over TCP**: Single connection multiplexing with application-level stream IDs. Mature, well-optimized OS stacks.
2. **NMP over QUIC**: Native QUIC streams with integrated TLS 1.3 and packet loss isolation.

## Evidence Required & Decision Threshold
- Run benchmarks `NET-L1` (LAN multiplexing) and `NET-W1` (WAN loss with 1% packet drop).
- **Threshold**: TCP is the conservative baseline for datacenter deployments. QUIC becomes default only if cross-stream loss isolation and connection migration provide measurable improvements that outweigh its higher user-space UDP/crypto CPU overhead. Both may remain supported.
