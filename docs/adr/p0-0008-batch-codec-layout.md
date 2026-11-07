# ADR-P0-008: Record Batch v0 Fixed-Width vs. Varint Layout

## Status
Experimental (Pending Codec Benchmark)

## Context
Encoding batch headers and record length prefixes can use fixed-width integer fields (`u32`, `u64`) or variable-length integers (`varint` / LEB128). Fixed-width enables direct struct mapping and fast SIMD alignment, whereas varints save space for small payloads.

## Candidates Considered
1. **Fixed-Width Encoding**: Constant byte offsets, zero bit-shifting loops.
2. **Varint Delta Encoding**: Variable-length integers for sequence and timestamp deltas.

## Evidence Required & Decision Threshold
- Measure decoding CPU overhead vs. wire bandwidth savings across 100B, 1KiB, and 100KiB records.
- **Threshold**: Fixed-width is preferred for core frame/batch headers unless varint compression saves $> 20\%$ total bandwidth on typical 1KiB batch workloads without measurable CPU degradation.
