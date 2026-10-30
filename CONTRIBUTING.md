# Contributing to `seiche`

## TigerStyle Invariants

1. **Zero Float Pricing:** All prices, fees, and exposures use integer centicents (`i64`, `$1.00 = 10_000 cc`).
2. **Invariants in Code:** Any new `RecomputeOp` must preserve `I-1..I-5` in `Graph::assert_invariants`.
3. **Deterministic Simulation First:** Every stateful mechanism must be exercised in `src/dst.rs` with non-vacuity assertions.
4. **Zero Warnings:** Run `cargo fmt --check`, `cargo clippy -- -D warnings`, and `cargo test --release` before opening a PR.
