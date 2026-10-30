# seiche

[![CI](https://github.com/copyleftdev/seiche/actions/workflows/ci.yml/badge.svg)](https://github.com/copyleftdev/seiche/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021%20edition-orange.svg)](Cargo.toml)

A **seiche** (*saysh*) is a standing resonant wave in an enclosed body of water where a small barometric or seismic impulse at one boundary propagates into a sharp, predictable swing at the other.

`seiche` is a zero-dependency, TigerStyle Rust incremental computation DAG, cutoff propagation engine, idempotent delta algebra, sequence-gap orderbook buffer, and VOPR deterministic simulator built for bounded prediction-market orderbooks (`0c..100c`).

```
→ 184 ns stabilization across 10,000 symbols (20,001 DAG nodes)
→ 5.43M+ events/sec single-core throughput
→ 3.00 nodes recomputed per tick out of 20,001 (O(changed), not O(N))
→ 2,020 algebraic delta & topological height invariants verified per run
```

---

## Why `seiche` Exists

Cron-polled trading loops recompute too much and react too slowly:
- **Batch polling wastes rate limits and misses intra-hour spikes:** Polling open positions once an hour (`60m` cron) misses transient 3-to-15 minute liquidity spikes where a live bid clears `entry + quadratic_taker_fee + buffer` and then fades before the top of the hour.
- **Naive event loops over-react to noise:** Firing an order amend on every sub-cent oracle wiggle burns exchange rate limits (`HTTP 429`).

`seiche` models the market as a **height-ordered incremental DAG** in integer centicents (`i64`, `$1.00 = 10_000 cc`):

```
External Anchor / WS Tick ──► [Leaf (h=0)] ──► [Cutoff::Band] ──► [MakerQuote / HarvestTrigger (h=1)] ──► [IncrFoldSum (h=2)] ──► Algebraic Delta
```

1. **Packed `(height, node_id)` Min-Heap (`DirtyHeap`):** Encodes `(height: u16, node_id: u32)` into a single `u64` key `((height as u64) << 32) | (node_id as u64)` so topological ordering and deterministic tie-breaking (`S3`) execute in a single CPU `<` instruction with zero heap allocations.
2. **Deadband Hysteresis (`Cutoff::Band`):** Stops sub-threshold oracle wiggles at Height 0 before they wake downstream quote or fee nodes.
3. **`O(changed)` Incremental Fold (`IncrFoldSum`):** Maintains portfolio-wide exposure and multi-bracket coherence sums by subtracting the previous parent value and adding the new value in `O(changed_parents)` instead of `O(all_parents)`.
4. **Algebraic Order Deltas (`Delta`):** Output transitions (`NoOp`, `Set`, `Patch`, `Remove`) satisfy strict idempotence, roundtrip, compatibility, and associativity laws, flushing only after `stabilize()` completes (`S4`).
5. **Sequence-Gap Orderbook Buffer (`DeltaBuffer`):** Reconstructs WebSocket `orderbook_delta` streams in strict sequence order, discarding duplicates idempotently, buffering out-of-order frames, and triggering `ResyncRequired` when gaps exceed the retransmit budget.

---

## Measured Performance

| Operation | Measured (`seiche bench`) |
|---|---|
| Stabilization latency (10,000 symbols / 20,001 nodes) | **184 ns / event** |
| Single-core throughput (10,000 symbols) | **5.43M events / sec** |
| Nodes recomputed per tick (out of 20,001) | **3.00** |
| External runtime dependencies | **0** (`std` only) |

---

## Quick Start

```bash
# Build & install
cargo test --release
cargo build --release
cp target/release/seiche ~/.local/bin/seiche

# 1. Benchmark 10,000 symbols (20,001 DAG nodes)
seiche bench --symbols 10000 --events 100000 --json

# 2. Run 100-seed VOPR simulation (Seiche event-driven DAG vs 60m cron poll)
seiche sim --seeds 100 --symbols 32 --steps 240 --json

# 3. Run shadow replay over intra-hour spikes vs hourly cron polling
seiche shadow --json

# 4. Prove algebraic Delta & DAG invariants
seiche prove --json

# 5. Exact integer Kalshi quadratic taker fee (centicents)
seiche fee --qty-h 500 --price-cc 9200 --json
```

---

## Invariants Verified on Every Stabilization

- **I-1 (Topological Height):** `child.height > parent.height` for every edge in the arena.
- **I-2 (Heap Bijection):** `DirtyHeap` contains all and only nodes with `is_dirty == true`.
- **I-3 (Post-Stabilize Cleanliness):** Immediately after `stabilize()`, zero nodes remain dirty and `DirtyHeap` is empty.
- **I-4 (Incremental Fold Completeness):** `IncrFoldSum` drains all `changed_indices` in `O(changed_parents)`.
- **I-5 (Differential Equivalence):** Every `IncrFoldSum` node's value equals the exact `O(N)` sum of its parents' current values.
- **S3 (Determinism):** Identical seeds produce bit-identical state hashes (`0x272581325173f554` across 24,000 VOPR transitions).
- **S4 (Output Batching):** No output deltas are emitted until `stabilize()` finishes.

---

## License

MIT — see [LICENSE](LICENSE).
