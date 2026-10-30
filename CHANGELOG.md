# Changelog

All notable changes to `seiche` are documented in this file.

## [0.1.0] - 2026-10-29

### Added
- `DirtyHeap`: Packed `u64` `(height, node_id)` binary min-heap with `O(1)` membership suppression.
- `Cutoff`: Integer centicent deadband hysteresis (`ExactEqual`, `Band`, `Always`, `Never`).
- `Delta` & `QuoteState`: Algebraic delta composition and diffing verified over 2,020 grid identities.
- `DeltaBuffer`: Sequence-gap orderbook buffer with duplicate suppression, out-of-order draining, and snapshot fallback.
- `Graph`: Zero-allocation arena DAG with `Leaf`, `LinearMap`, `EdgeGate`, `HarvestTrigger`, `MakerQuote`, `FullFoldSum`, and `IncrFoldSum`.
- `dst` & `shadow`: VOPR deterministic simulation harness and intra-hour harvest shadow evaluator.
- CLI subcommands (`bench`, `sim`, `shadow`, `prove`, `fee`) with `--json` output contracts.
