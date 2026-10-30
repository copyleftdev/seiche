//! Deterministic Simulation Testing (DST / VOPR) for `seiche`.
//!
//! Models an enclosed Kalshi multi-bracket series + open harvest positions driven by:
//! - Seeded SplitMix64 PRNG (zero external dependencies, 100% reproducible across platforms)
//! - Out-of-order, duplicated, and dropped WebSocket `orderbook_delta` packets routed through
//!   `DeltaBuffer`
//! - Anchor ticks with sub-cent noise filtered by `Cutoff::Band`
//! - Differential equivalence checks at EVERY tick:
//!   1. `IncrFoldSum` (`O(changed)`) == `FullFoldSum` (`O(N)`)
//!   2. Accumulated `Delta::compose` output state == terminal node `QuoteState`
//!   3. All 5 DAG invariants (`I-1..I-5`, `S3`, `S4`)
//! - Compares Event-Driven Seiche Harvest vs Hourly Cron Polling across seeded price paths.

use crate::cutoff::Cutoff;
use crate::delta::{Delta, QuoteState};
use crate::delta_buffer::{DeltaBuffer, ReceiveResult};
use crate::graph::{Graph, LeafHandle, NodeId};

#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline(always)]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[inline(always)]
    pub fn range_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() as usize) % bound
    }

    #[inline(always)]
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        let span = (hi - lo + 1) as u64;
        lo + (self.next_u64() % span) as i64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    pub seed: u64,
    pub steps: usize,
    pub symbols: usize,
    pub total_nodes: usize,
    pub total_recomputes: u64,
    pub avg_recomputes_per_step_milli: u64,
    pub output_deltas_emitted: u64,
    pub ws_packets_delivered: u64,
    pub ws_gaps_detected: u64,
    pub ws_resyncs: u64,
    pub seiche_harvest_exits: u32,
    pub cron_60m_harvest_exits: u32,
    pub seiche_pnl_cc: i64,
    pub cron_60m_pnl_cc: i64,
    pub state_hash: u64,
}

/// Run one deterministic VOPR simulation world for `seed`, checking all invariants at every step.
pub fn run_vopr_world(seed: u64, symbols: usize, steps: usize) -> SimReport {
    assert!(symbols >= 4 && steps >= 1);
    let mut rng = SplitMix64::new(seed);
    let mut g = Graph::with_capacity(symbols * 4 + 8);

    // 1. Build a realistic Kalshi multi-market DAG:
    //    - `anchor_leaf`: external oracle / sportsbook fair anchor with 25cc (0.25c) noise cutoff
    //    - `bid_leaves[i]`: Kalshi live orderbook best bid per strike
    //    - `maker_quotes[i]`: Height-1 MakerQuote nodes
    //    - `harvest_triggers[i]`: Height-1 HarvestTrigger nodes (entry = 3800cc = 38c, qty = 5.00, buffer = 50cc)
    //    - `incr_sum` vs `full_sum`: Height-2 O(1) vs O(N) folds over all `maker_quotes`
    let anchor_leaf = g.add_leaf("external_anchor_cc", 4200, Cutoff::Band(25));
    let mut bid_leaves: Vec<LeafHandle> = Vec::with_capacity(symbols);
    let mut maker_nodes: Vec<NodeId> = Vec::with_capacity(symbols);
    let mut harvest_nodes: Vec<NodeId> = Vec::with_capacity(symbols);

    for i in 0..symbols {
        let bl = g.add_leaf(&format!("bid_{}", i), 3700, Cutoff::ExactEqual);
        let mq = g.add_maker_quote(
            &format!("quote_{}", i),
            g.leaf_node(anchor_leaf),
            g.leaf_node(bl),
            100, // 1.00c min maker edge
            500, // 5 contracts
            Cutoff::ExactEqual,
        );
        g.mark_output(mq, 500);
        let ht = g.add_harvest_trigger(
            &format!("harvest_{}", i),
            g.leaf_node(bl),
            3800, // 38.00c entry price
            500,  // 5.00 contracts
            50,   // 0.50c net harvest buffer
            Cutoff::ExactEqual,
        );
        g.mark_output(ht, 500);
        bid_leaves.push(bl);
        maker_nodes.push(mq);
        harvest_nodes.push(ht);
    }

    let incr_sum = g.add_incr_fold_sum("incr_quote_sum", &maker_nodes, Cutoff::ExactEqual);
    let full_sum = g.add_full_fold_sum("full_quote_sum", &maker_nodes, Cutoff::ExactEqual);
    g.assert_invariants(true);
    assert_eq!(g.watch(incr_sum), g.watch(full_sum));

    // Track shadowed quote state via Delta::compose to verify S4 + Delta algebra equivalence
    let mut composed_quotes: Vec<QuoteState> = maker_nodes
        .iter()
        .map(|&nid| QuoteState::new(g.watch(nid), 500))
        .collect();

    // Track Seiche event-driven harvest vs 60-step (hourly) cron harvest
    let mut seiche_exited = vec![false; symbols];
    let mut cron_exited = vec![false; symbols];
    let mut seiche_pnl_cc = 0i64;
    let mut cron_pnl_cc = 0i64;

    // WebSocket sequence-gap buffer for market 0
    let mut ws_buf: DeltaBuffer<(usize, i64)> = DeltaBuffer::new(1, 32, 3);
    let mut next_ws_seq = 1u64;

    let mut total_recomputes = 0u64;
    let mut output_deltas_emitted = 0u64;

    for step in 1..=steps {
        let roll = rng.range_usize(100);
        if roll < 20 {
            // 20%: External anchor tick (sometimes sub-cutoff noise, sometimes real jump)
            let cur = g.watch(g.leaf_node(anchor_leaf));
            let delta_cc = rng.range_i64(-150, 150);
            let next_v = (cur + delta_cc).clamp(1500, 8500);
            g.set_leaf(anchor_leaf, next_v);
        } else if roll < 85 {
            // 65%: Single-market orderbook bid tick (may briefly spike above harvest target 3980cc)
            let sym = rng.range_usize(symbols);
            let cur = g.watch(g.leaf_node(bid_leaves[sym]));
            // Mean-reverting intraday spike/dip around 3700cc
            let revert = (3700 - cur) / 3;
            let shock = rng.range_i64(-250, 320);
            let next_bid = (cur + revert + shock).clamp(500, 9500);
            g.set_leaf(bid_leaves[sym], next_bid);
        } else {
            // 15%: Simulated WebSocket packet fault injection (out-of-order / duplicate / gap)
            let sym = rng.range_usize(symbols);
            let next_bid = rng.range_i64(3200, 4400);
            let fault = rng.range_usize(3);
            let seq_to_send = match fault {
                0 => next_ws_seq.saturating_sub(1).max(1), // duplicate old seq
                1 => next_ws_seq + 1,                      // out-of-order gap
                _ => {
                    let s = next_ws_seq;
                    next_ws_seq += 1;
                    s
                }
            };
            match ws_buf.receive(seq_to_send, (sym, next_bid)) {
                ReceiveResult::Deliver(items) => {
                    for (s_idx, b_val) in items {
                        g.set_leaf(bid_leaves[s_idx], b_val);
                    }
                }
                ReceiveResult::GapDetected { missing_seq, .. } => {
                    // Feed the missing seq on next opportunity half the time
                    if rng.range_usize(2) == 0 {
                        if let ReceiveResult::Deliver(items) =
                            ws_buf.receive(missing_seq, (sym, next_bid))
                        {
                            for (s_idx, b_val) in items {
                                g.set_leaf(bid_leaves[s_idx], b_val);
                            }
                            if next_ws_seq <= missing_seq {
                                next_ws_seq = ws_buf.next_expected_seq();
                            }
                        }
                    }
                }
                ReceiveResult::ResyncRequired { .. } => {
                    ws_buf.reset_at_snapshot(next_ws_seq);
                    next_ws_seq += 1;
                }
            }
            ws_buf.assert_invariants();
        }

        // Stabilize the DAG
        let events = g.stabilize();
        total_recomputes += g.last_recompute_count as u64;
        output_deltas_emitted += events.len() as u64;

        // Verify all 5 DAG invariants + differential O(1) vs O(N) fold equality
        g.assert_invariants(true);
        assert_eq!(
            g.watch(incr_sum),
            g.watch(full_sum),
            "Differential fold mismatch at seed {} step {}",
            seed,
            step
        );

        // Apply emitted output deltas & record Seiche event-driven harvest exits
        for ev in &events {
            if let Some(q_idx_str) = ev.name.strip_prefix("quote_") {
                if let Ok(q_idx) = q_idx_str.parse::<usize>() {
                    // Compose with identity to verify algebraic delta application matches watch()
                    let d = Delta::NoOp.compose(ev.delta);
                    composed_quotes[q_idx] = d.apply(composed_quotes[q_idx]);
                    let expected = QuoteState::new(g.watch(maker_nodes[q_idx]), 500);
                    assert_eq!(
                        composed_quotes[q_idx], expected,
                        "Delta algebra state drift at seed {} step {} quote_{}",
                        seed, step, q_idx
                    );
                }
            } else if let Some(h_idx_str) = ev.name.strip_prefix("harvest_") {
                if let Ok(h_idx) = h_idx_str.parse::<usize>() {
                    if !seiche_exited[h_idx] && ev.new_value > 0 {
                        seiche_exited[h_idx] = true;
                        let bid_cc = ev.new_value;
                        let fee_cc = crate::graph::kalshi_taker_fee_cc(500, bid_cc);
                        let net_cc = 5 * (bid_cc - 3800) - fee_cc;
                        seiche_pnl_cc += net_cc;
                    }
                }
            }
        }

        // Compare against 60-step (hourly) batch cron poll
        if step % 60 == 0 {
            for sym in 0..symbols {
                if !cron_exited[sym] {
                    let trig_px = g.watch(harvest_nodes[sym]);
                    if trig_px > 0 {
                        cron_exited[sym] = true;
                        let fee_cc = crate::graph::kalshi_taker_fee_cc(500, trig_px);
                        let net_cc = 5 * (trig_px - 3800) - fee_cc;
                        cron_pnl_cc += net_cc;
                    }
                }
            }
        }
    }

    // Settle any un-exited positions at terminal step (losers settle at 0 -> -5 * 3800cc = -19_000cc)
    for sym in 0..symbols {
        let terminal_bid = g.watch(g.leaf_node(bid_leaves[sym]));
        let won_at_expiry = terminal_bid >= 5000;
        let hold_pnl_cc = if won_at_expiry {
            5 * (10_000 - 3800)
        } else {
            -5 * 3800
        };
        if !seiche_exited[sym] {
            seiche_pnl_cc += hold_pnl_cc;
        }
        if !cron_exited[sym] {
            cron_pnl_cc += hold_pnl_cc;
        }
    }

    // Deterministic FNV-1a state hash over all node values
    let mut state_hash = 0xcbf2_9ce4_8422_2325u64;
    for i in 0..g.node_count() {
        let v = g.watch(NodeId(i as u32)) as u64;
        state_hash ^= v;
        state_hash = state_hash.wrapping_mul(0x0100_0000_01b3);
    }

    SimReport {
        seed,
        steps,
        symbols,
        total_nodes: g.node_count(),
        total_recomputes,
        avg_recomputes_per_step_milli: (total_recomputes * 1000) / (steps as u64),
        output_deltas_emitted,
        ws_packets_delivered: ws_buf.delivered_count,
        ws_gaps_detected: ws_buf.gap_count,
        ws_resyncs: ws_buf.resync_count,
        seiche_harvest_exits: seiche_exited.iter().filter(|&&x| x).count() as u32,
        cron_60m_harvest_exits: cron_exited.iter().filter(|&&x| x).count() as u32,
        seiche_pnl_cc,
        cron_60m_pnl_cc: cron_pnl_cc,
        state_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vopr_determinism_same_seed_produces_identical_state_hash() {
        let r1 = run_vopr_world(20261029, 32, 300);
        let r2 = run_vopr_world(20261029, 32, 300);
        assert_eq!(r1, r2);
    }

    #[test]
    fn vopr_multi_seed_campaign_holds_all_invariants_and_beats_cron_poll() {
        let mut total_seiche_exits = 0u32;
        let mut total_cron_exits = 0u32;
        let mut total_seiche_pnl = 0i64;
        let mut total_cron_pnl = 0i64;
        let mut total_gaps = 0u64;

        for seed in 1..=100 {
            let rep = run_vopr_world(seed, 24, 180);
            total_seiche_exits += rep.seiche_harvest_exits;
            total_cron_exits += rep.cron_60m_harvest_exits;
            total_seiche_pnl += rep.seiche_pnl_cc;
            total_cron_pnl += rep.cron_60m_pnl_cc;
            total_gaps += rep.ws_gaps_detected;
        }

        // Non-vacuity guards: fault injection actually triggered gaps, and event-driven
        // Seiche caught strictly more transient intra-hour harvest exits than 60m cron polling.
        assert!(total_gaps > 0, "Vacuous test: zero WS gaps triggered");
        assert!(
            total_seiche_exits > total_cron_exits,
            "Seiche exits ({}) must exceed 60m cron exits ({})",
            total_seiche_exits,
            total_cron_exits
        );
        assert!(
            total_seiche_pnl > total_cron_pnl,
            "Seiche PnL ({}) must exceed 60m cron PnL ({})",
            total_seiche_pnl,
            total_cron_pnl
        );
    }
}
