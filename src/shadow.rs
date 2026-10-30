//! Live / replay shadow evaluation (`seiche shadow`):
//!
//! Feeds real Kalshi price snapshots (or 1-minute candlesticks / tapehound JSON)
//! through a `seiche` incremental DAG (`Leaf -> HarvestTrigger -> IncrFoldSum`)
//! and compares event-driven `seiche` triggers against 60-minute batch cron polling.

use crate::cutoff::Cutoff;
use crate::graph::{kalshi_taker_fee_cc, Graph, LeafHandle, NodeId};

#[derive(Debug, Clone)]
pub struct PositionSpec {
    pub ticker: String,
    pub entry_px_cc: i64,
    pub qty_h: i64,
    pub buffer_cc: i64,
}

#[derive(Debug, Clone)]
pub struct TickSample {
    pub ts_sec: u64,
    pub ticker_idx: usize,
    pub bid_cc: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowComparison {
    pub positions: usize,
    pub ticks_processed: usize,
    pub nodes_in_dag: usize,
    pub total_recomputes: usize,
    pub seiche_triggers: usize,
    pub cron_60m_triggers: usize,
    pub seiche_triggered_tickers: Vec<String>,
}

pub fn run_shadow_session(positions: &[PositionSpec], ticks: &[TickSample]) -> ShadowComparison {
    assert!(!positions.is_empty(), "positions must not be empty");
    let n = positions.len();
    let mut g = Graph::with_capacity(n * 3 + 4);

    let mut bid_leaves: Vec<LeafHandle> = Vec::with_capacity(n);
    let mut trig_nodes: Vec<NodeId> = Vec::with_capacity(n);

    for pos in positions {
        let bl = g.add_leaf(&format!("bid:{}", pos.ticker), 0, Cutoff::ExactEqual);
        let ht = g.add_harvest_trigger(
            &format!("harvest:{}", pos.ticker),
            g.leaf_node(bl),
            pos.entry_px_cc,
            pos.qty_h,
            pos.buffer_cc,
            Cutoff::ExactEqual,
        );
        g.mark_output(ht, pos.qty_h);
        bid_leaves.push(bl);
        trig_nodes.push(ht);
    }

    let _active_sum = g.add_incr_fold_sum("active_harvest_sum_cc", &trig_nodes, Cutoff::ExactEqual);
    g.assert_invariants(true);

    let mut seiche_fired = vec![false; n];
    let mut cron_fired = vec![false; n];
    let mut total_recomputes = 0usize;
    let mut last_hour_bucket: Option<u64> = None;

    for tick in ticks {
        if tick.ticker_idx < n {
            g.set_leaf(bid_leaves[tick.ticker_idx], tick.bid_cc);
            let events = g.stabilize();
            total_recomputes += g.last_recompute_count;
            g.assert_invariants(true);

            for ev in events {
                if ev.new_value > 0 {
                    if let Some(tk) = ev.name.strip_prefix("harvest:") {
                        if let Some(idx) = positions.iter().position(|p| p.ticker == tk) {
                            let fee_cc = kalshi_taker_fee_cc(positions[idx].qty_h, ev.new_value);
                            let gross_cc = ((ev.new_value - positions[idx].entry_px_cc) as i128
                                * (positions[idx].qty_h as i128)
                                / 100) as i64;
                            assert!(
                                gross_cc - fee_cc
                                    >= ((positions[idx].buffer_cc as i128
                                        * positions[idx].qty_h as i128)
                                        / 100) as i64
                            );
                            seiche_fired[idx] = true;
                        }
                    }
                }
            }
        }

        let hour_bucket = tick.ts_sec / 3600;
        if last_hour_bucket != Some(hour_bucket) {
            last_hour_bucket = Some(hour_bucket);
            for idx in 0..n {
                if g.watch(trig_nodes[idx]) > 0 {
                    cron_fired[idx] = true;
                }
            }
        }
    }

    let seiche_triggered_tickers: Vec<String> = positions
        .iter()
        .enumerate()
        .filter(|&(i, _)| seiche_fired[i])
        .map(|(_, p)| p.ticker.clone())
        .collect();

    ShadowComparison {
        positions: n,
        ticks_processed: ticks.len(),
        nodes_in_dag: g.node_count(),
        total_recomputes,
        seiche_triggers: seiche_fired.iter().filter(|&&x| x).count(),
        cron_60m_triggers: cron_fired.iter().filter(|&&x| x).count(),
        seiche_triggered_tickers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_session_catches_mid_hour_spike_missed_by_hourly_cron() {
        let positions = vec![
            PositionSpec {
                ticker: "KXNHLGAME-26OCT29BUFANA-ANA".to_string(),
                entry_px_cc: 4300,
                qty_h: 500,
                buffer_cc: 50,
            },
            PositionSpec {
                ticker: "KXHIGHCHI-26OCT29-B72.5".to_string(),
                entry_px_cc: 8700,
                qty_h: 1100,
                buffer_cc: 50,
            },
        ];
        // At t=3600 (top of hour), bid is 4200 (below entry).
        // At t=4500 (15m into hour), bid spikes to 4600 (clears 4300 + 173cc fee + 50cc = 4523cc).
        // At t=5400 (30m into hour), bid fades back to 4100 before t=7200 top-of-hour poll.
        let ticks = vec![
            TickSample {
                ts_sec: 3600,
                ticker_idx: 0,
                bid_cc: 4200,
            },
            TickSample {
                ts_sec: 4500,
                ticker_idx: 0,
                bid_cc: 4600,
            },
            TickSample {
                ts_sec: 5400,
                ticker_idx: 0,
                bid_cc: 4100,
            },
            TickSample {
                ts_sec: 7200,
                ticker_idx: 0,
                bid_cc: 4100,
            },
        ];
        let cmp = run_shadow_session(&positions, &ticks);
        assert_eq!(cmp.seiche_triggers, 1);
        assert_eq!(cmp.cron_60m_triggers, 0);
        assert_eq!(
            cmp.seiche_triggered_tickers,
            vec!["KXNHLGAME-26OCT29BUFANA-ANA".to_string()]
        );
    }
}
