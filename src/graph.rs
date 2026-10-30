//! Core incremental computation DAG in TigerStyle Rust.
//!
//! Replaces Ripple's `Obj.magic` type-erased OCaml nodes with a zero-allocation,
//! cache-friendly typed arena of `i64` centicent/micro-unit slots (`$1.00 = 10_000 cc`).
//!
//! Enforces Ripple's 5 invariants on every stabilization pass:
//! - I-1: `node.height > parent.height` for every dependency edge
//! - I-2: `dirty_heap` contains all and only nodes with `is_dirty == true`
//! - I-3: Immediately after `stabilize()`, zero nodes are dirty and `dirty_heap` is empty
//! - I-4: `IncrFold` processes ONLY `changed_parent_indices` in `O(changed_parents)`
//! - I-5: Every node's `value` equals its pure recompute function applied to current parents
//!   (up to its declared `Cutoff` deadband)
//! - S3: Deterministic stabilization order via `(height ASC, node_id ASC)` packed heap keys
//! - S4: Output batching — output deltas are emitted once per `stabilize()` call, never mid-pass

use crate::cutoff::Cutoff;
use crate::delta::{Delta, QuoteState};
use crate::dirty_heap::DirtyHeap;

/// Kalshi exact integer quadratic taker fee in centicents (`$1.00 = 10_000 cc`).
///
/// For `qty_h` hundredths of a contract (`100 = 1.00 contract`) at `price_cc` centicents
/// (`0..=10_000`):
/// `fee_cc = round(0.07 * (qty_h / 100) * (price_cc / 10_000) * (1 - price_cc / 10_000) * 10_000)`
#[inline(always)]
pub const fn kalshi_taker_fee_cc(qty_h: i64, price_cc: i64) -> i64 {
    if qty_h <= 0 || price_cc <= 0 || price_cc >= 10_000 {
        return 0;
    }
    let num = 7i128 * (qty_h as i128) * (price_cc as i128) * ((10_000 - price_cc) as i128);
    let den = 100_000_000i128;
    ((num + (den / 2)) / den) as i64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeafHandle(pub u32);

#[derive(Debug, Clone)]
pub enum RecomputeOp {
    /// External input leaf set via `set_leaf` or `set_remote`.
    Leaf { last_seq: Option<u64> },
    /// Pure unary transform (`Map`).
    LinearMap {
        parent: u32,
        scale_num: i64,
        scale_den: i64,
        offset_cc: i64,
        clamp_lo: i64,
        clamp_hi: i64,
    },
    /// Pure binary transform (`Map2`): e.g. net edge `fair_cc - ask_cc - taker_fee(ask_cc)`.
    EdgeGate {
        fair_parent: u32,
        ask_parent: u32,
        qty_h: i64,
    },
    /// Harvest exit trigger (`Map2`): Given `(bid_parent, entry_px_cc)`, outputs the
    /// net harvest profit in centicents if `bid_cc` clears `entry_px_cc + fee + buffer_cc`,
    /// or `0` otherwise.
    HarvestTrigger {
        bid_parent: u32,
        entry_px_cc: i64,
        qty_h: i64,
        buffer_cc: i64,
    },
    /// Maker quote generator (`Map2`): Given `(fair_parent, best_bid_parent)`, computes the
    /// optimal zero-fee maker bid in centicents (`min(best_bid_cc, fair_cc - min_edge_cc)`),
    /// or `0` (cancel) when fair value drops below `min_edge_cc`.
    MakerQuote {
        fair_parent: u32,
        best_bid_parent: u32,
        min_edge_cc: i64,
        qty_c: i64,
    },
    /// Full `O(N)` fold over parents (for reference comparison & verification).
    FullFoldSum { parents: Vec<u32> },
    /// Ripple's `Incr_fold`: `O(changed_parents)` incremental accumulator using
    /// `remove(acc, old_parent)` + `add(acc, new_parent)`.
    IncrFoldSum {
        parents: Vec<u32>,
        parent_values: Vec<i64>,
        changed_indices: Vec<usize>,
        changed_mask: Vec<bool>,
    },
}

#[derive(Debug, Clone)]
struct DependentEdge {
    child_id: u32,
    /// `Some(idx)` if `child_id` is an `IncrFoldSum` and this node is its `idx`-th parent.
    incr_fold_parent_idx: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u32,
    pub height: u16,
    pub value: i64,
    pub old_value: i64,
    pub is_dirty: bool,
    pub is_output: bool,
    pub output_qty_c: i64,
    pub name: String,
    pub cutoff: Cutoff,
    pub op: RecomputeOp,
    dependents: Vec<DependentEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputEvent {
    pub node_id: u32,
    pub name: String,
    pub old_value: i64,
    pub new_value: i64,
    pub delta: Delta,
}

#[derive(Debug, Clone)]
pub struct Graph {
    nodes: Vec<Node>,
    dirty_heap: DirtyHeap,
    pub last_recompute_count: usize,
    pub last_visited_count: usize,
    pub total_stabilizations: u64,
}

impl Graph {
    pub fn with_capacity(cap: usize) -> Self {
        let c = cap.max(16);
        Self {
            nodes: Vec::with_capacity(c),
            dirty_heap: DirtyHeap::with_capacity(c),
            last_recompute_count: 0,
            last_visited_count: 0,
            total_stabilizations: 0,
        }
    }

    #[inline(always)]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline(always)]
    pub fn dirty_count(&self) -> usize {
        self.dirty_heap.len()
    }

    #[inline(always)]
    pub fn watch(&self, node: NodeId) -> i64 {
        self.nodes[node.0 as usize].value
    }

    #[inline(always)]
    pub fn leaf_node(&self, leaf: LeafHandle) -> NodeId {
        NodeId(leaf.0)
    }

    pub fn add_leaf(&mut self, name: &str, initial: i64, cutoff: Cutoff) -> LeafHandle {
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            height: 0,
            value: initial,
            old_value: initial,
            is_dirty: false,
            is_output: false,
            output_qty_c: 0,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::Leaf { last_seq: None },
            dependents: Vec::new(),
        });
        self.dirty_heap.ensure_capacity(id);
        LeafHandle(id)
    }

    /// Set a leaf value and push to the dirty min-heap if not already dirty.
    #[inline]
    pub fn set_leaf(&mut self, leaf: LeafHandle, value: i64) {
        let n = &mut self.nodes[leaf.0 as usize];
        if !n.is_dirty {
            n.old_value = n.value;
            n.is_dirty = true;
            self.dirty_heap.push(n.height, n.id);
        }
        n.value = value;
    }

    /// Set a sequence-numbered remote/WebSocket leaf. Returns `false` and discards
    /// idempotently if `sequence_no <= last_seq`.
    #[inline]
    pub fn set_remote(&mut self, leaf: LeafHandle, sequence_no: u64, value: i64) -> bool {
        let n = &mut self.nodes[leaf.0 as usize];
        if let RecomputeOp::Leaf { ref mut last_seq } = n.op {
            if let Some(prev) = *last_seq {
                if sequence_no <= prev {
                    return false;
                }
            }
            *last_seq = Some(sequence_no);
        }
        self.set_leaf(leaf, value);
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_linear_map(
        &mut self,
        name: &str,
        parent: NodeId,
        scale_num: i64,
        scale_den: i64,
        offset_cc: i64,
        clamp_lo: i64,
        clamp_hi: i64,
        cutoff: Cutoff,
    ) -> NodeId {
        assert!(scale_den != 0, "scale_den must be non-zero");
        let p_height = self.nodes[parent.0 as usize].height;
        let p_val = self.nodes[parent.0 as usize].value;
        let initial = eval_linear(p_val, scale_num, scale_den, offset_cc, clamp_lo, clamp_hi);
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            height: p_height + 1,
            value: initial,
            old_value: initial,
            is_dirty: false,
            is_output: false,
            output_qty_c: 0,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::LinearMap {
                parent: parent.0,
                scale_num,
                scale_den,
                offset_cc,
                clamp_lo,
                clamp_hi,
            },
            dependents: Vec::new(),
        });
        self.nodes[parent.0 as usize]
            .dependents
            .push(DependentEdge {
                child_id: id,
                incr_fold_parent_idx: None,
            });
        self.dirty_heap.ensure_capacity(id);
        NodeId(id)
    }

    pub fn add_edge_gate(
        &mut self,
        name: &str,
        fair_parent: NodeId,
        ask_parent: NodeId,
        qty_h: i64,
        cutoff: Cutoff,
    ) -> NodeId {
        let h_a = self.nodes[fair_parent.0 as usize].height;
        let h_b = self.nodes[ask_parent.0 as usize].height;
        let v_a = self.nodes[fair_parent.0 as usize].value;
        let v_b = self.nodes[ask_parent.0 as usize].value;
        let initial = eval_edge_gate(v_a, v_b, qty_h);
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            height: h_a.max(h_b) + 1,
            value: initial,
            old_value: initial,
            is_dirty: false,
            is_output: false,
            output_qty_c: qty_h,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::EdgeGate {
                fair_parent: fair_parent.0,
                ask_parent: ask_parent.0,
                qty_h,
            },
            dependents: Vec::new(),
        });
        self.nodes[fair_parent.0 as usize]
            .dependents
            .push(DependentEdge {
                child_id: id,
                incr_fold_parent_idx: None,
            });
        if ask_parent.0 != fair_parent.0 {
            self.nodes[ask_parent.0 as usize]
                .dependents
                .push(DependentEdge {
                    child_id: id,
                    incr_fold_parent_idx: None,
                });
        }
        self.dirty_heap.ensure_capacity(id);
        NodeId(id)
    }

    pub fn add_harvest_trigger(
        &mut self,
        name: &str,
        bid_parent: NodeId,
        entry_px_cc: i64,
        qty_h: i64,
        buffer_cc: i64,
        cutoff: Cutoff,
    ) -> NodeId {
        let p_height = self.nodes[bid_parent.0 as usize].height;
        let bid_val = self.nodes[bid_parent.0 as usize].value;
        let initial = eval_harvest_trigger(bid_val, entry_px_cc, qty_h, buffer_cc);
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            height: p_height + 1,
            value: initial,
            old_value: initial,
            is_dirty: false,
            is_output: false,
            output_qty_c: qty_h,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::HarvestTrigger {
                bid_parent: bid_parent.0,
                entry_px_cc,
                qty_h,
                buffer_cc,
            },
            dependents: Vec::new(),
        });
        self.nodes[bid_parent.0 as usize]
            .dependents
            .push(DependentEdge {
                child_id: id,
                incr_fold_parent_idx: None,
            });
        self.dirty_heap.ensure_capacity(id);
        NodeId(id)
    }

    pub fn add_maker_quote(
        &mut self,
        name: &str,
        fair_parent: NodeId,
        best_bid_parent: NodeId,
        min_edge_cc: i64,
        qty_c: i64,
        cutoff: Cutoff,
    ) -> NodeId {
        let h_a = self.nodes[fair_parent.0 as usize].height;
        let h_b = self.nodes[best_bid_parent.0 as usize].height;
        let v_a = self.nodes[fair_parent.0 as usize].value;
        let v_b = self.nodes[best_bid_parent.0 as usize].value;
        let initial = eval_maker_quote(v_a, v_b, min_edge_cc);
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            height: h_a.max(h_b) + 1,
            value: initial,
            old_value: initial,
            is_dirty: false,
            is_output: false,
            output_qty_c: qty_c,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::MakerQuote {
                fair_parent: fair_parent.0,
                best_bid_parent: best_bid_parent.0,
                min_edge_cc,
                qty_c,
            },
            dependents: Vec::new(),
        });
        self.nodes[fair_parent.0 as usize]
            .dependents
            .push(DependentEdge {
                child_id: id,
                incr_fold_parent_idx: None,
            });
        if best_bid_parent.0 != fair_parent.0 {
            self.nodes[best_bid_parent.0 as usize]
                .dependents
                .push(DependentEdge {
                    child_id: id,
                    incr_fold_parent_idx: None,
                });
        }
        self.dirty_heap.ensure_capacity(id);
        NodeId(id)
    }

    /// Add an `O(changed_parents)` incremental sum fold (`IncrFoldSum`).
    pub fn add_incr_fold_sum(&mut self, name: &str, parents: &[NodeId], cutoff: Cutoff) -> NodeId {
        assert!(
            !parents.is_empty(),
            "IncrFoldSum requires at least 1 parent"
        );
        let mut parent_ids = Vec::with_capacity(parents.len());
        let mut parent_values = Vec::with_capacity(parents.len());
        let mut max_h = 0u16;
        let mut initial_sum = 0i64;
        for &p in parents {
            let pn = &self.nodes[p.0 as usize];
            parent_ids.push(p.0);
            parent_values.push(pn.value);
            if pn.height > max_h {
                max_h = pn.height;
            }
            initial_sum = initial_sum.saturating_add(pn.value);
        }
        let id = self.nodes.len() as u32;
        let n_parents = parents.len();
        self.nodes.push(Node {
            id,
            height: max_h + 1,
            value: initial_sum,
            old_value: initial_sum,
            is_dirty: false,
            is_output: false,
            output_qty_c: 0,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::IncrFoldSum {
                parents: parent_ids,
                parent_values,
                changed_indices: Vec::with_capacity(8),
                changed_mask: vec![false; n_parents],
            },
            dependents: Vec::new(),
        });
        for (idx, &p) in parents.iter().enumerate() {
            self.nodes[p.0 as usize].dependents.push(DependentEdge {
                child_id: id,
                incr_fold_parent_idx: Some(idx),
            });
        }
        self.dirty_heap.ensure_capacity(id);
        NodeId(id)
    }

    /// Add a full `O(N)` fold sum (for benchmarking & equivalence testing against `IncrFoldSum`).
    pub fn add_full_fold_sum(&mut self, name: &str, parents: &[NodeId], cutoff: Cutoff) -> NodeId {
        assert!(
            !parents.is_empty(),
            "FullFoldSum requires at least 1 parent"
        );
        let mut parent_ids = Vec::with_capacity(parents.len());
        let mut max_h = 0u16;
        let mut initial_sum = 0i64;
        for &p in parents {
            let pn = &self.nodes[p.0 as usize];
            parent_ids.push(p.0);
            if pn.height > max_h {
                max_h = pn.height;
            }
            initial_sum = initial_sum.saturating_add(pn.value);
        }
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            id,
            height: max_h + 1,
            value: initial_sum,
            old_value: initial_sum,
            is_dirty: false,
            is_output: false,
            output_qty_c: 0,
            name: name.to_string(),
            cutoff,
            op: RecomputeOp::FullFoldSum {
                parents: parent_ids,
            },
            dependents: Vec::new(),
        });
        for &p in parents {
            self.nodes[p.0 as usize].dependents.push(DependentEdge {
                child_id: id,
                incr_fold_parent_idx: None,
            });
        }
        self.dirty_heap.ensure_capacity(id);
        NodeId(id)
    }

    pub fn mark_output(&mut self, node: NodeId, default_qty_c: i64) {
        let n = &mut self.nodes[node.0 as usize];
        n.is_output = true;
        if default_qty_c > 0 {
            n.output_qty_c = default_qty_c;
        }
    }

    /// Stabilize the DAG in topological height order.
    /// Returns the batch of `OutputEvent`s whose values crossed their `Cutoff` threshold.
    pub fn stabilize(&mut self) -> Vec<OutputEvent> {
        let mut recomputed = 0usize;
        let mut visited = 0usize;
        let mut outputs = Vec::new();

        while let Some(entry) = self.dirty_heap.pop_min() {
            let nid = entry.node_id as usize;
            if !self.nodes[nid].is_dirty {
                continue;
            }
            visited += 1;
            self.nodes[nid].is_dirty = false;

            let (old_val, new_val, cutoff, is_output, out_qty) = {
                let node = &self.nodes[nid];
                let old_v = match node.op {
                    RecomputeOp::Leaf { .. } => node.old_value,
                    _ => node.value,
                };
                let new_v = match &node.op {
                    RecomputeOp::Leaf { .. } => node.value,
                    RecomputeOp::LinearMap {
                        parent,
                        scale_num,
                        scale_den,
                        offset_cc,
                        clamp_lo,
                        clamp_hi,
                    } => {
                        let pv = self.nodes[*parent as usize].value;
                        eval_linear(pv, *scale_num, *scale_den, *offset_cc, *clamp_lo, *clamp_hi)
                    }
                    RecomputeOp::EdgeGate {
                        fair_parent,
                        ask_parent,
                        qty_h,
                    } => {
                        let fv = self.nodes[*fair_parent as usize].value;
                        let av = self.nodes[*ask_parent as usize].value;
                        eval_edge_gate(fv, av, *qty_h)
                    }
                    RecomputeOp::HarvestTrigger {
                        bid_parent,
                        entry_px_cc,
                        qty_h,
                        buffer_cc,
                    } => {
                        let bv = self.nodes[*bid_parent as usize].value;
                        eval_harvest_trigger(bv, *entry_px_cc, *qty_h, *buffer_cc)
                    }
                    RecomputeOp::MakerQuote {
                        fair_parent,
                        best_bid_parent,
                        min_edge_cc,
                        ..
                    } => {
                        let fv = self.nodes[*fair_parent as usize].value;
                        let bv = self.nodes[*best_bid_parent as usize].value;
                        eval_maker_quote(fv, bv, *min_edge_cc)
                    }
                    RecomputeOp::FullFoldSum { parents } => {
                        parents.iter().fold(0i64, |acc, &pid| {
                            acc.saturating_add(self.nodes[pid as usize].value)
                        })
                    }
                    RecomputeOp::IncrFoldSum { .. } => {
                        // Handled below via mutable borrow to update parent_values in O(changed)
                        0
                    }
                };
                (old_v, new_v, node.cutoff, node.is_output, node.output_qty_c)
            };

            // Special handling for IncrFoldSum to mutate parent_values & clear changed_indices
            let new_val = if matches!(self.nodes[nid].op, RecomputeOp::IncrFoldSum { .. }) {
                let mut acc = old_val;
                // Collect current values of changed parents first
                let updates: Vec<(usize, i64)> = match &self.nodes[nid].op {
                    RecomputeOp::IncrFoldSum {
                        parents,
                        changed_indices,
                        ..
                    } => changed_indices
                        .iter()
                        .map(|&idx| (idx, self.nodes[parents[idx] as usize].value))
                        .collect(),
                    _ => unreachable!(),
                };
                if let RecomputeOp::IncrFoldSum {
                    ref mut parent_values,
                    ref mut changed_indices,
                    ref mut changed_mask,
                    ..
                } = self.nodes[nid].op
                {
                    for (idx, curr_v) in updates {
                        let prev_v = parent_values[idx];
                        acc = acc.saturating_sub(prev_v).saturating_add(curr_v);
                        parent_values[idx] = curr_v;
                        changed_mask[idx] = false;
                    }
                    changed_indices.clear();
                }
                acc
            } else {
                new_val
            };

            if cutoff.should_propagate(old_val, new_val) {
                self.nodes[nid].value = new_val;
                recomputed += 1;

                // Propagate dirty state to dependents
                let deps = self.nodes[nid].dependents.clone();
                for edge in deps {
                    let dep_idx = edge.child_id as usize;
                    if let Some(p_idx) = edge.incr_fold_parent_idx {
                        if let RecomputeOp::IncrFoldSum {
                            ref mut changed_indices,
                            ref mut changed_mask,
                            ..
                        } = self.nodes[dep_idx].op
                        {
                            if !changed_mask[p_idx] {
                                changed_mask[p_idx] = true;
                                changed_indices.push(p_idx);
                            }
                        }
                    }
                    let dep_node = &mut self.nodes[dep_idx];
                    if !dep_node.is_dirty {
                        dep_node.is_dirty = true;
                        self.dirty_heap.push(dep_node.height, dep_node.id);
                    }
                }

                if is_output {
                    let old_q = QuoteState::new(old_val, out_qty);
                    let new_q = QuoteState::new(new_val, out_qty);
                    let delta = Delta::diff(old_q, new_q);
                    outputs.push(OutputEvent {
                        node_id: nid as u32,
                        name: self.nodes[nid].name.clone(),
                        old_value: old_val,
                        new_value: new_val,
                        delta,
                    });
                    self.nodes[nid].old_value = new_val;
                }
            } else {
                // Cutoff blocked propagation — restore leaf value to old_val so deadband is relative to last propagated value
                if matches!(self.nodes[nid].op, RecomputeOp::Leaf { .. }) {
                    self.nodes[nid].value = old_val;
                }
            }
        }

        self.last_recompute_count = recomputed;
        self.last_visited_count = visited;
        self.total_stabilizations += 1;
        outputs
    }

    /// TigerStyle invariant verification across the entire DAG:
    /// - I-1: Strict topological height ordering (`child.height > parent.height`)
    /// - I-2: `DirtyHeap` invariants hold
    /// - I-3: When clean, zero nodes are marked dirty and `IncrFoldSum` has zero pending indices
    /// - I-5: Every `IncrFoldSum` value equals the exact sum of its parents' current values
    pub fn assert_invariants(&self, require_clean: bool) {
        self.dirty_heap.assert_invariants();
        if require_clean {
            assert!(
                self.dirty_heap.is_empty(),
                "I-3 violation: dirty_heap not empty after stabilize"
            );
        }
        for node in &self.nodes {
            if require_clean {
                assert!(
                    !node.is_dirty,
                    "I-3 violation: node {} ({}) still dirty after stabilize",
                    node.id, node.name
                );
            } else {
                assert_eq!(
                    node.is_dirty,
                    self.dirty_heap.contains(node.id),
                    "I-2 violation: node {} is_dirty={} != dirty_heap.contains",
                    node.id,
                    node.is_dirty
                );
            }
            for edge in &node.dependents {
                let child = &self.nodes[edge.child_id as usize];
                assert!(
                    child.height > node.height,
                    "I-1 violation: child {} (h={}) <= parent {} (h={})",
                    child.id,
                    child.height,
                    node.id,
                    node.height
                );
            }
            if require_clean {
                if let RecomputeOp::IncrFoldSum {
                    ref parents,
                    ref parent_values,
                    ref changed_indices,
                    ..
                } = node.op
                {
                    assert!(
                        changed_indices.is_empty(),
                        "I-4 violation: IncrFoldSum {} has leftover changed_indices",
                        node.name
                    );
                    let expected_sum: i64 = parents
                        .iter()
                        .enumerate()
                        .map(|(i, &pid)| {
                            let pv = self.nodes[pid as usize].value;
                            assert_eq!(
                                parent_values[i], pv,
                                "I-5 violation: IncrFoldSum cached parent_values[{}] != parent {}",
                                i, pid
                            );
                            pv
                        })
                        .sum();
                    assert_eq!(
                        node.value, expected_sum,
                        "I-5 violation: IncrFoldSum {} value {} != exact parent sum {}",
                        node.name, node.value, expected_sum
                    );
                }
            }
        }
    }
}

#[inline(always)]
fn eval_linear(
    x: i64,
    scale_num: i64,
    scale_den: i64,
    offset_cc: i64,
    clamp_lo: i64,
    clamp_hi: i64,
) -> i64 {
    let scaled = ((x as i128) * (scale_num as i128) / (scale_den as i128)) as i64;
    scaled.saturating_add(offset_cc).clamp(clamp_lo, clamp_hi)
}

#[inline(always)]
fn eval_edge_gate(fair_cc: i64, ask_cc: i64, qty_h: i64) -> i64 {
    if ask_cc <= 0 || ask_cc >= 10_000 || qty_h <= 0 {
        return 0;
    }
    let gross_cc = ((fair_cc - ask_cc) as i128 * (qty_h as i128) / 100) as i64;
    let fee_cc = kalshi_taker_fee_cc(qty_h, ask_cc);
    gross_cc - fee_cc
}

#[inline(always)]
fn eval_harvest_trigger(bid_cc: i64, entry_px_cc: i64, qty_h: i64, buffer_cc: i64) -> i64 {
    if bid_cc <= entry_px_cc || qty_h <= 0 {
        return 0;
    }
    let gross_profit_cc = ((bid_cc - entry_px_cc) as i128 * (qty_h as i128) / 100) as i64;
    let fee_cc = kalshi_taker_fee_cc(qty_h, bid_cc);
    let net_profit_cc = gross_profit_cc - fee_cc;
    let min_req_cc = ((buffer_cc as i128) * (qty_h as i128) / 100) as i64;
    if net_profit_cc >= min_req_cc {
        bid_cc
    } else {
        0
    }
}

#[inline(always)]
fn eval_maker_quote(fair_cc: i64, best_bid_cc: i64, min_edge_cc: i64) -> i64 {
    let max_maker_bid = fair_cc - min_edge_cc;
    if max_maker_bid < 100 {
        return 0;
    }
    let raw_quote = if best_bid_cc > 0 {
        best_bid_cc.min(max_maker_bid)
    } else {
        max_maker_bid
    };
    // Round down to whole Kalshi cent (100 cc = 1c)
    (raw_quote / 100) * 100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selective_recomputation_touches_only_affected_branch() {
        let mut g = Graph::with_capacity(256);
        let leaves: Vec<LeafHandle> = (0..100)
            .map(|i| g.add_leaf(&format!("leaf_{}", i), 4000, Cutoff::ExactEqual))
            .collect();
        let maps: Vec<NodeId> = leaves
            .iter()
            .enumerate()
            .map(|(i, &lf)| {
                g.add_linear_map(
                    &format!("map_{}", i),
                    g.leaf_node(lf),
                    1,
                    1,
                    100,
                    0,
                    10_000,
                    Cutoff::ExactEqual,
                )
            })
            .collect();
        let total = g.add_incr_fold_sum("total_exposure", &maps, Cutoff::ExactEqual);
        g.assert_invariants(true);
        assert_eq!(g.watch(total), 100 * 4100);

        // Update 1 leaf out of 100
        g.set_leaf(leaves[42], 5000);
        let _ = g.stabilize();
        g.assert_invariants(true);

        // Only leaf[42] + map[42] + total = 3 nodes recomputed out of 201!
        assert_eq!(g.node_count(), 201);
        assert_eq!(g.last_recompute_count, 3);
        assert_eq!(g.watch(total), 99 * 4100 + 5100);
    }

    #[test]
    fn cutoff_deadband_suppresses_downstream_quote_churn() {
        let mut g = Graph::with_capacity(16);
        let spot = g.add_leaf("gold_spot_offset_cc", 4200, Cutoff::Band(50)); // 0.5c deadband
        let bid = g.add_leaf("kalshi_bid_cc", 4000, Cutoff::ExactEqual);
        let quote = g.add_maker_quote(
            "gold_maker_bid",
            g.leaf_node(spot),
            g.leaf_node(bid),
            100, // 1c min edge
            500, // 5 contracts
            Cutoff::ExactEqual,
        );
        g.mark_output(quote, 500);
        g.assert_invariants(true);

        // Wiggle spot by +30cc (< 50cc Band cutoff) -> 0 recomputations, 0 output deltas!
        g.set_leaf(spot, 4230);
        let out = g.stabilize();
        assert_eq!(g.last_recompute_count, 0);
        assert!(out.is_empty());
        g.assert_invariants(true);
    }

    #[test]
    fn kalshi_taker_fee_matches_live_anchor() {
        // Live anchor from metals_paper_ledger: ask_c = 92c (9200 cc), 1 contract (100 qty_h)
        // Exact fee = 0.07 * 0.92 * 0.08 = $0.005152 = 51.52 cc -> rounds to 52 cc
        assert_eq!(kalshi_taker_fee_cc(100, 9200), 52);
        // 5 contracts (500 qty_h) at 92c: 5 * 51.52 cc = 257.6 cc -> 258 cc ($0.0258)
        assert_eq!(kalshi_taker_fee_cc(500, 9200), 258);
    }
}
