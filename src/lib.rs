//! `seiche` — Height-ordered incremental computation DAG, cutoff propagation,
//! O(1) incremental folds, idempotent delta algebra, sequence-gap book reconstruction,
//! and VOPR simulation testing for prediction-market orderbooks.

pub mod cutoff;
pub mod delta;
pub mod delta_buffer;
pub mod dirty_heap;
pub mod dst;
pub mod graph;
pub mod serve;
pub mod shadow;
