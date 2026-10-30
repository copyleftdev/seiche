//! Cutoff rules for stopping redundant propagation in the incremental DAG.
//!
//! Directly inspired by Jane Street's `Incremental.Cutoff` and Ripple's `cutoff.ml`,
//! specialized for integer centicent (`i64`, `$1.00 = 10_000 cc`) pricing nodes.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cutoff {
    /// Propagate whenever `old != new` (exact integer inequality).
    ExactEqual,
    /// Propagate only when `|new - old| >= band_cc` (hysteresis / deadband filter).
    /// Stops tiny spot flickers from waking downstream orderbook/quote nodes.
    Band(i64),
    /// Always propagate on every stabilization visit.
    Always,
    /// Never propagate (constant after initialization).
    Never,
}

impl Cutoff {
    #[inline(always)]
    pub fn should_propagate(self, old_val: i64, new_val: i64) -> bool {
        match self {
            Self::ExactEqual => old_val != new_val,
            Self::Band(band) => {
                debug_assert!(band >= 0, "Cutoff::Band must be non-negative");
                (new_val - old_val).abs() >= band
            }
            Self::Always => true,
            Self::Never => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_cutoff_blocks_subthreshold_noise() {
        let c = Cutoff::Band(50); // 0.5c deadband
        assert!(!c.should_propagate(4200, 4249));
        assert!(c.should_propagate(4200, 4250));
        assert!(c.should_propagate(4200, 4150));
    }
}
