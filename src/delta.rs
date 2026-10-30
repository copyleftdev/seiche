//! Idempotent, associative delta algebra for state transitions and order actions.
//!
//! Mirrors Ripple's `delta.ml` (§2.3 & §6.1) in zero-allocation TigerStyle Rust:
//! - Identity:      `apply(NoOp, v) = v`
//! - Idempotence:   `apply(d, apply(d, v)) = apply(d, v)`
//! - Roundtrip:     `apply(diff(old, new), old) = new`
//! - Annihilation:  `compose(d, Remove) = Remove`
//! - Right-Set:     `compose(d, Set(v)) = Set(v)`
//! - Compatibility: `apply(compose(d1, d2), v) = apply(d2, apply(d1, v))`
//!
//! Specialized for prediction-market quote/order states (`QuoteState` in centicents).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteState {
    pub active: bool,
    pub price_cc: i64,
    pub qty_c: i64,
}

impl QuoteState {
    pub const INACTIVE: Self = Self {
        active: false,
        price_cc: 0,
        qty_c: 0,
    };

    pub const fn new(price_cc: i64, qty_c: i64) -> Self {
        Self {
            active: qty_c > 0 && price_cc > 0,
            price_cc,
            qty_c,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delta {
    /// No change (`Patch []` in Ripple).
    NoOp,
    /// Full state replacement (`Set v` in Ripple).
    Set(QuoteState),
    /// Field-level absolute patch (`Patch` in Ripple). `None` means leave field unchanged.
    Patch {
        price_cc: Option<i64>,
        qty_c: Option<i64>,
    },
    /// Key removal / order cancellation (`Remove` in Ripple).
    Remove,
}

impl Delta {
    /// Compose two sequential deltas `d1` then `d2` into a single delta `d`
    /// satisfying `apply(compose(d1, d2), s) == apply(d2, apply(d1, s))`.
    #[inline]
    pub const fn compose(self, next: Self) -> Self {
        match (self, next) {
            (_, Self::NoOp) => self,
            (Self::NoOp, d2) => d2,
            (_, Self::Remove) => Self::Remove,
            (_, Self::Set(v)) => Self::Set(v),
            (Self::Set(mut base), Self::Patch { price_cc, qty_c }) => {
                if let Some(p) = price_cc {
                    base.price_cc = p;
                }
                if let Some(q) = qty_c {
                    base.qty_c = q;
                }
                base.active = base.qty_c > 0 && base.price_cc > 0;
                Self::Set(base)
            }
            (Self::Remove, Self::Patch { price_cc, qty_c }) => {
                let p = match price_cc {
                    Some(p) => p,
                    None => 0,
                };
                let q = match qty_c {
                    Some(q) => q,
                    None => 0,
                };
                Self::Set(QuoteState {
                    active: q > 0 && p > 0,
                    price_cc: p,
                    qty_c: q,
                })
            }
            (
                Self::Patch {
                    price_cc: p1,
                    qty_c: q1,
                },
                Self::Patch {
                    price_cc: p2,
                    qty_c: q2,
                },
            ) => Self::Patch {
                price_cc: if p2.is_some() { p2 } else { p1 },
                qty_c: if q2.is_some() { q2 } else { q1 },
            },
        }
    }

    /// Apply delta to a `QuoteState`. Guaranteed idempotent:
    /// `d.apply(d.apply(s)) == d.apply(s)`.
    #[inline]
    pub const fn apply(self, state: QuoteState) -> QuoteState {
        match self {
            Self::NoOp => state,
            Self::Set(v) => v,
            Self::Remove => QuoteState::INACTIVE,
            Self::Patch { price_cc, qty_c } => {
                let p = match price_cc {
                    Some(p) => p,
                    None => state.price_cc,
                };
                let q = match qty_c {
                    Some(q) => q,
                    None => state.qty_c,
                };
                QuoteState {
                    active: q > 0 && p > 0,
                    price_cc: p,
                    qty_c: q,
                }
            }
        }
    }

    /// Compute the minimal delta from `old` to `new_state` satisfying
    /// `diff(old, new).apply(old) == new`.
    #[inline]
    pub const fn diff(old: QuoteState, new_state: QuoteState) -> Self {
        if old.active == new_state.active
            && old.price_cc == new_state.price_cc
            && old.qty_c == new_state.qty_c
        {
            return Self::NoOp;
        }
        if !new_state.active {
            return Self::Remove;
        }
        if !old.active {
            return Self::Set(new_state);
        }
        let price_patch = if old.price_cc != new_state.price_cc {
            Some(new_state.price_cc)
        } else {
            None
        };
        let qty_patch = if old.qty_c != new_state.qty_c {
            Some(new_state.qty_c)
        } else {
            None
        };
        Self::Patch {
            price_cc: price_patch,
            qty_c: qty_patch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algebraic_laws_hold_exhaustively_over_grid() {
        let states = [
            QuoteState::INACTIVE,
            QuoteState::new(4200, 500),
            QuoteState::new(4300, 500),
            QuoteState::new(4300, 1000),
            QuoteState::new(9200, 500),
        ];
        let deltas = [
            Delta::NoOp,
            Delta::Remove,
            Delta::Set(QuoteState::new(4200, 500)),
            Delta::Set(QuoteState::new(5500, 1200)),
            Delta::Patch {
                price_cc: Some(4400),
                qty_c: None,
            },
            Delta::Patch {
                price_cc: None,
                qty_c: Some(800),
            },
            Delta::Patch {
                price_cc: Some(9100),
                qty_c: Some(500),
            },
        ];

        // 1. Idempotence: apply(d, apply(d, s)) == apply(d, s)
        for &d in &deltas {
            for &s in &states {
                let once = d.apply(s);
                let twice = d.apply(once);
                assert_eq!(once, twice, "Idempotence failed for {:?} on {:?}", d, s);
            }
        }

        // 2. Roundtrip: apply(diff(old, new), old) == new
        for &old in &states {
            for &new_s in &states {
                let d = Delta::diff(old, new_s);
                assert_eq!(
                    d.apply(old),
                    new_s,
                    "Roundtrip failed from {:?} to {:?} via {:?}",
                    old,
                    new_s,
                    d
                );
            }
        }

        // 3. Compatibility: apply(compose(d1, d2), s) == apply(d2, apply(d1, s))
        for &d1 in &deltas {
            for &d2 in &deltas {
                let comp = d1.compose(d2);
                for &s in &states {
                    assert_eq!(
                        comp.apply(s),
                        d2.apply(d1.apply(s)),
                        "Compatibility failed for {:?} ; {:?} on {:?}",
                        d1,
                        d2,
                        s
                    );
                }
            }
        }

        // 4. Associativity: compose(a, compose(b, c)) == compose(compose(a, b), c)
        for &a in &deltas {
            for &b in &deltas {
                for &c in &deltas {
                    let lhs = a.compose(b.compose(c));
                    let rhs = (a.compose(b)).compose(c);
                    for &s in &states {
                        assert_eq!(lhs.apply(s), rhs.apply(s));
                    }
                }
            }
        }
    }
}
