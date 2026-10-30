//! Sequence-ordered delta buffer with gap detection and snapshot fallback.
//!
//! Direct TigerStyle Rust port of Ripple's `delta_buffer.ml` (§4.4), adapted for
//! Kalshi V2 WebSocket `orderbook_delta` streams (`seq: u64`):
//! - Messages with `seq < next_expected_seq` are idempotently discarded (`Deliver(empty)`)
//! - Messages with `seq == next_expected_seq` deliver immediately and drain any consecutive
//!   buffered messages in strict sequence order
//! - Messages with `seq > next_expected_seq` buffer in sorted order (deduping identical `seq`s)
//!   and emit `GapDetected { missing_seq, retransmits }`
//! - Overflowing `max_buffer_size` or exceeding `max_retransmits` flushes the buffer and emits
//!   `ResyncRequired` so the caller requests a fresh full-book snapshot instead of trading on
//!   phantom liquidity.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelta<T> {
    pub sequence_no: u64,
    pub payload: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapState {
    NoGap,
    WaitingFor { missing_seq: u64, retransmits: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiveResult<T> {
    /// Ordered sequence of deltas ready to apply to the book/graph.
    Deliver(Vec<T>),
    /// Out-of-order message buffered; caller should wait or request retransmit.
    GapDetected { missing_seq: u64, retransmits: u32 },
    /// Buffer full or retransmit budget exhausted; caller MUST resync via full snapshot.
    ResyncRequired { reason: &'static str },
}

#[derive(Debug, Clone)]
pub struct DeltaBuffer<T> {
    next_expected_seq: u64,
    buffer: Vec<PendingDelta<T>>,
    gap: GapState,
    max_buffer_size: usize,
    max_retransmits: u32,
    pub delivered_count: u64,
    pub duplicate_count: u64,
    pub reorder_count: u64,
    pub gap_count: u64,
    pub resync_count: u64,
}

impl<T: Clone> DeltaBuffer<T> {
    pub fn new(initial_seq: u64, max_buffer_size: usize, max_retransmits: u32) -> Self {
        assert!(max_buffer_size > 0, "max_buffer_size must be > 0");
        Self {
            next_expected_seq: initial_seq,
            buffer: Vec::with_capacity(max_buffer_size.min(64)),
            gap: GapState::NoGap,
            max_buffer_size,
            max_retransmits,
            delivered_count: 0,
            duplicate_count: 0,
            reorder_count: 0,
            gap_count: 0,
            resync_count: 0,
        }
    }

    #[inline(always)]
    pub fn next_expected_seq(&self) -> u64 {
        self.next_expected_seq
    }

    #[inline(always)]
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    #[inline(always)]
    pub fn gap_state(&self) -> GapState {
        self.gap
    }

    /// Reset sequence state after applying a full `orderbook_snapshot` at `snapshot_seq`.
    pub fn reset_at_snapshot(&mut self, snapshot_seq: u64) {
        self.next_expected_seq = snapshot_seq + 1;
        self.buffer.clear();
        self.gap = GapState::NoGap;
    }

    pub fn receive(&mut self, sequence_no: u64, payload: T) -> ReceiveResult<T> {
        if sequence_no < self.next_expected_seq {
            self.duplicate_count += 1;
            return ReceiveResult::Deliver(Vec::new());
        }

        if sequence_no == self.next_expected_seq {
            self.next_expected_seq += 1;
            self.delivered_count += 1;
            let mut out = vec![payload];
            // Drain any consecutive buffered messages
            while let Some(front) = self.buffer.first() {
                if front.sequence_no < self.next_expected_seq {
                    self.buffer.remove(0);
                } else if front.sequence_no == self.next_expected_seq {
                    let item = self.buffer.remove(0);
                    self.next_expected_seq += 1;
                    self.delivered_count += 1;
                    out.push(item.payload);
                } else {
                    break;
                }
            }
            self.gap = if self.buffer.is_empty() {
                GapState::NoGap
            } else {
                GapState::WaitingFor {
                    missing_seq: self.next_expected_seq,
                    retransmits: 1,
                }
            };
            return ReceiveResult::Deliver(out);
        }

        // Out of order: sequence_no > next_expected_seq
        self.reorder_count += 1;
        // Check if already buffered (idempotent duplicate of future seq)
        match self
            .buffer
            .binary_search_by_key(&sequence_no, |p| p.sequence_no)
        {
            Ok(_existing_idx) => {
                self.duplicate_count += 1;
            }
            Err(insert_idx) => {
                if self.buffer.len() >= self.max_buffer_size {
                    self.buffer.clear();
                    self.gap = GapState::NoGap;
                    self.resync_count += 1;
                    return ReceiveResult::ResyncRequired {
                        reason: "delta_buffer_overflow",
                    };
                }
                self.buffer.insert(
                    insert_idx,
                    PendingDelta {
                        sequence_no,
                        payload,
                    },
                );
            }
        }

        let missing_seq = self.next_expected_seq;
        match self.gap {
            GapState::WaitingFor {
                missing_seq: m,
                retransmits,
            } if m == missing_seq => {
                if retransmits >= self.max_retransmits {
                    self.buffer.clear();
                    self.gap = GapState::NoGap;
                    self.resync_count += 1;
                    ReceiveResult::ResyncRequired {
                        reason: "max_retransmits_exhausted",
                    }
                } else {
                    let next_r = retransmits + 1;
                    self.gap = GapState::WaitingFor {
                        missing_seq,
                        retransmits: next_r,
                    };
                    self.gap_count += 1;
                    ReceiveResult::GapDetected {
                        missing_seq,
                        retransmits: next_r,
                    }
                }
            }
            _ => {
                self.gap = GapState::WaitingFor {
                    missing_seq,
                    retransmits: 1,
                };
                self.gap_count += 1;
                ReceiveResult::GapDetected {
                    missing_seq,
                    retransmits: 1,
                }
            }
        }
    }

    pub fn assert_invariants(&self) {
        assert!(self.buffer.len() <= self.max_buffer_size);
        for w in self.buffer.windows(2) {
            assert!(
                w[0].sequence_no < w[1].sequence_no,
                "DeltaBuffer must be strictly sorted and deduplicated"
            );
        }
        if let Some(first) = self.buffer.first() {
            assert!(
                first.sequence_no > self.next_expected_seq,
                "Buffered delta seq {} <= next_expected_seq {}",
                first.sequence_no,
                self.next_expected_seq
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reorders_out_of_sequence_packets_and_drains_cleanly() {
        let mut buf = DeltaBuffer::new(1, 16, 3);
        assert_eq!(buf.receive(1, "d1"), ReceiveResult::Deliver(vec!["d1"]));
        assert_eq!(
            buf.receive(3, "d3"),
            ReceiveResult::GapDetected {
                missing_seq: 2,
                retransmits: 1
            }
        );
        assert_eq!(
            buf.receive(4, "d4"),
            ReceiveResult::GapDetected {
                missing_seq: 2,
                retransmits: 2
            }
        );
        buf.assert_invariants();

        // Missing seq 2 arrives -> drains 2, 3, 4 together
        assert_eq!(
            buf.receive(2, "d2"),
            ReceiveResult::Deliver(vec!["d2", "d3", "d4"])
        );
        assert_eq!(buf.next_expected_seq(), 5);
        assert_eq!(buf.buffered_len(), 0);
        buf.assert_invariants();
    }

    #[test]
    fn triggers_resync_when_retransmit_budget_exhausted() {
        let mut buf = DeltaBuffer::new(10, 16, 2);
        assert_eq!(
            buf.receive(11, 110),
            ReceiveResult::GapDetected {
                missing_seq: 10,
                retransmits: 1
            }
        );
        assert_eq!(
            buf.receive(12, 120),
            ReceiveResult::GapDetected {
                missing_seq: 10,
                retransmits: 2
            }
        );
        assert_eq!(
            buf.receive(13, 130),
            ReceiveResult::ResyncRequired {
                reason: "max_retransmits_exhausted"
            }
        );
        assert_eq!(buf.buffered_len(), 0);
        buf.reset_at_snapshot(20);
        assert_eq!(buf.receive(21, 210), ReceiveResult::Deliver(vec![210]));
    }
}
