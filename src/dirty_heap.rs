//! Packed `(height, node_id)` min-heap for deterministic topological stabilization.
//!
//! Mirrors Ripple's `dirty_heap.ml` in TigerStyle Rust:
//! - Array-backed 1-indexed binary min-heap pre-allocated to capacity (zero allocation on push/pop)
//! - Single `u64` key `((height as u64) << 32) | (node_id as u64)` so lexicographic comparison
//!   `(height ASC, node_id ASC)` reduces to one integer `<` instruction (Guarantee S3: determinism)
//! - `in_heap` bit/bool membership table for O(1) duplicate suppression (Invariant I-2)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapEntry {
    pub height: u16,
    pub node_id: u32,
}

impl HeapEntry {
    #[inline(always)]
    pub const fn pack(height: u16, node_id: u32) -> u64 {
        ((height as u64) << 32) | (node_id as u64)
    }

    #[inline(always)]
    pub const fn unpack(packed: u64) -> Self {
        Self {
            height: (packed >> 32) as u16,
            node_id: (packed & 0xFFFF_FFFF) as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DirtyHeap {
    /// 1-indexed binary min-heap of packed `(height, node_id)` keys.
    entries: Vec<u64>,
    size: usize,
    /// O(1) membership table indexed by `node_id`.
    in_heap: Vec<bool>,
}

impl DirtyHeap {
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.max(16);
        Self {
            entries: vec![0; cap + 1],
            size: 0,
            in_heap: vec![false; cap],
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline(always)]
    pub fn contains(&self, node_id: u32) -> bool {
        let idx = node_id as usize;
        idx < self.in_heap.len() && self.in_heap[idx]
    }

    pub fn clear(&mut self) {
        for i in 1..=self.size {
            let entry = HeapEntry::unpack(self.entries[i]);
            self.in_heap[entry.node_id as usize] = false;
        }
        self.size = 0;
    }

    pub fn ensure_capacity(&mut self, node_id: u32) {
        let idx = node_id as usize;
        if idx >= self.in_heap.len() {
            let new_cap = (idx + 1).max(self.in_heap.len() * 2);
            self.entries.resize(new_cap + 1, 0);
            self.in_heap.resize(new_cap, false);
        }
    }

    #[inline]
    pub fn push(&mut self, height: u16, node_id: u32) {
        self.ensure_capacity(node_id);
        let idx = node_id as usize;
        if self.in_heap[idx] {
            return;
        }
        self.size += 1;
        let packed = HeapEntry::pack(height, node_id);
        self.entries[self.size] = packed;
        self.in_heap[idx] = true;
        self.sift_up(self.size);
    }

    #[inline]
    pub fn pop_min(&mut self) -> Option<HeapEntry> {
        if self.size == 0 {
            return None;
        }
        let min_packed = self.entries[1];
        let min_entry = HeapEntry::unpack(min_packed);
        debug_assert!(self.in_heap[min_entry.node_id as usize]);
        self.in_heap[min_entry.node_id as usize] = false;
        self.entries[1] = self.entries[self.size];
        self.size -= 1;
        if self.size > 0 {
            self.sift_down(1);
        }
        Some(min_entry)
    }

    #[inline(always)]
    fn sift_up(&mut self, mut i: usize) {
        let val = self.entries[i];
        while i > 1 {
            let parent = i >> 1;
            let p_val = self.entries[parent];
            if val < p_val {
                self.entries[i] = p_val;
                i = parent;
            } else {
                break;
            }
        }
        self.entries[i] = val;
    }

    #[inline(always)]
    fn sift_down(&mut self, mut i: usize) {
        let val = self.entries[i];
        let n = self.size;
        loop {
            let left = i << 1;
            if left > n {
                break;
            }
            let right = left + 1;
            let smallest = if right <= n && self.entries[right] < self.entries[left] {
                right
            } else {
                left
            };
            if self.entries[smallest] < val {
                self.entries[i] = self.entries[smallest];
                i = smallest;
            } else {
                break;
            }
        }
        self.entries[i] = val;
    }

    /// TigerStyle invariant check (I-2): heap ordering + bijective membership array.
    pub fn assert_invariants(&self) {
        for i in 2..=self.size {
            let parent = i >> 1;
            assert!(
                self.entries[parent] <= self.entries[i],
                "DirtyHeap ordering violation at index {} (parent {})",
                i,
                parent
            );
        }
        let mut count_true = 0usize;
        for (nid, &flag) in self.in_heap.iter().enumerate() {
            if flag {
                count_true += 1;
                let found = (1..=self.size)
                    .any(|k| HeapEntry::unpack(self.entries[k]).node_id as usize == nid);
                assert!(found, "node {} marked in_heap but absent from entries", nid);
            }
        }
        assert_eq!(
            count_true, self.size,
            "DirtyHeap membership count mismatch ({} vs size {})",
            count_true, self.size
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_maintains_lexicographic_height_and_node_id_order() {
        let mut h = DirtyHeap::with_capacity(16);
        h.push(3, 0);
        h.push(1, 5);
        h.push(1, 2);
        h.push(2, 1);
        h.push(1, 2); // duplicate — suppressed in O(1)
        h.assert_invariants();
        assert_eq!(h.len(), 4);

        assert_eq!(
            h.pop_min(),
            Some(HeapEntry {
                height: 1,
                node_id: 2
            })
        );
        assert_eq!(
            h.pop_min(),
            Some(HeapEntry {
                height: 1,
                node_id: 5
            })
        );
        assert_eq!(
            h.pop_min(),
            Some(HeapEntry {
                height: 2,
                node_id: 1
            })
        );
        assert_eq!(
            h.pop_min(),
            Some(HeapEntry {
                height: 3,
                node_id: 0
            })
        );
        assert_eq!(h.pop_min(), None);
        h.assert_invariants();
    }
}
