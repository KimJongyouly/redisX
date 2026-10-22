//! List type — ported from `redis_src/src/t_list.c`, simplified to a single
//! `ListPack` (Phase 0's byte-compatible port) instead of real Redis's
//! quicklist (a doubly-linked list of listpack *nodes*, each capped by
//! `list-max-listpack-size`, so one huge list doesn't become one huge
//! contiguous allocation). Correctness is identical either way — `LPUSH`/
//! `RPUSH`/`LPOP`/`RPOP`/`LRANGE`/`LINDEX` behave the same — only the memory
//! layout for very large lists differs. Multi-node quicklist chaining is a
//! performance optimization, deferred per `redis_document/25_redisX.md`'s
//! "정확성 우선, 성능 동등은 2차" principle.

use redisx_core::ListPack;

pub struct ListObj {
    lp: ListPack,
}

impl Default for ListObj {
    fn default() -> Self {
        Self::new()
    }
}

impl ListObj {
    pub fn new() -> Self {
        ListObj { lp: ListPack::new() }
    }

    /// Phase 4 (RDB) loading a `RDB_TYPE_LIST_QUICKLIST_2` PACKED node.
    pub fn from_listpack(lp: ListPack) -> Self {
        ListObj { lp }
    }

    /// Phase 4 (RDB) saving — the exact bytes real Redis would put in a
    /// single-node quicklist's PACKED entry.
    pub fn as_listpack_bytes(&self) -> &[u8] {
        self.lp.as_bytes()
    }

    /// Active-defrag hook (`server.rs`'s `RObj::defrag_in_place`) — shrinks
    /// the underlying listpack's own over-allocated capacity, left behind by
    /// `LPOP`/`LREM`/`LTRIM` etc. (`ListPack::remove`'s `Vec::drain`, which
    /// never shrinks capacity on its own).
    pub fn shrink_to_fit(&mut self) {
        self.lp.shrink_to_fit();
    }

    pub fn len(&self) -> usize {
        self.lp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lp.is_empty()
    }

    /// LPUSH — each argument is prepended in turn, so `LPUSH k a b c` leaves
    /// the list as `[c, b, a]`, matching `lpushCommand`'s behavior.
    pub fn push_front(&mut self, items: &[Vec<u8>]) {
        for item in items {
            self.lp.insert_str(0, item);
        }
    }

    /// RPUSH
    pub fn push_back(&mut self, items: &[Vec<u8>]) {
        for item in items {
            self.lp.push_str(item);
        }
    }

    /// LPOP [count]
    pub fn pop_front(&mut self, count: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for _ in 0..count {
            match self.lp.remove(0) {
                Some(v) => out.push(v.to_bytes()),
                None => break,
            }
        }
        out
    }

    /// RPOP [count]
    pub fn pop_back(&mut self, count: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for _ in 0..count {
            if self.lp.is_empty() {
                break;
            }
            match self.lp.remove(self.lp.len() - 1) {
                Some(v) => out.push(v.to_bytes()),
                None => break,
            }
        }
        out
    }

    /// LINDEX — negative indices count from the end, like `sdsrange`.
    pub fn index(&self, idx: i64) -> Option<Vec<u8>> {
        let len = self.lp.len() as i64;
        let real = if idx < 0 { len + idx } else { idx };
        if real < 0 || real >= len {
            return None;
        }
        self.lp.get_bytes(real as usize)
    }

    /// LRANGE — inclusive `[start, end]`, negative indices from the end,
    /// clamped to the list bounds (never an error, matching real Redis).
    pub fn range(&self, start: i64, end: i64) -> Vec<Vec<u8>> {
        let len = self.lp.len() as i64;
        if len == 0 {
            return Vec::new();
        }
        let s = if start < 0 { (len + start).max(0) } else { start };
        let e = if end < 0 { len + end } else { end.min(len - 1) };
        if s > e || s >= len || e < 0 {
            return Vec::new();
        }
        (s as usize..=(e as usize)).filter_map(|i| self.lp.get_bytes(i)).collect()
    }

    /// LSET — replaces the element at `idx` (negative = from the end).
    /// Returns `false` if the index is out of range.
    pub fn set_at(&mut self, idx: i64, value: &[u8]) -> bool {
        let len = self.lp.len() as i64;
        let real = if idx < 0 { len + idx } else { idx };
        if real < 0 || real >= len {
            return false;
        }
        let real = real as usize;
        self.lp.remove(real);
        self.lp.insert_str(real, value);
        true
    }

    /// LINSERT — inserts `value` immediately before/after the first
    /// occurrence of `pivot`. Returns the new length, or `None` if `pivot`
    /// isn't found anywhere in the list.
    pub fn insert_pivot(&mut self, pivot: &[u8], value: &[u8], before: bool) -> Option<usize> {
        let pos = (0..self.lp.len()).find(|&i| self.lp.get_bytes(i).as_deref() == Some(pivot))?;
        let at = if before { pos } else { pos + 1 };
        self.lp.insert_str(at, value);
        Some(self.lp.len())
    }

    /// LREM — removes occurrences of `value`. `count > 0` removes the first
    /// `count` occurrences scanning head-to-tail, `count < 0` scans
    /// tail-to-head, `count == 0` removes every occurrence. Returns the
    /// number of elements removed.
    pub fn remove_value(&mut self, value: &[u8], count: i64) -> usize {
        let mut removed = 0usize;
        if count >= 0 {
            let limit = if count == 0 { usize::MAX } else { count as usize };
            let mut i = 0;
            while i < self.lp.len() && removed < limit {
                if self.lp.get_bytes(i).as_deref() == Some(value) {
                    self.lp.remove(i);
                    removed += 1;
                } else {
                    i += 1;
                }
            }
        } else {
            let limit = (-count) as usize;
            let mut i = self.lp.len();
            while i > 0 && removed < limit {
                i -= 1;
                if self.lp.get_bytes(i).as_deref() == Some(value) {
                    self.lp.remove(i);
                    removed += 1;
                }
            }
        }
        removed
    }

    /// LTRIM — keeps only the inclusive `[start, end]` range (same
    /// normalization as `range`), discarding everything else. An
    /// out-of-range/empty result trims the list down to empty.
    pub fn trim(&mut self, start: i64, end: i64) {
        let len = self.lp.len() as i64;
        if len == 0 {
            return;
        }
        let s = if start < 0 { (len + start).max(0) } else { start };
        let e = if end < 0 { len + end } else { end.min(len - 1) };
        if s > e || s >= len || e < 0 {
            while !self.lp.is_empty() {
                self.lp.remove(0);
            }
            return;
        }
        let (s, e) = (s as usize, e as usize);
        while self.lp.len() > e + 1 {
            self.lp.remove(self.lp.len() - 1);
        }
        for _ in 0..s {
            self.lp.remove(0);
        }
    }

    /// LPOS's raw ingredient — every 0-based index holding `value`, in list
    /// order. The command handler layers `RANK`/`COUNT`/`MAXLEN` on top.
    pub fn positions(&self, value: &[u8]) -> Vec<usize> {
        (0..self.lp.len()).filter(|&i| self.lp.get_bytes(i).as_deref() == Some(value)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<Vec<u8>> {
        items.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn lpush_reverses_argument_order() {
        let mut l = ListObj::new();
        l.push_front(&v(&["a", "b", "c"]));
        assert_eq!(l.range(0, -1), v(&["c", "b", "a"]));
    }

    #[test]
    fn rpush_preserves_argument_order() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "c"]));
        assert_eq!(l.range(0, -1), v(&["a", "b", "c"]));
    }

    #[test]
    fn pop_front_and_back() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "c", "d"]));
        assert_eq!(l.pop_front(2), v(&["a", "b"]));
        assert_eq!(l.pop_back(1), v(&["d"]));
        assert_eq!(l.range(0, -1), v(&["c"]));
        assert_eq!(l.pop_front(5), v(&["c"]), "popping more than available just returns what's there");
        assert!(l.is_empty());
    }

    #[test]
    fn index_supports_negative() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "c"]));
        assert_eq!(l.index(0), Some(b"a".to_vec()));
        assert_eq!(l.index(-1), Some(b"c".to_vec()));
        assert_eq!(l.index(99), None);
    }

    #[test]
    fn range_clamps_out_of_bounds() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "c"]));
        assert_eq!(l.range(-100, 100), v(&["a", "b", "c"]));
        assert_eq!(l.range(5, 10), Vec::<Vec<u8>>::new());
        assert_eq!(l.range(1, 1), v(&["b"]));
    }

    #[test]
    fn set_at_replaces_in_place() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "c"]));
        assert!(l.set_at(1, b"B"));
        assert_eq!(l.range(0, -1), v(&["a", "B", "c"]));
        assert!(l.set_at(-1, b"C"));
        assert_eq!(l.range(0, -1), v(&["a", "B", "C"]));
        assert!(!l.set_at(99, b"x"));
    }

    #[test]
    fn insert_pivot_before_and_after() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "c"]));
        assert_eq!(l.insert_pivot(b"c", b"b", true), Some(3));
        assert_eq!(l.range(0, -1), v(&["a", "b", "c"]));
        assert_eq!(l.insert_pivot(b"c", b"d", false), Some(4));
        assert_eq!(l.range(0, -1), v(&["a", "b", "c", "d"]));
        assert_eq!(l.insert_pivot(b"nope", b"x", true), None);
    }

    #[test]
    fn remove_value_respects_count_direction() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "a", "c", "a"]));
        assert_eq!(l.remove_value(b"a", 2), 2);
        assert_eq!(l.range(0, -1), v(&["b", "c", "a"]));

        let mut l2 = ListObj::new();
        l2.push_back(&v(&["a", "b", "a", "c", "a"]));
        assert_eq!(l2.remove_value(b"a", -2), 2);
        assert_eq!(l2.range(0, -1), v(&["a", "b", "c"]));

        let mut l3 = ListObj::new();
        l3.push_back(&v(&["a", "b", "a"]));
        assert_eq!(l3.remove_value(b"a", 0), 2);
        assert_eq!(l3.range(0, -1), v(&["b"]));
    }

    #[test]
    fn trim_keeps_only_the_given_range() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "c", "d", "e"]));
        l.trim(1, -2);
        assert_eq!(l.range(0, -1), v(&["b", "c", "d"]));

        let mut l2 = ListObj::new();
        l2.push_back(&v(&["a", "b"]));
        l2.trim(5, 10);
        assert!(l2.is_empty());
    }

    #[test]
    fn positions_finds_every_match_in_order() {
        let mut l = ListObj::new();
        l.push_back(&v(&["a", "b", "a", "c", "a"]));
        assert_eq!(l.positions(b"a"), vec![0, 2, 4]);
        assert_eq!(l.positions(b"z"), Vec::<usize>::new());
    }
}
