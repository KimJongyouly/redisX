//! Hash table with Redis-style incremental rehashing and the reverse-binary
//! SCAN cursor algorithm — ported from `redis_src/src/dict.c`.
//!
//! What's ported: the two-table (`ht[0]`/`ht[1]`) incremental rehashing scheme
//! (grow/shrink thresholds, step-by-N rehashing) and `dictScan`'s reverse-binary
//! cursor, which is a load-bearing correctness guarantee — no key present for
//! the whole duration of a scan is ever missed, even if the table resizes
//! mid-scan — that later phases (the `SCAN`/`HSCAN`/... commands) depend on.
//!
//! What's intentionally not ported: dictEntry pointer tagging and the
//! odd/even-address key optimizations (`dict.h`'s file-level comment on
//! "Dict usage of pointer tagging"), two-phase unlink, defrag callbacks, and
//! exact chain-order-on-collision. These are C memory-layout optimizations
//! with no behavior visible to callers and don't apply to safe Rust (no raw
//! pointer tagging); bucket chains here are `Vec<(K, V)>` instead of an
//! intrusive linked list. See `redis_document/25_redisX.md` Phase 0 notes.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// DICT_HT_INITIAL_EXP — initial table size is `1 << 2` = 4 buckets.
const INITIAL_EXP: i8 = 2;
/// HASHTABLE_MIN_FILL — shrink once used/size ratio drops below 1/8.
const MIN_FILL: usize = 8;

fn table_size(exp: i8) -> usize {
    if exp < 0 {
        0
    } else {
        1usize << exp
    }
}

/// `_dictNextExp` — smallest power-of-two table size able to hold `size`
/// elements at load factor 1, never below the initial size.
fn next_exp(size: usize) -> i8 {
    if size <= table_size(INITIAL_EXP) {
        return INITIAL_EXP;
    }
    (usize::BITS - (size - 1).leading_zeros()) as i8
}

/// Reverses the bits of a 64-bit cursor — the core trick behind `dictScan`'s
/// resize-safe iteration (see the long comment in `dict.c` above `dictScan`).
fn reverse_bits(mut v: u64) -> u64 {
    let mut out = 0u64;
    for _ in 0..64 {
        out = (out << 1) | (v & 1);
        v >>= 1;
    }
    out
}

struct Table<K, V> {
    buckets: Vec<Vec<(K, V)>>,
    size_exp: i8,
}

impl<K, V> Table<K, V> {
    fn empty() -> Self {
        Table { buckets: Vec::new(), size_exp: -1 }
    }

    fn with_exp(exp: i8) -> Self {
        Table { buckets: (0..table_size(exp)).map(|_| Vec::new()).collect(), size_exp: exp }
    }

    fn size(&self) -> usize {
        table_size(self.size_exp)
    }

    fn mask(&self) -> u64 {
        if self.size_exp < 0 {
            0
        } else {
            self.size() as u64 - 1
        }
    }
}

pub struct RDict<K, V> {
    ht: [Table<K, V>; 2],
    used: [usize; 2],
    /// rehashidx — -1 means "not rehashing".
    rehash_idx: isize,
}

impl<K, V> Default for RDict<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> RDict<K, V>
where
    K: Hash + Eq,
{
    /// dictCreate
    pub fn new() -> Self {
        RDict { ht: [Table::empty(), Table::empty()], used: [0, 0], rehash_idx: -1 }
    }

    fn hash_of(key: &K) -> u64 {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        h.finish()
    }

    /// dictSize
    pub fn len(&self) -> usize {
        self.used[0] + self.used[1]
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// dictIsRehashing
    pub fn is_rehashing(&self) -> bool {
        self.rehash_idx != -1
    }

    pub fn buckets(&self) -> usize {
        self.ht[0].size() + self.ht[1].size()
    }

    /// `dictRehash(d, n)` — move up to `n` non-empty buckets (visiting at most
    /// `n*10` empty ones) from `ht[0]` to `ht[1]`, completing the rehash and
    /// swapping tables if `ht[0]` empties out.
    fn rehash_step(&mut self, n: isize) {
        if !self.is_rehashing() {
            return;
        }
        let mut empty_visits = n * 10;
        let mut steps = n;
        while steps > 0 && self.used[0] != 0 {
            while self.ht[0].buckets[self.rehash_idx as usize].is_empty() {
                self.rehash_idx += 1;
                empty_visits -= 1;
                if empty_visits == 0 {
                    return;
                }
            }
            let idx = self.rehash_idx as usize;
            let bucket = std::mem::take(&mut self.ht[0].buckets[idx]);
            self.used[0] -= bucket.len();
            let m1 = self.ht[1].mask();
            for (k, v) in bucket {
                let b = (Self::hash_of(&k) & m1) as usize;
                self.ht[1].buckets[b].push((k, v));
                self.used[1] += 1;
            }
            self.rehash_idx += 1;
            steps -= 1;
        }
        if self.used[0] == 0 {
            self.ht[0] = std::mem::replace(&mut self.ht[1], Table::empty());
            self.used[0] = self.used[1];
            self.used[1] = 0;
            self.rehash_idx = -1;
        }
    }

    /// Public hook for callers that want to amortize rehashing themselves
    /// (e.g. a serverCron-style background step), mirroring
    /// `dictRehashMicroseconds`'s step-based cousin `dictRehash`.
    pub fn rehash_steps(&mut self, n: isize) -> bool {
        self.rehash_step(n);
        self.is_rehashing()
    }

    /// `dictExpandIfNeeded` — lazily allocate on first use, then double
    /// (to the next power of two) once load factor reaches 1.
    fn maybe_expand(&mut self) {
        if self.is_rehashing() {
            return;
        }
        if self.ht[0].size_exp < 0 {
            self.ht[0] = Table::with_exp(INITIAL_EXP);
            return;
        }
        if self.used[0] >= self.ht[0].size() {
            self.ht[1] = Table::with_exp(next_exp(self.used[0] + 1));
            self.rehash_idx = 0;
        }
    }

    /// `dictShrinkIfNeeded` — shrink once used/size ratio drops below 1/8,
    /// never below the initial size.
    fn maybe_shrink(&mut self) {
        if self.is_rehashing() {
            return;
        }
        if self.ht[0].size() <= table_size(INITIAL_EXP) {
            return;
        }
        if self.used[0] * MIN_FILL <= self.ht[0].size() {
            self.ht[1] = Table::with_exp(next_exp(self.used[0].max(1)));
            self.rehash_idx = 0;
        }
    }

    /// dictFind
    pub fn get(&self, key: &K) -> Option<&V> {
        if self.ht[0].size_exp < 0 {
            return None;
        }
        let h = Self::hash_of(key);
        let idx0 = (h & self.ht[0].mask()) as usize;
        if let Some((_, v)) = self.ht[0].buckets[idx0].iter().find(|(k, _)| k == key) {
            return Some(v);
        }
        if self.is_rehashing() {
            let idx1 = (h & self.ht[1].mask()) as usize;
            if let Some((_, v)) = self.ht[1].buckets[idx1].iter().find(|(k, _)| k == key) {
                return Some(v);
            }
        }
        None
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.ht[0].size_exp >= 0 {
            let idx0 = (Self::hash_of(key) & self.ht[0].mask()) as usize;
            if let Some(pos) = self.ht[0].buckets[idx0].iter().position(|(k, _)| k == key) {
                return Some(&mut self.ht[0].buckets[idx0][pos].1);
            }
        }
        if self.is_rehashing() {
            let idx1 = (Self::hash_of(key) & self.ht[1].mask()) as usize;
            if let Some(pos) = self.ht[1].buckets[idx1].iter().position(|(k, _)| k == key) {
                return Some(&mut self.ht[1].buckets[idx1][pos].1);
            }
        }
        None
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// dictReplace (also covers dictAdd: returns `None` on a fresh insert,
    /// `Some(old_value)` when an existing key's value was replaced).
    pub fn insert(&mut self, key: K, val: V) -> Option<V> {
        if self.is_rehashing() {
            self.rehash_step(1);
        }
        if let Some(slot) = self.get_mut(&key) {
            return Some(std::mem::replace(slot, val));
        }
        self.maybe_expand();
        let h = Self::hash_of(&key);
        let idx = if self.is_rehashing() { 1 } else { 0 };
        let bucket = (h & self.ht[idx].mask()) as usize;
        self.ht[idx].buckets[bucket].push((key, val));
        self.used[idx] += 1;
        None
    }

    /// dictDelete
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_rehashing() {
            self.rehash_step(1);
        }
        let h = Self::hash_of(key);
        for t in 0..2 {
            if self.ht[t].size_exp < 0 || (t == 1 && !self.is_rehashing()) {
                continue;
            }
            let bucket = (h & self.ht[t].mask()) as usize;
            if let Some(pos) = self.ht[t].buckets[bucket].iter().position(|(k, _)| k == key) {
                let (_, v) = self.ht[t].buckets[bucket].remove(pos);
                self.used[t] -= 1;
                self.maybe_shrink();
                return Some(v);
            }
        }
        None
    }

    /// `dictGetIterator`/`dictNext` — a plain one-shot full iterator over
    /// every live entry, distinct from [`RDict::scan`] (the cursor-resumable
    /// version `SCAN` needs). Real Redis commands that must see every key at
    /// once regardless of concurrent resizing — `HGETALL`, `SMEMBERS`, `KEYS`
    /// — use this kind of iterator, not `dictScan`.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.ht[0].buckets.iter().chain(self.ht[1].buckets.iter()).flatten().map(|(k, v)| (k, v))
    }

    /// `dictScan` — advance the reverse-binary cursor by one step, invoking
    /// `f` for every entry visited. Start with cursor `0`; the scan is
    /// complete once this returns `0` again. Safe to call `insert`/`remove`
    /// from within `f` is NOT guaranteed here (unlike the C "safe iterator"
    /// mode) — this only ports the non-rehashing-corrupting cursor math.
    pub fn scan<F: FnMut(&K, &V)>(&self, cursor: u64, mut f: F) -> u64 {
        if self.is_empty() {
            return 0;
        }
        let mut v = cursor;
        if !self.is_rehashing() {
            let m0 = self.ht[0].mask();
            for (k, val) in &self.ht[0].buckets[(v & m0) as usize] {
                f(k, val);
            }
            v |= !m0;
            v = reverse_bits(reverse_bits(v).wrapping_add(1));
        } else {
            let (small, big) = if self.ht[0].size() <= self.ht[1].size() { (0, 1) } else { (1, 0) };
            let m0 = self.ht[small].mask();
            let m1 = self.ht[big].mask();
            for (k, val) in &self.ht[small].buckets[(v & m0) as usize] {
                f(k, val);
            }
            loop {
                for (k, val) in &self.ht[big].buckets[(v & m1) as usize] {
                    f(k, val);
                }
                v |= !m1;
                v = reverse_bits(reverse_bits(v).wrapping_add(1));
                if v & (m0 ^ m1) == 0 {
                    break;
                }
            }
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn insert_get_remove_roundtrip() {
        let mut d: RDict<String, i32> = RDict::new();
        assert_eq!(d.insert("a".into(), 1), None);
        assert_eq!(d.insert("b".into(), 2), None);
        assert_eq!(d.get(&"a".to_string()), Some(&1));
        assert_eq!(d.insert("a".into(), 10), Some(1));
        assert_eq!(d.get(&"a".to_string()), Some(&10));
        assert_eq!(d.len(), 2);
        assert_eq!(d.remove(&"a".to_string()), Some(10));
        assert_eq!(d.get(&"a".to_string()), None);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn iter_visits_every_entry_exactly_once_including_mid_rehash() {
        use std::collections::HashSet;
        let mut d: RDict<i64, i64> = RDict::new();
        for i in 0..5000i64 {
            d.insert(i, i * 2);
        }
        assert!(d.is_rehashing(), "5000 inserts without draining rehash steps should leave it mid-rehash");
        let seen: HashSet<i64> = d.iter().map(|(k, _)| *k).collect();
        assert_eq!(seen.len(), 5000);
        for i in 0..5000i64 {
            assert!(seen.contains(&i));
        }
    }

    #[test]
    fn grows_and_rehashes_under_many_inserts() {
        let mut d: RDict<i64, i64> = RDict::new();
        for i in 0..5000 {
            d.insert(i, i * 2);
        }
        assert_eq!(d.len(), 5000);
        // Force any pending incremental rehash to completion.
        while d.is_rehashing() {
            d.rehash_steps(10);
        }
        for i in 0..5000 {
            assert_eq!(d.get(&i), Some(&(i * 2)));
        }
    }

    #[test]
    fn shrinks_after_bulk_removal() {
        let mut d: RDict<i64, i64> = RDict::new();
        for i in 0..5000 {
            d.insert(i, i);
        }
        while d.is_rehashing() {
            d.rehash_steps(100);
        }
        let big_buckets = d.buckets();
        for i in 0..4990 {
            d.remove(&i);
        }
        while d.is_rehashing() {
            d.rehash_steps(100);
        }
        assert!(d.buckets() < big_buckets, "table should shrink after most keys are removed");
        for i in 4990..5000 {
            assert_eq!(d.get(&i), Some(&i));
        }
    }

    #[test]
    fn scan_visits_every_key_even_while_table_grows_mid_scan() {
        let mut d: RDict<i64, i64> = RDict::new();
        for i in 0..50 {
            d.insert(i, i);
        }

        let mut seen: HashSet<i64> = HashSet::new();
        let mut cursor = 0u64;
        let mut iterations = 0;
        loop {
            cursor = d.scan(cursor, |k, _| {
                seen.insert(*k);
            });
            iterations += 1;
            // Grow the table mid-scan a couple of times, like a live server would.
            if iterations == 2 {
                for i in 50..400 {
                    d.insert(i, i);
                }
            }
            if cursor == 0 {
                break;
            }
            assert!(iterations < 100_000, "scan should terminate");
        }

        for i in 0..50 {
            assert!(seen.contains(&i), "key {i} present for the whole scan must be visited");
        }
    }

    #[test]
    fn scan_on_empty_dict_returns_zero_immediately() {
        let d: RDict<i32, i32> = RDict::new();
        assert_eq!(d.scan(0, |_, _| panic!("should not be called")), 0);
    }
}
