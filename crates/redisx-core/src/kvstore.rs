//! KVStore — an array of [`RDict`]s, ported from `redis_src/src/kvstore.c`.
//!
//! Redis Cluster keeps one dict per hash slot (16384 of them) so it can
//! cheaply enumerate "all keys mapping to this slot"; outside cluster mode
//! it's just a single dict. `KvStore` reproduces that shape and, crucially,
//! the **combined SCAN cursor encoding** (`addDictIndexToCursor` /
//! `getAndClearDictIndexFromCursor` in `kvstore.c`): the low `num_dicts_bits`
//! bits of the cursor select the dict index, the remaining high bits are that
//! dict's own [`RDict::scan`] cursor. Getting this bit-for-bit right now
//! matters for Phase 5 (cluster), where `CLUSTER`-aware `SCAN` must resume
//! correctly across resizes exactly like the single-dict case already does.
//!
//! Not ported: the Fenwick-tree-backed (`fwtree.c`) O(log n) "find dict by
//! cumulative key index" / "next non-empty dict" used for `kvstoreGetFairRandomDictIndex`
//! and the `rehashing` list used to bound incremental-rehash CPU budget across
//! many dicts at once. Both are pure performance optimizations over an
//! O(num_dicts) linear scan, which is what's implemented here (correctness
//! first, per `redis_document/25_redisX.md`'s Phase 0 principle); revisit if/when
//! `fwtree` is ported.

use crate::dict::RDict;
use std::hash::Hash;

pub struct KvStore<K, V> {
    dicts: Vec<Option<RDict<K, V>>>,
    num_dicts_bits: u32,
    allocate_on_demand: bool,
    free_empty_dicts: bool,
}

impl<K, V> KvStore<K, V>
where
    K: Hash + Eq,
{
    /// kvstoreCreate. `num_dicts_bits` is log2 of the number of dicts (0 for a
    /// single dict, 14 for Redis Cluster's 16384 slots, etc). When
    /// `allocate_on_demand` is false, every dict is created up front
    /// (matching `KVSTORE_ALLOCATE_DICTS_ON_DEMAND` unset); `free_empty_dicts`
    /// mirrors `KVSTORE_FREE_EMPTY_DICTS`.
    pub fn new(num_dicts_bits: u32, allocate_on_demand: bool, free_empty_dicts: bool) -> Self {
        let num_dicts = 1usize << num_dicts_bits;
        let dicts = if allocate_on_demand {
            (0..num_dicts).map(|_| None).collect()
        } else {
            (0..num_dicts).map(|_| Some(RDict::new())).collect()
        };
        KvStore { dicts, num_dicts_bits, allocate_on_demand, free_empty_dicts }
    }

    pub fn num_dicts(&self) -> usize {
        self.dicts.len()
    }

    /// kvstoreNumAllocatedDicts
    pub fn num_allocated_dicts(&self) -> usize {
        self.dicts.iter().filter(|d| d.is_some()).count()
    }

    /// kvstoreNumNonEmptyDicts
    pub fn num_non_empty_dicts(&self) -> usize {
        self.dicts.iter().filter(|d| d.as_ref().is_some_and(|d| !d.is_empty())).count()
    }

    /// kvstoreSize — total key count across every dict.
    pub fn size(&self) -> usize {
        self.dicts.iter().filter_map(|d| d.as_ref()).map(RDict::len).sum()
    }

    /// kvstoreBuckets — total bucket count across every dict.
    pub fn buckets(&self) -> usize {
        self.dicts.iter().filter_map(|d| d.as_ref()).map(RDict::buckets).sum()
    }

    /// kvstoreDictSize
    pub fn dict_size(&self, didx: usize) -> usize {
        self.dicts[didx].as_ref().map_or(0, RDict::len)
    }

    pub fn get_dict(&self, didx: usize) -> Option<&RDict<K, V>> {
        self.dicts[didx].as_ref()
    }

    /// createDictIfNeeded
    fn dict_mut_lazy(&mut self, didx: usize) -> &mut RDict<K, V> {
        self.dicts[didx].get_or_insert_with(RDict::new)
    }

    /// freeDictIfNeeded
    fn free_if_needed(&mut self, didx: usize) {
        if !self.free_empty_dicts {
            return;
        }
        if self.dicts[didx].as_ref().is_some_and(RDict::is_empty) {
            self.dicts[didx] = None;
        }
    }

    /// kvstoreDictAddRaw / kvstoreDictFind's write counterpart.
    pub fn insert(&mut self, didx: usize, key: K, val: V) -> Option<V> {
        self.dict_mut_lazy(didx).insert(key, val)
    }

    pub fn get(&self, didx: usize, key: &K) -> Option<&V> {
        self.dicts[didx].as_ref()?.get(key)
    }

    pub fn get_mut(&mut self, didx: usize, key: &K) -> Option<&mut V> {
        self.dicts[didx].as_mut()?.get_mut(key)
    }

    /// kvstoreDictDelete
    pub fn remove(&mut self, didx: usize, key: &K) -> Option<V> {
        let removed = self.dicts[didx].as_mut()?.remove(key);
        if removed.is_some() {
            self.free_if_needed(didx);
        }
        removed
    }

    /// kvstoreGetFirstNonEmptyDictIndex
    pub fn first_non_empty_dict_index(&self) -> Option<usize> {
        self.dicts.iter().position(|d| d.as_ref().is_some_and(|d| !d.is_empty()))
    }

    /// kvstoreGetNextNonEmptyDictIndex — strictly after `didx`.
    pub fn next_non_empty_dict_index(&self, didx: usize) -> Option<usize> {
        self.dicts[didx + 1..].iter().position(|d| d.as_ref().is_some_and(|d| !d.is_empty())).map(|i| i + didx + 1)
    }

    /// kvstoreExpand — pre-create (and no-op expand) every dict not skipped by `skip`.
    pub fn expand_all(&mut self, skip: impl Fn(usize) -> bool) {
        for didx in 0..self.dicts.len() {
            if skip(didx) {
                continue;
            }
            self.dict_mut_lazy(didx);
        }
    }

    fn mask(&self) -> u64 {
        (self.dicts.len() as u64) - 1
    }

    /// addDictIndexToCursor
    fn add_dict_index_to_cursor(&self, didx: Option<usize>, cursor: u64) -> u64 {
        if self.dicts.len() == 1 {
            return cursor;
        }
        match didx {
            None => cursor,
            Some(didx) => (cursor << self.num_dicts_bits) | didx as u64,
        }
    }

    /// getAndClearDictIndexFromCursor
    fn split_cursor(&self, cursor: u64) -> (usize, u64) {
        if self.dicts.len() == 1 {
            return (0, cursor);
        }
        let didx = (cursor & self.mask()) as usize;
        (didx, cursor >> self.num_dicts_bits)
    }

    /// kvstoreScan — one step of a cross-dict SCAN. `only_didx` restricts the
    /// scan to a single dict index (used by `CLUSTER`-aware `SCAN`), matching
    /// `kvstoreScan`'s `onlydidx` parameter; pass `None` to scan every dict.
    /// Returns the next cursor (`0` means the scan is complete).
    pub fn scan<F: FnMut(&K, &V)>(&mut self, cursor: u64, only_didx: Option<usize>, mut f: F) -> u64 {
        let (mut didx, mut sub_cursor) = self.split_cursor(cursor);

        if let Some(only) = only_didx {
            if didx < only {
                didx = only;
                sub_cursor = 0;
            } else if didx > only {
                return 0;
            }
        }

        let mut next_sub = 0u64;
        let skip = self.dicts[didx].is_none();
        if !skip {
            next_sub = self.dicts[didx].as_ref().unwrap().scan(sub_cursor, &mut f);
            self.free_if_needed(didx);
        }

        let next_didx = if next_sub == 0 || skip {
            if only_didx.is_some() {
                return 0;
            }
            self.next_non_empty_dict_index(didx)
        } else {
            Some(didx)
        };

        match next_didx {
            None => 0,
            Some(d) => self.add_dict_index_to_cursor(Some(d), next_sub),
        }
    }

    pub fn allocate_on_demand(&self) -> bool {
        self.allocate_on_demand
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn single_dict_behaves_like_a_plain_dict() {
        let mut kvs: KvStore<i64, i64> = KvStore::new(0, true, false);
        assert_eq!(kvs.num_dicts(), 1);
        kvs.insert(0, 1, 10);
        kvs.insert(0, 2, 20);
        assert_eq!(kvs.size(), 2);
        assert_eq!(kvs.get(0, &1), Some(&10));
        assert_eq!(kvs.remove(0, &1), Some(10));
        assert_eq!(kvs.size(), 1);
    }

    #[test]
    fn on_demand_allocation_only_creates_touched_dicts() {
        let mut kvs: KvStore<i64, i64> = KvStore::new(4, true, false); // 16 dicts
        assert_eq!(kvs.num_allocated_dicts(), 0);
        kvs.insert(3, 1, 1);
        kvs.insert(7, 2, 2);
        assert_eq!(kvs.num_allocated_dicts(), 2);
        assert_eq!(kvs.num_non_empty_dicts(), 2);
        assert_eq!(kvs.size(), 2);
    }

    #[test]
    fn eager_allocation_pre_creates_every_dict() {
        let kvs: KvStore<i64, i64> = KvStore::new(3, false, false); // 8 dicts
        assert_eq!(kvs.num_allocated_dicts(), 8);
        assert_eq!(kvs.num_non_empty_dicts(), 0);
    }

    #[test]
    fn free_empty_dicts_drops_allocation_once_last_key_removed() {
        let mut kvs: KvStore<i64, i64> = KvStore::new(2, true, true); // 4 dicts
        kvs.insert(1, 100, 1);
        assert_eq!(kvs.num_allocated_dicts(), 1);
        kvs.remove(1, &100);
        assert_eq!(kvs.num_allocated_dicts(), 0);
    }

    #[test]
    fn first_and_next_non_empty_dict_index() {
        let mut kvs: KvStore<i64, i64> = KvStore::new(3, true, false); // 8 dicts
        kvs.insert(2, 1, 1);
        kvs.insert(5, 2, 2);
        kvs.insert(6, 3, 3);
        assert_eq!(kvs.first_non_empty_dict_index(), Some(2));
        assert_eq!(kvs.next_non_empty_dict_index(2), Some(5));
        assert_eq!(kvs.next_non_empty_dict_index(5), Some(6));
        assert_eq!(kvs.next_non_empty_dict_index(6), None);
    }

    #[test]
    fn cross_dict_scan_visits_every_key_across_multiple_dicts() {
        let mut kvs: KvStore<i64, i64> = KvStore::new(3, true, false); // 8 dicts
        for i in 0..200i64 {
            let didx = (i % 8) as usize;
            kvs.insert(didx, i, i * 10);
        }
        assert_eq!(kvs.size(), 200);

        let mut seen: HashSet<i64> = HashSet::new();
        let mut cursor = 0u64;
        let mut guard = 0;
        loop {
            cursor = kvs.scan(cursor, None, |k, _| {
                seen.insert(*k);
            });
            guard += 1;
            assert!(guard < 100_000, "scan should terminate");
            if cursor == 0 {
                break;
            }
        }
        for i in 0..200i64 {
            assert!(seen.contains(&i));
        }
    }

    #[test]
    fn scan_restricted_to_only_didx_never_visits_other_dicts() {
        let mut kvs: KvStore<i64, i64> = KvStore::new(2, true, false); // 4 dicts
        kvs.insert(0, 1, 1);
        kvs.insert(1, 2, 2);
        kvs.insert(2, 3, 3);

        let mut seen: HashSet<i64> = HashSet::new();
        let mut cursor = 0u64;
        loop {
            cursor = kvs.scan(cursor, Some(1), |k, _| {
                seen.insert(*k);
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen, HashSet::from([2]));
    }

    #[test]
    fn scan_cursor_is_identity_for_single_dict_case() {
        // With only one dict, kvstore's cursor must be exactly the dict's own
        // cursor (no dict-index bits mixed in) -- this is what lets
        // non-cluster SCAN behave identically to a plain RDict scan.
        let mut kvs: KvStore<i64, i64> = KvStore::new(0, true, false);
        let mut plain: RDict<i64, i64> = RDict::new();
        for i in 0..100i64 {
            kvs.insert(0, i, i);
            plain.insert(i, i);
        }
        let mut kvs_cursor = 0u64;
        let mut plain_cursor = 0u64;
        loop {
            let mut a = Vec::new();
            let mut b = Vec::new();
            kvs_cursor = kvs.scan(kvs_cursor, None, |k, _| a.push(*k));
            plain_cursor = plain.scan(plain_cursor, |k, _| b.push(*k));
            assert_eq!(kvs_cursor, plain_cursor);
            assert_eq!(a, b);
            if kvs_cursor == 0 {
                break;
            }
        }
    }
}
