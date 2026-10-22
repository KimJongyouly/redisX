//! Set type — ported from `redis_src/src/t_set.c`'s encoding split, with one
//! deliberate simplification: real Redis has *three* encodings (intset →
//! listpack → hashtable, the middle one for small sets of short non-integer
//! members); this port only has two (`IntSet` → `RDict`-as-a-set). Skipping
//! the listpack-encoded variant loses a memory optimization for small string
//! sets, not correctness — every operation works identically either way, and
//! it halves the encoding-transition logic to get right for the phase that's
//! supposed to prove dispatch/types work, not re-litigate every encoding.

use redisx_core::listpack::parse_canonical_i64;
use redisx_core::{IntSet, RDict};

pub enum SetObj {
    IntSet(IntSet),
    Dict(RDict<Vec<u8>, ()>),
}

impl Default for SetObj {
    fn default() -> Self {
        Self::new()
    }
}

impl SetObj {
    pub fn new() -> Self {
        SetObj::IntSet(IntSet::new())
    }

    pub fn len(&self) -> usize {
        match self {
            SetObj::IntSet(s) => s.len(),
            SetObj::Dict(d) => d.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetObj::IntSet(s) => parse_canonical_i64(member).is_some_and(|v| s.find(v)),
            SetObj::Dict(d) => d.contains_key(&member.to_vec()),
        }
    }

    fn convert_to_dict(&mut self) {
        let SetObj::IntSet(s) = self else { return };
        let mut d = RDict::new();
        for v in s.to_vec() {
            d.insert(v.to_string().into_bytes(), ());
        }
        *self = SetObj::Dict(d);
    }

    /// SADD for one member — returns `true` if it was newly added.
    pub fn add(&mut self, member: &[u8], max_intset_entries: i64) -> bool {
        if let SetObj::Dict(d) = self {
            return d.insert(member.to_vec(), ()).is_none();
        }
        let SetObj::IntSet(s) = self else { unreachable!() };
        match parse_canonical_i64(member) {
            Some(v) => {
                let added = s.add(v);
                if added && s.len() > max_intset_entries.max(0) as usize {
                    self.convert_to_dict();
                }
                added
            }
            None => {
                // A non-integer member always forces the intset -> hashtable
                // conversion (there's no listpack middle ground here).
                self.convert_to_dict();
                let SetObj::Dict(d) = self else { unreachable!() };
                d.insert(member.to_vec(), ()).is_none()
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            SetObj::Dict(d) => d.remove(&member.to_vec()).is_some(),
            SetObj::IntSet(s) => parse_canonical_i64(member).is_some_and(|v| s.remove(v)),
        }
    }

    pub fn members(&self) -> Vec<Vec<u8>> {
        match self {
            SetObj::IntSet(s) => s.to_vec().into_iter().map(|v| v.to_string().into_bytes()).collect(),
            SetObj::Dict(d) => d.iter().map(|(k, _)| k.clone()).collect(),
        }
    }

    /// `SPOP [count]` — removes and returns up to `count` distinct random
    /// members (fewer if the set is smaller). `rand` is the caller's
    /// index-picking RNG (`crate::object::pseudo_random_index`, threaded in
    /// rather than called here so `set.rs` stays free of a `server.rs`
    /// dependency).
    pub fn pop_random(&mut self, count: usize, rand: impl Fn(usize) -> usize) -> Vec<Vec<u8>> {
        let mut pool = self.members();
        let n = count.min(pool.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let i = rand(pool.len());
            let member = pool.swap_remove(i);
            self.remove(&member);
            out.push(member);
        }
        out
    }

    /// `SRANDMEMBER [count]`'s sampling core. `count >= 0` samples up to
    /// `count` *distinct* members (capped at the set's size); `count < 0`
    /// samples exactly `-count` members independently, so the same member
    /// can repeat — matching real Redis's two modes.
    pub fn random_members(&self, count: i64, rand: impl Fn(usize) -> usize) -> Vec<Vec<u8>> {
        let pool = self.members();
        if pool.is_empty() {
            return Vec::new();
        }
        if count >= 0 {
            let n = (count as usize).min(pool.len());
            let mut idx: Vec<usize> = (0..pool.len()).collect();
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let i = rand(idx.len());
                out.push(pool[idx.remove(i)].clone());
            }
            out
        } else {
            (0..(-count) as usize).map(|_| pool[rand(pool.len())].clone()).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_members_stay_intset() {
        let mut s = SetObj::new();
        assert!(s.add(b"1", 512));
        assert!(s.add(b"2", 512));
        assert!(!s.add(b"1", 512), "duplicate add returns false");
        assert!(matches!(s, SetObj::IntSet(_)));
        assert!(s.contains(b"1"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn non_canonical_integer_string_is_treated_as_a_string_member() {
        let mut s = SetObj::new();
        s.add(b"007", 512); // leading zero -> not canonical -> forces conversion
        assert!(matches!(s, SetObj::Dict(_)));
        assert!(s.contains(b"007"));
        assert!(!s.contains(b"7"));
    }

    #[test]
    fn non_integer_member_converts_to_dict() {
        let mut s = SetObj::new();
        s.add(b"1", 512);
        s.add(b"hello", 512);
        assert!(matches!(s, SetObj::Dict(_)));
        assert!(s.contains(b"1"));
        assert!(s.contains(b"hello"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn converts_past_intset_entry_threshold() {
        let mut s = SetObj::new();
        for i in 0..5 {
            s.add(i.to_string().as_bytes(), 3);
        }
        assert!(matches!(s, SetObj::Dict(_)));
        assert_eq!(s.len(), 5);
    }

    #[test]
    fn remove_and_members() {
        let mut s = SetObj::new();
        for v in ["1", "2", "3"] {
            s.add(v.as_bytes(), 512);
        }
        assert!(s.remove(b"2"));
        assert!(!s.remove(b"2"));
        let mut m: Vec<i64> = s.members().iter().map(|b| String::from_utf8_lossy(b).parse().unwrap()).collect();
        m.sort();
        assert_eq!(m, vec![1, 3]);
    }

    #[test]
    fn pop_random_removes_distinct_members() {
        let mut s = SetObj::new();
        for v in ["1", "2", "3"] {
            s.add(v.as_bytes(), 512);
        }
        let popped = s.pop_random(2, |len| len - 1);
        assert_eq!(popped.len(), 2);
        assert_eq!(s.len(), 1);
        // Popping more than available just returns what's there, and never panics.
        let rest = s.pop_random(5, |len| len - 1);
        assert_eq!(rest.len(), 1);
        assert!(s.is_empty());
    }

    #[test]
    fn random_members_distinct_vs_with_replacement() {
        let mut s = SetObj::new();
        for v in ["1", "2", "3"] {
            s.add(v.as_bytes(), 512);
        }
        let distinct = s.random_members(10, |len| len - 1);
        assert_eq!(distinct.len(), 3, "count >= set size caps at the distinct member count");
        let with_repl = s.random_members(-5, |len| len - 1);
        assert_eq!(with_repl.len(), 5, "negative count samples with replacement, exact length");
    }
}
