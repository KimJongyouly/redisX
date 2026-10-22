//! Hash type — ported from `redis_src/src/t_hash.c`'s small/large encoding
//! split: a `ListPack` of alternating `field, value` pairs while small, a
//! full `RDict` once it grows past the configured thresholds
//! (`hash-max-listpack-entries`/`hash-max-listpack-value`). Unlike the C
//! version there is no downgrade path back to listpack (matching real
//! Redis — once converted, always a hashtable).
//!
//! Simplification: updating an existing field in listpack mode removes the
//! old `field, value` pair and re-appends the new one at the tail, instead of
//! `t_hash.c`'s in-place `lpReplace`. This can reorder fields relative to
//! real Redis's `HGETALL` output, which the protocol never guarantees an
//! order for anyway — not a correctness difference, just a cosmetic one.

use redisx_core::{ListPack, RDict};

pub enum HashObj {
    Listpack(ListPack),
    Dict(RDict<Vec<u8>, Vec<u8>>),
}

impl Default for HashObj {
    fn default() -> Self {
        Self::new()
    }
}

impl HashObj {
    pub fn new() -> Self {
        HashObj::Listpack(ListPack::new())
    }

    pub fn len(&self) -> usize {
        match self {
            HashObj::Listpack(lp) => lp.len() / 2,
            HashObj::Dict(d) => d.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn listpack_find(lp: &ListPack, field: &[u8]) -> Option<usize> {
        (0..lp.len() / 2).find(|&i| lp.get_bytes(i * 2).as_deref() == Some(field))
    }

    pub fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        match self {
            HashObj::Listpack(lp) => Self::listpack_find(lp, field).and_then(|i| lp.get_bytes(i * 2 + 1)),
            HashObj::Dict(d) => d.get(&field.to_vec()).cloned(),
        }
    }

    pub fn exists(&self, field: &[u8]) -> bool {
        match self {
            HashObj::Listpack(lp) => Self::listpack_find(lp, field).is_some(),
            HashObj::Dict(d) => d.contains_key(&field.to_vec()),
        }
    }

    fn convert_to_dict(&mut self) {
        let HashObj::Listpack(lp) = self else { return };
        let mut d = RDict::new();
        for i in 0..lp.len() / 2 {
            d.insert(lp.get_bytes(i * 2).unwrap(), lp.get_bytes(i * 2 + 1).unwrap());
        }
        *self = HashObj::Dict(d);
    }

    /// HSET for one field — returns `true` if this created a new field
    /// (matching `HSET`'s "number of fields that were added" semantics).
    pub fn set(&mut self, field: &[u8], value: &[u8], max_entries: i64, max_value: i64) -> bool {
        if let HashObj::Dict(d) = self {
            return d.insert(field.to_vec(), value.to_vec()).is_none();
        }
        let HashObj::Listpack(lp) = self else { unreachable!() };
        let existing = Self::listpack_find(lp, field);
        let is_new = existing.is_none();
        if let Some(i) = existing {
            lp.remove(i * 2 + 1);
            lp.remove(i * 2);
        }
        lp.push_str(field);
        lp.push_str(value);

        let too_many = lp.len() / 2 > max_entries.max(0) as usize;
        let too_big = field.len() as i64 > max_value || value.len() as i64 > max_value;
        if too_many || too_big {
            self.convert_to_dict();
        }
        is_new
    }

    pub fn del(&mut self, field: &[u8]) -> bool {
        match self {
            HashObj::Dict(d) => d.remove(&field.to_vec()).is_some(),
            HashObj::Listpack(lp) => match Self::listpack_find(lp, field) {
                Some(i) => {
                    lp.remove(i * 2 + 1);
                    lp.remove(i * 2);
                    true
                }
                None => false,
            },
        }
    }

    pub fn get_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            HashObj::Listpack(lp) => {
                (0..lp.len() / 2).map(|i| (lp.get_bytes(i * 2).unwrap(), lp.get_bytes(i * 2 + 1).unwrap())).collect()
            }
            HashObj::Dict(d) => d.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }

    /// `HINCRBY` — same canonical-decimal-integer convention as top-level
    /// `INCR`. `Err` carries the exact user-facing message for a non-integer
    /// existing value or an overflow.
    pub fn incr_by(&mut self, field: &[u8], delta: i64, max_entries: i64, max_value: i64) -> Result<i64, String> {
        let current = match self.get(field) {
            Some(v) => match std::str::from_utf8(&v).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(n) => n,
                None => return Err("ERR hash value is not an integer".to_string()),
            },
            None => 0,
        };
        let Some(next) = current.checked_add(delta) else {
            return Err("ERR increment or decrement would overflow".to_string());
        };
        self.set(field, next.to_string().as_bytes(), max_entries, max_value);
        Ok(next)
    }

    /// `HINCRBYFLOAT` — `fmt` formats the result the same trailing-zero-free
    /// way `fmt_score`/`INCRBYFLOAT` do; passed in rather than duplicated
    /// here since `hash.rs` has no reason to depend on `server.rs`.
    pub fn incr_by_float(&mut self, field: &[u8], delta: f64, max_entries: i64, max_value: i64, fmt: impl Fn(f64) -> Vec<u8>) -> Result<Vec<u8>, String> {
        let current = match self.get(field) {
            Some(v) => match std::str::from_utf8(&v).ok().and_then(|s| s.parse::<f64>().ok()) {
                Some(n) => n,
                None => return Err("ERR hash value is not a float".to_string()),
            },
            None => 0.0,
        };
        let next = current + delta;
        if !next.is_finite() {
            return Err("ERR increment would produce NaN or Infinity".to_string());
        }
        let formatted = fmt(next);
        self.set(field, &formatted, max_entries, max_value);
        Ok(formatted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_del_exists_roundtrip() {
        let mut h = HashObj::new();
        assert!(h.set(b"f1", b"v1", 128, 64));
        assert!(!h.set(b"f1", b"v2", 128, 64), "updating an existing field is not a new field");
        assert_eq!(h.get(b"f1"), Some(b"v2".to_vec()));
        assert!(h.exists(b"f1"));
        assert!(!h.exists(b"missing"));
        assert!(h.del(b"f1"));
        assert!(!h.del(b"f1"));
        assert!(h.is_empty());
    }

    #[test]
    fn converts_to_dict_past_entry_threshold() {
        let mut h = HashObj::new();
        for i in 0..5 {
            h.set(format!("f{i}").as_bytes(), b"v", 3, 64);
        }
        assert!(matches!(h, HashObj::Dict(_)));
        assert_eq!(h.len(), 5);
        for i in 0..5 {
            assert_eq!(h.get(format!("f{i}").as_bytes()), Some(b"v".to_vec()));
        }
    }

    #[test]
    fn converts_to_dict_past_value_length_threshold() {
        let mut h = HashObj::new();
        h.set(b"f", &[b'x'; 100], 128, 64);
        assert!(matches!(h, HashObj::Dict(_)));
    }

    #[test]
    fn get_all_returns_every_pair() {
        let mut h = HashObj::new();
        h.set(b"a", b"1", 128, 64);
        h.set(b"b", b"2", 128, 64);
        let mut pairs = h.get_all();
        pairs.sort();
        assert_eq!(pairs, vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())]);
    }

    #[test]
    fn stays_correct_across_the_conversion_boundary() {
        let mut h = HashObj::new();
        h.set(b"a", b"1", 2, 64);
        h.set(b"b", b"2", 2, 64);
        assert!(matches!(h, HashObj::Listpack(_)));
        h.set(b"c", b"3", 2, 64); // 3 > max_entries=2 -> converts
        assert!(matches!(h, HashObj::Dict(_)));
        assert_eq!(h.len(), 3);
        assert_eq!(h.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(h.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(h.get(b"c"), Some(b"3".to_vec()));
    }

    #[test]
    fn incr_by_tracks_an_integer_field() {
        let mut h = HashObj::new();
        assert_eq!(h.incr_by(b"n", 5, 128, 64), Ok(5));
        assert_eq!(h.incr_by(b"n", -2, 128, 64), Ok(3));
        h.set(b"notnum", b"abc", 128, 64);
        assert!(h.incr_by(b"notnum", 1, 128, 64).is_err());
    }

    #[test]
    fn incr_by_float_tracks_a_float_field() {
        let mut h = HashObj::new();
        let fmt = |f: f64| f.to_string().into_bytes();
        assert_eq!(h.incr_by_float(b"f", 10.5, 128, 64, fmt), Ok(b"10.5".to_vec()));
        assert_eq!(h.incr_by_float(b"f", 0.1, 128, 64, fmt), Ok(b"10.6".to_vec()));
    }
}
