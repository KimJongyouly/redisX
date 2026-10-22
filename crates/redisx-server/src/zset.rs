//! Sorted set type — ported from `redis_src/src/t_zset.c`'s encoding split:
//! a `ListPack` of `member, score` pairs kept in `(score, member)` order
//! while small, converted past `zset-max-listpack-entries`/`-value`.
//!
//! Real Redis's large encoding is a skiplist + dict pair (`O(log n)`
//! insert/rank/range). This port uses a plain `Vec<(Vec<u8>, f64)>` kept
//! sorted by binary-searched insertion (`O(n)` insert, `O(log n)` lookup by
//! rank) instead — identical externally-observable ordering and results for
//! `ZADD`/`ZSCORE`/`ZRANK`/`ZRANGE`/`ZREM`, just without the skiplist's
//! asymptotic insert advantage. Same tradeoff Phase 0 already made for
//! `kvstore`'s Fenwick tree: correctness first, this specific performance
//! optimization deferred (`redis_document/25_redisX.md`).

use redisx_core::ListPack;
use std::cmp::Ordering;

pub enum ZSetObj {
    Listpack(ListPack),
    Sorted(Vec<(Vec<u8>, f64)>),
}

/// One endpoint of a `ZRANGEBYLEX`/`ZLEXCOUNT` range. Lex ranges only make
/// sense when every member in the set shares the same score (the command's
/// own precondition, same as real Redis — not enforced here, just assumed).
#[derive(Debug, Clone, PartialEq)]
pub enum LexBound {
    NegInf,
    PosInf,
    Incl(Vec<u8>),
    Excl(Vec<u8>),
}

impl LexBound {
    /// True if `member` is on the "at or above this minimum" side.
    fn allows_above(&self, member: &[u8]) -> bool {
        match self {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Incl(b) => member >= b.as_slice(),
            LexBound::Excl(b) => member > b.as_slice(),
        }
    }

    /// True if `member` is on the "at or below this maximum" side.
    fn allows_below(&self, member: &[u8]) -> bool {
        match self {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Incl(b) => member <= b.as_slice(),
            LexBound::Excl(b) => member < b.as_slice(),
        }
    }
}

impl Default for ZSetObj {
    fn default() -> Self {
        Self::new()
    }
}

/// zset ordering: by score first, then lexicographically by member on ties
/// (`zslInsert`'s tie-break rule).
fn cmp_entry(a_score: f64, a_member: &[u8], b_score: f64, b_member: &[u8]) -> Ordering {
    a_score.partial_cmp(&b_score).unwrap_or(Ordering::Equal).then_with(|| a_member.cmp(b_member))
}

impl ZSetObj {
    pub fn new() -> Self {
        ZSetObj::Listpack(ListPack::new())
    }

    pub fn len(&self) -> usize {
        match self {
            ZSetObj::Listpack(lp) => lp.len() / 2,
            ZSetObj::Sorted(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn listpack_entries(lp: &ListPack) -> Vec<(Vec<u8>, f64)> {
        (0..lp.len() / 2)
            .map(|i| {
                let member = lp.get_bytes(i * 2).unwrap();
                let score: f64 = std::str::from_utf8(&lp.get_bytes(i * 2 + 1).unwrap()).unwrap().parse().unwrap();
                (member, score)
            })
            .collect()
    }

    fn listpack_find(lp: &ListPack, member: &[u8]) -> Option<usize> {
        (0..lp.len() / 2).find(|&i| lp.get_bytes(i * 2).as_deref() == Some(member))
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            ZSetObj::Listpack(lp) => {
                let i = Self::listpack_find(lp, member)?;
                std::str::from_utf8(&lp.get_bytes(i * 2 + 1)?).ok()?.parse().ok()
            }
            ZSetObj::Sorted(v) => v.iter().find(|(m, _)| m == member).map(|(_, s)| *s),
        }
    }

    fn convert_to_sorted(&mut self) {
        let ZSetObj::Listpack(lp) = self else { return };
        let mut entries = Self::listpack_entries(lp);
        entries.sort_by(|(am, asc), (bm, bsc)| cmp_entry(*asc, am, *bsc, bm));
        *self = ZSetObj::Sorted(entries);
    }

    /// `ZADD` for one member — returns `true` if it was newly added (as
    /// opposed to an existing member's score being updated).
    pub fn add(&mut self, member: &[u8], score: f64, max_entries: i64, max_value: i64) -> bool {
        if let ZSetObj::Sorted(v) = self {
            let is_new = match v.iter().position(|(m, _)| m == member) {
                Some(pos) => {
                    v.remove(pos);
                    false
                }
                None => true,
            };
            let idx = v.partition_point(|(m, s)| cmp_entry(*s, m, score, member) == Ordering::Less);
            v.insert(idx, (member.to_vec(), score));
            return is_new;
        }

        let ZSetObj::Listpack(lp) = self else { unreachable!() };
        let existing = Self::listpack_find(lp, member);
        let is_new = existing.is_none();
        if let Some(i) = existing {
            lp.remove(i * 2 + 1);
            lp.remove(i * 2);
        }
        // Re-scan for the sorted insertion point (any existing occurrence
        // was just removed above, so indices are stable for this pass).
        let n = lp.len() / 2;
        let mut pos = n;
        for i in 0..n {
            let m = lp.get_bytes(i * 2).unwrap();
            let s: f64 = std::str::from_utf8(&lp.get_bytes(i * 2 + 1).unwrap()).unwrap().parse().unwrap();
            if cmp_entry(score, member, s, &m) == Ordering::Less {
                pos = i;
                break;
            }
        }
        lp.insert_str(pos * 2, member);
        lp.insert_str(pos * 2 + 1, score.to_string().as_bytes());

        let too_many = lp.len() / 2 > max_entries.max(0) as usize;
        let too_big = member.len() as i64 > max_value;
        if too_many || too_big {
            self.convert_to_sorted();
        }
        is_new
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            ZSetObj::Sorted(v) => match v.iter().position(|(m, _)| m == member) {
                Some(pos) => {
                    v.remove(pos);
                    true
                }
                None => false,
            },
            ZSetObj::Listpack(lp) => match Self::listpack_find(lp, member) {
                Some(i) => {
                    lp.remove(i * 2 + 1);
                    lp.remove(i * 2);
                    true
                }
                None => false,
            },
        }
    }

    /// `ZRANK` — 0-based position in ascending `(score, member)` order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        match self {
            ZSetObj::Sorted(v) => v.iter().position(|(m, _)| m == member),
            ZSetObj::Listpack(lp) => Self::listpack_find(lp, member),
        }
    }

    /// Every `(member, score)` pair in ascending `(score, member)` order,
    /// regardless of encoding — the shared read path for `range`,
    /// `range_by_score`, `pop`, and `random_members`.
    fn entries(&self) -> Vec<(Vec<u8>, f64)> {
        match self {
            ZSetObj::Sorted(v) => v.clone(),
            ZSetObj::Listpack(lp) => Self::listpack_entries(lp),
        }
    }

    /// `ZRANGE start stop` by rank — inclusive, negative indices from the
    /// end, clamped (same convention as `ListObj::range`).
    pub fn range(&self, start: i64, end: i64) -> Vec<(Vec<u8>, f64)> {
        let entries = self.entries();
        let len = entries.len() as i64;
        if len == 0 {
            return Vec::new();
        }
        let s = if start < 0 { (len + start).max(0) } else { start };
        let e = if end < 0 { len + end } else { end.min(len - 1) };
        if s > e || s >= len || e < 0 {
            return Vec::new();
        }
        entries[(s as usize)..=(e as usize)].to_vec()
    }

    /// `ZRANGEBYSCORE`/`ZCOUNT` — inclusive bounds by default; `min_excl`/
    /// `max_excl` make either bound exclusive (the command's own `(` prefix
    /// syntax), matching `zslParseRange`.
    pub fn range_by_score(&self, min: f64, min_excl: bool, max: f64, max_excl: bool) -> Vec<(Vec<u8>, f64)> {
        self.entries()
            .into_iter()
            .filter(|(_, s)| {
                let above_min = if min_excl { *s > min } else { *s >= min };
                let below_max = if max_excl { *s < max } else { *s <= max };
                above_min && below_max
            })
            .collect()
    }

    /// One endpoint of a `ZRANGEBYLEX`-style range: `-`/`+` (open-ended) or
    /// an inclusive/exclusive member bound (the command's `[`/`(` prefix
    /// syntax), matching `zslParseLexRangeItem`.
    pub fn range_by_lex(&self, min: &LexBound, max: &LexBound) -> Vec<(Vec<u8>, f64)> {
        self.entries()
            .into_iter()
            .filter(|(m, _)| min.allows_above(m) && max.allows_below(m))
            .collect()
    }

    /// `ZINCRBY` — adds `delta` to the member's current score (0 if absent)
    /// and returns the new score.
    pub fn incr_by(&mut self, member: &[u8], delta: f64, max_entries: i64, max_value: i64) -> f64 {
        let next = self.score(member).unwrap_or(0.0) + delta;
        self.add(member, next, max_entries, max_value);
        next
    }

    /// `ZPOPMIN`/`ZPOPMAX` — removes and returns up to `count` entries from
    /// whichever end `from_min` selects (fewer if the set is smaller).
    pub fn pop(&mut self, count: usize, from_min: bool) -> Vec<(Vec<u8>, f64)> {
        let mut entries = self.entries();
        if !from_min {
            entries.reverse();
        }
        let picked: Vec<(Vec<u8>, f64)> = entries.into_iter().take(count).collect();
        for (m, _) in &picked {
            self.remove(m);
        }
        picked
    }

    /// `ZRANDMEMBER [count]`'s sampling core — same two-mode convention as
    /// `SetObj::random_members`: `count >= 0` samples up to `count` distinct
    /// entries, `count < 0` samples exactly `-count` independently (with
    /// repeats allowed).
    pub fn random_members(&self, count: i64, rand: impl Fn(usize) -> usize) -> Vec<(Vec<u8>, f64)> {
        let pool = self.entries();
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
    fn add_score_update_and_rank_order() {
        let mut z = ZSetObj::new();
        assert!(z.add(b"a", 3.0, 128, 64));
        assert!(z.add(b"b", 1.0, 128, 64));
        assert!(z.add(b"c", 2.0, 128, 64));
        assert_eq!(z.range(0, -1), vec![(b"b".to_vec(), 1.0), (b"c".to_vec(), 2.0), (b"a".to_vec(), 3.0)]);
        assert_eq!(z.rank(b"a"), Some(2));
        assert!(!z.add(b"a", 0.5, 128, 64), "re-adding an existing member updates score, isn't 'new'");
        assert_eq!(z.score(b"a"), Some(0.5));
        assert_eq!(z.rank(b"a"), Some(0), "lower score moves it to the front");
    }

    #[test]
    fn ties_break_lexicographically_by_member() {
        let mut z = ZSetObj::new();
        z.add(b"zebra", 1.0, 128, 64);
        z.add(b"apple", 1.0, 128, 64);
        assert_eq!(z.range(0, -1), vec![(b"apple".to_vec(), 1.0), (b"zebra".to_vec(), 1.0)]);
    }

    #[test]
    fn remove_and_card() {
        let mut z = ZSetObj::new();
        z.add(b"a", 1.0, 128, 64);
        z.add(b"b", 2.0, 128, 64);
        assert!(z.remove(b"a"));
        assert!(!z.remove(b"a"));
        assert_eq!(z.len(), 1);
    }

    #[test]
    fn converts_to_sorted_past_entry_threshold_and_stays_correct() {
        let mut z = ZSetObj::new();
        for i in 0..5 {
            z.add(format!("m{i}").as_bytes(), (5 - i) as f64, 3, 64);
        }
        assert!(matches!(z, ZSetObj::Sorted(_)));
        let ranked: Vec<Vec<u8>> = z.range(0, -1).into_iter().map(|(m, _)| m).collect();
        assert_eq!(ranked, vec![b"m4".to_vec(), b"m3".to_vec(), b"m2".to_vec(), b"m1".to_vec(), b"m0".to_vec()]);
    }

    #[test]
    fn converts_past_member_length_threshold() {
        let mut z = ZSetObj::new();
        z.add(&[b'x'; 100], 1.0, 128, 64);
        assert!(matches!(z, ZSetObj::Sorted(_)));
    }

    #[test]
    fn range_by_score_respects_exclusive_bounds() {
        let mut z = ZSetObj::new();
        for (m, s) in [("a", 1.0), ("b", 2.0), ("c", 3.0)] {
            z.add(m.as_bytes(), s, 128, 64);
        }
        assert_eq!(z.range_by_score(1.0, false, 3.0, false), vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), 2.0), (b"c".to_vec(), 3.0)]);
        assert_eq!(z.range_by_score(1.0, true, 3.0, true), vec![(b"b".to_vec(), 2.0)]);
        assert_eq!(z.range_by_score(2.0, false, 2.0, false), vec![(b"b".to_vec(), 2.0)]);
    }

    #[test]
    fn range_by_lex_respects_bounds_and_inclusivity() {
        let mut z = ZSetObj::new();
        for m in ["a", "b", "c", "d"] {
            z.add(m.as_bytes(), 0.0, 128, 64);
        }
        let all: Vec<Vec<u8>> = z.range_by_lex(&LexBound::NegInf, &LexBound::PosInf).into_iter().map(|(m, _)| m).collect();
        assert_eq!(all, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        let incl: Vec<Vec<u8>> = z.range_by_lex(&LexBound::Incl(b"b".to_vec()), &LexBound::Incl(b"c".to_vec())).into_iter().map(|(m, _)| m).collect();
        assert_eq!(incl, vec![b"b".to_vec(), b"c".to_vec()]);

        let excl: Vec<Vec<u8>> = z.range_by_lex(&LexBound::Excl(b"b".to_vec()), &LexBound::Excl(b"d".to_vec())).into_iter().map(|(m, _)| m).collect();
        assert_eq!(excl, vec![b"c".to_vec()]);
    }

    #[test]
    fn incr_by_adds_to_current_score() {
        let mut z = ZSetObj::new();
        assert_eq!(z.incr_by(b"a", 5.0, 128, 64), 5.0);
        assert_eq!(z.incr_by(b"a", -2.0, 128, 64), 3.0);
        assert_eq!(z.score(b"a"), Some(3.0));
    }

    #[test]
    fn pop_min_and_max() {
        let mut z = ZSetObj::new();
        for (m, s) in [("a", 1.0), ("b", 2.0), ("c", 3.0)] {
            z.add(m.as_bytes(), s, 128, 64);
        }
        assert_eq!(z.pop(1, true), vec![(b"a".to_vec(), 1.0)]);
        assert_eq!(z.pop(5, false), vec![(b"c".to_vec(), 3.0), (b"b".to_vec(), 2.0)], "popping more than available just returns what's there");
        assert!(z.is_empty());
    }

    #[test]
    fn random_members_distinct_vs_with_replacement() {
        let mut z = ZSetObj::new();
        for (m, s) in [("a", 1.0), ("b", 2.0), ("c", 3.0)] {
            z.add(m.as_bytes(), s, 128, 64);
        }
        let distinct = z.random_members(10, |len| len - 1);
        assert_eq!(distinct.len(), 3);
        let with_repl = z.random_members(-5, |len| len - 1);
        assert_eq!(with_repl.len(), 5);
    }
}
