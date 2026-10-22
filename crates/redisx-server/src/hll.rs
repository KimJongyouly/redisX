//! HyperLogLog — approximate cardinality estimation (`hyperloglog.c`),
//! stored as an ordinary `RObj::String` value (matching real Redis: a HLL
//! has no `RObj` variant of its own — `PFADD`/`PFCOUNT` just interpret a
//! string's bytes). "남은 부분" pass addition.
//!
//! This port deliberately does **not** reproduce real Redis's dense/sparse
//! dual encoding or its exact MurmurHash64A + 6-bit-packed register layout
//! — byte-compatibility with a real Redis-written HLL string was never a
//! goal (nothing else in this fork reads these bytes), so the simplest
//! correct implementation wins:
//! - One byte per register (not 6-bit-packed) — 16KB instead of 12KB,
//!   trivial for a toy workload and much less bug-prone than a bit-packed
//!   accessor.
//! - SHA-1 (already ported, `redisx_core::sha1`) truncated to 64 bits
//!   instead of MurmurHash64A — any well-distributed hash satisfies the
//!   algorithm's statistical guarantees; this reuses what's already in the
//!   codebase instead of porting a second hash function.
//! - Always "dense", no sparse encoding for low-cardinality sets (real
//!   Redis's main space optimization) — correctness/simplicity over memory
//!   efficiency, consistent with this whole port's standing bias (e.g.
//!   Phase 0's Fenwick-tree-to-linear-scan call).
//! - No cached cardinality byte — recomputed every `PFCOUNT`; 16384
//!   registers is cheap enough to sum on every call.

use redisx_core::sha1::sha1;

const REGISTERS: usize = 16384; // 2^14
const INDEX_BITS: u32 = 14;
const HEADER_LEN: usize = 16;
const MAGIC: &[u8; 4] = b"HYLL";

/// Whether `data` looks like one of this module's own HLL strings (used to
/// reject `PFADD`/`PFCOUNT`/`PFMERGE` against a string that just happens to
/// already exist under that key but isn't one — matching real Redis's own
/// "Key is not a valid HyperLogLog string value" check).
pub fn is_hll(data: &[u8]) -> bool {
    data.len() == HEADER_LEN + REGISTERS && data[0..4] == *MAGIC
}

pub fn new_empty() -> Vec<u8> {
    let mut buf = vec![0u8; HEADER_LEN + REGISTERS];
    buf[0..4].copy_from_slice(MAGIC);
    buf[4] = 1; // "dense" marker — kept for readability; this fork is always dense.
    buf
}

fn hash64(element: &[u8]) -> u64 {
    let digest = sha1(element);
    u64::from_be_bytes(digest[0..8].try_into().unwrap())
}

/// Updates `buf`'s registers for one element; returns whether any register
/// actually changed — what `PFADD`'s return value is based on.
pub fn add(buf: &mut [u8], element: &[u8]) -> bool {
    let h = hash64(element);
    let index = (h & (REGISTERS as u64 - 1)) as usize;
    let rest = h >> INDEX_BITS;
    // Position of the leftmost 1-bit among the remaining (64 - INDEX_BITS)
    // bits, 1-based — the classic HyperLogLog "rank" statistic. `rest`'s top
    // `INDEX_BITS` bits are always zero (shifted away), so
    // `rest.leading_zeros() - INDEX_BITS` is exactly the leading-zero count
    // within the bits that matter — including the `rest == 0` case, where
    // `leading_zeros()` on an all-zero `u64` is defined as 64.
    let rank = ((rest.leading_zeros() - INDEX_BITS) + 1).min(63) as u8;

    let slot = HEADER_LEN + index;
    if buf[slot] < rank {
        buf[slot] = rank;
        true
    } else {
        false
    }
}

/// `hllCount` — estimates cardinality from the registers, with the
/// small-range linear-counting correction real Redis also applies. The
/// large-range (near-2^32) correction doesn't apply here: this port always
/// hashes to a full 64 bits, never the narrower range that needs it.
pub fn count(buf: &[u8]) -> u64 {
    let registers = &buf[HEADER_LEN..HEADER_LEN + REGISTERS];
    let m = REGISTERS as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);

    let mut sum = 0.0;
    let mut zeros = 0u32;
    for &r in registers {
        sum += 1.0 / (1u64 << r) as f64;
        if r == 0 {
            zeros += 1;
        }
    }
    let mut estimate = alpha * m * m / sum;
    if estimate <= 2.5 * m && zeros > 0 {
        estimate = m * (m / f64::from(zeros)).ln();
    }
    estimate.round() as u64
}

/// `PFMERGE` — register-wise max across every source HLL.
pub fn merge(sources: &[&[u8]]) -> Vec<u8> {
    let mut out = new_empty();
    for src in sources {
        for i in 0..REGISTERS {
            if src[HEADER_LEN + i] > out[HEADER_LEN + i] {
                out[HEADER_LEN + i] = src[HEADER_LEN + i];
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hll_counts_as_zero() {
        assert_eq!(count(&new_empty()), 0);
    }

    #[test]
    fn is_hll_recognizes_its_own_format_and_rejects_plain_strings() {
        assert!(is_hll(&new_empty()));
        assert!(!is_hll(b"just a plain string"));
    }

    #[test]
    fn counts_roughly_match_actual_distinct_elements() {
        let mut buf = new_empty();
        let n = 10_000;
        for i in 0..n {
            add(&mut buf, format!("element-{i}").as_bytes());
        }
        let estimate = count(&buf);
        let error = (estimate as f64 - n as f64).abs() / n as f64;
        assert!(error < 0.05, "estimate {estimate} too far from actual {n} (error {error})");
    }

    #[test]
    fn adding_the_same_element_twice_does_not_grow_the_count() {
        let mut buf = new_empty();
        add(&mut buf, b"x");
        let first = count(&buf);
        add(&mut buf, b"x");
        assert_eq!(count(&buf), first);
    }

    #[test]
    fn pfadd_return_value_reflects_whether_a_register_actually_changed() {
        let mut buf = new_empty();
        assert!(add(&mut buf, b"x"), "first add always changes something from all-zero");
        assert!(!add(&mut buf, b"x"), "same element again shouldn't raise its own register");
    }

    #[test]
    fn merge_estimates_the_union_cardinality() {
        let mut a = new_empty();
        let mut b = new_empty();
        for i in 0..5000 {
            add(&mut a, format!("a-{i}").as_bytes());
        }
        for i in 0..5000 {
            add(&mut b, format!("b-{i}").as_bytes());
        }
        let merged = merge(&[&a, &b]);
        let estimate = count(&merged);
        let error = (estimate as f64 - 10_000.0).abs() / 10_000.0;
        assert!(error < 0.05, "merged estimate {estimate} too far from 10000 (error {error})");
    }
}
