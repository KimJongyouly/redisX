//! Keyspace value type — ported from `redis_src/src/object.c`'s `robj`,
//! widened in Phase 3 to cover the core composite types on top of the
//! `String` variant Phase 2 started with.
//!
//! Each composite type's own module documents its specific encoding
//! simplifications relative to the C original (`list.rs`, `hash.rs`,
//! `set.rs`, `zset.rs`, `stream.rs`). `HyperLogLog`/`Geo`/bitops/`SORT` have
//! no variant of their own — they're layered on top of `String`/`Set`/
//! `ZSet` exactly like real Redis, so they're additive follow-ups on
//! existing storage, not a new `RObj` case.

use crate::hash::HashObj;
use crate::list::ListObj;
use crate::set::SetObj;
use crate::stream::StreamObj;
use crate::zset::ZSetObj;

pub enum RObj {
    String(Vec<u8>),
    List(ListObj),
    Hash(HashObj),
    Set(SetObj),
    ZSet(ZSetObj),
    Stream(StreamObj),
    /// `--features polars` only — absent from the enum entirely in a
    /// default build, not just unreachable, so every other exhaustive match
    /// on `RObj` (there are only 3: this file's two methods below and
    /// `rdb.rs`'s `save_value`/`load_value`) needs no fallback arm when the
    /// feature is off. See `dataframe.rs`'s module doc for scope.
    #[cfg(feature = "polars")]
    DataFrame(crate::dataframe::DataFrameObj),
}

impl RObj {
    pub fn type_name(&self) -> &'static str {
        match self {
            RObj::String(_) => "string",
            RObj::List(_) => "list",
            RObj::Hash(_) => "hash",
            RObj::Set(_) => "set",
            RObj::ZSet(_) => "zset",
            RObj::Stream(_) => "stream",
            #[cfg(feature = "polars")]
            RObj::DataFrame(_) => "dataframe",
        }
    }

    /// `OBJECT ENCODING` — the real encoding name for every variant this
    /// fork actually has (`list.rs`'s always-one-listpack simplification
    /// means a `List` is always reported as `listpack`, never real Redis's
    /// `quicklist` — see its own module doc for why that's an internal
    /// memory-layout difference only, not an observable one). String uses
    /// the same three names/thresholds real Redis does (`OBJ_ENCODING_INT`,
    /// `OBJ_ENCODING_EMBSTR`'s 44-byte cutoff, `OBJ_ENCODING_RAW`).
    pub fn encoding_name(&self) -> &'static str {
        match self {
            RObj::String(v) => {
                if redisx_core::listpack::parse_canonical_i64(v).is_some() {
                    "int"
                } else if v.len() <= 44 {
                    "embstr"
                } else {
                    "raw"
                }
            }
            RObj::List(_) => "listpack",
            RObj::Hash(HashObj::Listpack(_)) => "listpack",
            RObj::Hash(HashObj::Dict(_)) => "hashtable",
            RObj::Set(SetObj::IntSet(_)) => "intset",
            RObj::Set(SetObj::Dict(_)) => "hashtable",
            RObj::ZSet(ZSetObj::Listpack(_)) => "listpack",
            RObj::ZSet(ZSetObj::Sorted(_)) => "skiplist",
            RObj::Stream(_) => "stream",
            #[cfg(feature = "polars")]
            RObj::DataFrame(_) => "dataframe",
        }
    }

    /// Active-defrag's per-value step (`server.rs`'s `defrag_step`/`MEMORY
    /// PURGE`) — shrinks whatever `Vec<u8>`/`Vec<T>` buffer backs this value
    /// down to its exact current length, recovering capacity left behind by
    /// `drain`-based removal (`ListPack::remove`/`IntSet::remove`, neither of
    /// which ever shrinks on their own) or by `Vec::remove` on a `ZSetObj::
    /// Sorted`. `RDict`-backed variants (`HashObj::Dict`/`SetObj::Dict`, and
    /// the db's own top-level dict) need no help here — `RDict::remove`
    /// already self-shrinks its table via `maybe_shrink` whenever the load
    /// factor drops below 1/8, the same policy real Redis's own dict uses.
    /// `Stream`/`DataFrame` are no-ops: streams are append-mostly (no
    /// equivalent capacity-creep path worth chasing for the scope of this
    /// pass) and a `DataFrame`'s memory is polars' own to manage.
    pub fn defrag_in_place(&mut self) {
        match self {
            RObj::String(v) => v.shrink_to_fit(),
            RObj::List(l) => l.shrink_to_fit(),
            RObj::Hash(HashObj::Listpack(lp)) => lp.shrink_to_fit(),
            RObj::Hash(HashObj::Dict(_)) => {}
            RObj::Set(SetObj::IntSet(is)) => is.shrink_to_fit(),
            RObj::Set(SetObj::Dict(_)) => {}
            RObj::ZSet(ZSetObj::Listpack(lp)) => lp.shrink_to_fit(),
            RObj::ZSet(ZSetObj::Sorted(v)) => v.shrink_to_fit(),
            RObj::Stream(_) => {}
            #[cfg(feature = "polars")]
            RObj::DataFrame(_) => {}
        }
    }
}

/// A per-call pseudo-random index in `[0, len)` — used by `SRANDMEMBER`/
/// `SPOP`/`HRANDFIELD`/`ZRANDMEMBER`. Reuses the same "std's own randomness
/// is enough, no `rand` crate needed" call Phase 0 already made for
/// `generate_replid`: nothing here needs cryptographically or even
/// statistically rigorous randomness, just "looks different across calls".
pub(crate) fn pseudo_random_index(len: usize) -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    if len == 0 {
        return 0;
    }
    (RandomState::new().build_hasher().finish() as usize) % len
}

/// A per-call pseudo-random `f64` in `[0, 1)` — same "std's own randomness is
/// enough" rationale as `pseudo_random_index` above, used by the LFU
/// maxmemory policies' probabilistic counter increment (`server.rs`'s
/// `lfu_log_incr`, porting `evict.c`'s `LFULogIncr`, which needs a uniform
/// draw to compare against a probability, not an index).
pub(crate) fn pseudo_random_unit_f64() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    // u32 (not the full u64 hash) so the division below is exact enough to
    // land in [0, 1) without rounding past 1.0, and so two calls in quick
    // succession (`RandomState::new()` is time/address-seeded) still differ.
    (RandomState::new().build_hasher().finish() as u32) as f64 / (u32::MAX as f64 + 1.0)
}
