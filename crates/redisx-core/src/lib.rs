//! redisx-core: Phase 0 (complete) of the redis_src -> redisX migration.
//!
//! Ports the core, dependency-free data structures/utilities that the rest of
//! the engine builds on. See `redis_document/25_redisX.md`'s Phase 0 section
//! for the full rationale behind every scope decision below.
//!
//! Byte/behavior-compatible ports, each cross-checked against either the C
//! source's own embedded self-test vectors or (for `listpack`/`lzf`) actual
//! bytes captured from a running `redis-server`:
//! - `sds`, `crc`: ports of `sds.c`, `crc16.c`, `crc64.c`.
//! - `intset`, `listpack`: on-disk/RDB byte layout preserved exactly.
//! - `dict`: `dict.c`'s two-table incremental rehashing and SCAN cursor
//!   algorithm. C-specific memory-layout optimizations (pointer tagging,
//!   no-value sets) are not ported — no observable effect, don't apply to
//!   safe Rust.
//! - `kvstore`: `kvstore.c`'s array-of-dicts and its combined SCAN cursor
//!   encoding (dict-index bits + per-dict cursor bits). The Fenwick-tree
//!   (`fwtree.c`) acceleration it uses for fair random-key selection is
//!   replaced by a linear scan — see the module doc for why that's fine.
//! - `lzf`: decompression only (`lzf_d.c`); compression is skippable since
//!   redisX can simply not compress when writing RDB files.
//! - `ziplist`, `zipmap`: decode-only legacy RDB formats real Redis replaced
//!   with listpack in 7.0 — this fork never writes either, only reads them
//!   back out of an old dump.
//!
//! Everything else originally scoped into Phase 0 was closed by an explicit
//! decision rather than ported, and moved to the phase that actually needs
//! it — see the Phase 0 table in `redis_document/25_redisX.md` for each:
//! `rax`/`ebuckets`/`estore`/`entry`/`keymeta`/`mstr` → Phase 3 (Stream /
//! hash-field-TTL are their only consumers), `zmalloc` → Phase 2 (blocked on
//! the allocator strategy decision). `sha1`/`sha256`/`ziplist`/`zipmap` were
//! deferred the same way but have since been ported once their actual
//! consumers showed up (`EVALSHA`/`SCRIPT` for `sha1`, ACL password hashing
//! for `sha256`, legacy RDB loading for `ziplist`/`zipmap` — see each
//! module's own doc comment).
//! `fwtree`, `siphash`, `mt19937-64`, `monotonic`, `pqsort`, `endianconv`,
//! `rand`, `adlist` need no port at all: each is either subsumed by a Rust
//! std type/std algorithm or was never observable outside this process
//! (internal hashing/PRNG/timing), so porting them would just be copying a
//! C-specific trick Rust doesn't need.

pub mod crc;
pub mod dict;
pub mod intset;
pub mod kvstore;
pub mod listpack;
pub mod lzf;
pub mod sds;
pub mod sha1;
pub mod sha256;
pub mod ziplist;
pub mod zipmap;

pub use dict::RDict;
pub use intset::IntSet;
pub use kvstore::KvStore;
pub use listpack::ListPack;
pub use sds::Sds;
