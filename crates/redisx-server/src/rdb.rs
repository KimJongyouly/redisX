//! RDB persistence — ported from `redis_src/src/rdb.c`'s length/string
//! encoding and the handful of object types this fork's `RObj` actually has
//! (`String`/`List`/`Hash`/`Set`/`ZSet`). Reuses Phase 0's byte-compatible
//! `listpack`/`intset`/`lzf`/`crc64` directly — this is exactly why those were
//! ported byte-for-byte instead of "close enough": an RDB file produced by
//! real `redis-server` embeds those exact structures as opaque byte blobs,
//! and this module needs to read them as-is.
//!
//! ## Format scope (what's supported)
//!
//! Written: the modern (Redis 7.2+ default) encodings only —
//! `RDB_TYPE_STRING`, `RDB_TYPE_LIST_QUICKLIST_2` (as a single PACKED node,
//! matching this fork's always-one-listpack `ListObj`), `RDB_TYPE_HASH`/
//! `RDB_TYPE_HASH_LISTPACK`, `RDB_TYPE_SET`/`RDB_TYPE_SET_INTSET`,
//! `RDB_TYPE_ZSET_2`/`RDB_TYPE_ZSET_LISTPACK`. String values use integer
//! encoding (`RDB_ENC_INT8/16/32`) but never LZF — consistent with Phase 0's
//! decision to port LZF *decompression* only (`lzf.rs`'s doc comment): this
//! fork simply never writes compressed strings, which real Redis can always
//! read back fine.
//!
//! Read (for interop with real `redis-server` dumps, which may well have been
//! written with compression and by various Redis versions): everything above
//! plus LZF-compressed strings, `RDB_TYPE_SET_LISTPACK` (decoded into
//! whichever of `SetObj`'s two encodings fits), the legacy ziplist/
//! zipmap-backed types (`RDB_TYPE_LIST_ZIPLIST`/`RDB_TYPE_HASH_ZIPLIST`/
//! `RDB_TYPE_ZSET_ZIPLIST`/`RDB_TYPE_HASH_ZIPMAP` — opcodes 10/13/12/9,
//! ported in `redisx_core::ziplist`/`redisx_core::zipmap`; decode-only,
//! re-encoded into this fork's native `ListPack` on load since those are
//! different byte formats from the modern listpack-backed types), and the
//! even-older pre-ziplist plain list/zset formats (`RDB_TYPE_LIST`/
//! `RDB_TYPE_ZSET` — opcodes 1/3: a flat count of elements with no
//! sub-container at all, and `rdbSaveDoubleValue`'s ASCII-string score
//! encoding, respectively — decode-only, same as the ziplist/zipmap types).
//! `AUX`/`RESIZEDB`/`FREQ`/`IDLE`/`EXPIRETIME[_MS]` opcodes are recognized
//! and skipped (expire times are parsed but not applied — there is no
//! key-expiry feature yet).
//!
//! `FUNCTION2` (opcode 245) is round-tripped for real: this fork's own
//! `FUNCTION LOAD`ed libraries (`script.rs`) are written as their original
//! source text and re-loaded through the same `Server::function_load` path
//! `FUNCTION LOAD` itself uses, so libraries survive `SAVE`/restart.
//! `SLOT_INFO`/`MODULE_AUX`/`KEY_META` (opcodes 244/247/243) are recognized
//! and gracefully skipped rather than erroring — this fork has no cluster
//! slots, module system, or metadata-class registry to apply them to (same
//! as real Redis's own "ignore gracefully" path for `SLOT_INFO` outside
//! cluster mode, or an unregistered metadata class under `KEY_META`'s
//! `ALLOW_IGNORE` fallback), but a real `redis-server` dump carrying them
//! (e.g. taken in cluster mode, or by a server with modules loaded) no
//! longer fails this fork's load outright. `MODULE_AUX`/`KEY_META`'s
//! payloads use the same generic opcode-tagged scalar stream
//! (`Reader::skip_module_value`), which is self-describing enough to skip
//! without understanding what it actually means.

use crate::hash::HashObj;
use crate::list::ListObj;
use crate::object::RObj;
use crate::server::Server;
use crate::set::SetObj;
use crate::zset::ZSetObj;
use redisx_core::listpack::parse_canonical_i64;
use redisx_core::{crc, lzf, IntSet, ListPack, RDict};
use redisx_net::resp::Reply;

const RDB_VERSION: u32 = 14;

const TYPE_STRING: u8 = 0;
/// Pre-`quicklist` plain linked-list encoding (predates even `ziplist`) —
/// a flat count followed by that many `rdbGenericLoadStringObject`-encoded
/// elements, no sub-container at all. Decode-only, same as the ziplist/
/// zipmap legacy types (Phase 12): nothing this fork writes uses it.
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
/// Pre-`ZSET_2` encoding: member/score pairs with the score as an ASCII
/// string (`rdbSaveDoubleValue`'s 1-byte-length-prefixed form, with 253/
/// 254/255 standing in for NaN/+inf/-inf) rather than `ZSET_2`'s raw
/// binary `f64`. Decode-only, same scope note as `TYPE_LIST` above.
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
const TYPE_HASH_ZIPMAP: u8 = 9;
const TYPE_LIST_ZIPLIST: u8 = 10;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_ZSET_ZIPLIST: u8 = 12;
const TYPE_HASH_ZIPLIST: u8 = 13;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_SET_LISTPACK: u8 = 20;
/// Not a real Redis RDB opcode (`t_stream.c`'s actual stream encodings are
/// `RDB_TYPE_STREAM_LISTPACKS{,_2,_3}` = 15/19/21, a `rax`-of-listpacks
/// format this fork doesn't reproduce — see `stream.rs`'s module doc for
/// why). This byte is this fork's own simple, self-consistent encoding:
/// round-trips through `SAVE`/load against itself, but a real
/// `redis-server` would reject it (and this fork would reject a real
/// stream-bearing dump for the same reason) — an accepted limitation, not
/// an oversight, since full Stream RDB interop was never this phase's goal.
const TYPE_STREAM_CUSTOM: u8 = 200;
/// `--features polars` only (see `dataframe.rs`'s module doc). Same
/// "fork-only, round-trips against itself" shape as `TYPE_STREAM_CUSTOM`
/// just above — real Redis has no DataFrame value at all, so there's no
/// byte-compatible format to target. The payload is the dataframe's own
/// Arrow IPC encoding (`DataFrameObj::to_ipc_bytes`/`from_ipc_bytes` —
/// the same bytes `DF.GET`/`DF.SET` move over the wire), wrapped in the
/// usual `write_string`/`read_string` length-prefixed framing, so this
/// module doesn't need to understand Arrow's own layout at all.
#[cfg(feature = "polars")]
const TYPE_DATAFRAME_CUSTOM: u8 = 201;

/// Prefixes a key's `[type][key][value]` triple with module-specific
/// per-class metadata (hash-field-TTL's `ebuckets`/`keymeta` being the one
/// real consumer — see Phase 0's doc note on why that feature itself is
/// still unported). The metadata *value* format is the same generic
/// opcode-tagged stream `OPCODE_MODULE_AUX` uses, so `Reader::skip_module_value`
/// is shared between both — this fork has no metadata class to actually
/// interpret, so every class is skipped, same as real Redis's own
/// `ALLOW_IGNORE` fallback for an unregistered class.
const OPCODE_KEY_META: u8 = 243;
const OPCODE_SLOT_INFO: u8 = 244;
const OPCODE_FUNCTION2: u8 = 245;
const OPCODE_MODULE_AUX: u8 = 247;
/// Generic opcode-tagged scalar stream terminator used by both
/// `OPCODE_MODULE_AUX` and `OPCODE_KEY_META`'s per-class values
/// (`RDB_MODULE_OPCODE_EOF`).
const MODULE_OPCODE_EOF: u64 = 0;
const MODULE_OPCODE_SINT: u64 = 1;
const MODULE_OPCODE_UINT: u64 = 2;
const MODULE_OPCODE_FLOAT: u64 = 3;
const MODULE_OPCODE_DOUBLE: u64 = 4;
const MODULE_OPCODE_STRING: u64 = 5;
const OPCODE_IDLE: u8 = 248;
const OPCODE_FREQ: u8 = 249;
const OPCODE_AUX: u8 = 250;
const OPCODE_RESIZEDB: u8 = 251;
const OPCODE_EXPIRETIME_MS: u8 = 252;
const OPCODE_EXPIRETIME: u8 = 253;
const OPCODE_SELECTDB: u8 = 254;
const OPCODE_EOF: u8 = 255;

const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

// ------------------------------------------------------------- encoding ---

/// rdbSaveLen
fn write_len(buf: &mut Vec<u8>, len: u64) {
    if len < 64 {
        buf.push(len as u8);
    } else if len < 16384 {
        buf.push((((len >> 8) as u8) & 0x3F) | 0x40);
        buf.push((len & 0xFF) as u8);
    } else if len <= u32::MAX as u64 {
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        buf.push(0x81);
        buf.extend_from_slice(&len.to_be_bytes());
    }
}

/// rdbSaveRawString — integer encoding when it fits, otherwise verbatim.
/// Never LZF-compresses (see module doc).
fn write_string(buf: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11 {
        if let Some(v) = parse_canonical_i64(s) {
            if let Ok(v8) = i8::try_from(v) {
                buf.push(0xC0);
                buf.push(v8 as u8);
                return;
            } else if let Ok(v16) = i16::try_from(v) {
                buf.push(0xC1);
                buf.extend_from_slice(&v16.to_le_bytes());
                return;
            } else if let Ok(v32) = i32::try_from(v) {
                buf.push(0xC2);
                buf.extend_from_slice(&v32.to_le_bytes());
                return;
            }
        }
    }
    write_len(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        let b = *self.data.get(self.pos).ok_or("unexpected end of RDB data")?;
        self.pos += 1;
        Ok(b)
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.pos.checked_add(n).ok_or("RDB length overflow")?;
        let slice = self.data.get(self.pos..end).ok_or("unexpected end of RDB data")?;
        self.pos = end;
        Ok(slice)
    }

    /// rdbLoadLenByRef — returns `(value, is_special_encoding)`.
    fn read_len(&mut self) -> Result<(u64, bool), String> {
        let b0 = self.read_u8()?;
        let kind = (b0 & 0xC0) >> 6;
        match kind {
            3 => Ok(((b0 & 0x3F) as u64, true)),
            0 => Ok(((b0 & 0x3F) as u64, false)),
            1 => {
                let b1 = self.read_u8()?;
                Ok(((((b0 & 0x3F) as u64) << 8) | b1 as u64, false))
            }
            _ if b0 == 0x80 => Ok((u32::from_be_bytes(self.read_exact(4)?.try_into().unwrap()) as u64, false)),
            _ if b0 == 0x81 => Ok((u64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()), false)),
            _ => Err(format!("unknown RDB length encoding byte {b0:#04x}")),
        }
    }

    /// rdbGenericLoadStringObject — plain length-prefixed, int8/16/32, or LZF.
    fn read_string(&mut self) -> Result<Vec<u8>, String> {
        let (val, is_encoded) = self.read_len()?;
        if !is_encoded {
            return Ok(self.read_exact(val as usize)?.to_vec());
        }
        match val {
            0 => Ok((self.read_u8()? as i8 as i64).to_string().into_bytes()),
            1 => Ok((i16::from_le_bytes(self.read_exact(2)?.try_into().unwrap()) as i64).to_string().into_bytes()),
            2 => Ok((i32::from_le_bytes(self.read_exact(4)?.try_into().unwrap()) as i64).to_string().into_bytes()),
            3 => {
                let (clen, _) = self.read_len()?;
                let (ulen, _) = self.read_len()?;
                let compressed = self.read_exact(clen as usize)?;
                lzf::decompress(compressed, ulen as usize).ok_or_else(|| "corrupt LZF-compressed string".to_string())
            }
            other => Err(format!("unknown RDB string special encoding {other}")),
        }
    }

    fn read_f64_le(&mut self) -> Result<f64, String> {
        Ok(f64::from_le_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    /// `rdbLoadDoubleValue` — the pre-`ZSET_2` ASCII double encoding: a
    /// 1-byte length (253/254/255 standing in for NaN/+inf/-inf), otherwise
    /// that many ASCII bytes parsed as a float.
    fn read_double_legacy(&mut self) -> Result<f64, String> {
        let len = self.read_u8()?;
        match len {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            _ => {
                let bytes = self.read_exact(len as usize)?;
                std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok()).ok_or_else(|| "corrupt legacy double value".to_string())
            }
        }
    }

    /// `rdbLoadCheckModuleValue` — a generic opcode-tagged scalar stream
    /// (int/float/double/string, any order, any count) terminated by
    /// `MODULE_OPCODE_EOF`. Used by both `OPCODE_MODULE_AUX` and
    /// `OPCODE_KEY_META`'s per-class metadata, neither of which this fork
    /// has an actual module/metadata-class registry to interpret — so every
    /// value is just skipped over, never kept.
    fn skip_module_value(&mut self) -> Result<(), String> {
        loop {
            let (opcode, _) = self.read_len()?;
            match opcode {
                MODULE_OPCODE_EOF => return Ok(()),
                MODULE_OPCODE_SINT | MODULE_OPCODE_UINT => {
                    self.read_len()?;
                }
                MODULE_OPCODE_FLOAT => {
                    self.read_exact(4)?;
                }
                MODULE_OPCODE_DOUBLE => {
                    self.read_exact(8)?;
                }
                MODULE_OPCODE_STRING => {
                    self.read_string()?;
                }
                other => return Err(format!("unknown module value opcode {other}")),
            }
        }
    }
}

/// A deep clone of any `RObj` — `COPY`'s only consumer. Implemented by
/// round-tripping through this module's own save/load value encoding
/// rather than deriving `Clone` on every composite type and everything
/// they're built from (`ListPack`/`RDict`/`IntSet`, over in `redisx-core`)
/// — one extra function here is cheaper than widening `Clone` derives
/// across two crates for a single command. Safe to `expect()` throughout:
/// the buffer was just written by this same module in this same call, so
/// it's always well-formed.
pub(crate) fn clone_value(obj: &RObj) -> RObj {
    let mut buf = Vec::new();
    save_value(&mut buf, b"", obj);
    let mut r = Reader::new(&buf);
    let rdb_type = r.read_u8().expect("just-written buffer must be well-formed");
    let _key = r.read_string().expect("just-written buffer must be well-formed");
    load_value(&mut r, rdb_type).expect("just-written buffer must be well-formed")
}

/// `DUMP key`'s payload (`cluster.c`'s `createDumpPayload`): this module's
/// own `[type byte][value]` value encoding — reused as-is, since `DUMP`'s
/// payload format and this module's own internal RDB value format are the
/// same thing by construction — with no key (DUMP's key comes from the
/// command's own argument, never the payload itself) followed by a 2-byte
/// RDB version and an 8-byte CRC64 trailer, both little-endian.
/// `save_value` always writes `[type byte][key string][value]`; an empty key
/// encodes as exactly one zero byte (`rdbSaveLen`'s 6-bit form for length
/// `0`), so removing `buf[1]` turns that into the key-less
/// `[type byte][value]` DUMP actually wants — cheaper than teaching
/// `save_value` a "skip the key" mode for its one caller that needs it.
pub(crate) fn dump_value(obj: &RObj) -> Vec<u8> {
    let mut buf = Vec::new();
    save_value(&mut buf, b"", obj);
    buf.remove(1);
    buf.extend_from_slice(&(RDB_VERSION as u16).to_le_bytes());
    let crc = crc::crc64(0, &buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// `RESTORE`'s footer check (`cluster.c`'s `verifyDumpPayload`): the RDB
/// version embedded in the payload must not be newer than this fork's own,
/// and the trailing CRC64 must match — unless it's all-zero, which (matching
/// real Redis's `skip_checksum` convention) means "no checksum, don't
/// verify" rather than "checksum happens to be zero".
pub(crate) fn verify_dump_payload(payload: &[u8]) -> bool {
    if payload.len() < 10 {
        return false;
    }
    let footer = &payload[payload.len() - 10..];
    let version = u16::from_le_bytes([footer[0], footer[1]]);
    if version as u32 > RDB_VERSION {
        return false;
    }
    let crc_in_payload = u64::from_le_bytes(footer[2..10].try_into().unwrap());
    if crc_in_payload == 0 {
        return true;
    }
    crc::crc64(0, &payload[..payload.len() - 8]) == crc_in_payload
}

/// Decodes an already-`verify_dump_payload`-checked `DUMP` payload's body
/// (`[type byte][value]`, footer stripped here) back into an `RObj` —
/// `restoreCommand`'s `rdbLoadObject` call, minus the key (which `RESTORE`
/// gets from its own command argument, not the payload).
pub(crate) fn restore_value(payload: &[u8]) -> Result<RObj, String> {
    let body = &payload[..payload.len() - 10];
    let mut r = Reader::new(body);
    let rdb_type = r.read_u8()?;
    load_value(&mut r, rdb_type)
}

// --------------------------------------------------------------- saving ---

fn save_value(buf: &mut Vec<u8>, key: &[u8], obj: &RObj) {
    match obj {
        RObj::String(v) => {
            buf.push(TYPE_STRING);
            write_string(buf, key);
            write_string(buf, v);
        }
        RObj::List(list) => {
            buf.push(TYPE_LIST_QUICKLIST_2);
            write_string(buf, key);
            write_len(buf, 1); // one quicklist node
            write_len(buf, QUICKLIST_NODE_CONTAINER_PACKED);
            write_string(buf, list.as_listpack_bytes());
        }
        RObj::Hash(HashObj::Listpack(lp)) => {
            buf.push(TYPE_HASH_LISTPACK);
            write_string(buf, key);
            write_string(buf, lp.as_bytes());
        }
        RObj::Hash(HashObj::Dict(d)) => {
            buf.push(TYPE_HASH);
            write_string(buf, key);
            write_len(buf, d.len() as u64);
            for (f, v) in d.iter() {
                write_string(buf, f);
                write_string(buf, v);
            }
        }
        RObj::Set(SetObj::IntSet(is)) => {
            buf.push(TYPE_SET_INTSET);
            write_string(buf, key);
            write_string(buf, &is.to_bytes());
        }
        RObj::Set(SetObj::Dict(d)) => {
            buf.push(TYPE_SET);
            write_string(buf, key);
            write_len(buf, d.len() as u64);
            for (m, ()) in d.iter() {
                write_string(buf, m);
            }
        }
        RObj::ZSet(ZSetObj::Listpack(lp)) => {
            buf.push(TYPE_ZSET_LISTPACK);
            write_string(buf, key);
            write_string(buf, lp.as_bytes());
        }
        RObj::ZSet(ZSetObj::Sorted(members)) => {
            buf.push(TYPE_ZSET_2);
            write_string(buf, key);
            write_len(buf, members.len() as u64);
            // rdb.c saves skiplist zsets greatest-to-smallest; `members` is
            // kept ascending, so iterate in reverse to match byte-for-byte.
            for (member, score) in members.iter().rev() {
                write_string(buf, member);
                buf.extend_from_slice(&score.to_le_bytes());
            }
        }
        RObj::Stream(stream) => {
            buf.push(TYPE_STREAM_CUSTOM);
            write_string(buf, key);
            let last_id = stream.last_id();
            write_len(buf, last_id.ms);
            write_len(buf, last_id.seq);
            write_len(buf, stream.len() as u64);
            for (id, fields) in stream.entries() {
                write_len(buf, id.ms);
                write_len(buf, id.seq);
                write_len(buf, fields.len() as u64);
                for (f, v) in fields {
                    write_string(buf, f);
                    write_string(buf, v);
                }
            }
            // Consumer groups, appended after the Phase 9 entry list — a
            // format bump specific to this fork's own non-standard
            // `TYPE_STREAM_CUSTOM` opcode (see this module's doc comment),
            // so there's no real-Redis compatibility to preserve either way.
            write_len(buf, stream.groups().len() as u64);
            for (name, group) in stream.groups() {
                write_string(buf, name);
                let gid = group.last_delivered_id();
                write_len(buf, gid.ms);
                write_len(buf, gid.seq);
                write_len(buf, group.entries_read().unsigned_abs());
                buf.push(if group.entries_read() < 0 { 1 } else { 0 });
                write_len(buf, group.pel().len() as u64);
                for (id, entry) in group.pel() {
                    write_len(buf, id.ms);
                    write_len(buf, id.seq);
                    write_string(buf, &entry.consumer);
                    write_len(buf, entry.delivery_time_ms);
                    write_len(buf, entry.delivery_count);
                }
                write_len(buf, group.consumers().len() as u64);
                for (cname, consumer) in group.consumers() {
                    write_string(buf, cname);
                    write_len(buf, consumer.seen_time_ms);
                    write_len(buf, consumer.active_time_ms);
                }
            }
        }
        #[cfg(feature = "polars")]
        RObj::DataFrame(df) => {
            buf.push(TYPE_DATAFRAME_CUSTOM);
            write_string(buf, key);
            // A malformed/corrupt in-memory DataFrame can't actually happen
            // (it would have failed `DF.FROMCSV`/`DF.SET`/`DF.QUERY` already
            // on the way in), so an encode failure here would mean a bug
            // elsewhere — same "this really shouldn't fail" assumption
            // `save`'s other arms make about their own encoders, just with
            // an explicit message instead of a silent `unwrap`.
            let ipc = df.to_ipc_bytes().unwrap_or_else(|e| panic!("BUG: in-memory DataFrame failed to re-encode for SAVE: {e}"));
            write_string(buf, &ipc);
        }
    }
}

/// rdbSaveRio — the whole file as one in-memory buffer: header, every
/// non-empty DB's keys, EOF opcode, trailing CRC64 (Phase 0's resumable
/// `crc64`, verified against real Redis's exact checksum behavior).
pub fn save(server: &Server) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("REDIS{RDB_VERSION:04}").as_bytes());

    // `rdbSaveFunctions` — written before the keyspace, matching real
    // Redis's own `rdbSaveRio` ordering.
    for lib in server.libraries.values() {
        buf.push(OPCODE_FUNCTION2);
        write_string(&mut buf, lib.source.as_bytes());
    }

    for (idx, db) in server.dbs.iter().enumerate() {
        let Some(dict) = db.dict.get_dict(0) else { continue };
        if dict.is_empty() {
            continue;
        }
        buf.push(OPCODE_SELECTDB);
        write_len(&mut buf, idx as u64);
        for (key, obj) in dict.iter() {
            save_value(&mut buf, key, obj);
        }
    }

    buf.push(OPCODE_EOF);
    let cksum = crc::crc64(0, &buf);
    buf.extend_from_slice(&cksum.to_le_bytes());
    buf
}

// -------------------------------------------------------------- loading ---

/// Decodes a `RDB_TYPE_SET_LISTPACK` blob (written by real Redis for small
/// non-integer sets, a third encoding this fork's `SetObj` doesn't produce —
/// see `set.rs`'s doc comment) into whichever of `SetObj`'s two encodings
/// actually fits the data, so reading one back is still lossless.
fn set_from_listpack(lp: &ListPack) -> SetObj {
    let values: Vec<Vec<u8>> = lp.iter().map(|v| v.to_bytes()).collect();
    if values.iter().all(|v| parse_canonical_i64(v).is_some()) {
        let mut is = IntSet::new();
        for v in &values {
            is.add(parse_canonical_i64(v).unwrap());
        }
        SetObj::IntSet(is)
    } else {
        let mut d = RDict::new();
        for v in values {
            d.insert(v, ());
        }
        SetObj::Dict(d)
    }
}

fn load_value(r: &mut Reader, rdb_type: u8) -> Result<RObj, String> {
    match rdb_type {
        TYPE_STRING => Ok(RObj::String(r.read_string()?)),
        TYPE_LIST => {
            let (n, _) = r.read_len()?;
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                items.push(r.read_string()?);
            }
            let mut list = ListObj::new();
            list.push_back(&items);
            Ok(RObj::List(list))
        }
        TYPE_ZSET => {
            let (n, _) = r.read_len()?;
            let mut members = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let member = r.read_string()?;
                let score = r.read_double_legacy()?;
                if score.is_nan() {
                    return Err("Zset with NAN score detected".to_string());
                }
                members.push((member, score));
            }
            // Same on-disk order as `ZSET_2` (greatest-to-smallest); see that
            // arm below for why the in-memory `Sorted` vec is kept ascending.
            members.reverse();
            Ok(RObj::ZSet(ZSetObj::Sorted(members)))
        }
        TYPE_LIST_QUICKLIST_2 => {
            let (n_nodes, _) = r.read_len()?;
            let mut list = ListObj::new();
            for _ in 0..n_nodes {
                let (container, _) = r.read_len()?;
                let data = r.read_string()?;
                if container == QUICKLIST_NODE_CONTAINER_PLAIN {
                    list.push_back(std::slice::from_ref(&data));
                } else if container == QUICKLIST_NODE_CONTAINER_PACKED {
                    let lp = ListPack::from_bytes(&data).ok_or("corrupt listpack in quicklist node")?;
                    for v in lp.iter() {
                        list.push_back(std::slice::from_ref(&v.to_bytes()));
                    }
                } else {
                    return Err(format!("unknown quicklist node container {container}"));
                }
            }
            Ok(RObj::List(list))
        }
        TYPE_HASH_LISTPACK => {
            let data = r.read_string()?;
            let lp = ListPack::from_bytes(&data).ok_or("corrupt hash listpack")?;
            Ok(RObj::Hash(HashObj::Listpack(lp)))
        }
        TYPE_LIST_ZIPLIST => {
            let data = r.read_string()?;
            let items = redisx_core::ziplist::entries(&data).ok_or("corrupt ziplist (list)")?;
            let mut list = ListObj::new();
            list.push_back(&items);
            Ok(RObj::List(list))
        }
        // `HASH_ZIPLIST`/`ZSET_ZIPLIST` both decode into flat field/value (or
        // member/score) pairs, which get re-encoded into this fork's own
        // native `ListPack` — a different byte format than a ziplist, so the
        // decoded bytes can't just be wrapped as-is the way `HASH_LISTPACK`/
        // `ZSET_LISTPACK` are above. For the zset case this relies on the
        // ziplist already being member/score-sorted on disk (real Redis
        // always writes it that way), the same assumption `ZSET_LISTPACK`'s
        // own load path makes.
        TYPE_HASH_ZIPLIST => {
            let data = r.read_string()?;
            let items = redisx_core::ziplist::entries(&data).ok_or("corrupt ziplist (hash)")?;
            if !items.len().is_multiple_of(2) {
                return Err("corrupt ziplist (hash): odd number of field/value entries".to_string());
            }
            let mut lp = ListPack::new();
            for item in &items {
                lp.push_str(item);
            }
            Ok(RObj::Hash(HashObj::Listpack(lp)))
        }
        TYPE_ZSET_ZIPLIST => {
            let data = r.read_string()?;
            let items = redisx_core::ziplist::entries(&data).ok_or("corrupt ziplist (zset)")?;
            if !items.len().is_multiple_of(2) {
                return Err("corrupt ziplist (zset): odd number of member/score entries".to_string());
            }
            let mut lp = ListPack::new();
            for item in &items {
                lp.push_str(item);
            }
            Ok(RObj::ZSet(ZSetObj::Listpack(lp)))
        }
        // Predates even `ziplist` — same re-encode-into-native-`ListPack`
        // treatment as `HASH_ZIPLIST` above.
        TYPE_HASH_ZIPMAP => {
            let data = r.read_string()?;
            let pairs = redisx_core::zipmap::entries(&data).ok_or("corrupt zipmap")?;
            let mut lp = ListPack::new();
            for (field, value) in &pairs {
                lp.push_str(field);
                lp.push_str(value);
            }
            Ok(RObj::Hash(HashObj::Listpack(lp)))
        }
        TYPE_HASH => {
            let (n, _) = r.read_len()?;
            let mut d = RDict::new();
            for _ in 0..n {
                let f = r.read_string()?;
                let v = r.read_string()?;
                d.insert(f, v);
            }
            Ok(RObj::Hash(HashObj::Dict(d)))
        }
        TYPE_SET_INTSET => {
            let data = r.read_string()?;
            let is = IntSet::from_bytes(&data).ok_or("corrupt intset")?;
            Ok(RObj::Set(SetObj::IntSet(is)))
        }
        TYPE_SET_LISTPACK => {
            let data = r.read_string()?;
            let lp = ListPack::from_bytes(&data).ok_or("corrupt set listpack")?;
            Ok(RObj::Set(set_from_listpack(&lp)))
        }
        TYPE_SET => {
            let (n, _) = r.read_len()?;
            let mut d = RDict::new();
            for _ in 0..n {
                d.insert(r.read_string()?, ());
            }
            Ok(RObj::Set(SetObj::Dict(d)))
        }
        TYPE_ZSET_LISTPACK => {
            let data = r.read_string()?;
            let lp = ListPack::from_bytes(&data).ok_or("corrupt zset listpack")?;
            Ok(RObj::ZSet(ZSetObj::Listpack(lp)))
        }
        TYPE_ZSET_2 => {
            let (n, _) = r.read_len()?;
            let mut members = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let member = r.read_string()?;
                let score = r.read_f64_le()?;
                members.push((member, score));
            }
            members.reverse(); // stored greatest-to-smallest; keep ascending internally
            Ok(RObj::ZSet(ZSetObj::Sorted(members)))
        }
        TYPE_STREAM_CUSTOM => {
            let (last_ms, _) = r.read_len()?;
            let (last_seq, _) = r.read_len()?;
            let (n_entries, _) = r.read_len()?;
            let mut entries = std::collections::BTreeMap::new();
            for _ in 0..n_entries {
                let (ms, _) = r.read_len()?;
                let (seq, _) = r.read_len()?;
                let (n_fields, _) = r.read_len()?;
                let mut fields = Vec::with_capacity(n_fields as usize);
                for _ in 0..n_fields {
                    let f = r.read_string()?;
                    let v = r.read_string()?;
                    fields.push((f, v));
                }
                entries.insert(crate::stream::StreamId { ms, seq }, fields);
            }
            let (n_groups, _) = r.read_len()?;
            let mut groups = std::collections::BTreeMap::new();
            for _ in 0..n_groups {
                let name = r.read_string()?;
                let (gms, _) = r.read_len()?;
                let (gseq, _) = r.read_len()?;
                let (entries_read_abs, _) = r.read_len()?;
                let negative = r.read_u8()? != 0;
                let entries_read = if negative { -(entries_read_abs as i64) } else { entries_read_abs as i64 };
                let mut group = crate::stream::StreamGroup::new(crate::stream::StreamId { ms: gms, seq: gseq }, entries_read);
                let (n_pel, _) = r.read_len()?;
                for _ in 0..n_pel {
                    let (ms, _) = r.read_len()?;
                    let (seq, _) = r.read_len()?;
                    let consumer = r.read_string()?;
                    let (delivery_time_ms, _) = r.read_len()?;
                    let (delivery_count, _) = r.read_len()?;
                    group
                        .pel_mut()
                        .insert(crate::stream::StreamId { ms, seq }, crate::stream::PelEntry { consumer, delivery_time_ms, delivery_count });
                }
                let (n_consumers, _) = r.read_len()?;
                for _ in 0..n_consumers {
                    let cname = r.read_string()?;
                    let (seen_time_ms, _) = r.read_len()?;
                    let (active_time_ms, _) = r.read_len()?;
                    group.consumers_mut().insert(cname, crate::stream::Consumer { seen_time_ms, active_time_ms });
                }
                groups.insert(name, group);
            }
            Ok(RObj::Stream(crate::stream::StreamObj::from_parts(entries, crate::stream::StreamId { ms: last_ms, seq: last_seq }, groups)))
        }
        #[cfg(feature = "polars")]
        TYPE_DATAFRAME_CUSTOM => {
            let bytes = r.read_string()?;
            crate::dataframe::DataFrameObj::from_ipc_bytes(&bytes).map(RObj::DataFrame)
        }
        other => Err(format!("unsupported RDB object type {other}")),
    }
}

/// rdbLoadRio's main loop. Populates `server.dbs` (which must already have
/// enough databases configured — a `SELECTDB` past that range is an error,
/// matching real Redis's fatal "configured to handle more databases" exit,
/// just returned as a `Result` instead of calling `exit(1)`).
pub fn load(server: &mut Server, data: &[u8]) -> Result<(), String> {
    let mut r = Reader::new(data);
    let header = r.read_exact(9)?;
    if &header[0..5] != b"REDIS" {
        return Err("wrong signature trying to load DB from data".to_string());
    }
    let version: u32 =
        std::str::from_utf8(&header[5..9]).ok().and_then(|s| s.parse().ok()).ok_or("bad RDB version field")?;
    if !(1..=RDB_VERSION).contains(&version) {
        return Err(format!("can't handle RDB format version {version}"));
    }

    let mut current_db = 0usize;
    loop {
        let opcode = r.read_u8()?;
        match opcode {
            OPCODE_EOF => break,
            OPCODE_SELECTDB => {
                let (idx, _) = r.read_len()?;
                current_db = idx as usize;
                if current_db >= server.dbs.len() {
                    return Err(format!(
                        "RDB selects database {current_db} but only {} are configured",
                        server.dbs.len()
                    ));
                }
            }
            OPCODE_RESIZEDB => {
                r.read_len()?;
                r.read_len()?;
            }
            OPCODE_EXPIRETIME => {
                r.read_exact(4)?;
            }
            OPCODE_EXPIRETIME_MS => {
                r.read_exact(8)?;
            }
            OPCODE_FREQ => {
                r.read_u8()?;
            }
            OPCODE_IDLE => {
                r.read_len()?;
            }
            OPCODE_AUX => {
                r.read_string()?;
                r.read_string()?;
            }
            OPCODE_SLOT_INFO => {
                // slot id, slot size, expires-slot size — meaningless outside
                // cluster mode, which this fork never is; "ignore gracefully"
                // matches real Redis's own non-cluster behavior.
                r.read_len()?;
                r.read_len()?;
                r.read_len()?;
            }
            OPCODE_MODULE_AUX => {
                r.read_len()?; // module id
                r.read_len()?; // when_opcode (always MODULE_OPCODE_UINT)
                r.read_len()?; // when
                r.skip_module_value()?;
            }
            OPCODE_FUNCTION2 => {
                let code = r.read_string()?;
                let code = String::from_utf8(code).map_err(|_| "FUNCTION2 library source is not valid UTF-8".to_string())?;
                if let Reply::Error(e) = server.function_load(&code, true) {
                    return Err(format!("failed loading library: {e}"));
                }
            }
            OPCODE_KEY_META => {
                let (num_classes, _) = r.read_len()?;
                for _ in 0..num_classes {
                    r.read_exact(4)?; // encoded class spec (name/version/flags)
                    r.skip_module_value()?;
                }
                // Layout is `[KEY_META][TYPE][KEY][VALUE]` — fall through to
                // the actual type/key/value triple immediately following.
                let type_byte = r.read_u8()?;
                let key = r.read_string()?;
                let obj = load_value(&mut r, type_byte)?;
                server.dbs[current_db].dict.insert(0, key, obj);
            }
            type_byte => {
                let key = r.read_string()?;
                let obj = load_value(&mut r, type_byte)?;
                server.dbs[current_db].dict.insert(0, key, obj);
            }
        }
    }

    let consumed_before_trailer = r.pos;
    let trailer = r.read_exact(8)?;
    let expected = u64::from_le_bytes(trailer.try_into().unwrap());
    if expected != 0 {
        let actual = crc::crc64(0, &data[..consumed_before_trailer]);
        if actual != expected {
            return Err(format!("RDB checksum mismatch: file says {expected:#018x}, computed {actual:#018x}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientState;

    fn argv(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn empty_server_round_trips() {
        let s = Server::new(4);
        let blob = save(&s);
        assert!(blob.starts_with(b"REDIS0014"));
        let mut s2 = Server::new(4);
        load(&mut s2, &blob).expect("load should succeed");
        for db in &s2.dbs {
            assert_eq!(db.dict.size(), 0);
        }
    }

    #[test]
    fn string_round_trips_including_int_encoding() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "greeting", "hello world"]));
        s.dispatch(&mut c, &argv(&["SET", "counter", "12345"])); // should use int encoding
        s.dispatch(&mut c, &argv(&["SET", "leadingzero", "007"])); // must NOT use int encoding

        let blob = save(&s);
        let mut s2 = Server::new(1);
        load(&mut s2, &blob).unwrap();
        let mut c2 = ClientState::default();
        assert_eq!(s2.dispatch(&mut c2, &argv(&["GET", "greeting"])).to_bytes(2), b"$11\r\nhello world\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["GET", "counter"])).to_bytes(2), b"$5\r\n12345\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["GET", "leadingzero"])).to_bytes(2), b"$3\r\n007\r\n");
    }

    #[test]
    fn list_hash_set_zset_round_trip_across_both_encodings() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "mylist", "a", "b", "c"]));

        s.dispatch(&mut c, &argv(&["HSET", "smallhash", "f1", "v1"]));
        for i in 0..200 {
            s.dispatch(&mut c, &argv(&["HSET", "bighash", &format!("f{i}"), "v"]));
        }

        s.dispatch(&mut c, &argv(&["SADD", "intset", "1", "2", "3"]));
        s.dispatch(&mut c, &argv(&["SADD", "strset", "hello", "world"]));

        s.dispatch(&mut c, &argv(&["ZADD", "smallzset", "1", "a", "2", "b"]));
        for i in 0..200 {
            s.dispatch(&mut c, &argv(&["ZADD", "bigzset", &i.to_string(), &format!("m{i}")]));
        }

        let blob = save(&s);
        let mut s2 = Server::new(1);
        load(&mut s2, &blob).expect("load should succeed");
        let mut c2 = ClientState::default();

        assert_eq!(s2.dispatch(&mut c2, &argv(&["LRANGE", "mylist", "0", "-1"])).to_bytes(2), b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["HGET", "smallhash", "f1"])).to_bytes(2), b"$2\r\nv1\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["HLEN", "bighash"])).to_bytes(2), b":200\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["SISMEMBER", "intset", "2"])).to_bytes(2), b":1\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["SCARD", "strset"])).to_bytes(2), b":2\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["ZSCORE", "smallzset", "b"])).to_bytes(2), b"$1\r\n2\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["ZCARD", "bigzset"])).to_bytes(2), b":200\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["ZRANK", "bigzset", "m0"])).to_bytes(2), b":0\r\n");
    }

    // Minimal raw ziplist/zipmap builders for exercising `load_value`'s
    // legacy-opcode branches directly — the encodings themselves (every
    // string/int length form) are already covered by `redisx_core::ziplist`/
    // `zipmap`'s own unit tests, so these only need short strings (ZIP_STR_06B,
    // 1-byte zipmap lengths) to prove the RDB-opcode-to-`RObj` wiring works.

    fn build_ziplist(items: &[&[u8]]) -> Vec<u8> {
        let mut entries = Vec::new();
        let mut prev_entry_len: u8 = 0;
        for item in items {
            assert!(item.len() < 64, "test helper only supports short strings");
            entries.push(prev_entry_len);
            entries.push(item.len() as u8); // ZIP_STR_06B | len, since len < 64
            entries.extend_from_slice(item);
            prev_entry_len = (1 + item.len()) as u8; // encoding byte + data
        }
        let mut out = vec![0u8; 10]; // zlbytes/zltail/zllen — not read by the decoder
        out.extend_from_slice(&entries);
        out.push(0xFF);
        out
    }

    fn build_zipmap(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut out = vec![pairs.len().min(253) as u8]; // zmlen
        for (k, v) in pairs {
            assert!(k.len() < 254 && v.len() < 254, "test helper only supports short strings");
            out.push(k.len() as u8);
            out.extend_from_slice(k);
            out.push(v.len() as u8);
            out.push(0); // free
            out.extend_from_slice(v);
        }
        out.push(0xFF);
        out
    }

    fn load_legacy_value(rdb_type: u8, blob: &[u8]) -> RObj {
        let mut buf = Vec::new();
        write_string(&mut buf, blob);
        let mut r = Reader::new(&buf);
        load_value(&mut r, rdb_type).expect("legacy value should decode")
    }

    #[test]
    fn loads_a_legacy_list_ziplist() {
        let blob = build_ziplist(&[b"a", b"b", b"c"]);
        let RObj::List(list) = load_legacy_value(TYPE_LIST_ZIPLIST, &blob) else { panic!("expected a list") };
        assert_eq!(list.range(0, -1), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn loads_a_legacy_hash_ziplist() {
        let blob = build_ziplist(&[b"f1", b"v1", b"f2", b"v2"]);
        let RObj::Hash(hash) = load_legacy_value(TYPE_HASH_ZIPLIST, &blob) else { panic!("expected a hash") };
        assert_eq!(hash.get(b"f1"), Some(b"v1".to_vec()));
        assert_eq!(hash.get(b"f2"), Some(b"v2".to_vec()));
        assert_eq!(hash.len(), 2);
    }

    #[test]
    fn loads_a_legacy_zset_ziplist() {
        // Must already be member/score-sorted on disk, same assumption
        // `ZSET_LISTPACK` makes — "a" (score 1) before "b" (score 2).
        let blob = build_ziplist(&[b"a", b"1", b"b", b"2"]);
        let RObj::ZSet(zset) = load_legacy_value(TYPE_ZSET_ZIPLIST, &blob) else { panic!("expected a zset") };
        assert_eq!(zset.score(b"a"), Some(1.0));
        assert_eq!(zset.score(b"b"), Some(2.0));
        assert_eq!(zset.range(0, -1), vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), 2.0)]);
    }

    #[test]
    fn rejects_a_ziplist_hash_with_an_odd_number_of_entries() {
        let blob = build_ziplist(&[b"f1", b"v1", b"f2"]);
        let mut buf = Vec::new();
        write_string(&mut buf, &blob);
        let mut r = Reader::new(&buf);
        assert!(load_value(&mut r, TYPE_HASH_ZIPLIST).is_err());
    }

    #[test]
    fn loads_a_legacy_hash_zipmap() {
        let blob = build_zipmap(&[(b"f1", b"v1"), (b"f2", b"v2")]);
        let RObj::Hash(hash) = load_legacy_value(TYPE_HASH_ZIPMAP, &blob) else { panic!("expected a hash") };
        assert_eq!(hash.get(b"f1"), Some(b"v1".to_vec()));
        assert_eq!(hash.get(b"f2"), Some(b"v2".to_vec()));
        assert_eq!(hash.len(), 2);
    }

    /// Full-file coverage, as opposed to the `load_value`-only tests above:
    /// hand-assembles a complete `REDIS0014` file (header, `SELECTDB`,
    /// legacy-typed keys, `EOF`, a real CRC64 trailer) exactly the way a
    /// genuinely old `redis-server` dump would look, and loads it through
    /// `load()`'s actual opcode-dispatch loop rather than calling
    /// `load_value` directly — proving the opcode bytes 9/10 route to the
    /// right decoder, not just that the decoders themselves work. Uses
    /// `zipmap.c`'s own worked example bytes for the zipmap half.
    #[test]
    fn full_rdb_file_with_legacy_ziplist_and_zipmap_opcodes_loads_correctly() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"REDIS0014");
        buf.push(OPCODE_SELECTDB);
        write_len(&mut buf, 0);

        buf.push(TYPE_HASH_ZIPMAP);
        write_string(&mut buf, b"legacyhash");
        write_string(&mut buf, b"\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff");

        buf.push(TYPE_LIST_ZIPLIST);
        write_string(&mut buf, b"legacylist");
        write_string(&mut buf, &build_ziplist(&[b"a", b"b", b"c"]));

        buf.push(OPCODE_EOF);
        let cksum = crc::crc64(0, &buf);
        buf.extend_from_slice(&cksum.to_le_bytes());

        let mut s = Server::new(1);
        load(&mut s, &buf).expect("hand-built legacy RDB file should load");
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["HGET", "legacyhash", "foo"])).to_bytes(2), b"$3\r\nbar\r\n");
        assert_eq!(s.dispatch(&mut c, &argv(&["HGET", "legacyhash", "hello"])).to_bytes(2), b"$5\r\nworld\r\n");
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "legacylist", "0", "-1"])).to_bytes(2), b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    }

    fn write_double_legacy(buf: &mut Vec<u8>, s: &str) {
        buf.push(s.len() as u8);
        buf.extend_from_slice(s.as_bytes());
    }

    #[test]
    fn loads_a_legacy_plain_list() {
        let mut buf = Vec::new();
        write_len(&mut buf, 3);
        write_string(&mut buf, b"a");
        write_string(&mut buf, b"b");
        write_string(&mut buf, b"c");
        let mut r = Reader::new(&buf);
        let RObj::List(list) = load_value(&mut r, TYPE_LIST).expect("legacy plain list should decode") else { panic!("expected a list") };
        assert_eq!(list.range(0, -1), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn loads_a_legacy_plain_zset() {
        // On-disk order is greatest-to-smallest, same as `ZSET_2`.
        let mut buf = Vec::new();
        write_len(&mut buf, 3);
        write_string(&mut buf, b"c");
        write_double_legacy(&mut buf, "3");
        write_string(&mut buf, b"b");
        write_double_legacy(&mut buf, "2");
        write_string(&mut buf, b"a");
        write_double_legacy(&mut buf, "1");
        let mut r = Reader::new(&buf);
        let RObj::ZSet(zset) = load_value(&mut r, TYPE_ZSET).expect("legacy plain zset should decode") else { panic!("expected a zset") };
        assert_eq!(zset.range(0, -1), vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), 2.0), (b"c".to_vec(), 3.0)]);
    }

    #[test]
    fn legacy_plain_zset_rejects_a_nan_score() {
        let mut buf = Vec::new();
        write_len(&mut buf, 1);
        write_string(&mut buf, b"a");
        buf.push(253); // NaN marker, no following bytes
        let mut r = Reader::new(&buf);
        assert!(load_value(&mut r, TYPE_ZSET).is_err());
    }

    /// `SLOT_INFO`/`MODULE_AUX`/`KEY_META` all have to be skippable without
    /// aborting the rest of the file — this builds one of each around an
    /// ordinary key to prove the surrounding keyspace still loads correctly.
    #[test]
    fn full_rdb_file_tolerates_slot_info_module_aux_and_key_meta_opcodes() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"REDIS0014");
        buf.push(OPCODE_SELECTDB);
        write_len(&mut buf, 0);

        // SLOT_INFO: slot id, slot size, expires-slot size — cluster-only,
        // always ignored outside cluster mode.
        buf.push(OPCODE_SLOT_INFO);
        write_len(&mut buf, 0);
        write_len(&mut buf, 5);
        write_len(&mut buf, 1);

        // MODULE_AUX: module id, when_opcode (always UINT), when, then a
        // generic tagged-value stream (one STRING, terminated by EOF).
        buf.push(OPCODE_MODULE_AUX);
        write_len(&mut buf, 12345);
        write_len(&mut buf, MODULE_OPCODE_UINT);
        write_len(&mut buf, 1);
        write_len(&mut buf, MODULE_OPCODE_STRING);
        write_string(&mut buf, b"module-specific-bytes");
        write_len(&mut buf, MODULE_OPCODE_EOF);

        // KEY_META: one metadata class (4-byte class spec + a tagged-value
        // stream of its own), then the actual [TYPE][KEY][VALUE] triple.
        buf.push(OPCODE_KEY_META);
        write_len(&mut buf, 1);
        buf.extend_from_slice(&[0, 0, 0, 0]); // class spec, contents irrelevant — always skipped
        write_len(&mut buf, MODULE_OPCODE_UINT);
        write_len(&mut buf, 7);
        write_len(&mut buf, MODULE_OPCODE_EOF);
        buf.push(TYPE_STRING);
        write_string(&mut buf, b"metakey");
        write_string(&mut buf, b"metavalue");

        // An ordinary key with no metadata prefix at all, proving normal
        // keys still load fine around these opcodes.
        buf.push(TYPE_STRING);
        write_string(&mut buf, b"plainkey");
        write_string(&mut buf, b"plainvalue");

        buf.push(OPCODE_EOF);
        let cksum = crc::crc64(0, &buf);
        buf.extend_from_slice(&cksum.to_le_bytes());

        let mut s = Server::new(1);
        load(&mut s, &buf).expect("SLOT_INFO/MODULE_AUX/KEY_META should be gracefully skipped");
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "metakey"])).to_bytes(2), b"$9\r\nmetavalue\r\n");
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "plainkey"])).to_bytes(2), b"$10\r\nplainvalue\r\n");
    }

    #[test]
    fn function_library_round_trips_through_save_and_load() {
        let mut s = Server::new(1);
        let code = "#!lua name=mylib\nredis.register_function('myecho', function(keys, args) return args[1] end)";
        assert!(!matches!(s.function_load(code, false), Reply::Error(_)));

        let blob = save(&s);
        let mut s2 = Server::new(1);
        load(&mut s2, &blob).expect("a saved FUNCTION library should reload");

        let mut c2 = ClientState::default();
        assert_eq!(s2.dispatch(&mut c2, &argv(&["FCALL", "myecho", "0", "hello"])).to_bytes(2), b"$5\r\nhello\r\n");
    }

    #[test]
    fn select_db_keeps_keys_in_their_own_database() {
        let mut s = Server::new(3);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "db0"]));
        s.dispatch(&mut c, &argv(&["SELECT", "2"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "db2"]));

        let blob = save(&s);
        let mut s2 = Server::new(3);
        load(&mut s2, &blob).unwrap();
        let mut c2 = ClientState::default();
        assert_eq!(s2.dispatch(&mut c2, &argv(&["GET", "k"])).to_bytes(2), b"$3\r\ndb0\r\n");
        s2.dispatch(&mut c2, &argv(&["SELECT", "2"]));
        assert_eq!(s2.dispatch(&mut c2, &argv(&["GET", "k"])).to_bytes(2), b"$3\r\ndb2\r\n");
    }

    #[test]
    fn corrupted_checksum_is_rejected() {
        let s = Server::new(1);
        let mut blob = save(&s);
        let last = blob.len() - 1;
        blob[last] ^= 0xFF; // flip a bit in the checksum trailer
        let mut s2 = Server::new(1);
        assert!(load(&mut s2, &blob).is_err());
    }

    #[test]
    fn wrong_signature_is_rejected() {
        let mut s2 = Server::new(1);
        assert!(load(&mut s2, b"NOTREDIS0014").is_err());
    }

    #[test]
    fn stream_round_trips_entries_and_last_id_watermark() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "field", "value"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "field", "value2"]));
        // The last-id watermark must survive even past its own entry's
        // deletion, so a later XADD still can't reuse 2-0 or anything
        // smaller — exactly why `last_id` is saved/loaded separately from
        // the entries themselves.
        s.dispatch(&mut c, &argv(&["XDEL", "st", "2-0"]));

        let blob = save(&s);
        let mut s2 = Server::new(1);
        load(&mut s2, &blob).expect("load should succeed");
        let mut c2 = ClientState::default();

        assert_eq!(s2.dispatch(&mut c2, &argv(&["XLEN", "st"])).to_bytes(2), b":1\r\n");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["XADD", "st", "2-0", "f", "v"])).to_bytes(2)[0], b'-', "2-0 must stay retired");
        assert_eq!(s2.dispatch(&mut c2, &argv(&["XADD", "st", "3-0", "f", "v"])).to_bytes(2), b"$3\r\n3-0\r\n");
    }

    #[test]
    fn stream_consumer_groups_and_pel_round_trip() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "f", "b"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"]));
        s.dispatch(&mut c, &argv(&["XACK", "st", "g", "1-0"]));

        let blob = save(&s);
        let mut s2 = Server::new(1);
        load(&mut s2, &blob).expect("load should succeed");
        let mut c2 = ClientState::default();

        // 1-0 was acked (not pending), 2-0 is still pending for cons1.
        let Reply::Array(pending) = s2.dispatch(&mut c2, &argv(&["XPENDING", "st", "g"])) else { panic!("expected array") };
        assert_eq!(pending[0], Reply::Integer(1));
        let Reply::Array(groups) = s2.dispatch(&mut c2, &argv(&["XINFO", "GROUPS", "st"])) else { panic!("expected array") };
        assert_eq!(groups.len(), 1, "the group itself survived the round trip");
        let Reply::Array(consumers) = s2.dispatch(&mut c2, &argv(&["XINFO", "CONSUMERS", "st", "g"])) else { panic!("expected array") };
        assert_eq!(consumers.len(), 1, "the consumer survived the round trip");
    }

    #[test]
    fn select_db_out_of_configured_range_is_rejected() {
        let mut buf = b"REDIS0014".to_vec();
        buf.push(OPCODE_SELECTDB);
        write_len(&mut buf, 5); // only 1 db configured below
        buf.push(OPCODE_EOF);
        let cksum = crc::crc64(0, &buf);
        buf.extend_from_slice(&cksum.to_le_bytes());

        let mut s = Server::new(1);
        assert!(load(&mut s, &buf).is_err());
    }
}
