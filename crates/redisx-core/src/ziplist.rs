//! Ziplist — ported from `redis_src/src/ziplist.c`, the legacy compact
//! encoding real Redis used for small lists/hashes/zsets before `listpack`
//! replaced it (Redis 7's RDB opcodes `RDB_TYPE_LIST_ZIPLIST`/
//! `RDB_TYPE_HASH_ZIPLIST`/`RDB_TYPE_ZSET_ZIPLIST` = 10/13/12). **Decode-only**:
//! this fork never writes ziplists — nothing in a modern Redis or in this
//! port produces one — it only needs to read one back out of an old RDB
//! dump a real `redis-server` instance might still be holding (`rdb.rs`'s
//! Phase 4 scope note). There is accordingly no `Ziplist` type here, just a
//! single `entries()` function that walks the blob and returns every
//! element's canonical byte representation (same convention as
//! `listpack::Value::to_bytes()` — the Int/Str distinction only matters
//! while decoding, not to any caller).
//!
//! ## Layout
//!
//! `<zlbytes:u32 LE><zltail:u32 LE><zllen:u16 LE><entry>...<entry><0xFF end>`.
//! `zlbytes`/`zltail` are maintenance fields real Redis uses to resize/pop
//! without a full scan — irrelevant to a read-only forward walk, so this
//! module skips past the 10-byte header and never looks at them again.
//! `zllen` saturates at `u16::MAX` once there are more than 2^16-2 entries
//! (same `dictScan`-adjacent "don't trust the cached count past a point"
//! idea as elsewhere in this port) — decoding always walks to the `0xFF`
//! terminator instead of trusting it, exactly like `ziplistLen`'s own
//! fallback.
//!
//! Each entry is `<prevlen><encoding+rawlen><data>`. `prevlen` is 1 byte if
//! the previous entry was under 254 bytes, else `0xFE` + a 4-byte length —
//! only needed for *backward* traversal, so a forward-only reader just has
//! to skip the right number of bytes without decoding the value. The
//! encoding byte distinguishes six string-length forms and ten integer
//! forms (see `redis_src/src/ziplist.c`'s own top-of-file comment for the
//! full bit-pattern table) — ported in `decode_header` below, byte-for-byte
//! compatible with `ZIP_DECODE_LENGTH`.

const ZIP_BIG_PREVLEN: u8 = 254;
const ZIP_END: u8 = 0xFF;

const ZIP_STR_MASK: u8 = 0xC0;
const ZIP_STR_06B: u8 = 0x00;
const ZIP_STR_14B: u8 = 0x40;
const ZIP_STR_32B: u8 = 0x80;
const ZIP_INT_16B: u8 = 0xC0;
const ZIP_INT_32B: u8 = 0xD0;
const ZIP_INT_64B: u8 = 0xE0;
const ZIP_INT_24B: u8 = 0xF0;
const ZIP_INT_8B: u8 = 0xFE;
const ZIP_INT_IMM_MIN: u8 = 0xF1;
const ZIP_INT_IMM_MAX: u8 = 0xFD;

enum Decoded {
    Str(Vec<u8>),
    Int(i64),
}

impl Decoded {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Decoded::Str(s) => s.clone(),
            Decoded::Int(v) => v.to_string().into_bytes(),
        }
    }
}

/// How many bytes the `prevlen` field at `data[at]` occupies — 1 or 5,
/// matching `ZIP_DECODE_PREVLENSIZE`. The value itself is never needed by a
/// forward-only reader.
fn prevlen_size(data: &[u8], at: usize) -> Option<usize> {
    let first = *data.get(at)?;
    Some(if first < ZIP_BIG_PREVLEN { 1 } else { 5 })
}

/// Decodes the entry at `data[at]` (the encoding+length header), returning
/// `(value, total header bytes consumed)`. Ports `ZIP_ENTRY_ENCODING` +
/// `ZIP_DECODE_LENGTH` + `zipLoadInteger` as one step, since nothing here
/// ever needs the raw encoding byte or length on their own.
fn decode_header(data: &[u8], at: usize) -> Option<(Decoded, usize)> {
    let b = *data.get(at)?;
    if b < ZIP_STR_MASK {
        // String entry: the top 2 bits (masked out of `b`) pick the
        // length-encoding width; the data immediately follows the header.
        let (hdr_len, str_len) = match b & ZIP_STR_MASK {
            ZIP_STR_06B => (1, (b & 0x3F) as usize),
            ZIP_STR_14B => {
                let b1 = *data.get(at + 1)?;
                (2, (((b & 0x3F) as usize) << 8) | b1 as usize)
            }
            ZIP_STR_32B => {
                let w = data.get(at + 1..at + 5)?;
                (5, u32::from_be_bytes([w[0], w[1], w[2], w[3]]) as usize)
            }
            _ => return None,
        };
        let bytes = data.get(at + hdr_len..at + hdr_len + str_len)?.to_vec();
        Some((Decoded::Str(bytes), hdr_len + str_len))
    } else if (ZIP_INT_IMM_MIN..=ZIP_INT_IMM_MAX).contains(&b) {
        // 4-bit immediate: the value lives in the encoding byte itself
        // (encoded 1..=13, representing 0..=12 — see `ZIP_INT_IMM_MASK`'s
        // own comment), no trailing data bytes at all.
        Some((Decoded::Int(((b & 0x0F) as i64) - 1), 1))
    } else {
        let data_len = match b {
            ZIP_INT_8B => 1,
            ZIP_INT_16B => 2,
            ZIP_INT_24B => 3,
            ZIP_INT_32B => 4,
            ZIP_INT_64B => 8,
            _ => return None,
        };
        let w = data.get(at + 1..at + 1 + data_len)?;
        let value = match b {
            ZIP_INT_8B => w[0] as i8 as i64,
            ZIP_INT_16B => i16::from_le_bytes([w[0], w[1]]) as i64,
            ZIP_INT_32B => i32::from_le_bytes([w[0], w[1], w[2], w[3]]) as i64,
            ZIP_INT_64B => i64::from_le_bytes([w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]]),
            // 24-bit: ports `zipLoadInteger`'s own trick exactly — place the
            // 3 stored bytes into the top 3 bytes of an i32 (byte 0 left
            // zero) and arithmetic-shift right by 8 to sign-extend.
            ZIP_INT_24B => (i32::from_le_bytes([0, w[0], w[1], w[2]]) >> 8) as i64,
            _ => unreachable!(),
        };
        Some((Decoded::Int(value), 1 + data_len))
    }
}

/// Decodes a complete ziplist blob into every element's canonical byte
/// representation, in order. `None` on any structural inconsistency
/// (truncated buffer, bad encoding byte) rather than panicking — the same
/// "untrusted on-disk bytes" posture `ListPack::from_bytes`/`IntSet::from_bytes`
/// take.
pub fn entries(data: &[u8]) -> Option<Vec<Vec<u8>>> {
    const HDR_SIZE: usize = 10; // zlbytes(4) + zltail(4) + zllen(2)
    if data.len() < HDR_SIZE + 1 || data[data.len() - 1] != ZIP_END {
        return None;
    }
    let mut out = Vec::new();
    let mut p = HDR_SIZE;
    while p < data.len() && data[p] != ZIP_END {
        p += prevlen_size(data, p)?;
        let (value, consumed) = decode_header(data, p)?;
        p += consumed;
        out.push(value.to_bytes());
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal, well-formed ziplist header (zlbytes/zltail are
    /// never read by `entries`, so they're left at 0) around raw entry
    /// bytes, closed with the `0xFF` terminator.
    fn wrap(entries_bytes: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; 10];
        v.extend_from_slice(entries_bytes);
        v.push(ZIP_END);
        v
    }

    #[test]
    fn decodes_small_strings() {
        // prevlen=0, ZIP_STR_06B|1 ("a"), prevlen=2, ZIP_STR_06B|1 ("b")
        let data = wrap(&[0x00, 0x01, b'a', 0x02, 0x01, b'b']);
        assert_eq!(entries(&data), Some(vec![b"a".to_vec(), b"b".to_vec()]));
    }

    #[test]
    fn decodes_the_14_bit_string_length_as_big_endian() {
        let s = vec![b'x'; 300];
        let mut raw = vec![0x00]; // prevlen
        raw.push(ZIP_STR_14B | ((300 >> 8) as u8 & 0x3F));
        raw.push((300 & 0xFF) as u8);
        raw.extend_from_slice(&s);
        let data = wrap(&raw);
        assert_eq!(entries(&data), Some(vec![s]));
    }

    #[test]
    #[allow(clippy::vec_init_then_push)] // built up incrementally, one encoding at a time, each worth its own comment
    fn decodes_every_integer_encoding() {
        let mut raw = Vec::new();
        // ZIP_INT_IMM: 4-bit immediate, value 5 (encoded as 0xF1 + 5).
        raw.push(0x00);
        raw.push(ZIP_INT_IMM_MIN + 5);
        // ZIP_INT_8B: -100
        raw.push(0x01);
        raw.push(ZIP_INT_8B);
        raw.push((-100i8) as u8);
        // ZIP_INT_16B: -30000
        raw.push(0x02);
        raw.push(ZIP_INT_16B);
        raw.extend_from_slice(&(-30000i16).to_le_bytes());
        // ZIP_INT_24B: -70000 (outside i16 range, fits in 24 bits)
        raw.push(0x03);
        raw.push(ZIP_INT_24B);
        let v: i32 = -70000;
        let le = v.to_le_bytes();
        raw.extend_from_slice(&le[0..3]);
        // ZIP_INT_32B: -2_000_000_000
        raw.push(0x05);
        raw.push(ZIP_INT_32B);
        raw.extend_from_slice(&(-2_000_000_000i32).to_le_bytes());
        // ZIP_INT_64B: i64::MIN
        raw.push(0x05);
        raw.push(ZIP_INT_64B);
        raw.extend_from_slice(&i64::MIN.to_le_bytes());

        let data = wrap(&raw);
        assert_eq!(
            entries(&data),
            Some(vec![b"5".to_vec(), b"-100".to_vec(), b"-30000".to_vec(), b"-70000".to_vec(), b"-2000000000".to_vec(), i64::MIN.to_string().into_bytes(),])
        );
    }

    #[test]
    fn rejects_a_buffer_missing_the_end_marker() {
        let mut data = wrap(&[0x00, 0x01, b'a']);
        data.pop(); // drop the 0xFF terminator
        assert_eq!(entries(&data), None);
    }

    #[test]
    fn rejects_truncated_entry_data() {
        // Claims a 14-bit string of length 300 but the buffer is cut short.
        let data = wrap(&[0x00, ZIP_STR_14B | 0x01, 0x2C, b'x', b'x']);
        assert_eq!(entries(&data), None);
    }
}
