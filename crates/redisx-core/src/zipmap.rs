//! Zipmap — ported from `redis_src/src/zipmap.c`, the legacy small-hash
//! encoding Redis used before ziplist/listpack-backed hashes existed
//! (`RDB_TYPE_HASH_ZIPMAP`, opcode 9 — predates even `ziplist.rs`'s format).
//! **Decode-only**, same reasoning as `ziplist.rs`: this fork never writes
//! one, it only needs to read one back out of an old RDB dump.
//!
//! ## Layout
//!
//! `<zmlen:u8><key><value>...<key><value><0xFF end>`. `zmlen` is
//! informational only and pins at 254 once the map has that many entries or
//! more (`zipmapLen`'s own fallback) — like `ziplist.rs`, this always walks
//! to the `0xFF` terminator rather than trusting it.
//!
//! A key is `<len-encoding><bytes>`. A value is
//! `<len-encoding><free:u8><bytes><free padding bytes>` — `free` is a count
//! of trailing unused bytes `zipmapSet` left behind so a later same-size-or-
//! smaller update could reuse the slot without a realloc; irrelevant once
//! decoded, just skipped over. A length is 1 byte for values under 254, or
//! `0xFE` followed by a 4-byte length. That 4-byte length is stored, per the
//! C source's own comment, in **host byte order** — an acknowledged
//! portability quirk of this legacy format itself (`zipmapDecodeLength`
//! calls `memrev32ifbe`, a no-op on the little-endian hosts essentially
//! every real-world Redis instance runs on, so this reads it as
//! little-endian; a zipmap-era RDB produced on a big-endian host, a
//! vanishingly rare case even when this format was current, would need a
//! byte-swap this port doesn't attempt).

const ZIPMAP_END: u8 = 0xFF;
const ZIPMAP_BIGLEN: u8 = 254;

/// Returns `(decoded_length, bytes_consumed_by_the_length_encoding_itself)`.
fn decode_length(data: &[u8], at: usize) -> Option<(usize, usize)> {
    let first = *data.get(at)?;
    if first < ZIPMAP_BIGLEN {
        Some((first as usize, 1))
    } else if first == ZIPMAP_BIGLEN {
        let w = data.get(at + 1..at + 5)?;
        Some((u32::from_le_bytes([w[0], w[1], w[2], w[3]]) as usize, 5))
    } else {
        None // 255 is reserved for the end marker, never a valid length byte
    }
}

/// Decodes a complete zipmap blob into its `(key, value)` pairs, in order.
/// `None` on any structural inconsistency (truncated buffer, a length that
/// runs past the end) rather than panicking, matching `ziplist::entries`'s
/// posture toward untrusted on-disk bytes.
pub fn entries(data: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if data.is_empty() || data[data.len() - 1] != ZIPMAP_END {
        return None;
    }
    let mut out = Vec::new();
    let mut p = 1usize; // skip the 1-byte zmlen (unreliable past 253)
    while p < data.len() && data[p] != ZIPMAP_END {
        let (klen, kused) = decode_length(data, p)?;
        p += kused;
        let key = data.get(p..p + klen)?.to_vec();
        p += klen;

        let (vlen, vused) = decode_length(data, p)?;
        p += vused;
        let free = *data.get(p)? as usize;
        p += 1;
        let value = data.get(p..p + vlen)?.to_vec();
        p += vlen + free;

        out.push((key, value));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_example_from_the_c_source_comment() {
        // `redis_src/src/zipmap.c`'s own worked example, for
        // {"foo": "bar", "hello": "world"}.
        let data = b"\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff";
        assert_eq!(entries(data), Some(vec![(b"foo".to_vec(), b"bar".to_vec()), (b"hello".to_vec(), b"world".to_vec())]));
    }

    #[test]
    fn empty_zipmap_has_no_entries() {
        assert_eq!(entries(b"\x00\xff"), Some(vec![]));
    }

    #[test]
    fn respects_free_padding_bytes_after_a_value() {
        // Value "bar" with 2 bytes of trailing free padding that must be
        // skipped over, not mistaken for the start of the next key.
        let mut data = b"\x01\x03foo\x03\x02bar".to_vec();
        data.extend_from_slice(&[0, 0]); // the 2 free bytes
        data.push(0xff);
        assert_eq!(entries(&data), Some(vec![(b"foo".to_vec(), b"bar".to_vec())]));
    }

    #[test]
    fn decodes_the_big_length_encoding() {
        let long_value = vec![b'v'; 300];
        let mut data = vec![1u8]; // zmlen (irrelevant past 253, here just 1)
        data.push(3);
        data.extend_from_slice(b"key");
        data.push(ZIPMAP_BIGLEN);
        data.extend_from_slice(&300u32.to_le_bytes());
        data.push(0); // free
        data.extend_from_slice(&long_value);
        data.push(0xff);
        assert_eq!(entries(&data), Some(vec![(b"key".to_vec(), long_value)]));
    }

    #[test]
    fn rejects_a_buffer_missing_the_end_marker() {
        assert_eq!(entries(b"\x01\x03foo\x03\x00bar"), None);
    }

    #[test]
    fn rejects_truncated_entry_data() {
        assert_eq!(entries(b"\x01\x05fo\xff"), None);
    }
}
