//! LZF decompression — ported from `redis_src/src/lzf_d.c` (original algorithm
//! by Marc Alexander Lehmann). Needed because real Redis RDB files compress
//! large strings with LZF by default (`rdbcompression yes`); without this,
//! redisX could not load a stock Redis dump at all once Phase 4 (RDB) lands.
//!
//! Only decompression is ported. LZF compression (`lzf_c.c`) is not: redisX
//! can simply always write RDB files with `rdbcompression` effectively off
//! (plain strings), which real Redis itself also supports and can read —
//! compression is a space optimization, not a format requirement. If a
//! compressed *write* path is ever wanted, prefer an existing LZF-compatible
//! crate over hand-porting `lzf_c.c`'s greedy matcher.
//!
//! Unlike the C version (which only bounds-checks when compiled with
//! `CHECK_INPUT`), this port always validates every read/write against the
//! buffer bounds and returns `None` on any malformed/truncated input — Rust
//! has no "trust the caller, skip the checks" escape hatch, which is a
//! correctness improvement for a function whose whole job is parsing
//! untrusted bytes from an RDB file.

/// lzf_decompress. `expected_len` is the uncompressed size as recorded by the
/// RDB string header (`rdbLoadLzfStringObject`'s `len`); decompression stops
/// once the input is exhausted and the result is rejected if its length
/// doesn't match, matching the stored expectation.
pub fn decompress(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(expected_len);
    let mut ip = 0usize;

    while ip < input.len() {
        let ctrl = input[ip] as usize;
        ip += 1;

        if ctrl < 32 {
            // Literal run of `ctrl + 1` bytes.
            let len = ctrl + 1;
            let end = ip.checked_add(len)?;
            out.extend_from_slice(input.get(ip..end)?);
            ip = end;
        } else {
            // Back-reference.
            let mut len = ctrl >> 5;
            if ip >= input.len() {
                return None;
            }
            if len == 7 {
                len += input[ip] as usize;
                ip += 1;
                if ip >= input.len() {
                    return None;
                }
            }
            let ref_low = input[ip] as usize;
            ip += 1;

            let back = ((ctrl & 0x1f) << 8) + ref_low + 1;
            if back > out.len() {
                return None;
            }
            let start = out.len() - back;
            let copy_len = len + 2;
            for ref_pos in start..start + copy_len {
                let b = *out.get(ref_pos)?;
                out.push(b);
            }
        }
    }

    if out.len() != expected_len {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deliberately naive LZF encoder used only to generate test fixtures:
    /// it emits nothing but literal runs (chunked to the 32-byte max), which
    /// is valid (if uncompressed) LZF data per the format. This lets tests
    /// construct inputs without reimplementing the real greedy matcher.
    fn literal_only_compress(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in data.chunks(32) {
            out.push((chunk.len() - 1) as u8);
            out.extend_from_slice(chunk);
        }
        out
    }

    #[test]
    fn round_trips_literal_only_encoding() {
        let original = b"The quick brown fox jumps over the lazy dog, repeatedly, many times over.";
        let compressed = literal_only_compress(original);
        let out = decompress(&compressed, original.len()).unwrap();
        assert_eq!(out, original);
    }

    #[test]
    fn decodes_a_short_back_reference() {
        // Literal "abc" (ctrl=2 means length 3), then a back-reference
        // reproducing "abc" again. LZF's minimum match is 3 bytes: the
        // control byte's top 3 bits (ctrl>>5) must be >= 1 (0 would be
        // ambiguous with a literal run, since ctrl<32 means literal), and
        // the actual copy length is (ctrl>>5)+2 -- so ctrl>>5=1 copies 3
        // bytes. Distance 3, encoded as ctrl=0x20 (len=1, high 5 bits=0)
        // plus ref_low=2: back = (0<<8) + 2 + 1 = 3.
        let input = vec![2, b'a', b'b', b'c', 0x20, 2];
        let out = decompress(&input, 6).unwrap();
        assert_eq!(out, b"abcabc");
    }

    #[test]
    fn rejects_truncated_literal_run() {
        let input = vec![5, b'a', b'b']; // claims 6 literal bytes but only 2 follow
        assert!(decompress(&input, 6).is_none());
    }

    #[test]
    fn rejects_back_reference_past_start_of_output() {
        // ctrl=0x20 (len=1, i.e. a valid 3-byte match) but there is no prior
        // output at all to reference -- must be rejected, not panic.
        let input = vec![0x20, 0x05];
        assert!(decompress(&input, 3).is_none());
    }

    #[test]
    fn rejects_length_mismatch_with_declared_expected_len() {
        let compressed = literal_only_compress(b"hello");
        assert!(decompress(&compressed, 999).is_none());
    }
}
