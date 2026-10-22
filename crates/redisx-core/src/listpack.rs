//! Listpack — compact, byte-compatible port of `redis_src/src/listpack.c`.
//!
//! A listpack blob is kept as a single `Vec<u8>` in exactly Redis's on-disk
//! layout (`total_bytes: u32 LE, num_elements: u16 LE` header, then entries,
//! then a trailing `0xFF` EOF marker), so `as_bytes()`/`from_bytes()` round-trip
//! through the same bytes a real Redis instance would produce or accept in an
//! RDB file (needed for Phase 4). Each entry is `[encoding+length][data][backlen]`,
//! where `backlen` reverse-encodes the entry's own `[encoding+length][data]`
//! size so the structure can be walked backwards without a separate index.
//!
//! Ported: entry encode/decode for all 9 encodings (7-bit uint, 6/12/32-bit
//! string, 13/16/24/32/64-bit int), the backlen varint, forward/backward
//! iteration, append, and integrity validation. Strings that are the *exact*
//! canonical decimal form of an `i64` are auto-stored as integers (matching
//! `lpEncodeGetType`'s behavior) and transparently returned as the same bytes
//! via `get_bytes`/`iter_bytes` — only `get`/`iter` expose the raw `Int`/`Str`
//! distinction, mirroring `lpGet`'s dual (with/without `intbuf`) contract.
//!
//! Ported on top of the original Phase 0 pass once Phase 3 (data types)
//! needed it: `insert_str`/`insert_int` (`lpInsert`, arbitrary position —
//! `LPUSH` is `insert_str(0, ...)`) and `remove` (`lpDelete`, `LPOP`/`RPOP`).
//! Still not ported: `lpReplace`, `lpMerge`, `lpRandomPair*`, `lpFind` —
//! straightforward extensions of what's here, added if a later phase needs them.

const HDR_SIZE: usize = 6;
const NUMELE_UNKNOWN: u16 = u16::MAX;
const EOF_BYTE: u8 = 0xFF;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Str(Vec<u8>),
}

impl Value {
    /// The bytes this value would compare/print as, regardless of which
    /// encoding it was stored with (mirrors `lpGet(p, &count, intbuf)`).
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Int(v) => v.to_string().into_bytes(),
            Value::Str(s) => s.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPack {
    data: Vec<u8>,
}

fn get_total_bytes(data: &[u8]) -> u32 {
    u32::from_le_bytes([data[0], data[1], data[2], data[3]])
}

fn set_total_bytes(data: &mut [u8], v: u32) {
    data[0..4].copy_from_slice(&v.to_le_bytes());
}

fn get_num_elements(data: &[u8]) -> u16 {
    u16::from_le_bytes([data[4], data[5]])
}

fn set_num_elements(data: &mut [u8], v: u16) {
    data[4..6].copy_from_slice(&v.to_le_bytes());
}

/// lpStringToInt64 — strict canonical decimal parse: no leading/trailing
/// junk, no leading zeros (other than the literal string "0"), no leading
/// '+'. Guarantees `value.to_string().as_bytes() == s` on success, which is
/// what makes the int<->string round-trip in listpack lossless.
pub fn parse_canonical_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() >= 21 {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (negative, digits) = if s[0] == b'-' { (true, &s[1..]) } else { (false, s) };
    if digits.is_empty() || !(b'1'..=b'9').contains(&digits[0]) {
        return None;
    }
    let mut v: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    if negative {
        if v > i64::MAX as u64 + 1 {
            return None;
        }
        Some((-(v as i128)) as i64)
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

/// lpEncodeIntegerGetType — encoding+data bytes (no backlen) for an integer.
fn encode_int_content(v: i64) -> Vec<u8> {
    if (0..=127).contains(&v) {
        vec![v as u8]
    } else if (-4096..=4095).contains(&v) {
        let enc = (if v < 0 { (1i64 << 13) + v } else { v }) as u16;
        vec![((enc >> 8) as u8) | 0xC0, (enc & 0xff) as u8]
    } else if (-32768..=32767).contains(&v) {
        let enc = (if v < 0 { (1i64 << 16) + v } else { v }) as u16;
        vec![0xF1, (enc & 0xff) as u8, (enc >> 8) as u8]
    } else if (-8388608..=8388607).contains(&v) {
        let enc = (if v < 0 { (1i64 << 24) + v } else { v }) as u32;
        vec![0xF2, (enc & 0xff) as u8, ((enc >> 8) & 0xff) as u8, ((enc >> 16) & 0xff) as u8]
    } else if (-2147483648..=2147483647).contains(&v) {
        let enc = (if v < 0 { (1i64 << 32) + v } else { v }) as u32;
        std::iter::once(0xF3u8).chain(enc.to_le_bytes()).collect()
    } else {
        let uv = v as u64;
        std::iter::once(0xF4u8).chain(uv.to_le_bytes()).collect()
    }
}

/// lpEncodeString — encoding+length+data bytes (no backlen) for a string.
fn encode_str_content(s: &[u8]) -> Vec<u8> {
    let len = s.len();
    let mut out = Vec::with_capacity(len + 5);
    if len < 64 {
        out.push((len as u8) | 0x80); // LP_ENCODING_6BIT_STR
        out.extend_from_slice(s);
    } else if len < 4096 {
        out.push(((len >> 8) as u8) | 0xE0); // LP_ENCODING_12BIT_STR
        out.push((len & 0xff) as u8);
        out.extend_from_slice(s);
    } else {
        out.push(0xF0); // LP_ENCODING_32BIT_STR
        out.extend_from_slice(&(len as u32).to_le_bytes());
        out.extend_from_slice(s);
    }
    out
}

/// lpEncodeGetType — a string that is the exact canonical form of an i64 is
/// stored as an integer instead (smaller, and losslessly reversible).
fn encode_value_content(v: &Value) -> Vec<u8> {
    match v {
        Value::Int(i) => encode_int_content(*i),
        Value::Str(s) => match parse_canonical_i64(s) {
            Some(i) => encode_int_content(i),
            None => encode_str_content(s),
        },
    }
}

/// lpEncodeBacklen
fn encode_backlen(l: u64) -> Vec<u8> {
    if l <= 127 {
        vec![l as u8]
    } else if l < 16383 {
        vec![(l >> 7) as u8, ((l & 127) | 128) as u8]
    } else if l < 2097151 {
        vec![(l >> 14) as u8, (((l >> 7) & 127) | 128) as u8, ((l & 127) | 128) as u8]
    } else if l < 268435455 {
        vec![
            (l >> 21) as u8,
            (((l >> 14) & 127) | 128) as u8,
            (((l >> 7) & 127) | 128) as u8,
            ((l & 127) | 128) as u8,
        ]
    } else {
        vec![
            (l >> 28) as u8,
            (((l >> 21) & 127) | 128) as u8,
            (((l >> 14) & 127) | 128) as u8,
            (((l >> 7) & 127) | 128) as u8,
            ((l & 127) | 128) as u8,
        ]
    }
}

fn backlen_bytes_needed(l: u64) -> usize {
    if l <= 127 {
        1
    } else if l < 16383 {
        2
    } else if l < 2097151 {
        3
    } else if l < 268435455 {
        4
    } else {
        5
    }
}

/// lpDecodeBacklen — `data[end]` is the last byte of the backlen field
/// (reading backwards). Returns `(value, width_in_bytes)`, or `None` if more
/// than 5 bytes would be needed (malformed input).
fn decode_backlen(data: &[u8], end: usize) -> Option<(u64, usize)> {
    let mut val: u64 = 0;
    for width in 1..=5usize {
        let i = end + 1 - width;
        let b = data[i];
        val |= ((b & 127) as u64) << (7 * (width - 1));
        if b & 128 == 0 {
            return Some((val, width));
        }
    }
    None
}

/// lpCurrentEncodedSizeUnsafe — size in bytes of `[encoding+length][data]`
/// (excluding backlen) for the entry starting at `data[off]`.
fn content_size(data: &[u8], off: usize) -> Option<usize> {
    let b0 = *data.get(off)?;
    Some(if b0 & 0x80 == 0 {
        1
    } else if b0 & 0xC0 == 0x80 {
        1 + (b0 & 0x3F) as usize
    } else if b0 & 0xE0 == 0xC0 {
        2
    } else if b0 == 0xF1 {
        3
    } else if b0 == 0xF2 {
        4
    } else if b0 == 0xF3 {
        5
    } else if b0 == 0xF4 {
        9
    } else if b0 & 0xF0 == 0xE0 {
        let len = ((b0 & 0x0F) as usize) << 8 | *data.get(off + 1)? as usize;
        2 + len
    } else if b0 == 0xF0 {
        let len = u32::from_le_bytes(data.get(off + 1..off + 5)?.try_into().ok()?) as usize;
        5 + len
    } else {
        // EOF_BYTE (0xFF) or any other invalid/unrecognized encoding byte.
        return None;
    })
}

fn sign_extend(raw: u64, bits: u32) -> i64 {
    if bits == 64 {
        return raw as i64;
    }
    let negstart = 1u64 << (bits - 1);
    if raw >= negstart {
        raw as i64 - (1i64 << bits)
    } else {
        raw as i64
    }
}

/// lpGetWithBuf — decode the entry at `data[off]` (content only, i.e. not
/// including backlen). Returns `None` if `off` is EOF or out of range.
fn decode_at(data: &[u8], off: usize) -> Option<Value> {
    let b0 = *data.get(off)?;
    if b0 & 0x80 == 0 {
        return Some(Value::Int((b0 & 0x7f) as i64));
    }
    if b0 & 0xC0 == 0x80 {
        let len = (b0 & 0x3F) as usize;
        return Some(Value::Str(data.get(off + 1..off + 1 + len)?.to_vec()));
    }
    if b0 & 0xE0 == 0xC0 {
        let raw = (((b0 & 0x1f) as u64) << 8) | *data.get(off + 1)? as u64;
        return Some(Value::Int(sign_extend(raw, 13)));
    }
    if b0 == 0xF1 {
        let raw = *data.get(off + 1)? as u64 | (*data.get(off + 2)? as u64) << 8;
        return Some(Value::Int(sign_extend(raw, 16)));
    }
    if b0 == 0xF2 {
        let raw = *data.get(off + 1)? as u64 | (*data.get(off + 2)? as u64) << 8 | (*data.get(off + 3)? as u64) << 16;
        return Some(Value::Int(sign_extend(raw, 24)));
    }
    if b0 == 0xF3 {
        let raw = u32::from_le_bytes(data.get(off + 1..off + 5)?.try_into().ok()?) as u64;
        return Some(Value::Int(sign_extend(raw, 32)));
    }
    if b0 == 0xF4 {
        let raw = u64::from_le_bytes(data.get(off + 1..off + 9)?.try_into().ok()?);
        return Some(Value::Int(sign_extend(raw, 64)));
    }
    if b0 & 0xF0 == 0xE0 {
        let len = ((b0 & 0x0F) as usize) << 8 | *data.get(off + 1)? as usize;
        return Some(Value::Str(data.get(off + 2..off + 2 + len)?.to_vec()));
    }
    if b0 == 0xF0 {
        let len = u32::from_le_bytes(data.get(off + 1..off + 5)?.try_into().ok()?) as usize;
        return Some(Value::Str(data.get(off + 5..off + 5 + len)?.to_vec()));
    }
    None
}

impl Default for ListPack {
    fn default() -> Self {
        Self::new()
    }
}

impl ListPack {
    /// lpNew
    pub fn new() -> Self {
        let mut data = vec![0u8; HDR_SIZE + 1];
        set_total_bytes(&mut data, (HDR_SIZE + 1) as u32);
        set_num_elements(&mut data, 0);
        data[HDR_SIZE] = EOF_BYTE;
        ListPack { data }
    }

    pub fn is_empty(&self) -> bool {
        self.data[HDR_SIZE] == EOF_BYTE
    }

    /// lpLength — uses the cached header count when available, otherwise
    /// walks the whole structure (mirrors the `NUMELE_UNKNOWN` sentinel path).
    pub fn len(&self) -> usize {
        let n = get_num_elements(&self.data);
        if n != NUMELE_UNKNOWN {
            return n as usize;
        }
        self.offsets().count()
    }

    fn total_bytes(&self) -> usize {
        get_total_bytes(&self.data) as usize
    }

    /// Byte offsets of every entry's first byte, in order (lpFirst/lpNext).
    fn offsets(&self) -> impl Iterator<Item = usize> + '_ {
        std::iter::successors(Some(HDR_SIZE), move |&off| {
            let csize = content_size(&self.data, off)?;
            let next = off + csize + backlen_bytes_needed(csize as u64);
            if self.data.get(next) == Some(&EOF_BYTE) {
                None
            } else {
                Some(next)
            }
        })
    }

    /// lpSeek / lpGet combined — the `index`-th entry (0-based), decoded.
    pub fn get(&self, index: usize) -> Option<Value> {
        let off = self.offsets().nth(index)?;
        decode_at(&self.data, off)
    }

    /// Like `get`, but normalizes integers back to their decimal string form
    /// (mirrors `lpGet(p, &count, intbuf)` with a non-NULL `intbuf`).
    pub fn get_bytes(&self, index: usize) -> Option<Vec<u8>> {
        self.get(index).map(|v| v.to_bytes())
    }

    pub fn iter(&self) -> impl Iterator<Item = Value> + '_ {
        self.offsets().filter_map(move |off| decode_at(&self.data, off))
    }

    pub fn to_vec(&self) -> Vec<Value> {
        self.iter().collect()
    }

    fn grow_header_count(data: &mut [u8]) {
        let n = get_num_elements(data);
        if n != NUMELE_UNKNOWN {
            let next = n as u32 + 1;
            set_num_elements(data, if next >= NUMELE_UNKNOWN as u32 { NUMELE_UNKNOWN } else { next as u16 });
        }
    }

    fn shrink_header_count(data: &mut [u8]) {
        let n = get_num_elements(data);
        if n != NUMELE_UNKNOWN {
            set_num_elements(data, n - 1);
        }
    }

    /// Inserts `content` (encoding+length+data, as produced by
    /// `encode_value_content`/`encode_int_content`) plus its backlen at byte
    /// offset `at` (which must be the start of an existing entry, or the EOF
    /// position to append at the tail).
    fn splice_content_at(&mut self, at: usize, content: Vec<u8>) {
        let backlen = encode_backlen(content.len() as u64);
        let insert_len = content.len() + backlen.len();
        self.data.splice(at..at, content.into_iter().chain(backlen));
        let new_total = self.total_bytes() + insert_len;
        set_total_bytes(&mut self.data, new_total as u32);
        Self::grow_header_count(&mut self.data);
    }

    fn push_content(&mut self, content: Vec<u8>) {
        let eof_pos = self.data.len() - 1;
        debug_assert_eq!(self.data[eof_pos], EOF_BYTE);
        self.splice_content_at(eof_pos, content);
    }

    /// lpAppend — appends a string (auto-stored as an integer if it is the
    /// exact canonical decimal form of an `i64`, matching `lpEncodeGetType`).
    pub fn push_str(&mut self, s: &[u8]) {
        let content = encode_value_content(&Value::Str(s.to_vec()));
        self.push_content(content);
    }

    /// lpAppendInteger
    pub fn push_int(&mut self, v: i64) {
        self.push_content(encode_int_content(v));
    }

    /// lpPrepend / `lpInsert(..., LP_BEFORE)` at index 0 — inserts before the
    /// current first element (or appends, if the listpack is empty).
    pub fn insert_str(&mut self, index: usize, s: &[u8]) {
        let content = encode_value_content(&Value::Str(s.to_vec()));
        self.insert_content(index, content);
    }

    pub fn insert_int(&mut self, index: usize, v: i64) {
        self.insert_content(index, encode_int_content(v));
    }

    fn insert_content(&mut self, index: usize, content: Vec<u8>) {
        let at = self.offsets().nth(index).unwrap_or(self.data.len() - 1);
        self.splice_content_at(at, content);
    }

    /// `lpDelete` — removes and decodes the `index`-th entry, if present.
    pub fn remove(&mut self, index: usize) -> Option<Value> {
        let off = self.offsets().nth(index)?;
        let csize = content_size(&self.data, off)?;
        let value = decode_at(&self.data, off);
        let entry_len = csize + backlen_bytes_needed(csize as u64);
        self.data.drain(off..off + entry_len);
        let new_total = self.total_bytes() - entry_len;
        set_total_bytes(&mut self.data, new_total as u32);
        Self::shrink_header_count(&mut self.data);
        value
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// `remove`/`splice_content_at` shrink the *logical* listpack (via
    /// `drain`) but never the backing `Vec<u8>`'s own allocated capacity —
    /// same as any `Vec::drain` call, real Redis doesn't have this exact
    /// issue since `lpDelete` uses `realloc`, which a C allocator can shrink
    /// in place. A listpack that grew large and then had most entries
    /// removed (e.g. a big list drained down to a few items) keeps paying
    /// for the old peak size until something calls this — the active-defrag
    /// pass's whole reason to exist (`server.rs`'s `RObj::defrag_in_place`).
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    /// lpValidateIntegrity — checks the header, walks every entry verifying
    /// its backlen matches its own content size, and confirms the structure
    /// ends in a single `0xFF` exactly at the header's declared `total_bytes`.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < HDR_SIZE + 1 {
            return None;
        }
        if get_total_bytes(buf) as usize != buf.len() {
            return None;
        }
        if buf[buf.len() - 1] != EOF_BYTE {
            return None;
        }
        let mut off = HDR_SIZE;
        let mut count = 0u32;
        while buf.get(off) != Some(&EOF_BYTE) {
            let csize = content_size(buf, off)?;
            let entry_end = off.checked_add(csize)?;
            if entry_end >= buf.len() {
                return None;
            }
            let backlen_end = entry_end + backlen_bytes_needed(csize as u64) - 1;
            let (backlen_val, backlen_width) = decode_backlen(buf, backlen_end)?;
            if backlen_val as usize != csize || backlen_width != backlen_bytes_needed(csize as u64) {
                return None;
            }
            off = backlen_end + 1;
            count += 1;
        }
        if off != buf.len() - 1 {
            return None;
        }
        let declared = get_num_elements(buf);
        if declared != NUMELE_UNKNOWN && declared as u32 != count {
            return None;
        }
        Some(ListPack { data: buf.to_vec() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_listpack_layout() {
        let lp = ListPack::new();
        assert!(lp.is_empty());
        assert_eq!(lp.len(), 0);
        assert_eq!(lp.as_bytes(), &[7, 0, 0, 0, 0, 0, 0xFF]);
    }

    #[test]
    fn shrink_to_fit_recovers_capacity_left_by_removal() {
        let mut lp = ListPack::new();
        for i in 0..2000 {
            lp.push_str(format!("entry-{i}-padding-padding-padding").as_bytes());
        }
        let grown_capacity = lp.data.capacity();
        for _ in 0..1999 {
            lp.remove(0);
        }
        assert_eq!(lp.len(), 1);
        // `remove`'s `Vec::drain` leaves capacity at the historical peak —
        // this is the exact fragmentation `shrink_to_fit` exists to recover.
        assert_eq!(lp.data.capacity(), grown_capacity, "drain must not have shrunk capacity on its own");
        lp.shrink_to_fit();
        assert!(lp.data.capacity() < grown_capacity, "shrink_to_fit must actually free the unused capacity");
        assert_eq!(lp.data.capacity(), lp.data.len(), "shrink_to_fit should leave no slack at all");
        // Correctness: the remaining entry must be untouched by the shrink.
        assert_eq!(lp.get_bytes(0).unwrap(), b"entry-1999-padding-padding-padding");
    }

    #[test]
    fn push_and_get_strings() {
        let mut lp = ListPack::new();
        lp.push_str(b"hello");
        lp.push_str(b"world");
        assert_eq!(lp.len(), 2);
        assert_eq!(lp.get_bytes(0).unwrap(), b"hello");
        assert_eq!(lp.get_bytes(1).unwrap(), b"world");
        assert_eq!(lp.to_vec(), vec![Value::Str(b"hello".to_vec()), Value::Str(b"world".to_vec())]);
    }

    #[test]
    fn numeric_strings_are_auto_stored_as_int_but_round_trip_as_bytes() {
        let mut lp = ListPack::new();
        lp.push_str(b"123");
        lp.push_str(b"-45");
        lp.push_str(b"007"); // NOT canonical (leading zero) -> must stay a string
        lp.push_str(b"0");
        assert_eq!(lp.get(0), Some(Value::Int(123)));
        assert_eq!(lp.get(1), Some(Value::Int(-45)));
        assert_eq!(lp.get(2), Some(Value::Str(b"007".to_vec())));
        assert_eq!(lp.get(3), Some(Value::Int(0)));
        assert_eq!(lp.get_bytes(0).unwrap(), b"123");
        assert_eq!(lp.get_bytes(1).unwrap(), b"-45");
        assert_eq!(lp.get_bytes(2).unwrap(), b"007");
        assert_eq!(lp.get_bytes(3).unwrap(), b"0");
    }

    #[test]
    fn integer_encoding_boundaries_round_trip() {
        let boundary_values: [i64; 18] = [
            0, 1, 127, 128, -1, -4096, 4095, 4096, -4097, 32767, -32768, 32768, -32769, 8388607, -8388608, 8388608,
            i64::MAX, i64::MIN,
        ];
        for &v in &boundary_values {
            let mut lp = ListPack::new();
            lp.push_int(v);
            assert_eq!(lp.get(0), Some(Value::Int(v)), "round-trip failed for {v}");
        }
    }

    #[test]
    fn string_length_encoding_boundaries_round_trip() {
        for len in [0usize, 1, 63, 64, 65, 4095, 4096, 4097, 70000] {
            let s = vec![b'x'; len];
            let mut lp = ListPack::new();
            lp.push_str(&s);
            assert_eq!(lp.get_bytes(0).unwrap(), s, "round-trip failed for len {len}");
        }
    }

    #[test]
    fn mixed_entries_iterate_in_order() {
        let mut lp = ListPack::new();
        let items: Vec<Value> = vec![
            Value::Int(42),
            Value::Str(b"hello world, this is a longer string past 63 bytes to hit the 12-bit encoding".to_vec()),
            Value::Int(-1),
            Value::Str(Vec::new()),
        ];
        for item in &items {
            match item {
                Value::Int(v) => lp.push_int(*v),
                Value::Str(s) => lp.push_str(s),
            }
        }
        assert_eq!(lp.to_vec(), items);
    }

    #[test]
    fn insert_at_front_mirrors_lpush_semantics() {
        // LPUSH key a b c should leave the list as [c, b, a], since each
        // argument is prepended in turn.
        let mut lp = ListPack::new();
        lp.insert_str(0, b"a");
        lp.insert_str(0, b"b");
        lp.insert_str(0, b"c");
        assert_eq!(lp.to_vec(), vec![Value::Str(b"c".to_vec()), Value::Str(b"b".to_vec()), Value::Str(b"a".to_vec())]);
    }

    #[test]
    fn insert_in_the_middle() {
        let mut lp = ListPack::new();
        lp.push_str(b"a");
        lp.push_str(b"c");
        lp.insert_str(1, b"b");
        assert_eq!(lp.to_vec(), vec![Value::Str(b"a".to_vec()), Value::Str(b"b".to_vec()), Value::Str(b"c".to_vec())]);
    }

    #[test]
    fn insert_past_the_end_appends() {
        let mut lp = ListPack::new();
        lp.push_str(b"a");
        lp.insert_int(5, 99); // out-of-range index falls back to append, like lpInsert at EOF
        assert_eq!(lp.to_vec(), vec![Value::Str(b"a".to_vec()), Value::Int(99)]);
    }

    #[test]
    fn remove_front_back_and_middle() {
        let mut lp = ListPack::new();
        for v in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()] {
            lp.push_str(&v);
        }
        assert_eq!(lp.remove(0), Some(Value::Str(b"a".to_vec())));
        assert_eq!(lp.to_vec(), vec![Value::Str(b"b".to_vec()), Value::Str(b"c".to_vec()), Value::Str(b"d".to_vec())]);
        assert_eq!(lp.remove(lp.len() - 1), Some(Value::Str(b"d".to_vec())));
        assert_eq!(lp.to_vec(), vec![Value::Str(b"b".to_vec()), Value::Str(b"c".to_vec())]);
        assert_eq!(lp.remove(10), None);
        assert_eq!(lp.len(), 2);
    }

    #[test]
    fn push_insert_remove_keeps_header_and_eof_consistent() {
        let mut lp = ListPack::new();
        for i in 0..50i64 {
            lp.push_int(i);
        }
        lp.insert_int(0, -1);
        lp.insert_int(25, 1000);
        lp.remove(10);
        let blob = lp.as_bytes().to_vec();
        ListPack::from_bytes(&blob).expect("structure must stay valid after a mix of insert/remove");
        assert_eq!(lp.len(), 51);
    }

    #[test]
    fn to_bytes_from_bytes_round_trip_and_validates() {
        let mut lp = ListPack::new();
        lp.push_str(b"a");
        lp.push_int(999999999999);
        lp.push_str(&vec![b'z'; 5000]);
        let blob = lp.as_bytes().to_vec();
        let back = ListPack::from_bytes(&blob).expect("valid listpack should parse");
        assert_eq!(back, lp);
    }

    #[test]
    fn from_bytes_rejects_corrupted_total_bytes_header() {
        let mut lp = ListPack::new();
        lp.push_str(b"hello");
        let mut blob = lp.as_bytes().to_vec();
        blob[0] = 0xAA; // corrupt total_bytes
        assert!(ListPack::from_bytes(&blob).is_none());
    }

    #[test]
    fn from_bytes_rejects_missing_eof() {
        let mut lp = ListPack::new();
        lp.push_str(b"hello");
        let mut blob = lp.as_bytes().to_vec();
        let last = blob.len() - 1;
        blob[last] = 0x00;
        assert!(ListPack::from_bytes(&blob).is_none());
    }

    #[test]
    fn large_push_keeps_header_count_accurate() {
        let mut lp = ListPack::new();
        for i in 0..1000i64 {
            lp.push_int(i);
        }
        assert_eq!(lp.len(), 1000);
        assert_eq!(lp.get(999), Some(Value::Int(999)));
    }
}
