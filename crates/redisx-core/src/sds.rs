//! Binary-safe string buffer — ported from `redis_src/src/sds.c`.
//!
//! The C implementation uses a 5-tier variable-size header (`sdshdr5/8/16/32/64`)
//! purely to minimize per-allocation overhead on top of a bare C allocator. In
//! Rust, `Vec<u8>` already tracks length/capacity as plain machine words with no
//! such overhead, so `Sds` is a thin wrapper over `Vec<u8>` rather than a
//! reimplementation of the C header layout. Only the *behavioral* semantics of
//! each ported function are preserved (each is tested against the equivalent
//! C function's documented behavior / doc-comment examples in `sds.c`).
//!
//! Not ported here (out of Phase 0 scope): `sdscatfmt`'s `%S`-tagged template
//! format, `sdstemplate`, `sdsmapchars`, `sdsjoin*`. Add as later phases need them.

/// `Vec<u8>`'s derived `Ord` is exactly `sdscmp`'s contract: lexicographic
/// (memcmp-equivalent) comparison on the shared prefix, then the longer
/// string sorts greater when one is a prefix of the other.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sds(Vec<u8>);

fn is_space_c(b: u8) -> bool {
    // Matches C's isspace() in the "C" locale: space, \t, \n, \v, \f, \r.
    matches!(b, b' ' | 0x09 | 0x0a | 0x0b | 0x0c | 0x0d)
}

fn is_print_c(b: u8) -> bool {
    // Matches C's isprint() in the "C" locale: printable ASCII, space through '~'.
    (0x20..0x7f).contains(&b)
}

fn is_hex_digit(b: u8) -> bool {
    b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || (b'A'..=b'F').contains(&b)
}

fn hex_digit_to_int(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

impl Sds {
    /// sdsempty / sdsnewlen
    pub fn new() -> Self {
        Sds(Vec::new())
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Sds(data.to_vec())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str_lossy(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// sdscatlen
    pub fn cat(&mut self, data: &[u8]) -> &mut Self {
        self.0.extend_from_slice(data);
        self
    }

    /// sdscat
    pub fn cat_str(&mut self, s: &str) -> &mut Self {
        self.cat(s.as_bytes())
    }

    /// sdscatsds
    pub fn cat_sds(&mut self, other: &Sds) -> &mut Self {
        self.cat(&other.0)
    }

    /// sdscatprintf — Rust has no C varargs, so callers pass `format_args!(...)`
    /// directly (e.g. `s.cat_fmt(format_args!("{} {}", a, b))`).
    pub fn cat_fmt(&mut self, args: std::fmt::Arguments<'_>) -> &mut Self {
        use std::fmt::Write as _;
        let mut tmp = String::new();
        let _ = tmp.write_fmt(args);
        self.cat(tmp.as_bytes())
    }

    /// sdstrim — strip any leading/trailing byte present in `cset`.
    pub fn trim(&mut self, cset: &[u8]) -> &mut Self {
        if self.0.is_empty() {
            return self;
        }
        let start = self.0.iter().position(|b| !cset.contains(b)).unwrap_or(self.0.len());
        if start == self.0.len() {
            self.0.clear();
            return self;
        }
        let end = self.0.iter().rposition(|b| !cset.contains(b)).unwrap();
        self.0 = self.0[start..=end].to_vec();
        self
    }

    /// sdsrange — inclusive `[start, end]`, negative indices count from the end
    /// (-1 is the last byte). Out-of-range inputs clamp rather than panic.
    pub fn range(&mut self, start: isize, end: isize) -> &mut Self {
        let len = self.0.len() as isize;
        if len == 0 {
            return self;
        }
        let mut start = if start < 0 { (len + start).max(0) } else { start };
        let end = if end < 0 { len + end } else { end };
        if start > len {
            start = len;
        }
        let newlen = if start > end { 0isize } else { end - start + 1 };
        let start = start as usize;
        let avail = self.0.len().saturating_sub(start);
        let newlen = (newlen.max(0) as usize).min(avail);
        self.0 = self.0[start..start + newlen].to_vec();
        self
    }

    /// sdscatrepr — quote + escape `data` in the exact form `split_args` can
    /// parse back. Used by the AOF MONITOR-style writer and SLOWLOG file format.
    pub fn cat_repr(&mut self, data: &[u8]) -> &mut Self {
        self.0.reserve(data.len() + 2);
        self.0.push(b'"');
        for &b in data {
            match b {
                b'\\' | b'"' => {
                    self.0.push(b'\\');
                    self.0.push(b);
                }
                b'\n' => self.0.extend_from_slice(b"\\n"),
                b'\r' => self.0.extend_from_slice(b"\\r"),
                b'\t' => self.0.extend_from_slice(b"\\t"),
                0x07 => self.0.extend_from_slice(b"\\a"),
                0x08 => self.0.extend_from_slice(b"\\b"),
                _ if is_print_c(b) => self.0.push(b),
                _ => self.0.extend_from_slice(format!("\\x{:02x}", b).as_bytes()),
            }
        }
        self.0.push(b'"');
        self
    }

    /// sdsneedsrepr
    pub fn needs_repr(&self) -> bool {
        self.0.iter().any(|&b| {
            matches!(b, b'\\' | b'"' | b'\n' | b'\r' | b'\t' | 0x07 | 0x08)
                || !is_print_c(b)
                || is_space_c(b)
        })
    }
}

impl From<&str> for Sds {
    fn from(s: &str) -> Self {
        Sds(s.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for Sds {
    fn from(v: Vec<u8>) -> Self {
        Sds(v)
    }
}

/// sdssplitlen — split `data` on (possibly multi-byte) `sep`. Unlike the C
/// version, an empty `sep` or empty `data` returns an empty `Vec` rather than
/// `NULL`; this is not behaviorally significant for any Phase 0+ caller.
pub fn split(data: &[u8], sep: &[u8]) -> Vec<Vec<u8>> {
    if sep.is_empty() || data.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut j = 0usize;
    while j + sep.len() <= data.len() {
        if &data[j..j + sep.len()] == sep {
            out.push(data[start..j].to_vec());
            j += sep.len();
            start = j;
        } else {
            j += 1;
        }
    }
    out.push(data[start..].to_vec());
    out
}

/// sdssplitargs — quote-aware command-line-style tokenizer. Double-quoted
/// segments support `\xHH` and C escapes (`\n \r \t \b \a`); single-quoted
/// segments only support `\'`. Returns `None` on unbalanced quotes or a
/// closing quote not followed by whitespace, matching the C function's
/// NULL-on-error contract.
pub fn split_args(line: &str) -> Option<Vec<Sds>> {
    let bytes = line.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();

    loop {
        while i < bytes.len() && is_space_c(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() {
            return Some(out);
        }

        let mut current = Sds::new();
        let (mut inq, mut insq) = (false, false);
        loop {
            if i >= bytes.len() {
                if inq || insq {
                    return None;
                }
                break;
            }
            let c = bytes[i];
            if inq {
                if c == b'\\'
                    && bytes.get(i + 1) == Some(&b'x')
                    && bytes.get(i + 2).copied().is_some_and(is_hex_digit)
                    && bytes.get(i + 3).copied().is_some_and(is_hex_digit)
                {
                    let byte = (hex_digit_to_int(bytes[i + 2]) << 4) | hex_digit_to_int(bytes[i + 3]);
                    current.cat(&[byte]);
                    i += 3;
                } else if c == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                    let esc = match bytes[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        other => other,
                    };
                    current.cat(&[esc]);
                } else if c == b'"' {
                    if let Some(&next) = bytes.get(i + 1) {
                        if !is_space_c(next) {
                            return None;
                        }
                    }
                    i += 1;
                    break;
                } else {
                    current.cat(&[c]);
                }
            } else if insq {
                if c == b'\\' && bytes.get(i + 1) == Some(&b'\'') {
                    current.cat(b"'");
                    i += 1;
                } else if c == b'\'' {
                    if let Some(&next) = bytes.get(i + 1) {
                        if !is_space_c(next) {
                            return None;
                        }
                    }
                    i += 1;
                    break;
                } else {
                    current.cat(&[c]);
                }
            } else {
                match c {
                    b' ' | b'\n' | b'\r' | b'\t' => {
                        i += 1;
                        break;
                    }
                    b'"' => inq = true,
                    b'\'' => insq = true,
                    _ => {
                        current.cat(&[c]);
                    }
                }
            }
            i += 1;
        }
        out.push(current);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn cat_and_len() {
        let mut s = Sds::from("foo");
        s.cat_str("bar");
        assert_eq!(s.as_bytes(), b"foobar");
        assert_eq!(s.len(), 6);
    }

    #[test]
    fn cmp_matches_memcmp_then_length() {
        assert_eq!(Sds::from("abc").cmp(&Sds::from("abd")), Ordering::Less);
        assert_eq!(Sds::from("ab").cmp(&Sds::from("abc")), Ordering::Less);
        assert_eq!(Sds::from("abc").cmp(&Sds::from("abc")), Ordering::Equal);
    }

    #[test]
    fn trim_strips_cset_from_both_ends() {
        let mut s = Sds::from("  hello world  \t");
        s.trim(b" \t");
        assert_eq!(s.as_bytes(), b"hello world");
    }

    #[test]
    fn range_matches_sdsrange_doc_example() {
        // s = sdsnew("Hello World"); sdsrange(s,1,-1); => "ello World"
        let mut s = Sds::from("Hello World");
        s.range(1, -1);
        assert_eq!(s.as_bytes(), b"ello World");
    }

    #[test]
    fn range_start_greater_than_end_yields_empty() {
        let mut s = Sds::from("Hello");
        s.range(3, 1);
        assert!(s.is_empty());
    }

    #[test]
    fn cat_repr_round_trips_through_split_args() {
        let mut s = Sds::new();
        s.cat_repr(b"hello\nworld\x01\"quoted\"");
        let line = s.as_str_lossy().into_owned();
        let args = split_args(&line).unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].as_bytes(), b"hello\nworld\x01\"quoted\"");
    }

    #[test]
    fn cat_repr_matches_known_c_output() {
        let mut s = Sds::new();
        s.cat_repr(b"a\\b");
        assert_eq!(s.as_bytes(), b"\"a\\\\b\"");
    }

    #[test]
    fn split_args_handles_monitor_style_line() {
        let args = split_args(r#""set" "foo" "bar baz""#).unwrap();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0].as_bytes(), b"set");
        assert_eq!(args[1].as_bytes(), b"foo");
        assert_eq!(args[2].as_bytes(), b"bar baz");
    }

    #[test]
    fn split_args_hex_escape() {
        let args = split_args(r#""\xff\x00tail""#).unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].as_bytes(), [0xffu8, 0x00, b't', b'a', b'i', b'l']);
    }

    #[test]
    fn split_args_rejects_unbalanced_quotes() {
        assert!(split_args(r#""unterminated"#).is_none());
        assert!(split_args(r#""foo"bar"#).is_none());
    }

    #[test]
    fn split_args_empty_input_returns_empty_vec_not_none() {
        assert_eq!(split_args("").unwrap().len(), 0);
        assert_eq!(split_args("   ").unwrap().len(), 0);
    }

    #[test]
    fn split_multi_byte_separator() {
        let parts = split(b"foo_-_bar_-_baz", b"_-_");
        assert_eq!(parts, vec![b"foo".to_vec(), b"bar".to_vec(), b"baz".to_vec()]);
    }

    #[test]
    fn needs_repr_detects_space_and_control_chars() {
        assert!(!Sds::from("plain").needs_repr());
        assert!(Sds::from("has space").needs_repr());
        assert!(Sds::from_bytes(b"a\nb").needs_repr());
    }
}
