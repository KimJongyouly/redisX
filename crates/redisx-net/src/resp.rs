//! RESP2/RESP3 wire protocol — reply encoding ported from the `addReply*`
//! family in `redis_src/src/networking.c`, request parsing ported from
//! `processInlineBuffer`/`processMultibulkBuffer` in the same file.
//!
//! The real `networking.c` parser is a stateful per-client incremental state
//! machine (`c->qb_pos`, `c->multibulklen`, `c->bulklen` persisted across
//! event-loop iterations so a command split across several `read()`s is
//! resumed exactly where it left off). This port is instead a **stateless**
//! "try to parse one command from this buffer" function that the caller
//! re-invokes as more bytes arrive, returning [`ParseOutcome::NeedMore`] when
//! the buffer doesn't yet hold a complete command. This is a performance
//! simplification (re-scanning already-seen bytes on every call), not a
//! correctness one — same protocol, same error conditions, same framing.

/// PROTO_INLINE_MAX_SIZE
const INLINE_MAX_SIZE: usize = 64 * 1024;
/// server.proto_max_bulk_len default (`proto-max-bulk-len`).
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum ParseOutcome {
    /// Not enough bytes yet to determine the next command.
    NeedMore,
    /// Malformed input; the connection should be closed after reporting this.
    Protocol(&'static str),
    /// A complete command (possibly empty, from a bare `*0\r\n` or a blank
    /// inline line, which the caller should just discard and keep reading).
    Command { argv: Vec<Vec<u8>>, consumed: usize },
}

/// Strict ASCII integer parse matching `string2ll`'s contract (no leading
/// '+', no leading zeros other than "0" itself, optional leading '-').
fn strict_parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (neg, digits) = if s[0] == b'-' { (true, &s[1..]) } else { (false, s) };
    if digits.is_empty() || !(b'1'..=b'9').contains(&digits[0]) || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let magnitude: i64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some(if neg { -magnitude } else { magnitude })
}

/// Index of the first `\r\n` in `buf[from..]`, if any (position of the `\r`).
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..].windows(2).position(|w| w == b"\r\n").map(|p| p + from)
}

fn parse_inline(buf: &[u8]) -> ParseOutcome {
    let newline = match buf.iter().position(|&b| b == b'\n') {
        Some(p) => p,
        None => {
            return if buf.len() > INLINE_MAX_SIZE {
                ParseOutcome::Protocol("too big inline request")
            } else {
                ParseOutcome::NeedMore
            };
        }
    };
    let line_end = if newline > 0 && buf[newline - 1] == b'\r' { newline - 1 } else { newline };
    let line = String::from_utf8_lossy(&buf[..line_end]);
    match redisx_core::sds::split_args(&line) {
        None => ParseOutcome::Protocol("unbalanced quotes in request"),
        Some(tokens) => {
            ParseOutcome::Command { argv: tokens.iter().map(|s| s.as_bytes().to_vec()).collect(), consumed: newline + 1 }
        }
    }
}

fn parse_multibulk(buf: &[u8]) -> ParseOutcome {
    debug_assert_eq!(buf[0], b'*');
    let first_crlf = match find_crlf(buf, 0) {
        Some(p) => p,
        None => {
            return if buf.len() > INLINE_MAX_SIZE {
                ParseOutcome::Protocol("too big mbulk count string")
            } else {
                ParseOutcome::NeedMore
            };
        }
    };
    let count = match strict_parse_i64(&buf[1..first_crlf]) {
        Some(n) if n <= i32::MAX as i64 => n,
        _ => return ParseOutcome::Protocol("invalid multibulk length"),
    };
    let mut pos = first_crlf + 2;
    if count <= 0 {
        return ParseOutcome::Command { argv: Vec::new(), consumed: pos };
    }

    let mut argv = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let line_end = match find_crlf(buf, pos) {
            Some(p) => p,
            None => {
                return if buf.len() - pos > INLINE_MAX_SIZE {
                    ParseOutcome::Protocol("too big bulk count string")
                } else {
                    ParseOutcome::NeedMore
                };
            }
        };
        if buf.get(pos) != Some(&b'$') {
            return ParseOutcome::Protocol("expected '$', got something else");
        }
        let bulk_len = match strict_parse_i64(&buf[pos + 1..line_end]) {
            Some(n) if (0..=MAX_BULK_LEN).contains(&n) => n as usize,
            _ => return ParseOutcome::Protocol("invalid bulk length"),
        };
        let data_start = line_end + 2;
        let data_end = data_start + bulk_len;
        if buf.len() < data_end + 2 {
            return ParseOutcome::NeedMore;
        }
        if &buf[data_end..data_end + 2] != b"\r\n" {
            return ParseOutcome::Protocol("bulk data not terminated by CRLF");
        }
        argv.push(buf[data_start..data_end].to_vec());
        pos = data_end + 2;
    }

    ParseOutcome::Command { argv, consumed: pos }
}

/// Attempt to parse exactly one command (inline or multibulk, auto-detected
/// by the first byte) from the front of `buf`.
pub fn parse_request(buf: &[u8]) -> ParseOutcome {
    if buf.is_empty() {
        return ParseOutcome::NeedMore;
    }
    if buf[0] == b'*' {
        parse_multibulk(buf)
    } else {
        parse_inline(buf)
    }
}

/// A reply value covering every RESP3 type (`resp_parser.h`'s
/// `ReplyParserCallbacks` enumerates the same set on the decode side).
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<Reply>),
    NullArray,
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    VerbatimString { format: [u8; 3], data: Vec<u8> },
    Map(Vec<(Reply, Reply)>),
    Set(Vec<Reply>),
    Push(Vec<Reply>),
    /// Not a RESP type of its own — a sequence of independent top-level
    /// frames encoded back-to-back with no wrapping envelope. Needed because
    /// `dispatch()` returns exactly one `Reply` per command, but
    /// `SUBSCRIBE ch1 ch2` must send *two separate* reply frames (one
    /// `subscribe` confirmation per channel), not one frame containing two
    /// channels (`pubsub.c`'s `addReplyPubsubSubscribed` is called once per
    /// channel). A single-element `Multi` degrades to exactly that one
    /// frame, so normal commands are unaffected.
    Multi(Vec<Reply>),
}

fn fmt_double(d: f64) -> String {
    if d.is_infinite() {
        if d > 0.0 { "inf".to_string() } else { "-inf".to_string() }
    } else if d.is_nan() {
        "nan".to_string()
    } else {
        // d2string's general-purpose formatting: shortest round-trippable form.
        let mut s = format!("{d}");
        if !s.contains('.') && !s.contains('e') && !s.contains("inf") && !s.contains("nan") {
            // Integral doubles still print without a decimal point in Redis (e.g. "3").
        }
        if s == "-0" {
            s = "0".to_string();
        }
        s
    }
}

impl Reply {
    pub fn ok() -> Reply {
        Reply::SimpleString("OK".to_string())
    }

    pub fn bulk(s: impl Into<Vec<u8>>) -> Reply {
        Reply::BulkString(s.into())
    }

    /// Encode this reply for a client on protocol version 2 or 3, matching
    /// the `addReply*` family's RESP2-fallback rules exactly.
    pub fn encode(&self, resp: u8, out: &mut Vec<u8>) {
        match self {
            Reply::SimpleString(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::Integer(n) => {
                out.push(b':');
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Reply::BulkString(data) => {
                out.push(b'$');
                out.extend_from_slice(data.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            Reply::NullBulkString => {
                out.extend_from_slice(if resp == 2 { b"$-1\r\n" } else { b"_\r\n" });
            }
            Reply::NullArray => {
                out.extend_from_slice(if resp == 2 { b"*-1\r\n" } else { b"_\r\n" });
            }
            Reply::Null => {
                out.extend_from_slice(if resp == 2 { b"$-1\r\n" } else { b"_\r\n" });
            }
            Reply::Array(items) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(resp, out);
                }
            }
            Reply::Boolean(b) => {
                if resp == 2 {
                    Reply::Integer(if *b { 1 } else { 0 }).encode(resp, out);
                } else {
                    out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
                }
            }
            Reply::Double(d) => {
                let s = fmt_double(*d);
                if resp == 2 {
                    Reply::BulkString(s.into_bytes()).encode(resp, out);
                } else {
                    out.push(b',');
                    out.extend_from_slice(s.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            }
            Reply::BigNumber(s) => {
                if resp == 2 {
                    Reply::BulkString(s.clone().into_bytes()).encode(resp, out);
                } else {
                    out.push(b'(');
                    out.extend_from_slice(s.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            }
            Reply::VerbatimString { format, data } => {
                if resp == 2 {
                    Reply::BulkString(data.clone()).encode(resp, out);
                } else {
                    out.push(b'=');
                    out.extend_from_slice((data.len() + 4).to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(format);
                    out.push(b':');
                    out.extend_from_slice(data);
                    out.extend_from_slice(b"\r\n");
                }
            }
            Reply::Map(pairs) => {
                if resp == 2 {
                    out.push(b'*');
                    out.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode(resp, out);
                        v.encode(resp, out);
                    }
                } else {
                    out.push(b'%');
                    out.extend_from_slice(pairs.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for (k, v) in pairs {
                        k.encode(resp, out);
                        v.encode(resp, out);
                    }
                }
            }
            Reply::Set(items) => {
                out.push(if resp == 2 { b'*' } else { b'~' });
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(resp, out);
                }
            }
            Reply::Push(items) => {
                // RESP2 has no push frame type; real Redis never sends push
                // messages to RESP2 clients at all, but if asked to, the
                // closest valid encoding is a plain array.
                out.push(if resp == 2 { b'*' } else { b'>' });
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(resp, out);
                }
            }
            Reply::Multi(frames) => {
                for frame in frames {
                    frame.encode(resp, out);
                }
            }
        }
    }

    pub fn to_bytes(&self, resp: u8) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(resp, &mut out);
        out
    }
}

/// Outcome of trying to decode one `Reply` from the front of a buffer —
/// `redisx-cli`'s (Phase 8) client-side counterpart to `parse_request`: that
/// function decodes a *request* (always a flat multibulk array of bulk
/// strings); this one decodes a *reply* (any RESP2/3 type, recursively for
/// aggregates). Same incremental "try again as more bytes arrive" shape and
/// same "re-scan from the start every call" performance tradeoff — a client
/// REPL's reply stream is nowhere near hot enough for that to matter.
#[derive(Debug, PartialEq)]
pub enum ReplyParseOutcome {
    NeedMore,
    Protocol(&'static str),
    Reply { value: Reply, consumed: usize },
}

/// One line's content (without the trailing CRLF) plus the position right
/// after that CRLF, or `None` if the buffer doesn't yet hold a full line.
fn take_line(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let end = find_crlf(buf, pos)?;
    Some((&buf[pos..end], end + 2))
}

fn parse_i64_line(buf: &[u8], pos: usize) -> Result<Option<(i64, usize)>, &'static str> {
    let Some((line, next)) = take_line(buf, pos) else { return Ok(None) };
    let n = std::str::from_utf8(line).ok().and_then(|s| s.parse().ok()).ok_or("invalid integer reply")?;
    Ok(Some((n, next)))
}

/// Parses exactly one reply (recursively, for aggregate types) starting at
/// `pos`. `Ok(None)` means the buffer doesn't yet hold a complete reply.
fn parse_one(buf: &[u8], pos: usize) -> Result<Option<(Reply, usize)>, &'static str> {
    let Some(&kind) = buf.get(pos) else { return Ok(None) };
    match kind {
        b'+' => {
            let Some((line, next)) = take_line(buf, pos + 1) else { return Ok(None) };
            Ok(Some((Reply::SimpleString(String::from_utf8_lossy(line).into_owned()), next)))
        }
        b'-' => {
            let Some((line, next)) = take_line(buf, pos + 1) else { return Ok(None) };
            Ok(Some((Reply::Error(String::from_utf8_lossy(line).into_owned()), next)))
        }
        b':' => match parse_i64_line(buf, pos + 1)? {
            Some((n, next)) => Ok(Some((Reply::Integer(n), next))),
            None => Ok(None),
        },
        b'_' => match take_line(buf, pos + 1) {
            Some((_, next)) => Ok(Some((Reply::Null, next))),
            None => Ok(None),
        },
        b'#' => {
            let Some(&flag) = buf.get(pos + 1) else { return Ok(None) };
            let Some((_, next)) = take_line(buf, pos + 2) else { return Ok(None) };
            match flag {
                b't' => Ok(Some((Reply::Boolean(true), next))),
                b'f' => Ok(Some((Reply::Boolean(false), next))),
                _ => Err("invalid boolean reply"),
            }
        }
        b',' => {
            let Some((line, next)) = take_line(buf, pos + 1) else { return Ok(None) };
            let s = std::str::from_utf8(line).map_err(|_| "invalid double reply")?;
            let d = match s {
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                "nan" => f64::NAN,
                _ => s.parse().map_err(|_| "invalid double reply")?,
            };
            Ok(Some((Reply::Double(d), next)))
        }
        b'(' => {
            let Some((line, next)) = take_line(buf, pos + 1) else { return Ok(None) };
            Ok(Some((Reply::BigNumber(String::from_utf8_lossy(line).into_owned()), next)))
        }
        b'$' | b'=' => {
            let Some((len, after_len)) = parse_i64_line(buf, pos + 1)? else { return Ok(None) };
            if len == -1 {
                return Ok(Some((Reply::NullBulkString, after_len)));
            }
            if len < 0 {
                return Err("invalid bulk length");
            }
            let data_end = after_len + len as usize;
            if buf.len() < data_end + 2 {
                return Ok(None);
            }
            if &buf[data_end..data_end + 2] != b"\r\n" {
                return Err("bulk data not terminated by CRLF");
            }
            let data = buf[after_len..data_end].to_vec();
            let next = data_end + 2;
            if kind == b'=' {
                // `<3-byte-format>:<payload>` per RESP3's verbatim string.
                if data.len() < 4 || data[3] != b':' {
                    return Err("invalid verbatim string");
                }
                Ok(Some((Reply::VerbatimString { format: [data[0], data[1], data[2]], data: data[4..].to_vec() }, next)))
            } else {
                Ok(Some((Reply::BulkString(data), next)))
            }
        }
        b'*' | b'%' | b'~' | b'>' => {
            let Some((count, after_count)) = parse_i64_line(buf, pos + 1)? else { return Ok(None) };
            if count == -1 && kind == b'*' {
                return Ok(Some((Reply::NullArray, after_count)));
            }
            if count < 0 {
                return Err("invalid aggregate count");
            }
            let n_items = if kind == b'%' { count as usize * 2 } else { count as usize };
            let mut items = Vec::with_capacity(n_items);
            let mut cur = after_count;
            for _ in 0..n_items {
                match parse_one(buf, cur)? {
                    Some((item, next)) => {
                        items.push(item);
                        cur = next;
                    }
                    None => return Ok(None),
                }
            }
            let reply = match kind {
                b'*' => Reply::Array(items),
                b'~' => Reply::Set(items),
                b'>' => Reply::Push(items),
                b'%' => {
                    let mut pairs = Vec::with_capacity(count as usize);
                    let mut it = items.into_iter();
                    while let (Some(k), Some(v)) = (it.next(), it.next()) {
                        pairs.push((k, v));
                    }
                    Reply::Map(pairs)
                }
                _ => unreachable!(),
            };
            Ok(Some((reply, cur)))
        }
        _ => Err("unknown reply type byte"),
    }
}

/// Attempt to decode exactly one reply from the front of `buf`.
pub fn parse_reply(buf: &[u8]) -> ReplyParseOutcome {
    match parse_one(buf, 0) {
        Ok(Some((value, consumed))) => ReplyParseOutcome::Reply { value, consumed },
        Ok(None) => ReplyParseOutcome::NeedMore,
        Err(e) => ReplyParseOutcome::Protocol(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multibulk_ping() {
        let buf = b"*1\r\n$4\r\nPING\r\n";
        match parse_request(buf) {
            ParseOutcome::Command { argv, consumed } => {
                assert_eq!(argv, vec![b"PING".to_vec()]);
                assert_eq!(consumed, buf.len());
            }
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[test]
    fn parses_multibulk_set_with_binary_arg() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nbar\x00z\r\n";
        match parse_request(buf) {
            ParseOutcome::Command { argv, consumed } => {
                assert_eq!(argv, vec![b"SET".to_vec(), b"foo".to_vec(), b"bar\x00z".to_vec()]);
                assert_eq!(consumed, buf.len());
            }
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[test]
    fn parses_inline_command() {
        let buf = b"PING\r\n";
        match parse_request(buf) {
            ParseOutcome::Command { argv, consumed } => {
                assert_eq!(argv, vec![b"PING".to_vec()]);
                assert_eq!(consumed, buf.len());
            }
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[test]
    fn inline_command_supports_quoted_args() {
        let buf = b"SET foo \"bar baz\"\r\n";
        match parse_request(buf) {
            ParseOutcome::Command { argv, .. } => {
                assert_eq!(argv, vec![b"SET".to_vec(), b"foo".to_vec(), b"bar baz".to_vec()]);
            }
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[test]
    fn needs_more_on_partial_multibulk() {
        assert_eq!(parse_request(b"*2\r\n$3\r\nSET\r\n$3\r\nfo"), ParseOutcome::NeedMore);
        assert_eq!(parse_request(b"*1\r\n$4\r\nPI"), ParseOutcome::NeedMore);
        assert_eq!(parse_request(b"*1\r\n"), ParseOutcome::NeedMore);
        assert_eq!(parse_request(b"*"), ParseOutcome::NeedMore);
    }

    #[test]
    fn needs_more_on_partial_inline() {
        assert_eq!(parse_request(b"PIN"), ParseOutcome::NeedMore);
    }

    #[test]
    fn rejects_invalid_multibulk_length() {
        assert!(matches!(parse_request(b"*abc\r\n"), ParseOutcome::Protocol(_)));
        assert!(matches!(parse_request(b"*99999999999999999999\r\n"), ParseOutcome::Protocol(_)));
    }

    #[test]
    fn rejects_missing_dollar_sign() {
        assert!(matches!(parse_request(b"*1\r\nXXXX\r\n"), ParseOutcome::Protocol(_)));
    }

    #[test]
    fn rejects_unbalanced_inline_quotes() {
        assert!(matches!(parse_request(b"SET foo \"unterminated\r\n"), ParseOutcome::Protocol(_)));
    }

    #[test]
    fn empty_multibulk_is_a_discardable_empty_command() {
        assert_eq!(parse_request(b"*0\r\n"), ParseOutcome::Command { argv: vec![], consumed: 4 });
    }

    #[test]
    fn parses_second_command_after_consuming_the_first() {
        let buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPONG\r\n";
        let ParseOutcome::Command { argv, consumed } = parse_request(buf) else { panic!("expected Command") };
        assert_eq!(argv, vec![b"PING".to_vec()]);
        let rest = &buf[consumed..];
        let ParseOutcome::Command { argv, .. } = parse_request(rest) else { panic!("expected Command") };
        assert_eq!(argv, vec![b"PONG".to_vec()]);
    }

    #[test]
    fn encodes_simple_types_identically_on_resp2_and_resp3() {
        assert_eq!(Reply::ok().to_bytes(2), b"+OK\r\n");
        assert_eq!(Reply::ok().to_bytes(3), b"+OK\r\n");
        assert_eq!(Reply::Error("ERR bad".into()).to_bytes(2), b"-ERR bad\r\n");
        assert_eq!(Reply::Integer(42).to_bytes(2), b":42\r\n");
        assert_eq!(Reply::bulk("hi").to_bytes(2), b"$2\r\nhi\r\n");
    }

    #[test]
    fn null_differs_between_resp2_and_resp3() {
        assert_eq!(Reply::Null.to_bytes(2), b"$-1\r\n");
        assert_eq!(Reply::Null.to_bytes(3), b"_\r\n");
        assert_eq!(Reply::NullArray.to_bytes(2), b"*-1\r\n");
        assert_eq!(Reply::NullArray.to_bytes(3), b"_\r\n");
    }

    #[test]
    fn boolean_differs_between_resp2_and_resp3() {
        assert_eq!(Reply::Boolean(true).to_bytes(2), b":1\r\n");
        assert_eq!(Reply::Boolean(false).to_bytes(2), b":0\r\n");
        assert_eq!(Reply::Boolean(true).to_bytes(3), b"#t\r\n");
        assert_eq!(Reply::Boolean(false).to_bytes(3), b"#f\r\n");
    }

    #[test]
    fn double_differs_between_resp2_and_resp3() {
        assert_eq!(Reply::Double(3.0).to_bytes(3), b",3\r\n");
        assert_eq!(Reply::Double(3.0).to_bytes(2), b"$1\r\n3\r\n");
        assert_eq!(Reply::Double(f64::INFINITY).to_bytes(3), b",inf\r\n");
    }

    #[test]
    fn map_flattens_to_array_on_resp2() {
        let map = Reply::Map(vec![(Reply::bulk("a"), Reply::Integer(1)), (Reply::bulk("b"), Reply::Integer(2))]);
        assert_eq!(map.to_bytes(2), b"*4\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n");
        assert_eq!(map.to_bytes(3), b"%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n");
    }

    #[test]
    fn set_differs_between_resp2_and_resp3() {
        let set = Reply::Set(vec![Reply::Integer(1), Reply::Integer(2)]);
        assert_eq!(set.to_bytes(2), b"*2\r\n:1\r\n:2\r\n");
        assert_eq!(set.to_bytes(3), b"~2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn verbatim_string_matches_networking_c_format() {
        let v = Reply::VerbatimString { format: *b"txt", data: b"Some string".to_vec() };
        assert_eq!(v.to_bytes(2), b"$11\r\nSome string\r\n");
        assert_eq!(v.to_bytes(3), b"=15\r\ntxt:Some string\r\n");
    }

    #[test]
    fn big_number_differs_between_resp2_and_resp3() {
        let digits = "1234567999999999999999999999999999";
        let n = Reply::BigNumber(digits.to_string());
        assert_eq!(n.to_bytes(2), format!("${}\r\n{digits}\r\n", digits.len()).into_bytes());
        assert_eq!(n.to_bytes(3), format!("({digits}\r\n").into_bytes());
    }

    #[test]
    fn multi_encodes_as_several_concatenated_top_level_frames() {
        let v = Reply::Multi(vec![Reply::Integer(1), Reply::ok(), Reply::Integer(2)]);
        assert_eq!(v.to_bytes(2), b":1\r\n+OK\r\n:2\r\n");
        let single = Reply::Multi(vec![Reply::Integer(7)]);
        assert_eq!(single.to_bytes(2), Reply::Integer(7).to_bytes(2));
    }

    #[test]
    fn nested_array_encodes_recursively() {
        let v = Reply::Array(vec![Reply::Integer(1), Reply::Array(vec![Reply::bulk("x")])]);
        assert_eq!(v.to_bytes(2), b"*2\r\n:1\r\n*1\r\n$1\r\nx\r\n");
    }

    fn assert_round_trips(reply: Reply, resp: u8) {
        let bytes = reply.to_bytes(resp);
        match parse_reply(&bytes) {
            ReplyParseOutcome::Reply { value, consumed } => {
                assert_eq!(consumed, bytes.len(), "didn't consume the whole encoding for {reply:?}");
                assert_eq!(value, reply);
            }
            other => panic!("expected a full reply for {reply:?}, got {other:?}"),
        }
    }

    #[test]
    fn parse_reply_round_trips_every_scalar_type_on_both_resp_versions() {
        for resp in [2, 3] {
            assert_round_trips(Reply::ok(), resp);
            assert_round_trips(Reply::Error("ERR bad".into()), resp);
            assert_round_trips(Reply::Integer(-42), resp);
            assert_round_trips(Reply::bulk("hello"), resp);
            assert_round_trips(Reply::bulk(""), resp);
        }
        // Null/Boolean/Double/BigNumber/VerbatimString each degrade to a
        // RESP2-safe encoding that round-trips to a *different* (but
        // equally valid) Reply value on RESP2, so those are only checked at
        // RESP3 where the wire form is unambiguous.
        assert_round_trips(Reply::Null, 3);
        assert_round_trips(Reply::Boolean(true), 3);
        assert_round_trips(Reply::Boolean(false), 3);
        assert_round_trips(Reply::Double(3.5), 3);
        assert_round_trips(Reply::Double(f64::INFINITY), 3);
        assert_round_trips(Reply::BigNumber("123456789012345678901234567890".into()), 3);
        assert_round_trips(Reply::VerbatimString { format: *b"txt", data: b"hi there".to_vec() }, 3);
    }

    #[test]
    fn parse_reply_round_trips_aggregates() {
        for resp in [2, 3] {
            assert_round_trips(Reply::Array(vec![Reply::Integer(1), Reply::bulk("x"), Reply::Array(vec![Reply::bulk("y")])]), resp);
            assert_round_trips(Reply::Array(vec![]), resp);
        }
        assert_round_trips(Reply::Set(vec![Reply::Integer(1), Reply::Integer(2)]), 3);
        assert_round_trips(Reply::Push(vec![Reply::bulk("message"), Reply::bulk("ch"), Reply::bulk("payload")]), 3);
        assert_round_trips(Reply::Map(vec![(Reply::bulk("a"), Reply::Integer(1)), (Reply::bulk("b"), Reply::Integer(2))]), 3);
    }

    #[test]
    fn parse_reply_null_bulk_and_null_array_both_decode_to_null_on_resp2() {
        assert_eq!(parse_reply(b"$-1\r\n"), ReplyParseOutcome::Reply { value: Reply::NullBulkString, consumed: 5 });
        assert_eq!(parse_reply(b"*-1\r\n"), ReplyParseOutcome::Reply { value: Reply::NullArray, consumed: 5 });
    }

    #[test]
    fn parse_reply_needs_more_on_partial_input() {
        assert_eq!(parse_reply(b""), ReplyParseOutcome::NeedMore);
        assert_eq!(parse_reply(b"$5\r\nhel"), ReplyParseOutcome::NeedMore);
        assert_eq!(parse_reply(b"*2\r\n:1\r\n"), ReplyParseOutcome::NeedMore);
        assert_eq!(parse_reply(b"+OK"), ReplyParseOutcome::NeedMore);
    }

    #[test]
    fn parse_reply_rejects_malformed_input() {
        assert!(matches!(parse_reply(b"X garbage\r\n"), ReplyParseOutcome::Protocol(_)));
        assert!(matches!(parse_reply(b"$3\r\nabXY\r\n"), ReplyParseOutcome::Protocol(_)));
    }

    #[test]
    fn parse_reply_leaves_the_remainder_for_the_next_call() {
        let buf = b"+OK\r\n:42\r\n";
        let ReplyParseOutcome::Reply { value, consumed } = parse_reply(buf) else { panic!("expected a reply") };
        assert_eq!(value, Reply::ok());
        let ReplyParseOutcome::Reply { value, .. } = parse_reply(&buf[consumed..]) else { panic!("expected a reply") };
        assert_eq!(value, Reply::Integer(42));
    }
}
