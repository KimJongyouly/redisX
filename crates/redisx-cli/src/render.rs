//! Reply rendering — ported from the relevant slice of `redis-cli.c`'s
//! `cliFormatReplyTTY`/`cliOutputCommandHelp`-adjacent raw-output path
//! (`cliFormatReplyRaw`). Two independent renderers, chosen once per
//! invocation by `main.rs` (real redis-cli decides the same way: formatted
//! when stdout is a terminal, raw when it's piped/redirected, either one
//! forced by `--raw`/`--no-raw`):
//!
//! - [`render_formatted`]: human-oriented — `(integer) N`, `(nil)`,
//!   `(error) ...`, bulk/simple strings double-quoted and binary-safely
//!   escaped (`redisx_core::Sds::cat_repr`, the exact same escaping
//!   `aof.rs`/`slowlog.rs` already use for the same reason), and arrays
//!   numbered with real redis-cli's accumulating-indent nested style (see
//!   its own doc comment for why that's the one genuinely fiddly part).
//! - [`render_raw`]: script/pipe-oriented — just the data, one value per
//!   line, no annotations, no quoting.
//!
//! RESP3 `Map` is rendered as its flattened key/value sequence in both
//! modes (real redis-cli's RESP3 build instead shows `1# "key" => "value"`
//! pairs) — a documented simplification, not a fidelity goal this phase's
//! completion criterion singles out.

use redisx_core::Sds;
use redisx_net::resp::Reply;
use std::io::Write;

/// Quotes `bytes` for formatted (`--no-raw`-style) display. For genuinely
/// non-UTF-8 binary data this falls back to `redisx_core::Sds::cat_repr`
/// (the byte-for-byte port of real Redis's `sdscatrepr`, kept untouched here
/// since `aof.rs`/`slowlog.rs` depend on its exact byte output for their
/// file formats) — but for valid UTF-8 text, unlike `cat_repr`, this passes
/// printable non-ASCII characters straight through instead of hex-escaping
/// every byte above 0x7f. `cat_repr`'s escaping matches C's locale-independent
/// `isprint()`, which only recognizes ASCII — byte-correct for a file format,
/// but it means ordinary UTF-8 text (Korean, Chinese, emoji, accented Latin,
/// ...) comes out as a wall of `\xHH` on screen even though nothing about it
/// is actually unprintable. This display path has no round-trip/compatibility
/// constraint, so it can just check `char::is_control()` instead.
fn repr(bytes: &[u8]) -> String {
    let Ok(s) = std::str::from_utf8(bytes) else {
        return byte_repr(bytes);
    };
    let mut out = String::with_capacity(bytes.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' | '"' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{07}' => out.push_str("\\a"),
            '\u{08}' => out.push_str("\\b"),
            c if c.is_ascii() => {
                let b = c as u8;
                if (0x20..0x7f).contains(&b) {
                    out.push(c);
                } else {
                    out.push_str(&format!("\\x{b:02x}"));
                }
            }
            // A non-ASCII Unicode control character (e.g. a C1 control) is
            // the only case left that's genuinely not printable — escape its
            // UTF-8 encoding byte by byte, same spirit as the ASCII branch.
            c if c.is_control() => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("\\x{b:02x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn byte_repr(bytes: &[u8]) -> String {
    let mut s = Sds::new();
    s.cat_repr(bytes);
    s.as_str_lossy().into_owned()
}

// ----------------------------------------------------------------- raw ---

pub fn render_raw(reply: &Reply, out: &mut impl Write) {
    match reply {
        Reply::SimpleString(s) => {
            let _ = writeln!(out, "{s}");
        }
        Reply::Error(e) => {
            let _ = writeln!(out, "{e}");
        }
        Reply::Integer(n) => {
            let _ = writeln!(out, "{n}");
        }
        Reply::BulkString(b) | Reply::VerbatimString { data: b, .. } => {
            let _ = out.write_all(b);
            let _ = out.write_all(b"\n");
        }
        Reply::BigNumber(s) => {
            let _ = writeln!(out, "{s}");
        }
        Reply::Null | Reply::NullBulkString | Reply::NullArray => {
            let _ = writeln!(out);
        }
        Reply::Boolean(b) => {
            let _ = writeln!(out, "{}", i64::from(*b));
        }
        Reply::Double(d) => {
            let _ = writeln!(out, "{d}");
        }
        Reply::Array(items) | Reply::Set(items) | Reply::Push(items) | Reply::Multi(items) => {
            for item in items {
                render_raw(item, out);
            }
        }
        Reply::Map(pairs) => {
            for (k, v) in pairs {
                render_raw(k, out);
                render_raw(v, out);
            }
        }
    }
}

// ----------------------------------------------------------- formatted ---

pub fn render_formatted(reply: &Reply, out: &mut impl Write) {
    match reply {
        Reply::Array(items) | Reply::Set(items) | Reply::Push(items) | Reply::Multi(items) => render_seq(items, "", out),
        Reply::Map(pairs) => render_seq(&flatten(pairs), "", out),
        other => render_scalar(other, out),
    }
}

fn flatten(pairs: &[(Reply, Reply)]) -> Vec<Reply> {
    pairs.iter().flat_map(|(k, v)| [k.clone(), v.clone()]).collect()
}

fn render_scalar(reply: &Reply, out: &mut impl Write) {
    match reply {
        Reply::SimpleString(s) => {
            let _ = writeln!(out, "{s}");
        }
        Reply::Error(e) => {
            let _ = writeln!(out, "(error) {e}");
        }
        Reply::Integer(n) => {
            let _ = writeln!(out, "(integer) {n}");
        }
        Reply::BulkString(b) | Reply::VerbatimString { data: b, .. } => {
            let _ = writeln!(out, "{}", repr(b));
        }
        Reply::BigNumber(s) => {
            let _ = writeln!(out, "{s}");
        }
        Reply::Null | Reply::NullBulkString | Reply::NullArray => {
            let _ = writeln!(out, "(nil)");
        }
        Reply::Boolean(b) => {
            let _ = writeln!(out, "{}", if *b { "(true)" } else { "(false)" });
        }
        Reply::Double(d) => {
            let _ = writeln!(out, "(double) {d}");
        }
        // Non-empty containers are handled by `render_seq`/`render_inline`
        // before ever reaching here; an empty one still needs a line.
        Reply::Array(_) | Reply::Set(_) | Reply::Push(_) | Reply::Multi(_) | Reply::Map(_) => {
            let _ = writeln!(out, "(empty array)");
        }
    }
}

/// Writes a numbered list of `items` at `indent`, one per line (or more, for
/// nested containers — see [`render_inline`]).
fn render_seq(items: &[Reply], indent: &str, out: &mut impl Write) {
    if items.is_empty() {
        let _ = writeln!(out, "(empty array)");
        return;
    }
    let width = items.len().to_string().len();
    for (i, item) in items.iter().enumerate() {
        let label = format!("{:>width$}) ", i + 1, width = width);
        let _ = write!(out, "{indent}{label}");
        let child_indent = format!("{indent}{}", " ".repeat(label.len()));
        render_inline(item, &child_indent, out);
    }
}

/// Like [`render_seq`] but its first item's own label continues on
/// whatever line the caller already started (no indent/newline before it) —
/// this is what makes `redis-cli`'s nested-array sample look like
/// `4) 1) "SLOWLOG"` / `   2) "LEN"` instead of each level starting its own
/// fresh line.
fn render_seq_first_inline(items: &[Reply], indent: &str, out: &mut impl Write) {
    let width = items.len().to_string().len();
    for (i, item) in items.iter().enumerate() {
        let label = format!("{:>width$}) ", i + 1, width = width);
        if i == 0 {
            let _ = write!(out, "{label}");
        } else {
            let _ = write!(out, "{indent}{label}");
        }
        let child_indent = format!("{indent}{}", " ".repeat(label.len()));
        render_inline(item, &child_indent, out);
    }
}

/// Renders `item` continuing on the current output line for its first line;
/// any further lines it produces (because it's itself a non-empty
/// container) use `indent`.
fn render_inline(item: &Reply, indent: &str, out: &mut impl Write) {
    match item {
        Reply::Array(items) | Reply::Set(items) | Reply::Push(items) | Reply::Multi(items) if !items.is_empty() => {
            render_seq_first_inline(items, indent, out);
        }
        Reply::Map(pairs) if !pairs.is_empty() => {
            render_seq_first_inline(&flatten(pairs), indent, out);
        }
        other => render_scalar(other, out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn formatted(reply: &Reply) -> String {
        let mut out = Vec::new();
        render_formatted(reply, &mut out);
        String::from_utf8(out).unwrap()
    }

    fn raw(reply: &Reply) -> String {
        let mut out = Vec::new();
        render_raw(reply, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn formatted_scalars() {
        assert_eq!(formatted(&Reply::ok()), "OK\n");
        assert_eq!(formatted(&Reply::Error("ERR bad".into())), "(error) ERR bad\n");
        assert_eq!(formatted(&Reply::Integer(42)), "(integer) 42\n");
        assert_eq!(formatted(&Reply::bulk("hi")), "\"hi\"\n");
        assert_eq!(formatted(&Reply::Null), "(nil)\n");
        assert_eq!(formatted(&Reply::Array(vec![])), "(empty array)\n");
    }

    #[test]
    fn formatted_binary_unsafe_bulk_string_is_repr_escaped() {
        assert_eq!(formatted(&Reply::bulk(b"line\nwith\tcontrol".to_vec())), "\"line\\nwith\\tcontrol\"\n");
    }

    #[test]
    fn formatted_valid_utf8_text_is_not_hex_escaped() {
        // Korean (and any other printable non-ASCII UTF-8) should display
        // as-is, unlike `redisx_core::Sds::cat_repr`'s file-format escaping
        // (which — matching real Redis's own locale-independent `isprint()`
        // — would turn this into a wall of `\xHH` bytes).
        assert_eq!(formatted(&Reply::bulk("테스트회사".as_bytes().to_vec())), "\"테스트회사\"\n");
        assert_eq!(formatted(&Reply::bulk("caf\u{e9} \u{1f600}".as_bytes().to_vec())), "\"caf\u{e9} \u{1f600}\"\n");
    }

    #[test]
    fn formatted_non_utf8_binary_still_falls_back_to_byte_repr() {
        // Not valid UTF-8 at all (a lone continuation byte) — must still go
        // through the fully byte-safe `cat_repr` escaping, unchanged.
        assert_eq!(formatted(&Reply::bulk(vec![0xff, 0xfe])), "\"\\xff\\xfe\"\n");
    }

    #[test]
    fn formatted_matches_real_redis_cli_nested_sample() {
        // The exact SLOWLOG GET-shaped reply this project cross-checked
        // against a real `redis-cli --no-raw` session during Phase 7.
        let reply = Reply::Array(vec![Reply::Array(vec![
            Reply::Integer(4),
            Reply::Integer(1791926834),
            Reply::Integer(38),
            Reply::Array(vec![Reply::bulk("SLOWLOG"), Reply::bulk("LEN")]),
            Reply::bulk("127.0.0.1"),
            Reply::bulk(""),
        ])]);
        let expected = "1) 1) (integer) 4\n   2) (integer) 1791926834\n   3) (integer) 38\n   4) 1) \"SLOWLOG\"\n      2) \"LEN\"\n   5) \"127.0.0.1\"\n   6) \"\"\n";
        assert_eq!(formatted(&reply), expected);
    }

    #[test]
    fn formatted_map_flattens_to_numbered_pairs() {
        let reply = Reply::Map(vec![(Reply::bulk("a"), Reply::Integer(1)), (Reply::bulk("b"), Reply::Integer(2))]);
        assert_eq!(formatted(&reply), "1) \"a\"\n2) (integer) 1\n3) \"b\"\n4) (integer) 2\n");
    }

    #[test]
    fn raw_scalars_have_no_annotations() {
        assert_eq!(raw(&Reply::ok()), "OK\n");
        assert_eq!(raw(&Reply::Integer(42)), "42\n");
        assert_eq!(raw(&Reply::bulk("hi")), "hi\n");
        assert_eq!(raw(&Reply::Null), "\n");
    }

    #[test]
    fn raw_array_is_one_value_per_line_unnumbered() {
        let reply = Reply::Array(vec![Reply::bulk("a"), Reply::bulk("b"), Reply::Array(vec![Reply::bulk("c")])]);
        assert_eq!(raw(&reply), "a\nb\nc\n");
    }
}
