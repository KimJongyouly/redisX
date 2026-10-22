//! AOF — ported from this fork's already-simplified `redis_src/src/aof.c`
//! (`CHANGE.md` §2 in the original C fork): not a recovery mechanism at all
//! anymore, just a human-readable, `redis-cli MONITOR`-style write-only log,
//! one line per write command:
//!
//! ```text
//! <unix-ts>.<usec> [<dbid> <peer>] "ARG1" "ARG2" ...\r\n
//! ```
//!
//! Ported: the line format itself (`catAppendOnlyGenericCommand`), using
//! Phase 0's `Sds::cat_repr` for the exact same quoting/escaping the C
//! version uses (`sdscatrepr`) — so a line this module writes is
//! byte-for-byte what the C fork would have written for the same command.
//!
//! Not ported, by the same scope decision the C fork already made: AOF
//! *loading* at startup, `BGREWRITEAOF`, and the multi-part manifest/BASE+INCR
//! file scheme — none of that applies once recovery itself is out of scope.
//! A single flat file (`dir`/`appendfilename`) is appended to directly.
//! Also not reproduced: the C version's `[dbid lua]`/`[dbid unix:path]`
//! client-flag cases — this fork has no Lua/Python command path or Unix
//! socket listener yet (Phase 6 / not yet built), so every entry uses either
//! a real `ip:port` peer or `internal` (e.g. commands applied from a
//! replication link — see `server.rs`'s `ClientState::peer`).

use crate::config::ConfigStore;
use redisx_core::Sds;
use std::io::Write;
use std::path::PathBuf;

/// `dir`/`appendfilename` joined, the same convention `rdb_path` uses for
/// `dir`/`dbfilename`.
pub fn aof_path(config: &ConfigStore) -> PathBuf {
    let dir = config.get_str("dir").unwrap_or(".");
    let filename = config.get_str("appendfilename").unwrap_or("appendonly.aof");
    std::path::Path::new(dir).join(filename)
}

pub fn is_enabled(config: &ConfigStore) -> bool {
    config.get_str("appendonly") == Some("yes")
}

/// `catAppendOnlyGenericCommand` — one log line, including the trailing
/// `\r\n` (matching the C version's `sdscatlen(dst,"\r\n",2)`).
pub fn format_entry(dbid: usize, peer: &str, argv: &[Vec<u8>]) -> Vec<u8> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let mut out = Sds::new();
    out.cat_fmt(format_args!("{}.{:06} [{dbid} {peer}] ", now.as_secs(), now.subsec_micros()));
    for (i, arg) in argv.iter().enumerate() {
        if i != 0 {
            out.cat(b" ");
        }
        out.cat_repr(arg);
    }
    out.cat(b"\r\n");
    out.as_bytes().to_vec()
}

/// Appends one formatted entry, creating the file if needed. Opens and
/// closes the file on every call rather than keeping a long-lived handle —
/// simple and correct; a persistent fd is a performance optimization left
/// for later, the same call already made for `SAVE`'s synchronous (no
/// `BGSAVE`) behavior in `rdb.rs`.
pub fn append(config: &ConfigStore, dbid: usize, peer: &str, argv: &[Vec<u8>]) -> std::io::Result<()> {
    let entry = format_entry(dbid, peer, argv);
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(aof_path(config))?;
    file.write_all(&entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn format_matches_monitor_style_layout() {
        let line = format_entry(0, "127.0.0.1:54206", &argv(&["set", "foo", "bar"]));
        let text = String::from_utf8(line).unwrap();
        // "<ts>.<usec> [0 127.0.0.1:54206] \"set\" \"foo\" \"bar\"\r\n"
        assert!(text.ends_with("\"set\" \"foo\" \"bar\"\r\n"), "got: {text:?}");
        assert!(text.contains("[0 127.0.0.1:54206] "), "got: {text:?}");
        let ts_part = text.split(' ').next().unwrap();
        assert!(ts_part.parse::<f64>().is_ok(), "timestamp not numeric: {ts_part:?}");
    }

    #[test]
    fn binary_unsafe_args_are_repr_escaped() {
        let mut argv2 = argv(&["set"]);
        argv2.push(b"line\nwith\tcontrol\x01chars".to_vec());
        let line = format_entry(2, "internal", &argv2);
        let text = String::from_utf8(line).unwrap();
        assert!(text.contains("\"line\\nwith\\tcontrol\\x01chars\""), "got: {text:?}");
    }

    #[test]
    fn append_creates_file_and_accumulates_lines() {
        let dir = std::env::temp_dir().join(format!("redisx_aof_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = ConfigStore::new();
        cfg.set("dir", dir.to_str().unwrap());
        cfg.set("appendfilename", "test.aof");

        append(&cfg, 0, "127.0.0.1:1", &argv(&["set", "a", "1"])).unwrap();
        append(&cfg, 0, "127.0.0.1:1", &argv(&["set", "b", "2"])).unwrap();

        let contents = std::fs::read_to_string(aof_path(&cfg)).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with("\"set\" \"a\" \"1\""));
        assert!(lines[1].ends_with("\"set\" \"b\" \"2\""));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn is_enabled_reflects_config() {
        let mut cfg = ConfigStore::new();
        assert!(!is_enabled(&cfg));
        cfg.set("appendonly", "yes");
        assert!(is_enabled(&cfg));
    }
}
