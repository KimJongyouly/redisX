//! SLOWLOG — ported from this fork's already-customized `redis_src/src/slowlog.c`
//! (`CHANGE.md` §1 in the original C fork): no in-memory ring buffer at all
//! any more. A slow command is appended as one line to the file configured
//! via `slowlog-file-path` (empty = disabled, matching the config default),
//! and `SLOWLOG GET`/`LEN`/`RESET` work by reading that file back — the same
//! "treat the file as the only source of truth" design `aof.rs` already
//! established for the write-only AOF log.
//!
//! ## Line format (must match the C fork byte-for-byte, parser included)
//!
//! ```text
//! [수행된 yyyy-mm-dd hh:mm:ss] <client-ip-without-port> "cmd" "arg" ... <duration>us
//! ```
//!
//! `사행된` ("executed at") is a literal fixed label baked into both the C
//! writer and its paired parser (`SLOWLOG_LINE_LABEL`) — not a translatable
//! string, just a constant the two sides must agree on, so it's reproduced
//! verbatim here. Args are rendered the same binary-safe-escaped way MONITOR
//! and this fork's AOF already do (`Sds::cat_repr`, i.e. `sdscatrepr`) — the
//! C version's extra `OBJ_ENCODING_INT` fast path (`"%ld"` instead of
//! `sdscatrepr`) produces byte-identical output for plain-digit args anyway,
//! so it's not reproduced as a separate code path (`aof.rs` already made and
//! validated this same simplification for its own, differently-formatted,
//! log lines).
//!
//! ## One intentional deviation from the C fork: UTC instead of local time
//!
//! The C version's `strftime`/`localtime_r`/`mktime` render and parse the
//! timestamp in the **system's local timezone**, with no timezone marker in
//! the file. Reproducing that exactly would need a TZ-database lookup this
//! fork has no dependency for (and deliberately doesn't want one just for
//! this). Instead, every timestamp here is rendered and parsed as UTC. This
//! only changes what a human reading the file *sees* in the date/time
//! columns when the host's local timezone isn't UTC — `SLOWLOG GET`'s `time`
//! field (the actual Unix-epoch integer returned to clients) is unaffected,
//! since write and parse use the same convention and round-trip exactly.
//! Flagged here rather than left implicit, same spirit as every other
//! documented simplification in this port.

use crate::config::ConfigStore;
use redisx_core::Sds;
use redisx_net::resp::Reply;

const LINE_LABEL: &str = "수행된 ";

pub struct SlowlogEntry {
    /// 1-based absolute line number in the file.
    pub id: i64,
    /// Unix epoch seconds.
    pub time: i64,
    pub duration_us: i64,
    pub argv: Vec<Vec<u8>>,
    pub peer: String,
}

impl SlowlogEntry {
    /// `SLOWLOG GET`'s per-entry 6-element array: id, timestamp, duration,
    /// argv, peer `ip:port`, client name (always empty — never recorded in
    /// the file, matching the C fork's documented decision).
    pub fn to_reply(&self) -> Reply {
        Reply::Array(vec![
            Reply::Integer(self.id),
            Reply::Integer(self.time),
            Reply::Integer(self.duration_us),
            Reply::Array(self.argv.iter().map(|a| Reply::bulk(a.clone())).collect()),
            Reply::bulk(self.peer.clone().into_bytes()),
            Reply::bulk(Vec::new()),
        ])
    }
}

// ---------------------------------------------------- civil <-> Unix time ---
// Howard Hinnant's `days_from_civil`/`civil_from_days` (public-domain,
// proleptic-Gregorian, valid for the full `i64` range) — a small
// self-contained substitute for a `chrono`-style dependency, since all this
// needs is UTC <-> (Y,M,D,h,m,s) conversion for one log line format.

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn unix_from_civil(y: i64, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    days_from_civil(y, mo, d) * 86400 + h as i64 * 3600 + mi as i64 * 60 + s as i64
}

fn civil_from_unix(ts: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = ts.div_euclid(86400);
    let secs_of_day = ts.rem_euclid(86400);
    let (y, mo, d) = civil_from_days(days);
    (y, mo, d, (secs_of_day / 3600) as u32, (secs_of_day % 3600 / 60) as u32, (secs_of_day % 60) as u32)
}

fn parse_timestamp(s: &str) -> Option<(i64, u32, u32, u32, u32, u32)> {
    let (date, time) = s.split_once(' ')?;
    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let mo: u32 = dp.next()?.parse().ok()?;
    let d: u32 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let mut tp = time.split(':');
    let h: u32 = tp.next()?.parse().ok()?;
    let mi: u32 = tp.next()?.parse().ok()?;
    let se: u32 = tp.next()?.parse().ok()?;
    if tp.next().is_some() {
        return None;
    }
    Some((y, mo, d, h, mi, se))
}

// --------------------------------------------------------------- writing ---

fn file_path(config: &ConfigStore) -> Option<String> {
    match config.get_str("slowlog-file-path") {
        Some(p) if !p.is_empty() => Some(p.to_string()),
        _ => None,
    }
}

/// `getClientPeerId`'s IP (no port) extraction, IPv6-bracket-aware like the
/// C version even though this fork currently only ever hands it plain IPv4
/// `ip:port` peers.
fn strip_port(peer: &str) -> String {
    if let Some(rest) = peer.strip_prefix('[') {
        return match rest.find(']') {
            Some(end) => rest[..end].to_string(),
            None => peer.to_string(),
        };
    }
    match peer.rfind(':') {
        Some(idx) => peer[..idx].to_string(),
        None => peer.to_string(),
    }
}

/// `slowlogPushEntryIfNeeded` — writes one line if `slowlog-file-path` is
/// configured. The slower-than threshold check (`slowlog-log-slower-than`)
/// is the caller's responsibility (`server.rs`'s `maybe_log_slow`), matching
/// how the C version splits the two concerns across two functions too.
pub fn write_entry(config: &ConfigStore, peer: &str, argv: &[Vec<u8>], duration_us: i64) {
    let Some(path) = file_path(config) else { return };

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (y, mo, d, h, mi, s) = civil_from_unix(now);

    let mut out = Sds::new();
    out.cat_fmt(format_args!("[{LINE_LABEL}{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}] {} ", strip_port(peer)));
    for (i, arg) in argv.iter().enumerate() {
        if i != 0 {
            out.cat(b" ");
        }
        out.cat_repr(arg);
    }
    out.cat_fmt(format_args!(" {duration_us}us\n"));

    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        use std::io::Write;
        let _ = file.write_all(out.as_bytes());
    }
}

// --------------------------------------------------------------- reading ---

fn read_lines(config: &ConfigStore) -> Vec<String> {
    let Some(path) = file_path(config) else { return Vec::new() };
    match std::fs::read_to_string(path) {
        Ok(contents) => contents.lines().map(str::to_string).collect(),
        Err(_) => Vec::new(),
    }
}

/// `slowlogParseLine` — parses one line back into its components; `None` for
/// a line that doesn't match the expected format (skipped silently, matching
/// the C version's "a damaged line is just dropped from the results" policy).
fn parse_line(line: &str, id: i64) -> Option<SlowlogEntry> {
    if !line.starts_with('[') {
        return None;
    }
    let close = line.find(']')?;
    let inside = &line[1..close];
    let ts_str = inside.strip_prefix(LINE_LABEL)?;
    let (y, mo, d, h, mi, s) = parse_timestamp(ts_str)?;
    let time = unix_from_civil(y, mo, d, h, mi, s);

    let rest = line[close + 1..].trim_start();
    let sp = rest.find(' ')?;
    let peer = rest[..sp].to_string();

    let tokens = redisx_core::sds::split_args(&rest[sp + 1..])?;
    if tokens.is_empty() {
        return None;
    }
    let (last, argv_toks) = tokens.split_last().unwrap();
    let dur_str = last.as_str_lossy();
    let duration_us: i64 = dur_str.strip_suffix("us")?.parse().ok()?;
    let argv = argv_toks.iter().map(|t| t.as_bytes().to_vec()).collect();

    Some(SlowlogEntry { id, time, duration_us, argv, peer })
}

pub fn len(config: &ConfigStore) -> usize {
    read_lines(config).len()
}

pub fn reset(config: &ConfigStore) {
    if let Some(path) = file_path(config) {
        let _ = std::fs::File::create(path);
    }
}

/// `SLOWLOG GET [count]` — tail `count` entries (default 10, `-1` = all),
/// most-recent first, matching the C version's iteration order.
pub fn get(config: &ConfigStore, count: i64) -> Vec<SlowlogEntry> {
    let lines = read_lines(config);
    let numlines = lines.len() as i64;
    let count = if count == -1 || count > numlines { numlines } else { count };
    let mut out = Vec::new();
    let mut i = numlines - 1;
    while i >= 0 && i >= numlines - count {
        if let Some(e) = parse_line(&lines[i as usize], i + 1) {
            out.push(e);
        }
        i -= 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn test_config(dir: &std::path::Path) -> ConfigStore {
        let mut cfg = ConfigStore::new();
        cfg.set("slowlog-file-path", dir.join("slow.log").to_str().unwrap());
        cfg
    }

    #[test]
    fn civil_unix_round_trips() {
        for ts in [0i64, 1_700_000_000, 1_000_000_000, 1_900_000_000] {
            let (y, mo, d, h, mi, s) = civil_from_unix(ts);
            assert_eq!(unix_from_civil(y, mo, d, h, mi, s), ts, "round trip failed for {ts}");
        }
    }

    #[test]
    fn disabled_when_no_file_path_configured() {
        let cfg = ConfigStore::new();
        assert_eq!(len(&cfg), 0);
        assert!(get(&cfg, 10).is_empty());
        write_entry(&cfg, "127.0.0.1:1", &argv(&["set", "a", "b"]), 999_999);
        assert_eq!(len(&cfg), 0);
    }

    #[test]
    fn write_then_get_round_trips_fields() {
        let dir = std::env::temp_dir().join(format!("redisx_slowlog_test_{}_{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = test_config(&dir);

        write_entry(&cfg, "127.0.0.1:54321", &argv(&["set", "foo", "bar baz"]), 1234);
        assert_eq!(len(&cfg), 1);

        let entries = get(&cfg, 10);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.id, 1);
        assert_eq!(e.duration_us, 1234);
        assert_eq!(e.peer, "127.0.0.1"); // port stripped
        assert_eq!(e.argv, argv(&["set", "foo", "bar baz"]));
        assert!(e.time > 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn get_returns_most_recent_first_and_respects_count() {
        let dir = std::env::temp_dir().join(format!("redisx_slowlog_test_{}_{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = test_config(&dir);

        for i in 0..5 {
            write_entry(&cfg, "10.0.0.1:1", &argv(&["set", &format!("k{i}"), "v"]), 100);
        }
        assert_eq!(len(&cfg), 5);

        let all = get(&cfg, -1);
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].argv[1], b"k4"); // most recent first
        assert_eq!(all[4].argv[1], b"k0");

        let top2 = get(&cfg, 2);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].argv[1], b"k4");
        assert_eq!(top2[1].argv[1], b"k3");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reset_truncates_file() {
        let dir = std::env::temp_dir().join(format!("redisx_slowlog_test_{}_{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = test_config(&dir);

        write_entry(&cfg, "127.0.0.1:1", &argv(&["set", "a", "b"]), 1);
        assert_eq!(len(&cfg), 1);
        reset(&cfg);
        assert_eq!(len(&cfg), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn binary_unsafe_args_round_trip_through_repr_escaping() {
        let dir = std::env::temp_dir().join(format!("redisx_slowlog_test_{}_{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = test_config(&dir);

        let mut a = argv(&["set"]);
        a.push(b"line\nwith\tcontrol\x01chars".to_vec());
        write_entry(&cfg, "127.0.0.1:1", &a, 42);

        let entries = get(&cfg, 10);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].argv, a);

        std::fs::remove_dir_all(&dir).ok();
    }
}
