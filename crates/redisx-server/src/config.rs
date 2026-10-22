//! `CONFIG GET`/`SET` — a minimal registry ported from the parts of
//! `redis_src/src/config.c` relevant to Phase 2: glob-pattern lookup and a
//! flat string-keyed store. The full `config.c` also handles typed configs
//! (bool/int/enum) with validation callbacks and apply-hooks
//! (`createStringConfig`/`createBoolConfig`/... and friends) — deferred until
//! a later phase actually needs a non-string config to be rejected/coerced
//! rather than just stored.

use std::collections::BTreeMap;

pub struct ConfigStore {
    values: BTreeMap<String, String>,
}

impl Default for ConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigStore {
    /// A handful of defaults carried over from `config.c`'s own defaults,
    /// enough to make `CONFIG GET`/`SET` demonstrably useful against a real
    /// client. `slowlog-file-path` in particular is this fork's own addition
    /// (`CHANGE.md` §1) — reproduced here as a concrete "custom config
    /// survives the port" check.
    pub fn new() -> Self {
        let mut values = BTreeMap::new();
        for (k, v) in [
            ("maxmemory", "0"),
            ("maxmemory-policy", "noeviction"),
            // `maxmemory-samples`: real Redis's `performEvictions()` samples
            // this many random keys per candidate round and picks the best
            // (oldest-LRU/least-LFU/soonest-TTL) of the batch — an
            // *approximate* eviction order, cheap at real Redis's scale.
            // `server.rs`'s `pick_eviction_candidate` instead does an exact
            // full scan across every db (same "correctness over micro-
            // optimization" call `sweep_expired` already made for active
            // expiry) — a strictly *better* eviction order (always the true
            // oldest/least-frequent/soonest-to-expire key, never a sampled
            // approximation of it), so this key is accepted for `CONFIG
            // SET`/config-file compatibility but not actually read.
            ("maxmemory-samples", "5"),
            // LFU counter tuning (`evict.c`'s `LFULogIncr`/`LFUDecrAndReturn`)
            // — `server.rs`'s `lfu_log_incr`/`lfu_decay` port the same two
            // formulas verbatim, so these two knobs have real effect here,
            // unlike `maxmemory-samples` above.
            ("lfu-log-factor", "10"),
            ("lfu-decay-time", "1"),
            // `lazyfree-lazy-*`: whether each of these five deletion paths
            // frees the old value off the calling thread (a background
            // thread draining an `mpsc::Receiver<LazyFreeJob>` — see
            // `server.rs`'s `Server::lazyfree_sender`/`lazyfree_if_enabled`)
            // instead of inline. All default to `no`, matching real Redis —
            // `UNLINK` itself is the one exception that's *always* lazy
            // regardless of these flags, same as real Redis (it's the
            // explicit "I want this async" command, these flags are for the
            // *implicit* paths). `-server-del` is wired at this fork's main
            // key-overwrite call sites (`SET`, `RENAME`'s destination,
            // `COPY`/`RESTORE ... REPLACE`) — real Redis's `dbOverwrite` is a
            // single chokepoint every overwrite goes through, which this
            // fork doesn't have, so this is "the common cases", not
            // exhaustively every overwrite in every command.
            ("lazyfree-lazy-eviction", "no"),
            ("lazyfree-lazy-expire", "no"),
            ("lazyfree-lazy-server-del", "no"),
            ("lazyfree-lazy-user-del", "no"),
            ("lazyfree-lazy-user-flush", "no"),
            // Active defrag — real Redis's `activedefrag`/`active-defrag-
            // threshold-lower`/`-upper`/`-cycle-min`/`-cycle-max` five-knob
            // jemalloc-arena-fragmentation-percentage system, collapsed to
            // one knob: `active-defrag-threshold` is compared directly
            // against `mem_fragmentation_ratio` (`server.rs`'s real,
            // OS-RSS-based ratio — no jemalloc needed for *this* fragmentation
            // signal, unlike real Redis's own). `server.rs`'s `defrag_step`/
            // `maybe_run_active_defrag` is what this actually drives.
            ("activedefrag", "no"),
            ("active-defrag-threshold", "1.5"),
            ("appendonly", "no"),
            ("slowlog-file-path", ""),
            ("slowlog-log-slower-than", "10000"),
            ("save", "3600 1 300 100 60 10000"),
            ("timeout", "0"),
            ("databases", "16"),
            // Small-encoding thresholds for Phase 3's data types (t_hash.c/t_list.c/
            // t_set.c/t_zset.c), same names and defaults as real Redis.
            ("hash-max-listpack-entries", "128"),
            ("hash-max-listpack-value", "64"),
            ("list-max-listpack-size", "128"),
            ("set-max-intset-entries", "512"),
            ("set-max-listpack-entries", "128"),
            ("set-max-listpack-value", "64"),
            ("zset-max-listpack-entries", "128"),
            ("zset-max-listpack-value", "64"),
            // Phase 4 (RDB persistence): where `SAVE` writes/startup reads from.
            ("dir", "."),
            ("dbfilename", "dump.rdb"),
            // Phase 5 (replication): the binary sets this to its actual bound
            // port at startup, so `REPLICAOF`'s handshake can tell a master
            // what port to list us under via `REPLCONF listening-port`.
            ("port", "0"),
            // Phase 4 (AOF remainder): the MONITOR-style write-only log file
            // (`aof.rs`), off by default like real Redis.
            ("appendfilename", "appendonly.aof"),
            // "남은 부분" pass: minimal ACL-lite (`AUTH`) and keyspace
            // notifications (`notify.rs`), both off by default like real Redis.
            ("requirepass", ""),
            ("notify-keyspace-events", ""),
            // Phase 17 (TLS, `tls.rs`): same names as real Redis's own
            // `tls-port`/`tls-cert-file`/`tls-key-file`, but a much smaller
            // subset — no `tls-ca-cert-file`/`tls-auth-clients`/
            // `tls-protocols`/`tls-ciphers`. `0` (like `port`'s own default)
            // means "don't bind a TLS listener at all"; the binary only does
            // so once a nonzero `tls-port` is paired with both cert and key
            // files that actually load.
            ("tls-port", "0"),
            ("tls-cert-file", ""),
            ("tls-key-file", ""),
            // `tls-replication`: whether this fork's own outbound
            // `REPLICAOF` handshake connects to the master over TLS instead
            // of plain TCP (see `tls.rs`'s module doc for the client-side
            // half — there's no `tls-ca-cert-file` here, so unlike real
            // Redis this never validates the master's certificate chain,
            // same narrowed-scope tradeoff Phase 18 already made for the
            // inbound side's lack of client-cert verification).
            ("tls-replication", "no"),
            // Phase 23 (`cluster.rs`): cluster mode is off by default, same
            // as real Redis. `cluster-port` `0` means "derive it as
            // `port + 10000`" (real Redis's own default when unset);
            // `cluster-require-full-coverage` is the only other one actually
            // read (`check_cluster_redirect`/`cluster::info_text`). Three
            // are accepted for config-file parity but have no effect yet:
            // `cluster-config-file` (this fork keeps the whole cluster
            // topology in memory only, see `cluster.rs`'s module doc),
            // `cluster-node-timeout` (the gossip cron tick's fail-detection
            // threshold is hardcoded — 3 missed ticks at a fixed 1s interval
            // — not derived from this value), and `cluster-announce-ip`
            // (every listener this binary opens, cluster bus included, is
            // hardcoded to `127.0.0.1` — a pre-existing, cluster-unrelated
            // limitation of `main()` — so there's no real external address
            // to announce yet; wiring this one up alone, without also
            // fixing the bind address, would tell peers to connect to an
            // address nothing is actually listening on).
            ("cluster-enabled", "no"),
            ("cluster-port", "0"),
            ("cluster-config-file", "nodes.conf"),
            ("cluster-node-timeout", "15000"),
            ("cluster-require-full-coverage", "yes"),
            ("cluster-announce-ip", ""),
        ] {
            values.insert(k.to_string(), v.to_string());
        }
        ConfigStore { values }
    }

    /// `CONFIG GET <pattern>` — glob-matched (case-insensitive, like real
    /// Redis config names), returned in a stable key order.
    pub fn get(&self, pattern: &str) -> Vec<(String, String)> {
        self.values.iter().filter(|(k, _)| glob_match(pattern, k)).map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// A single exact-key lookup parsed as an integer — used by the data
    /// types (Phase 3) to read their small-encoding thresholds (e.g.
    /// `hash-max-listpack-entries`) without going through the glob-matching
    /// `get()`.
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.values.get(key).and_then(|v| v.parse().ok())
    }

    /// A single exact-key lookup as a plain string (e.g. `dir`/`dbfilename`).
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// A single exact-key lookup parsed as a byte count, real Redis's
    /// `memtoull` (`config.c`) — accepts a bare integer (bytes) or one with a
    /// `b`/`k`/`kb`/`m`/`mb`/`g`/`gb` suffix (case-insensitive; `k`/`m`/`g`
    /// are the 1000-based SI units, `kb`/`mb`/`gb` the 1024-based ones, same
    /// split real Redis's own parser makes). `maxmemory`'s only reader —
    /// **not** `get_int`: `get_int("maxmemory").parse()` on a human-typed
    /// value like `"100mb"` silently fails to `None`, which a naive `unwrap_
    /// or(0)` reads as "no limit" — exactly backwards from what the person
    /// setting it meant. Found by checking what `CONFIG SET maxmemory
    /// 100mb`(already exercised by this module's own tests, just never fed
    /// through a parser before this) actually round-trips as.
    pub fn get_bytes(&self, key: &str) -> Option<u64> {
        let raw = self.values.get(key)?.trim();
        if raw.is_empty() {
            return None;
        }
        let lower = raw.to_ascii_lowercase();
        let (digits, multiplier) = if let Some(d) = lower.strip_suffix("kb") {
            (d, 1024u64)
        } else if let Some(d) = lower.strip_suffix("mb") {
            (d, 1024 * 1024)
        } else if let Some(d) = lower.strip_suffix("gb") {
            (d, 1024 * 1024 * 1024)
        } else if let Some(d) = lower.strip_suffix('k') {
            (d, 1_000)
        } else if let Some(d) = lower.strip_suffix('m') {
            (d, 1_000_000)
        } else if let Some(d) = lower.strip_suffix('g') {
            (d, 1_000_000_000)
        } else if let Some(d) = lower.strip_suffix('b') {
            (d, 1)
        } else {
            (lower.as_str(), 1)
        };
        digits.trim().parse::<u64>().ok().map(|n| n.saturating_mul(multiplier))
    }

    /// `CONFIG SET <key> <value>` — only known keys may be set (matching real
    /// Redis's "Unknown option" rejection); returns `false` on an unknown key.
    pub fn set(&mut self, key: &str, value: &str) -> bool {
        let lk = key.to_ascii_lowercase();
        if let Some(slot) = self.values.get_mut(&lk) {
            *slot = value.to_string();
            true
        } else {
            false
        }
    }

    /// Loads a `redis.conf`-style file: one `key value` pair per line,
    /// blank lines and `#`-prefixed comments ignored, applied on top of
    /// `Self::new()`'s defaults (so a config file only needs to mention the
    /// keys it actually wants to override — matching real Redis's own
    /// "anything not in the file keeps its compiled-in default" behavior).
    /// Unknown keys in the file are silently accepted (unlike `set()`'s
    /// normal "reject an unknown key" rule) rather than making the server
    /// refuse to start over a typo or a since-removed option — this fork
    /// has no interactive config reload to surface that error through
    /// anyway.
    pub fn load_conf_file(path: &std::path::Path) -> std::io::Result<ConfigStore> {
        let text = std::fs::read_to_string(path)?;
        let mut store = ConfigStore::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once(char::is_whitespace) {
                let k = k.trim().to_ascii_lowercase();
                let v = v.trim().to_string();
                store.values.insert(k, v);
            }
        }
        Ok(store)
    }

    /// Writes this store out as a `redis.conf`-style file — used to
    /// generate a starter config the first time the server runs and finds
    /// none at the expected path (`src/bin/redisx-server.rs`'s `main`).
    /// `port`'s internal `"0"` sentinel (meaning "not bound yet", set for
    /// real once the binary knows what port it's actually listening on) is
    /// never what a human should see or edit, so it's written out as the
    /// real default (`2417`) instead.
    pub fn write_conf_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        let mut out = String::new();
        out.push_str("# redisX configuration file — one `key value` pair per line.\n");
        out.push_str("# Generated automatically because no config file existed at this path yet.\n");
        out.push_str("# Edit this file to change settings, then restart the server to apply them.\n");
        out.push_str("# See redis_document/25_redisX.md and README.md in the redisX source tree\n");
        out.push_str("# for what each key does.\n\n");
        for (k, v) in &self.values {
            let v = if k == "port" && v == "0" { "2417" } else { v.as_str() };
            out.push_str(k);
            out.push(' ');
            out.push_str(v);
            out.push('\n');
        }
        std::fs::write(path, out)
    }
}

/// Minimal case-sensitive, binary-safe glob core (`stringmatchlen`'s `*`/`?`
/// subset — character classes like `[abc]` aren't supported). Shared by this
/// module's case-insensitive `CONFIG GET` matching below and `server.rs`'s
/// case-sensitive, binary-safe `PUBSUB`/`PSUBSCRIBE` channel-name matching
/// (channel names, unlike config keys, are arbitrary bytes and matched
/// case-sensitively in real Redis).
pub fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    match (pattern.first(), text.first()) {
        (None, None) => true,
        (Some(b'*'), _) => glob_match_bytes(&pattern[1..], text) || (!text.is_empty() && glob_match_bytes(pattern, &text[1..])),
        (Some(b'?'), Some(_)) => glob_match_bytes(&pattern[1..], &text[1..]),
        (Some(pc), Some(tc)) if pc == tc => glob_match_bytes(&pattern[1..], &text[1..]),
        _ => false,
    }
}

/// Case-insensitive wrapper used by `CONFIG GET`.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.to_ascii_lowercase().as_bytes(), text.to_ascii_lowercase().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_exact_match() {
        let cfg = ConfigStore::new();
        let got = cfg.get("maxmemory");
        assert_eq!(got, vec![("maxmemory".to_string(), "0".to_string())]);
    }

    #[test]
    fn get_glob_star_matches_multiple_keys() {
        let cfg = ConfigStore::new();
        let got = cfg.get("maxmemory*");
        let keys: Vec<_> = got.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["maxmemory", "maxmemory-policy", "maxmemory-samples"]);
    }

    #[test]
    fn get_is_case_insensitive() {
        let cfg = ConfigStore::new();
        assert_eq!(cfg.get("MaxMemory").len(), 1);
    }

    #[test]
    fn set_known_key_succeeds_and_persists() {
        let mut cfg = ConfigStore::new();
        assert!(cfg.set("maxmemory", "100mb"));
        assert_eq!(cfg.get("maxmemory"), vec![("maxmemory".to_string(), "100mb".to_string())]);
    }

    #[test]
    fn set_unknown_key_fails() {
        let mut cfg = ConfigStore::new();
        assert!(!cfg.set("not-a-real-config", "x"));
    }

    #[test]
    fn get_bytes_parses_plain_and_suffixed_values() {
        let mut cfg = ConfigStore::new();
        cfg.set("maxmemory", "12345");
        assert_eq!(cfg.get_bytes("maxmemory"), Some(12345));
        cfg.set("maxmemory", "100mb");
        assert_eq!(cfg.get_bytes("maxmemory"), Some(100 * 1024 * 1024));
        cfg.set("maxmemory", "1gb");
        assert_eq!(cfg.get_bytes("maxmemory"), Some(1024 * 1024 * 1024));
        cfg.set("maxmemory", "2k");
        assert_eq!(cfg.get_bytes("maxmemory"), Some(2_000));
        cfg.set("maxmemory", "3MB");
        assert_eq!(cfg.get_bytes("maxmemory"), Some(3 * 1024 * 1024), "suffix is case-insensitive");
        cfg.set("maxmemory", "0");
        assert_eq!(cfg.get_bytes("maxmemory"), Some(0));
    }

    #[test]
    fn get_bytes_rejects_garbage() {
        let mut cfg = ConfigStore::new();
        cfg.set("maxmemory", "not-a-size");
        assert_eq!(cfg.get_bytes("maxmemory"), None);
    }

    #[test]
    fn custom_slowlog_file_path_config_round_trips() {
        let mut cfg = ConfigStore::new();
        assert!(cfg.set("slowlog-file-path", "/var/log/redis/slow.log"));
        assert_eq!(cfg.get("slowlog-file-path"), vec![("slowlog-file-path".to_string(), "/var/log/redis/slow.log".to_string())]);
    }

    #[test]
    fn get_int_parses_known_threshold_defaults() {
        let cfg = ConfigStore::new();
        assert_eq!(cfg.get_int("hash-max-listpack-entries"), Some(128));
        assert_eq!(cfg.get_int("not-a-key"), None);
    }

    #[test]
    fn glob_question_mark_matches_single_char() {
        assert!(glob_match("sav?", "save"));
        assert!(!glob_match("sav?", "saveX2"));
    }

    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("redisx_config_test_{}_{name}", std::process::id()))
    }

    #[test]
    fn write_then_load_conf_file_round_trips_a_changed_value() {
        let path = scratch_path("roundtrip.conf");
        let mut cfg = ConfigStore::new();
        cfg.set("maxmemory", "104857600");
        cfg.write_conf_file(&path).unwrap();

        let loaded = ConfigStore::load_conf_file(&path).unwrap();
        assert_eq!(loaded.get_int("maxmemory"), Some(104857600));
        // Untouched keys still carry their compiled-in default.
        assert_eq!(loaded.get_str("maxmemory-policy"), Some("noeviction"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn write_conf_file_exposes_the_real_default_port_not_the_internal_sentinel() {
        let path = scratch_path("port.conf");
        ConfigStore::new().write_conf_file(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.lines().any(|l| l == "port 2417"), "expected a `port 2417` line, got:\n{text}");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_conf_file_ignores_blank_lines_and_comments() {
        let path = scratch_path("comments.conf");
        std::fs::write(&path, "# a comment\n\n  \nmaxmemory 2048\n# another\nappendonly yes\n").unwrap();
        let cfg = ConfigStore::load_conf_file(&path).unwrap();
        assert_eq!(cfg.get_int("maxmemory"), Some(2048));
        assert_eq!(cfg.get_str("appendonly"), Some("yes"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_conf_file_missing_path_returns_not_found() {
        let result = ConfigStore::load_conf_file(&scratch_path("does-not-exist.conf"));
        assert!(matches!(result, Err(e) if e.kind() == std::io::ErrorKind::NotFound));
    }
}
