//! ACL — the full per-user permission system (`acl.c`), the last item from
//! the "전부 다" audit's original B-group list (`redis_document/25_redisX.md`,
//! §3's Phase 9 note: "전체 ACL 사용자 테이블 대신 전역 비밀번호 하나만 지원").
//! `server.rs`'s pre-existing "ACL-lite" (`requirepass`/`AUTH`, no per-user
//! anything) stays exactly as it was — see below for why — and this module
//! adds a real multi-user table alongside it.
//!
//! ## Scope
//!
//! - **The `default` user's own authentication is still governed solely by
//!   `requirepass`**, unchanged from before this module existed
//!   (`server.rs`'s `cmd_auth`/`requires_auth_fn`). Real Redis's `requirepass`
//!   is sugar for mutating the default ACL user's password list; replicating
//!   that bidirectional sync would mean every `CONFIG SET requirepass`/`CONFIG
//!   GET requirepass` call site also has to stay in lockstep with this
//!   module's own user table, for a default-user-only feature this fork
//!   already had fully working and fully tested. Instead: `requirepass`
//!   keeps working exactly as before, and `ACL SETUSER default ...` can
//!   *additionally* grant the default user more passwords or restrict its
//!   command/key/channel permissions — both are checked, neither overrides
//!   the other. Every *other* user lives entirely in this module's table.
//! - **Permission enforcement happens once, in `Server::dispatch`** — the
//!   exact same choke point (and therefore the exact same bypass surface)
//!   `NOAUTH` already used before this module existed. `PSYNC`/`REPLICAOF`
//!   and the `BLPOP`-family/`XREAD`/`XREADGROUP` `BLOCK` commands are
//!   intercepted by `redisx-server.rs` *before* reaching `dispatch` (Phase 5/
//!   14/17's own module docs explain why), so they bypass ACL the same way
//!   they already bypass `NOAUTH` — a pre-existing gap this module inherits,
//!   not one it introduces.
//! - **Key/channel checks use the same single-key approximation `CLIENT
//!   TRACKING` already made** (`server.rs`'s `tracked_key`: "the key is
//!   `argv[1]`, if the command has one at all") rather than a full per-command
//!   key-spec table (`getKeysUsingLegacyRangeSpec` and friends, which real
//!   Redis needs custom logic for commands like `SORT`/`GEORADIUS`/`ZADD`/
//!   `EVAL`). A multi-key command (`MSET`, `SINTER` with several keys, ...)
//!   only has its *first* key checked — consistent with, not a regression
//!   from, the existing approximation.
//! - **No selectors** (`ACL SETUSER user (command rule...)` — per-rule extra
//!   permission sets layered on a user) and **no subcommand-level rules**
//!   (`+config|get`) — both rejected with a clear error rather than silently
//!   accepted and ignored.
//! - **Command categories are a small curated set** (`CATEGORIES` below —
//!   `all`/`read`/`write`/`keyspace`/`admin`/`dangerous`/`connection`/
//!   `transaction`/`scripting`/`pubsub`), not real Redis's full per-command
//!   taxonomy (`@string`/`@list`/`@fast`/`@slow`/... — dozens of categories,
//!   each command tagged individually in `commands.def`). `+@read`/`-@admin`/
//!   `+@all` style rules work; a category outside this list is rejected as
//!   unknown rather than silently matching nothing.
//! - **`ACL SAVE`/`ACL LOAD` error out**, matching real Redis's own behavior
//!   when no `aclfile` is configured (this fork has no `aclfile` config key
//!   at all, so the error is unconditional rather than config-dependent) —
//!   not a corner cut, a real code path real Redis also takes.

use std::collections::BTreeSet;

/// A deliberately small category set — see this module's doc comment.
pub const CATEGORIES: &[&str] = &["all", "read", "write", "keyspace", "admin", "dangerous", "connection", "transaction", "scripting", "pubsub"];

const ADMIN_COMMANDS: &[&str] =
    &["CONFIG", "SHUTDOWN", "SLOWLOG", "FAILOVER", "BGSAVE", "SAVE", "LASTSAVE", "REPLCONF", "CLIENT", "MEMORY", "COMMAND", "INFO", "WAIT", "ACL", "CLUSTER"];
const DANGEROUS_EXTRA_COMMANDS: &[&str] = &["FLUSHALL", "FLUSHDB", "KEYS", "RESTORE"];
const CONNECTION_COMMANDS: &[&str] = &["AUTH", "HELLO", "PING", "ECHO", "SELECT", "QUIT", "RESET", "CLIENT"];
const TRANSACTION_COMMANDS: &[&str] = &["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"];
const SCRIPTING_COMMANDS: &[&str] = &["EVAL", "EVALSHA", "FCALL", "FCALL_RO", "FUNCTION", "SCRIPT"];
const PUBSUB_COMMANDS: &[&str] = &["PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBSUB"];
const KEYSPACE_COMMANDS: &[&str] = &[
    "DEL", "EXISTS", "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME", "PERSIST", "RENAME", "RENAMENX", "TYPE",
    "KEYS", "SCAN", "RANDOMKEY", "DBSIZE", "FLUSHDB", "FLUSHALL", "COPY", "MOVE", "UNLINK", "TOUCH", "OBJECT", "DUMP", "RESTORE",
];

fn category_contains(cat: &str, cmd_name: &str) -> bool {
    match cat {
        "all" => true,
        "read" => crate::server::find_command(cmd_name).map(|s| !s.is_write).unwrap_or(false),
        "write" => crate::server::find_command(cmd_name).map(|s| s.is_write).unwrap_or(false),
        "keyspace" => KEYSPACE_COMMANDS.contains(&cmd_name),
        "admin" => ADMIN_COMMANDS.contains(&cmd_name),
        "dangerous" => ADMIN_COMMANDS.contains(&cmd_name) || DANGEROUS_EXTRA_COMMANDS.contains(&cmd_name),
        "connection" => CONNECTION_COMMANDS.contains(&cmd_name),
        "transaction" => TRANSACTION_COMMANDS.contains(&cmd_name),
        "scripting" => SCRIPTING_COMMANDS.contains(&cmd_name),
        "pubsub" => PUBSUB_COMMANDS.contains(&cmd_name),
        _ => false,
    }
}

/// `ACL CAT category` — every command this fork considers part of `category`.
pub fn commands_in_category(cat: &str) -> Vec<&'static str> {
    crate::server::COMMANDS.iter().map(|s| s.name).filter(|name| category_contains(cat, name)).collect()
}

fn password_hash(password: &[u8]) -> String {
    redisx_core::sha256::sha256_hex(password)
}

/// One `+cmd`/`-cmd`/`+@category`/`-@category` rule, kept in the order
/// `ACL SETUSER` applied them — `AclUser::can_run` replays them in order so a
/// later rule overrides an earlier one that also matches, exactly like
/// `acl.c`'s own rule evaluation.
#[derive(Clone, Debug)]
enum RuleSelector {
    Command(String),
    Category(String),
}

impl RuleSelector {
    fn matches(&self, cmd_name: &str) -> bool {
        match self {
            RuleSelector::Command(c) => c == cmd_name,
            RuleSelector::Category(cat) => category_contains(cat, cmd_name),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AclUser {
    pub enabled: bool,
    pub nopass: bool,
    /// SHA-256 hex digests — never plaintext, matching the default user's
    /// own `requirepass` comparison (`server.rs`'s `cmd_auth`).
    pub passwords: BTreeSet<String>,
    pub all_keys: bool,
    pub key_patterns: Vec<String>,
    pub all_channels: bool,
    pub channel_patterns: Vec<String>,
    default_allow: bool,
    command_rules: Vec<(bool, RuleSelector)>,
}

impl AclUser {
    /// A brand new user's starting point — `ACL SETUSER newname` with no
    /// prior state, or `reset`'s target state: locked down on every axis,
    /// matching real Redis's actual "off nopass sanitize-payload
    /// resetchannels -@all" default for a never-configured user.
    pub fn blank() -> Self {
        AclUser {
            enabled: false,
            nopass: false,
            passwords: BTreeSet::new(),
            all_keys: false,
            key_patterns: Vec::new(),
            all_channels: false,
            channel_patterns: Vec::new(),
            default_allow: false,
            command_rules: Vec::new(),
        }
    }

    /// The `default` user's starting point at `Server::new()` — fully open,
    /// matching this fork's pre-ACL behavior (no `requirepass` = no
    /// restrictions at all).
    pub fn default_user() -> Self {
        AclUser { enabled: true, nopass: true, all_keys: true, all_channels: true, default_allow: true, ..AclUser::blank() }
    }

    pub fn can_run(&self, cmd_name: &str) -> bool {
        let mut allowed = self.default_allow;
        for (allow, sel) in &self.command_rules {
            if sel.matches(cmd_name) {
                allowed = *allow;
            }
        }
        allowed
    }

    pub fn can_access_key(&self, key: &[u8]) -> bool {
        self.all_keys || self.key_patterns.iter().any(|p| crate::config::glob_match_bytes(p.as_bytes(), key))
    }

    pub fn can_access_channel(&self, channel: &[u8]) -> bool {
        self.all_channels || self.channel_patterns.iter().any(|p| crate::config::glob_match_bytes(p.as_bytes(), channel))
    }

    /// Applies one `ACL SETUSER` rule token. Callers apply every token to a
    /// *clone* of the current (or blank, for a new user) state and only
    /// commit it once every token has succeeded — matching real Redis's
    /// all-or-nothing `ACL SETUSER` semantics (`cmd_acl_setuser` in
    /// `server.rs`).
    fn apply_rule(&mut self, rule: &str) -> Result<(), String> {
        match rule.to_ascii_lowercase().as_str() {
            "on" => {
                self.enabled = true;
                return Ok(());
            }
            "off" => {
                self.enabled = false;
                return Ok(());
            }
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
                return Ok(());
            }
            "resetpass" => {
                self.nopass = false;
                self.passwords.clear();
                return Ok(());
            }
            "allkeys" => {
                self.all_keys = true;
                self.key_patterns.clear();
                return Ok(());
            }
            "resetkeys" => {
                self.all_keys = false;
                self.key_patterns.clear();
                return Ok(());
            }
            "allchannels" => {
                self.all_channels = true;
                self.channel_patterns.clear();
                return Ok(());
            }
            "resetchannels" => {
                self.all_channels = false;
                self.channel_patterns.clear();
                return Ok(());
            }
            "allcommands" => {
                self.default_allow = true;
                self.command_rules.clear();
                return Ok(());
            }
            "nocommands" => {
                self.default_allow = false;
                self.command_rules.clear();
                return Ok(());
            }
            "reset" => {
                *self = AclUser::blank();
                return Ok(());
            }
            // Accepted as a no-op: this fork doesn't implement RDB/RESTORE
            // payload sanitization or selectors to clear.
            "sanitize-payload" | "nosanitize-payload" | "skip-sanitize-payload" | "clearselectors" => return Ok(()),
            _ => {}
        }

        if let Some(pw) = rule.strip_prefix('>') {
            self.nopass = false;
            self.passwords.insert(password_hash(pw.as_bytes()));
            return Ok(());
        }
        if let Some(pw) = rule.strip_prefix('<') {
            self.passwords.remove(&password_hash(pw.as_bytes()));
            return Ok(());
        }
        if let Some(hash) = rule.strip_prefix('#') {
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(format!(
                    "Error in ACL SETUSER modifier '{rule}': Invalid password hash provided. \
                     It must be exactly 64 characters and contain only lowercase hexadecimal characters"
                ));
            }
            self.nopass = false;
            self.passwords.insert(hash.to_ascii_lowercase());
            return Ok(());
        }
        if let Some(hash) = rule.strip_prefix('!') {
            self.passwords.remove(&hash.to_ascii_lowercase());
            return Ok(());
        }
        if let Some(pattern) = rule.strip_prefix('~') {
            if self.all_keys {
                return Err(format!(
                    "Error in ACL SETUSER modifier '{rule}': Adding a pattern after the * pattern (or the 'allkeys' flag) \
                     is not valid and does not have any effect. Try 'resetkeys' to start with an empty list of patterns"
                ));
            }
            if pattern == "*" {
                self.all_keys = true;
                self.key_patterns.clear();
            } else {
                self.key_patterns.push(pattern.to_string());
            }
            return Ok(());
        }
        if rule.starts_with('%') {
            return Err(format!(
                "Error in ACL SETUSER modifier '{rule}': read/write-restricted key patterns ('%RW~'/'%R~'/'%W~') \
                 are not supported by this fork — use a plain '~pattern'"
            ));
        }
        if let Some(pattern) = rule.strip_prefix('&') {
            if self.all_channels {
                return Err(format!(
                    "Error in ACL SETUSER modifier '{rule}': Adding a pattern after the * pattern (or the 'allchannels' flag) \
                     is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels"
                ));
            }
            if pattern == "*" {
                self.all_channels = true;
                self.channel_patterns.clear();
            } else {
                self.channel_patterns.push(pattern.to_string());
            }
            return Ok(());
        }
        if let Some(rest) = rule.strip_prefix("+@") {
            return self.apply_category_rule(rule, rest, true);
        }
        if let Some(rest) = rule.strip_prefix("-@") {
            return self.apply_category_rule(rule, rest, false);
        }
        if let Some(cmd) = rule.strip_prefix('+') {
            return self.apply_command_rule(rule, cmd, true);
        }
        if let Some(cmd) = rule.strip_prefix('-') {
            return self.apply_command_rule(rule, cmd, false);
        }

        Err(format!("Error in ACL SETUSER modifier '{rule}': Syntax error"))
    }

    fn apply_category_rule(&mut self, rule: &str, cat: &str, allow: bool) -> Result<(), String> {
        let cat = cat.to_ascii_lowercase();
        if cat == "all" {
            self.default_allow = allow;
            self.command_rules.clear();
            return Ok(());
        }
        if !CATEGORIES.contains(&cat.as_str()) {
            return Err(format!("Error in ACL SETUSER modifier '{rule}': Unknown command or category name in ACL"));
        }
        self.command_rules.push((allow, RuleSelector::Category(cat)));
        Ok(())
    }

    fn apply_command_rule(&mut self, rule: &str, cmd: &str, allow: bool) -> Result<(), String> {
        if cmd.contains('|') {
            return Err(format!("Error in ACL SETUSER modifier '{rule}': subcommand-level ACL rules are not supported by this fork"));
        }
        let cmd_upper = cmd.to_ascii_uppercase();
        if crate::server::find_command(&cmd_upper).is_none() {
            return Err(format!("Error in ACL SETUSER modifier '{rule}': Unknown command or category name in ACL"));
        }
        self.command_rules.push((allow, RuleSelector::Command(cmd_upper)));
        Ok(())
    }

    /// Applies every rule token to a clone of `self`, committing only if all
    /// of them succeed — real Redis's `ACL SETUSER` is all-or-nothing, never
    /// partially applied on error.
    pub fn apply_rules(&self, rules: &[String]) -> Result<AclUser, String> {
        let mut next = self.clone();
        for rule in rules {
            next.apply_rule(rule)?;
        }
        Ok(next)
    }

    pub fn keys_string(&self) -> String {
        if self.all_keys {
            "~*".to_string()
        } else {
            self.key_patterns.iter().map(|p| format!("~{p}")).collect::<Vec<_>>().join(" ")
        }
    }

    pub fn channels_string(&self) -> String {
        if self.all_channels {
            "&*".to_string()
        } else if self.channel_patterns.is_empty() {
            "resetchannels".to_string()
        } else {
            self.channel_patterns.iter().map(|p| format!("&{p}")).collect::<Vec<_>>().join(" ")
        }
    }

    pub fn commands_string(&self) -> String {
        let mut parts = vec![if self.default_allow { "+@all".to_string() } else { "-@all".to_string() }];
        for (allow, sel) in &self.command_rules {
            let sign = if *allow { "+" } else { "-" };
            let name = match sel {
                RuleSelector::Command(c) => c.to_ascii_lowercase(),
                RuleSelector::Category(c) => format!("@{c}"),
            };
            parts.push(format!("{sign}{name}"));
        }
        parts.join(" ")
    }

    /// `ACL LIST`'s per-user line — `"user <name> on|off [nopass|#hash...]
    /// sanitize-payload ~keys &channels +@all ...commands"`, this module's
    /// own canonical rendering (real Redis's own `ACLDescribeUser` output
    /// isn't guaranteed to be the literal input either).
    pub fn describe(&self, name: &str) -> String {
        let mut out = format!("user {name} {}", if self.enabled { "on" } else { "off" });
        if self.nopass {
            out.push_str(" nopass");
        } else {
            for hash in &self.passwords {
                out.push_str(" #");
                out.push_str(hash);
            }
        }
        out.push_str(" sanitize-payload ");
        out.push_str(&self.keys_string());
        out.push(' ');
        out.push_str(&self.channels_string());
        out.push(' ');
        out.push_str(&self.commands_string());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_user_is_fully_open() {
        let u = AclUser::default_user();
        assert!(u.enabled && u.nopass);
        assert!(u.can_run("FLUSHALL"));
        assert!(u.can_access_key(b"anything"));
        assert!(u.can_access_channel(b"anything"));
    }

    #[test]
    fn blank_user_is_fully_locked_down() {
        let u = AclUser::blank();
        assert!(!u.enabled);
        assert!(!u.can_run("GET"));
        assert!(!u.can_access_key(b"k"));
        assert!(!u.can_access_channel(b"c"));
    }

    #[test]
    fn setuser_rules_apply_in_order_and_commit_only_on_full_success() {
        let base = AclUser::blank();
        let rules: Vec<String> = ["on", ">secret", "~foo:*", "+@read", "-get", "+get"].iter().map(|s| s.to_string()).collect();
        let user = base.apply_rules(&rules).unwrap();
        assert!(user.enabled);
        assert!(user.passwords.contains(&password_hash(b"secret")));
        assert!(user.can_access_key(b"foo:1"));
        assert!(!user.can_access_key(b"bar:1"));
        assert!(user.can_run("GET"), "the later +get should override the earlier -get");
        assert!(!user.can_run("SET"), "SET was never allowed (+@read only covers non-write commands)");

        let bad_rules: Vec<String> = vec!["on".to_string(), "~bogus-but-fine".to_string(), "+nosuchcommand".to_string()];
        assert!(base.apply_rules(&bad_rules).is_err(), "an invalid rule must reject the whole batch");
        // Base is untouched — the error happened on a clone.
        assert!(!base.enabled);
    }

    #[test]
    fn category_rules_cover_read_write_and_curated_groups() {
        let user = AclUser::blank().apply_rules(&["+@write".to_string()]).unwrap();
        assert!(user.can_run("SET"));
        assert!(!user.can_run("GET"));

        let admin = AclUser::blank().apply_rules(&["+@admin".to_string()]).unwrap();
        assert!(admin.can_run("CONFIG"));
        assert!(admin.can_run("CLUSTER"), "CLUSTER must be classified as @admin (Phase 23 added it alongside Phase 22's ACL categories)");
        assert!(!admin.can_run("GET"));

        // A restricted user allowed only reads must NOT also get CLUSTER's
        // admin-grade subcommands (ADDSLOTS/RESET/FORGET/...) just because
        // CLUSTER's own `CommandSpec.is_write` is `false` (its reply is a
        // read, but what it lets you *do* is admin-grade) — this was a real
        // gap until `CLUSTER` was added to `ADMIN_COMMANDS`.
        let read_only = AclUser::blank().apply_rules(&["+@read".to_string(), "-@admin".to_string()]).unwrap();
        assert!(!read_only.can_run("CLUSTER"));
    }

    #[test]
    fn unknown_category_and_command_are_rejected() {
        assert!(AclUser::blank().apply_rules(&["+@nosuchcategory".to_string()]).is_err());
        assert!(AclUser::blank().apply_rules(&["+nosuchcommand".to_string()]).is_err());
        assert!(AclUser::blank().apply_rules(&["+config|get".to_string()]).is_err(), "subcommand-level rules are explicitly unsupported");
    }

    #[test]
    fn allkeys_then_a_pattern_is_rejected_same_as_real_redis() {
        let user = AclUser::blank().apply_rules(&["allkeys".to_string()]).unwrap();
        assert!(user.apply_rules(&["~foo:*".to_string()]).is_err());
    }

    #[test]
    fn hash_password_form_round_trips() {
        let hash = password_hash(b"secret");
        let user = AclUser::blank().apply_rules(&[format!("#{hash}")]).unwrap();
        assert!(user.passwords.contains(&hash));
        assert!(AclUser::blank().apply_rules(&["#not-a-valid-hash".to_string()]).is_err());
    }

    #[test]
    fn describe_renders_the_default_user_canonically() {
        let u = AclUser::default_user();
        assert_eq!(u.describe("default"), "user default on nopass sanitize-payload ~* &* +@all");
    }
}
