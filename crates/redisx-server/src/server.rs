//! Command dispatch & server core — ported from the dispatch path in
//! `redis_src/src/server.c`'s `processCommand()`/`call()` and the MULTI/EXEC
//! state machine in `multi.c`.
//!
//! Scope for Phase 2 (`redis_document/25_redisX.md`): command-table lookup,
//! arity checking, `CONFIG GET/SET`, and `MULTI`/`QUEUED`/`EXEC`/`DISCARD`.
//! Phase 3 adds List/Hash/Set/ZSet commands on top of `object.rs`'s new
//! `RObj` variants, plus the `WRONGTYPE` check every real Redis command does
//! before touching a key (`lookupKeyWriteOrReply`'s type-checking siblings).
//! Phase 5 adds basic master/replica replication: write-command propagation
//! (`propagate`/`take_pending_propagation`, drained by the binary and fanned
//! out to replica sockets) and replicated-command application
//! (`apply_replicated_command`, used by the replica-side link handler). The
//! actual network plumbing (accepting `PSYNC`, opening the outbound
//! connection for `REPLICAOF`) lives in `src/bin/redisx-server.rs` — see its
//! module doc for why that split exists.
//!
//! Not in scope here (left for later phases, noted inline below): ACL,
//! blocking commands, `WATCH`'s optimistic-locking (`CLIENT_DIRTY_CAS`),
//! AOF propagation, keyspace notifications, and Stream/HLL/Geo (Phase 3's own
//! carry-over list), and cluster gossip (Phase 5's own carry-over — see
//! `redis_document/25_redisX.md`).

use crate::cluster;
use crate::config::ConfigStore;
use crate::hash::HashObj;
use crate::list::ListObj;
use crate::object::RObj;
use crate::set::SetObj;
use crate::zset::{LexBound, ZSetObj};
use redisx_core::KvStore;
use redisx_net::resp::Reply;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

pub struct Database {
    pub dict: KvStore<Vec<u8>, RObj>,
    /// `db->expires` — a separate dict from `dict` mapping a key to its
    /// expiration time in Unix epoch **milliseconds**, exactly mirroring real
    /// Redis's "two dicts, same keys, separate lifetimes" design rather than
    /// storing the TTL inline on `RObj` (keeps every existing `RObj`-reading
    /// command oblivious to expiry — only key-removal paths and the small
    /// set of `EXPIRE`-family commands below need to know this map exists).
    pub expires: std::collections::HashMap<Vec<u8>, i64>,
    /// `robj.lru`'s redisX equivalent — a third dict, same "parallel to
    /// `dict`, same keys, separate lifetime" shape as `expires` above,
    /// instead of widening `RObj` itself with an LRU/LFU field (keeps every
    /// existing `RObj`-reading command oblivious to this, exactly the same
    /// rationale `expires` already gives for TTL). Only `touch_access`
    /// (recency/frequency bump on every key access) and the `maxmemory`
    /// eviction picker (`collect_eviction_order`) read this; `OBJECT
    /// IDLETIME`/`FREQ` read it too, now that there's something real to
    /// report instead of the old stubbed constants.
    pub access: std::collections::HashMap<Vec<u8>, AccessInfo>,
}

/// One key's recency/frequency bookkeeping for `maxmemory-policy`'s LRU/LFU
/// variants and `OBJECT IDLETIME`/`FREQ` — the redisX analogue of real
/// Redis's 24-bit `robj.lru` field, which packs either a raw LRU clock
/// timestamp or an LFU "access-minute + logarithmic counter" pair into the
/// same bits depending on policy. This fork doesn't need that packing trick
/// (no memory pressure to save a few bits per object), so it just keeps both
/// pieces of information plainly, all the time, regardless of which policy
/// is active — `idle_since_ms` for LRU/`OBJECT IDLETIME`, `freq` for LFU/
/// `OBJECT FREQ`.
#[derive(Clone, Copy, Debug)]
pub struct AccessInfo {
    /// Unix epoch milliseconds of the last access (read or write) — real
    /// Redis's `LRU_CLOCK()`-stamped `o->lru` in LRU mode. `OBJECT IDLETIME`
    /// reports `now - idle_since_ms`.
    pub idle_since_ms: i64,
    /// `evict.c`'s `LFUDecrAndReturn`-then-`LFULogIncr` counter — an 8-bit
    /// saturating value (`0..=255`) incremented probabilistically (see
    /// `lfu_log_incr`'s doc comment) rather than on every access, so hot
    /// keys don't all saturate to the same value and lose relative ordering.
    pub freq: u8,
}

impl AccessInfo {
    /// A freshly-touched key's starting point — `LFU_INIT_VAL = 5` in real
    /// Redis's `evict.c` (not `0`): a brand new key starts with *some*
    /// credit so it isn't the single most-evictable key in an `allkeys-lfu`
    /// pool the instant it's created, before anything has had a chance to
    /// access it even once.
    fn new(now_ms: i64) -> Self {
        AccessInfo { idle_since_ms: now_ms, freq: 5 }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// `pub` so the binary layer (which owns the actual replication
    /// networking — see `src/bin/redisx-server.rs`'s module doc) can reset a
    /// db wholesale when loading a master's RDB on `REPLICAOF`.
    pub fn new() -> Self {
        // num_dicts_bits=0: a single dict, matching non-cluster Redis. See
        // redisx-core's kvstore.rs: with one dict the combined SCAN cursor is
        // byte-identical to a plain dict's cursor, so this costs nothing now
        // and is already cluster-ready for Phase 5.
        Database { dict: KvStore::new(0, true, false), expires: std::collections::HashMap::new(), access: std::collections::HashMap::new() }
    }

    /// Removes a key and, just as importantly, its TTL/access-info entries
    /// if it has them — real Redis's `dbGenericDelete` always deletes every
    /// per-key side dict together, never leaving a dangling entry pointing
    /// at nothing. Every place that fully removes a key (`DEL`, a container
    /// emptied by a pop, `FLUSHALL`, the active-expire sweep itself, the
    /// `maxmemory` evictor) goes through this.
    pub fn remove_key(&mut self, key: &[u8]) -> Option<RObj> {
        self.expires.remove(key);
        self.access.remove(key);
        self.dict.remove(SLOT, &key.to_vec())
    }
}

/// Per-connection state that `server.c` keeps on `client` — the slice of it
/// relevant to Phase 2 (protocol version, selected DB, MULTI/EXEC queue).
pub struct ClientState {
    pub resp: u8,
    pub db_index: usize,
    pub in_multi: bool,
    /// CLIENT_DIRTY_EXEC: set when a command fails to queue (unknown command
    /// or bad arity) while inside MULTI, forcing EXEC to abort.
    pub dirty_exec: bool,
    pub queued: Vec<Vec<Vec<u8>>>,
    pub should_close: bool,
    /// `getClientPeerId(c)` — the `ip:port` string `aof.rs`'s MONITOR-style
    /// log lines embed as `[dbid peer]`. Set once by the binary layer when a
    /// connection is accepted (it owns the socket, `Server` doesn't); stays
    /// `"internal"` for contexts with no real peer, such as commands applied
    /// from a replication link.
    pub peer: String,
    /// A stable identifier the binary layer assigns once per connection
    /// (its `RawFd`, widened to `i64`) — Phase 7's PUBSUB and CLIENT
    /// TRACKING both need to target a push message at one specific *other*
    /// client, something no previous phase needed (replication only ever
    /// broadcasts to every replica alike). Stays `0` for contexts with no
    /// real connection (matches `peer`'s `"internal"` convention) — safe
    /// because `0` is never handed out as a real `RawFd`-derived id in
    /// practice, and even if it collided, the worst case is one extra/missing
    /// push to a synthetic context nothing reads from anyway.
    pub id: i64,
    pub subscribed_channels: std::collections::HashSet<Vec<u8>>,
    pub subscribed_patterns: std::collections::HashSet<Vec<u8>>,
    /// `CLIENT TRACKING ON` (non-`BCAST` mode) — `true` means `run_command`
    /// should record this client against every key it reads in
    /// `Server::tracking_table`. A `BCAST` client never sets this (it's
    /// tracked by id in `Server::bcast_tracking` instead, matched by prefix
    /// rather than by which keys it happened to read).
    pub tracking: bool,
    /// `AUTH`/`HELLO ... AUTH` — `true` once this connection has presented
    /// the correct `requirepass`. Irrelevant (never checked) when
    /// `requirepass` is empty, matching real Redis's "no auth required at
    /// all" default.
    pub authenticated: bool,
    /// The ACL user this connection is currently acting as (`Server::acl_users`'
    /// key) — `"default"` until a successful non-default `AUTH user pass`
    /// changes it. Drives `ACL WHOAMI` and every per-command/key/channel
    /// permission check in `dispatch`. The `default` user's own
    /// authentication keeps going through `requirepass` exactly as before
    /// (see `acl.rs`'s module doc) — this field matters once a connection
    /// authenticates as anyone else.
    pub user: String,
    /// `WATCH`'s optimistic-lock set: `(db_index, key, version-at-watch-time)`
    /// for every key this connection is currently watching. Checked lazily at
    /// `EXEC` against `Server::watched_key_versions` rather than having the
    /// server proactively flag other clients dirty the moment a write lands
    /// (which this fork's architecture can't do — `Server` only ever touches
    /// the one `ClientState` passed into the command it's running, never a
    /// *different* connection's state, the same constraint `pending_pushes`/
    /// `tracking_table` exist to work around). See `cmd_watch`'s doc comment.
    pub watching: Vec<(usize, Vec<u8>, u64)>,
    /// `CLIENT_READONLY` (Phase 23, `cluster.rs`) — `true` after this
    /// connection has issued `READONLY`, letting it read (never write) keys
    /// whose slot this node holds as a *replica* rather than being `MOVED`
    /// to the master like every other client. `READWRITE` turns it back off.
    /// `ASKING` is accepted for client compatibility but never consulted —
    /// see `cluster.rs`'s module doc on why there's no `ASK`/migration dance
    /// for it to matter to.
    pub cluster_readonly: bool,
}

impl Default for ClientState {
    fn default() -> Self {
        ClientState {
            resp: 2,
            db_index: 0,
            in_multi: false,
            dirty_exec: false,
            queued: Vec::new(),
            should_close: false,
            peer: "internal".to_string(),
            id: 0,
            subscribed_channels: std::collections::HashSet::new(),
            subscribed_patterns: std::collections::HashSet::new(),
            tracking: false,
            authenticated: false,
            user: "default".to_string(),
            watching: Vec::new(),
            cluster_readonly: false,
        }
    }
}

pub struct Server {
    pub dbs: Vec<Database>,
    pub config: ConfigStore,
    /// `server.replid` — a 40-hex-char identifier handed to replicas in the
    /// `+FULLRESYNC <replid> <offset>` line. Generated once at startup from
    /// OS randomness (`std::collections::hash_map::RandomState`, the same
    /// "no need for a `rand` crate, std's own randomness suffices" call Phase
    /// 0 made for `mt19937-64` — this ID isn't persisted or compared
    /// bit-for-bit against anything, just needs to look unique per process).
    pub replid: String,
    /// `server.master_repl_offset` — bytes of the replication stream produced
    /// so far. This fork always does a full resync (no partial-resync/backlog
    /// support), so this exists mainly to put a plausible number in
    /// `+FULLRESYNC`; it isn't used to decide full vs. partial sync.
    pub master_repl_offset: u64,
    pub role: Role,
    /// Which DB the last propagated write command targeted, so a `SELECT` is
    /// only injected into the replication stream when it actually changes
    /// (`feedReplicationBuffer`'s `server.slaveseldb` equivalent).
    last_propagated_db: Option<usize>,
    /// RESP-encoded commands waiting to be fanned out to replica sockets —
    /// drained by the binary layer after each `dispatch()`/`apply_replicated_command()`
    /// call via `take_pending_propagation()`, since `Server` itself has no
    /// socket to write to.
    pending_propagation: Vec<u8>,
    /// `server.repl_backlog` — a trailing window of every byte ever fed to
    /// `pending_propagation`, capped at `REPL_BACKLOG_MAX` (oldest bytes
    /// dropped first). What `try_partial_resync` slices from when a
    /// reconnecting replica's `PSYNC <replid> <offset>` names an offset
    /// still inside this window — see its own doc comment for the "남은
    /// 부분" pass this was added in (Phase 5 originally scoped partial
    /// resync out entirely).
    repl_backlog: Vec<u8>,
    /// The absolute `master_repl_offset` value that `repl_backlog[0]`
    /// corresponds to — advances every time bytes get trimmed off the
    /// backlog's front.
    repl_backlog_start_offset: u64,
    /// Phase 6 (scripting) — one persistent `mlua::Lua` VM for the process's
    /// lifetime (reused across every `FCALL`, never recreated per call; see
    /// `script.rs`'s module doc on why `redis.call`'s reentrancy needs a
    /// thread-local context rather than a fresh interpreter per call).
    pub(crate) lua: mlua::Lua,
    pub libraries: std::collections::HashMap<String, crate::script::Library>,
    /// Phase 22 (`acl.rs`) — the full ACL user table, always containing at
    /// least `"default"`. See `acl.rs`'s module doc for the (deliberate)
    /// split between this table and `requirepass`-based auth.
    pub acl_users: std::collections::BTreeMap<String, crate::acl::AclUser>,
    /// Phase 23 (`cluster.rs`) — hash-slot ownership, known peers, and this
    /// node's own id/epoch. `enabled: false` (the `ClusterState::new()`
    /// default) until `src/bin/redisx-server.rs`'s `main` calls `activate`
    /// for a `cluster-enabled yes` startup; every cluster-aware code path in
    /// this module checks `cluster.enabled` first and is a complete no-op
    /// when it's off, so non-cluster behavior is unchanged bit-for-bit.
    pub cluster: cluster::ClusterState,
    /// Function name -> owning library name (for `FCALL` lookup and
    /// `FUNCTION DELETE` cleanup).
    pub(crate) functions: std::collections::HashMap<String, String>,
    /// Function name -> the actual compiled callable.
    pub(crate) compiled: std::collections::HashMap<String, crate::script::CompiledFunction>,
    /// `EVALSHA`'s cache: sha1-hex digest -> the exact script body `EVAL`
    /// or `SCRIPT LOAD` last stored it under (`script.c`'s `lua_scripts`
    /// dict, simplified to a plain map with no refcounting — this fork
    /// never evicts anything short of `SCRIPT FLUSH`).
    pub(crate) script_cache: std::collections::HashMap<String, String>,
    /// Channel -> set of subscribed client ids (`ClientState::id`) — the
    /// reverse index `PUBLISH` needs to find who to push to; each
    /// `ClientState::subscribed_channels` is this map's forward counterpart,
    /// consulted by `SUBSCRIBE`/`UNSUBSCRIBE` to report this client's own
    /// subscription count.
    pub(crate) channel_subs: std::collections::HashMap<Vec<u8>, std::collections::HashSet<i64>>,
    pub(crate) pattern_subs: std::collections::HashMap<Vec<u8>, std::collections::HashSet<i64>>,
    /// Messages ready to be pushed to a specific *other* client — drained by
    /// the binary layer (`take_pending_pushes`) the same way
    /// `pending_propagation` is, since `Server` has no socket of its own to
    /// write to. Used by both `PUBLISH` (message/pmessage) and `CLIENT
    /// TRACKING`'s invalidation pushes.
    pending_pushes: Vec<(i64, Reply)>,
    /// Key -> set of client ids with `CLIENT TRACKING ON` that read this key
    /// since their last invalidation (default, non-`BCAST` mode only) —
    /// cleared for a key the moment a write invalidates it.
    pub(crate) tracking_table: std::collections::HashMap<Vec<u8>, std::collections::HashSet<i64>>,
    /// `CLIENT TRACKING ON BCAST [PREFIX p ...]` — client id -> its prefix
    /// list (empty = match every key). Checked against every write's key
    /// independently of whether that client ever read it.
    pub(crate) bcast_tracking: std::collections::HashMap<i64, Vec<Vec<u8>>>,
    /// `CLIENT TRACKING ON REDIRECT <id>` — tracking client id -> the id
    /// its invalidation pushes actually get delivered to instead.
    pub(crate) tracking_redirect: std::collections::HashMap<i64, i64>,
    /// `CLIENT TRACKING ON NOLOOP` — client ids that should *not* receive
    /// an invalidation for a write they made themselves.
    pub(crate) tracking_noloop: std::collections::HashSet<i64>,
    /// `WAIT numreplicas timeout` — see `cmd_wait`'s doc comment for why
    /// this is a plain connected-replica count rather than real Redis's
    /// offset-acknowledgement wait. Kept in sync by the binary layer (which
    /// owns the actual replica socket list `Server` has no access to) via
    /// `set_connected_replicas`.
    connected_replicas: usize,
    /// `WATCH`'s server-side half: a write-counter per `(db_index, key)` ever
    /// touched by a write command, bumped on every write regardless of
    /// whether anyone's currently watching it (cheap, and avoids having to
    /// first check who's watching). `ClientState::watching` records the
    /// value it saw at `WATCH` time; `EXEC` compares the two. Entries are
    /// never removed once created (even after the key itself is deleted) —
    /// an accepted unbounded-but-slow memory-growth tradeoff, the same kind
    /// Phase 0 already made elsewhere (e.g. `expires` never shrinking its
    /// capacity) rather than adding refcounting for a server that's not
    /// expected to run for weeks touching millions of distinct keys.
    pub(crate) watched_key_versions: std::collections::HashMap<(usize, Vec<u8>), u64>,
    /// Unix timestamp (seconds) of this process's own startup — `INFO`'s
    /// `uptime_in_seconds` and `TIME`'s reference point for "how long has
    /// this instance been up", matching `server.stat_starttime`.
    pub(crate) start_time_unix: i64,
    /// Unix timestamp (seconds) of the last successful `SAVE`/`BGSAVE` —
    /// `None` until the first one. `LASTSAVE` and `INFO`'s
    /// `rdb_last_save_time` both read this; `server.lastsave` is its real
    /// Redis counterpart (initialized there to the process start time rather
    /// than `None`, but exposing "never saved yet" accurately seemed more
    /// useful than a real Redis quirk this fork has no reason to copy).
    pub(crate) last_save_unix: Option<i64>,
    /// `INFO`'s `connected_clients` — unlike `connected_replicas` (also kept
    /// in sync by the binary layer, which owns the real connection list
    /// `Server` has no access to), this counts *every* open connection,
    /// replicas included, matching real Redis's own `connected_clients`
    /// (which also doesn't subtract replica links).
    pub(crate) connected_clients: usize,
    /// `INFO`'s `total_commands_processed` — every command `run_command`
    /// executes, success or error alike, matching `server.stat_numcommands`.
    pub(crate) total_commands_processed: u64,
    /// `INFO`'s `rdb_changes_since_last_save` — every successful write since
    /// the last `SAVE`/`BGSAVE`, matching `server.dirty` (reset to `0` by
    /// `save_rdb_to_disk`).
    pub(crate) dirty_since_save: u64,
    /// Blocking commands (`BLPOP`/.../`XREAD BLOCK`): client id -> what it's
    /// waiting for. See `try_blocking_command`'s module-doc-sized comment
    /// for the whole design — in short, this fork can't have the server
    /// reach into another connection's state directly (same constraint
    /// `pending_pushes`/`watched_key_versions` exist to work around), so a
    /// blocked client's *original command* is stashed here and literally
    /// re-attempted (via the ordinary non-blocking command it's equivalent
    /// to, e.g. `BLPOP` retries as `LPOP`) whenever a write touches a key
    /// it's waiting on, or its deadline passes.
    pub(crate) blocked: std::collections::HashMap<i64, BlockedClient>,
    /// `(db_index, key) -> [client_id, ...]` in arrival order (oldest
    /// first) — the reverse index `maybe_unblock_waiters` uses to find who
    /// to retry, in the FIFO-fairness order real Redis's own
    /// `ready_keys`/`blocking_keys` give the same guarantee for.
    pub(crate) blocking_keys: std::collections::HashMap<(usize, Vec<u8>), Vec<i64>>,
    /// Replies ready to deliver to a now-unblocked (or timed-out) client —
    /// drained by the binary layer (`take_unblocked`) the same way
    /// `pending_pushes` is; the binary layer also resumes normal command
    /// processing for that connection once it sees one of these.
    unblocked: Vec<(i64, Reply)>,
    /// `bio.c`'s `BIO_LAZY_FREE` background-job queue, collapsed to a single
    /// `mpsc` channel feeding a single background thread — real Redis's
    /// entire point for lazy-freeing is "don't block the event loop dropping
    /// a huge value/dict inline", and one extra thread accomplishes that
    /// just as well as a whole bio-job-type system when there's only one
    /// kind of job (drop something). `None` until the first value actually
    /// needs lazy-freeing (`lazyfree_unconditional`'s `get_or_insert_with`)
    /// — every `lazyfree-lazy-*` flag defaults to `no` and `UNLINK` is rare
    /// in most of this fork's own test suite, so the overwhelming majority
    /// of `Server` instances (hundreds, in `cargo test`) never pay for the
    /// thread spawn at all.
    lazyfree_sender: Option<std::sync::mpsc::Sender<LazyFreeJob>>,
    /// Active-defrag's per-db resume point — `defrag_step`'s own cursor,
    /// indexed the same as `dbs`. Lets `maybe_run_active_defrag` make
    /// steady, bounded-per-call progress across a huge keyspace instead of
    /// walking it all in one call (see `defrag_step`'s doc).
    defrag_cursors: Vec<u64>,
    /// Throttles how often `maybe_run_active_defrag` even *looks* at
    /// `mem_fragmentation_ratio` — which calls `rss_bytes()`, a `ps`
    /// subprocess spawn on every non-Linux platform. Checking that on every
    /// single command (`run_command` runs for all of them) would trade one
    /// performance problem for a worse one; `None` means "never checked
    /// yet", so the very first command after `activedefrag yes` still runs
    /// a check immediately rather than waiting out the first interval.
    last_defrag_check: Option<std::time::Instant>,
    /// A short-lived cache of `used_memory()`'s result (timestamp + value),
    /// consulted by `used_memory_cached` so `enforce_maxmemory`'s common
    /// "well under `maxmemory`" path doesn't re-walk the entire keyspace on
    /// every single growing write command — see `used_memory_cached`'s own
    /// doc for the full rationale and the bound on how stale this can get.
    used_memory_cache: Option<(std::time::Instant, usize)>,
    /// `DEBUG SET-ACTIVE-EXPIRE 0/1` — `true` (real Redis's default) means
    /// `sweep_expired` runs normally. Unlike real Redis, this fork has no
    /// separate "passive expire on key access" path at all (`sweep_expired`
    /// running before every command *is* this fork's only expiry mechanism
    /// — nothing else checks `expires` independently), so setting this to
    /// `false` disables TTL enforcement entirely while it's off, not just
    /// the background sweep — a stronger effect than real Redis's own flag,
    /// but an honest one given this fork's simpler single-mechanism design.
    active_expire_enabled: bool,
}

/// One client parked on a blocking command — see `Server::blocked`'s doc
/// comment.
pub struct BlockedClient {
    db_index: usize,
    /// The exact original command (`BLPOP key1 key2 timeout`, etc.) —
    /// re-parsed by `retry_blocking_attempt` on every wake attempt, so the
    /// retry logic is just "run the same parser again", not a second
    /// hand-maintained code path.
    argv: Vec<Vec<u8>>,
    /// Every key this client is currently registered under in
    /// `blocking_keys` — needed to remove *all* of them in one shot once
    /// the client is satisfied via any one of them (a `BLPOP k1 k2 k3 0`
    /// unblocked by `k2` must stop waiting on `k1`/`k3` too).
    keys: Vec<Vec<u8>>,
    /// `None` = block forever (timeout `0`, matching real Redis).
    deadline_ms: Option<i64>,
}

/// What `try_blocking_command` decides for one blocking command: either it
/// could run right now (ordinary reply, possibly an error), or it couldn't
/// and the client is now registered in `Server::blocked`.
pub enum BlockingOutcome {
    Immediate(Reply),
    Blocked,
}

enum BlockAttempt {
    Immediate(Reply),
    StillBlocked,
}

/// Bundles `attempt_blocking_xreadgroup`'s arguments past clippy's
/// `too_many_arguments` — purely a parameter-count workaround, not a
/// reusable/stored type.
struct XReadGroupAttempt<'a> {
    group: &'a [u8],
    consumer: &'a [u8],
    keys: &'a [Vec<u8>],
    ids: &'a [Vec<u8>],
    count: Option<usize>,
    noack: bool,
}

type Handler = fn(&mut Server, &mut ClientState, &[Vec<u8>]) -> Reply;

pub struct CommandSpec {
    pub name: &'static str,
    /// Positive: exact argc (including the command name itself). Negative:
    /// minimum argc, `-N` meaning "at least N" — same convention as
    /// `redisCommand.arity` in `commands.def`.
    pub arity: i32,
    pub handler: Handler,
    /// CMD_WRITE: a successful call is propagated verbatim to connected
    /// replicas (`replicationFeedSlaves`'s simplified Phase 5 counterpart —
    /// no command rewriting for determinism, since none of this fork's
    /// commands are non-deterministic yet).
    pub is_write: bool,
}

/// This fork's replication role — `server.masterhost`'s two states, made
/// explicit. `Replica` only records who we're following; the actual TCP link
/// is owned by the binary layer (see this module's doc comment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Master,
    Replica { host: String, port: u16 },
}

fn ok_bytes_as_string(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

fn cmd_ping(_s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() > 1 {
        Reply::bulk(argv[1].clone())
    } else {
        Reply::SimpleString("PONG".to_string())
    }
}

fn cmd_echo(_s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    Reply::bulk(argv[1].clone())
}

fn cmd_quit(_s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    c.should_close = true;
    Reply::ok()
}

/// `RESET` — discards MULTI state, drops every subscription and `CLIENT
/// TRACKING` setting (via `client_disconnected`, which only ever touches
/// id-indexed server-side maps, so it's safe to call on a connection that's
/// staying open), deauthenticates, and reselects db 0. Matches real Redis's
/// documented behavior except it leaves the negotiated RESP version alone
/// (real `RESET` does too — only `HELLO` changes that).
fn cmd_reset(s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    c.in_multi = false;
    c.dirty_exec = false;
    c.queued.clear();
    c.watching.clear();
    c.db_index = 0;
    c.authenticated = false;
    c.user = "default".to_string();
    c.tracking = false;
    c.subscribed_channels.clear();
    c.subscribed_patterns.clear();
    c.cluster_readonly = false;
    s.client_disconnected(c.id);
    Reply::SimpleString("RESET".to_string())
}

fn cmd_select(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match ok_bytes_as_string(&argv[1]).parse::<usize>() {
        Ok(idx) if idx < s.dbs.len() => {
            if s.cluster.enabled && idx != 0 {
                return Reply::Error("ERR SELECT is not allowed in cluster mode".to_string());
            }
            c.db_index = idx;
            Reply::ok()
        }
        _ => Reply::Error("ERR DB index is out of range".to_string()),
    }
}

fn cmd_dbsize(s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    Reply::Integer(s.dbs[c.db_index].dict.size() as i64)
}

/// Flushing an entire (potentially huge) db inline means dropping every
/// value it held right here on the calling thread — exactly what
/// `lazyfree-lazy-user-flush yes` exists to avoid. Handing the *whole old
/// `Database`* to the lazy-free thread (not key-by-key) is what actually
/// avoids the cost: building a key-by-key list here would already have
/// done the expensive part before the background thread ever saw it.
fn cmd_flushall(s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    for i in 0..s.dbs.len() {
        let old = std::mem::take(&mut s.dbs[i]);
        s.lazyfree_if_enabled("lazyfree-lazy-user-flush", LazyFreeJob::Db(old));
    }
    // Every key in every db just vanished — dirty every WATCH on record,
    // not just the ones for keys that happened to still exist (same
    // "flushing invalidates every optimistic lock" rule real Redis's
    // `touchWatchedKeysInDb` applies via `flushallCommand`'s own sweep).
    for v in s.watched_key_versions.values_mut() {
        *v += 1;
    }
    Reply::ok()
}

fn cmd_flushdb(s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    let old = std::mem::take(&mut s.dbs[c.db_index]);
    s.lazyfree_if_enabled("lazyfree-lazy-user-flush", LazyFreeJob::Db(old));
    for (&(db, _), v) in s.watched_key_versions.iter_mut() {
        if db == c.db_index {
            *v += 1;
        }
    }
    Reply::ok()
}

/// `dir`/`dbfilename` joined, same convention `rdbSave()` uses.
fn rdb_path(config: &ConfigStore) -> std::path::PathBuf {
    let dir = config.get_str("dir").unwrap_or(".");
    let filename = config.get_str("dbfilename").unwrap_or("dump.rdb");
    std::path::Path::new(dir).join(filename)
}

/// SAVE — synchronous (this fork has no background-fork `BGSAVE`; see the
/// roadmap's Phase 4 completion criterion, which only requires a working
/// SAVE/LOAD round trip, not fork-based non-blocking saves).
fn cmd_save(s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    match save_rdb_to_disk(s) {
        Ok(()) => Reply::ok(),
        Err(e) => Reply::Error(format!("ERR {e}")),
    }
}

/// Shared by `SAVE`/`BGSAVE`/`SHUTDOWN` — writes the RDB file and, on
/// success, stamps `last_save_unix` (`LASTSAVE`/`INFO`'s `rdb_last_save_time`).
fn save_rdb_to_disk(s: &mut Server) -> Result<(), String> {
    let blob = crate::rdb::save(s);
    std::fs::write(rdb_path(&s.config), blob).map_err(|e| e.to_string())?;
    s.last_save_unix = Some(now_ms() / 1000);
    s.dirty_since_save = 0;
    Ok(())
}

/// `BGSAVE` — real Redis forks a child process so the save doesn't block the
/// main event loop; this fork has no `fork()` equivalent to reach for (and a
/// single-threaded reactor to not want to block anyway), so it just does the
/// save synchronously and reports the same `Background saving started`
/// reply real clients expect to parse — a small, documented fiction (same
/// spirit as `WAIT`'s approximation) rather than a real async save. A disk
/// error surfaces immediately as an error reply instead of only showing up
/// later in `INFO`'s `rdb_last_bgsave_status`, which is more useful than
/// silently "starting" a save that's actually already failed.
fn cmd_bgsave(s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    match save_rdb_to_disk(s) {
        Ok(()) => Reply::SimpleString("Background saving started".to_string()),
        Err(e) => Reply::Error(format!("ERR {e}")),
    }
}

fn cmd_lastsave(s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    Reply::Integer(s.last_save_unix.unwrap_or(s.start_time_unix))
}

/// `TIME` — `[seconds, microseconds]` as two bulk strings, matching
/// `timeCommand`'s `gettimeofday` split (this fork only has millisecond
/// resolution internally — `now_ms()` — so the microseconds field is that
/// millisecond remainder scaled up, not genuine microsecond precision; still
/// monotonically consistent with the seconds field, which is all callers
/// actually rely on).
fn cmd_time(_s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    let ms = now_ms();
    Reply::Array(vec![Reply::bulk((ms / 1000).to_string().into_bytes()), Reply::bulk(((ms % 1000) * 1000).to_string().into_bytes())])
}

/// `SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]` — saves (unless
/// `NOSAVE`) and then exits the process immediately via `std::process::exit`,
/// same as real Redis's `prepareForShutdown()` tail. `NOW`/`FORCE` are
/// accepted but no-ops: there's no slow fsync or lagging replica to force
/// past, since this fork's save is always synchronous and its replicas have
/// nothing to wait for from the master's shutdown. `ABORT` always errors —
/// there's no scheduled/delayed shutdown here to cancel, matching real
/// Redis's own error when no shutdown is in progress. Since `exit()` never
/// returns, no reply is ever written back to the caller — exactly what a
/// real `redis-cli` sees too (the connection just closes).
fn cmd_shutdown(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut nosave = false;
    for arg in &argv[1..] {
        match ok_bytes_as_string(arg).to_ascii_uppercase().as_str() {
            "NOSAVE" => nosave = true,
            "SAVE" | "NOW" | "FORCE" => {}
            "ABORT" => return Reply::Error("ERR No shutdown in progress".to_string()),
            other => return Reply::Error(format!("ERR Unsupported option {other}")),
        }
    }
    if !nosave {
        if let Err(e) = save_rdb_to_disk(s) {
            return Reply::Error(format!("ERR {e}"));
        }
    }
    std::process::exit(0);
}

/// REPLCONF — real Redis uses this for listening-port/capa/GETACK/ACK
/// negotiation; this fork just acknowledges every subcommand (matching how
/// tolerant real masters already are of unrecognized `REPLCONF` options —
/// see `replication.c`'s "(Non critical)" log lines) rather than parsing and
/// acting on any of them. `PSYNC`/`SYNC` are deliberately *not* handled here:
/// unlike every other command, a successful `PSYNC` must send a raw RDB byte
/// stream and then change how the connection's bytes are interpreted from
/// that point on (plain replicated commands, not request/reply) — something
/// a `fn(&mut Server, &mut ClientState, &[Vec<u8>]) -> Reply` signature can't
/// express. `src/bin/redisx-server.rs` intercepts `PSYNC` before it would
/// ever reach command dispatch, for the same reason it intercepts
/// `REPLICAOF` (which needs to open a whole new outbound connection).
fn cmd_replconf(_s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    Reply::ok()
}

/// `slowlogCommand` — see `slowlog.rs`'s module doc for the file-based
/// design this fork already committed to before this phase.
fn cmd_slowlog(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "RESET" if argv.len() == 2 => {
            crate::slowlog::reset(&s.config);
            Reply::ok()
        }
        "LEN" if argv.len() == 2 => Reply::Integer(crate::slowlog::len(&s.config) as i64),
        "GET" if argv.len() == 2 || argv.len() == 3 => {
            let count = if argv.len() == 3 {
                match ok_bytes_as_string(&argv[2]).parse::<i64>() {
                    Ok(n) if n >= -1 => n,
                    _ => return Reply::Error("ERR count should be greater than or equal to -1".to_string()),
                }
            } else {
                10
            };
            Reply::Array(crate::slowlog::get(&s.config, count).into_iter().map(|e| e.to_reply()).collect())
        }
        _ => Reply::Error("ERR Unknown SLOWLOG subcommand or wrong number of arguments".to_string()),
    }
}

// --------------------------------------------------------------- PubSub ---
// `pubsub.c` ported: `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`,
// `PUBLISH`, `PUBSUB CHANNELS/NUMSUB/NUMPAT`. Shard pub/sub
// (`SSUBSCRIBE`/`SPUBLISH`/`SUNSUBSCRIBE`) is cluster-only and out of scope,
// same as cluster itself (see the roadmap's Phase 5 section). `PUBLISH`
// isn't forwarded across replication links or into the AOF log — a
// deliberate scope cut (`redis_document/25_redisX.md`'s Phase 7 section):
// real Redis *does* forward it to replicas so their own subscribers still
// see it, but not to the AOF (pub/sub has no persistence semantics).
// `SUBSCRIBE` inside a `MULTI` block is also not specially rejected the way
// real Redis rejects it — an accepted simplification, not a deliberately
// tested path.

fn sub_count(c: &ClientState) -> i64 {
    (c.subscribed_channels.len() + c.subscribed_patterns.len()) as i64
}

fn cmd_subscribe(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut frames = Vec::with_capacity(argv.len() - 1);
    for channel in &argv[1..] {
        if c.subscribed_channels.insert(channel.clone()) {
            s.channel_subs.entry(channel.clone()).or_default().insert(c.id);
        }
        frames.push(Reply::Push(vec![Reply::bulk(b"subscribe".to_vec()), Reply::bulk(channel.clone()), Reply::Integer(sub_count(c))]));
    }
    Reply::Multi(frames)
}

fn cmd_unsubscribe(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let channels: Vec<Vec<u8>> = if argv.len() > 1 { argv[1..].to_vec() } else { c.subscribed_channels.iter().cloned().collect() };
    if channels.is_empty() {
        return Reply::Multi(vec![Reply::Push(vec![Reply::bulk(b"unsubscribe".to_vec()), Reply::Null, Reply::Integer(sub_count(c))])]);
    }
    let mut frames = Vec::with_capacity(channels.len());
    for channel in channels {
        c.subscribed_channels.remove(&channel);
        if let Some(subs) = s.channel_subs.get_mut(&channel) {
            subs.remove(&c.id);
            if subs.is_empty() {
                s.channel_subs.remove(&channel);
            }
        }
        frames.push(Reply::Push(vec![Reply::bulk(b"unsubscribe".to_vec()), Reply::bulk(channel), Reply::Integer(sub_count(c))]));
    }
    Reply::Multi(frames)
}

fn cmd_psubscribe(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut frames = Vec::with_capacity(argv.len() - 1);
    for pattern in &argv[1..] {
        if c.subscribed_patterns.insert(pattern.clone()) {
            s.pattern_subs.entry(pattern.clone()).or_default().insert(c.id);
        }
        frames.push(Reply::Push(vec![Reply::bulk(b"psubscribe".to_vec()), Reply::bulk(pattern.clone()), Reply::Integer(sub_count(c))]));
    }
    Reply::Multi(frames)
}

fn cmd_punsubscribe(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let patterns: Vec<Vec<u8>> = if argv.len() > 1 { argv[1..].to_vec() } else { c.subscribed_patterns.iter().cloned().collect() };
    if patterns.is_empty() {
        return Reply::Multi(vec![Reply::Push(vec![Reply::bulk(b"punsubscribe".to_vec()), Reply::Null, Reply::Integer(sub_count(c))])]);
    }
    let mut frames = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        c.subscribed_patterns.remove(&pattern);
        if let Some(subs) = s.pattern_subs.get_mut(&pattern) {
            subs.remove(&c.id);
            if subs.is_empty() {
                s.pattern_subs.remove(&pattern);
            }
        }
        frames.push(Reply::Push(vec![Reply::bulk(b"punsubscribe".to_vec()), Reply::bulk(pattern), Reply::Integer(sub_count(c))]));
    }
    Reply::Multi(frames)
}

/// Shared core of `PUBLISH` and keyspace notifications (both just need to
/// push a `message`/`pmessage` to every matching subscriber) — factored out
/// so `notify_keyspace_event` below doesn't have to duplicate the direct +
/// pattern subscriber fan-out logic.
fn publish_to_channel(s: &mut Server, channel: &[u8], payload: &[u8]) -> i64 {
    let mut n = 0i64;
    if let Some(subs) = s.channel_subs.get(channel) {
        for &id in subs {
            s.pending_pushes.push((id, Reply::Push(vec![Reply::bulk(b"message".to_vec()), Reply::bulk(channel.to_vec()), Reply::bulk(payload.to_vec())])));
            n += 1;
        }
    }
    // Staged into a `Vec` first so the borrow of `s.pattern_subs` ends
    // before `s.pending_pushes` (a different field, but the borrow checker
    // can't see through `s.pattern_subs.iter()` far enough to know that).
    let matches: Vec<(Vec<u8>, Vec<i64>)> = s
        .pattern_subs
        .iter()
        .filter(|(pattern, _)| crate::config::glob_match_bytes(pattern, channel))
        .map(|(pattern, subs)| (pattern.clone(), subs.iter().copied().collect()))
        .collect();
    for (pattern, ids) in matches {
        for id in ids {
            s.pending_pushes.push((
                id,
                Reply::Push(vec![Reply::bulk(b"pmessage".to_vec()), Reply::bulk(pattern.clone()), Reply::bulk(channel.to_vec()), Reply::bulk(payload.to_vec())]),
            ));
            n += 1;
        }
    }
    n
}

fn cmd_publish(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    Reply::Integer(publish_to_channel(s, &argv[1], &argv[2]))
}

/// `notifyKeyspaceEvent` (`notify.c`) — "남은 부분" pass addition, built
/// directly on Phase 7's PUBSUB plumbing above. `notify-keyspace-events`
/// flag letters this fork recognizes: `K`=publish to the `__keyspace@<db>__:`
/// channel (message = event name), `E`=publish to `__keyevent@<db>__:`
/// (message = key), `A`=alias for `g$lshzxe` (every per-type class this fork
/// actually fires — matches the C default `A` expansion, minus the classes
/// below that never fire here). Per-type class letters this fork's call
/// sites actually use: `g` (generic: `del`/`expire`/`expired`/`persist`),
/// `$` (string), `l`/`s`/`h`/`z` (list/set/hash/zset), `x` (`expired`
/// events specifically, fired alongside `g` — matches real Redis firing
/// both). `t`(stream)/`d`(module)/`m`(key-miss)/`e`(evicted) are accepted in
/// the flag string (so `CONFIG SET`/`GET` round-trip it cleanly) but never
/// fire — no consumer groups, modules, key-miss tracking, or eviction exist
/// in this fork to call this with those classes.
fn notify_keyspace_event(s: &mut Server, dbidx: usize, class: char, event: &str, key: &[u8]) {
    let flags = s.config.get_str("notify-keyspace-events").unwrap_or("").to_string();
    if flags.is_empty() {
        return;
    }
    let has_class = flags.contains(class) || (flags.contains('A') && "g$lshzxe".contains(class));
    if !has_class {
        return;
    }
    if flags.contains('K') {
        let mut channel = format!("__keyspace@{dbidx}__:").into_bytes();
        channel.extend_from_slice(key);
        publish_to_channel(s, &channel, event.as_bytes());
    }
    if flags.contains('E') {
        let channel = format!("__keyevent@{dbidx}__:{event}").into_bytes();
        publish_to_channel(s, &channel, key);
    }
}

fn cmd_pubsub(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "CHANNELS" => {
            let pattern = argv.get(2);
            let channels: Vec<Reply> = s
                .channel_subs
                .keys()
                .filter(|ch| match pattern {
                    Some(p) => crate::config::glob_match_bytes(p, ch),
                    None => true,
                })
                .map(|ch| Reply::bulk(ch.clone()))
                .collect();
            Reply::Array(channels)
        }
        "NUMSUB" => {
            let mut out = Vec::new();
            for ch in &argv[2..] {
                let n = s.channel_subs.get(ch).map_or(0, |subs| subs.len());
                out.push(Reply::bulk(ch.clone()));
                out.push(Reply::Integer(n as i64));
            }
            Reply::Array(out)
        }
        "NUMPAT" => Reply::Integer(s.pattern_subs.len() as i64),
        _ => Reply::Error("ERR Unknown PUBSUB subcommand or wrong number of arguments".to_string()),
    }
}

/// `HELLO [protover [AUTH user pass] [SETNAME name]]` — this fork had no
/// RESP3 negotiation at all until now (deferred past Phase 2, per that
/// phase's own completion notes, and never picked up since). `CLIENT
/// TRACKING`'s invalidation push frames only make sense on RESP3 (see
/// `cmd_client`'s own RESP-version check), so without `HELLO` a real client
/// would have no way to ever reach RESP3 and this phase's completion
/// criterion ("CLIENT TRACKING 기본 동작") couldn't be demonstrated against
/// a real client at all — a blocking prerequisite, not optional scope
/// creep. `AUTH`/`SETNAME` options are accepted and ignored (no ACL/auth in
/// this fork to check against, and no client name storage yet).
/// `LOLWUT [VERSION n]` — `lolwut.c`'s own doc comment says it best: "The
/// command should do something fun and interesting, and should be replaced
/// by a new implementation at each new version." Real Redis's own
/// `lolwutUnstableCommand` (what an unreleased/dev build falls back to, not
/// one of the rotating-square-ASCII-art numbered versions) is just a
/// one-line verbatim string — that's the one reproduced here; the
/// elaborate Bresenham-line canvas art from `lolwut5Command`/`6`/`8` is a
/// pure Easter egg with zero functional value, not worth porting.
fn cmd_lolwut(_s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    Reply::VerbatimString { format: *b"txt", data: b"redisX ver. 7.0.0\n".to_vec() }
}

// --------------------------------------------------------------- INFO ---

/// `INFO [section ...]` — a deliberately honest subset of real Redis's many
/// sections: every field below comes from state this fork actually tracks
/// (`server`/`clients`/`memory`/`persistence`/`replication`/`stats`/
/// `keyspace`). Sections real Redis has but this fork has no underlying data
/// for — `cpu` (no portable rusage without a new dependency), `commandstats`/
/// `latencystats`/`errorstats` (no per-command histograms kept), `cluster`
/// (always disabled) — are simply omitted rather than filled with invented
/// numbers; a client parsing `INFO` for a field this fork doesn't emit just
/// won't find that key, same as asking a real Redis build with that feature
/// compiled out. With no section argument (or `default`/`all`/`everything` —
/// this fork doesn't have the extra sections those three real Redis levels
/// would otherwise reveal, so they all collapse to the same output here),
/// every section below is included.
fn cmd_info(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let wanted: Vec<String> = if argv.len() > 1 { argv[1..].iter().map(|a| ok_bytes_as_string(a).to_ascii_lowercase()).collect() } else { vec!["default".to_string()] };
    let want = |section: &str| wanted.iter().any(|w| matches!(w.as_str(), "all" | "everything" | "default") || w == section);

    let mut out = String::new();
    let uptime = (now_ms() / 1000 - s.start_time_unix).max(0);

    if want("server") {
        out.push_str("# Server\r\n");
        out.push_str("redis_version:7.0.0\r\n");
        out.push_str("redisx_fork:1\r\n");
        out.push_str(&format!("os:{}\r\n", std::env::consts::OS));
        out.push_str(&format!("arch_bits:{}\r\n", if cfg!(target_pointer_width = "64") { 64 } else { 32 }));
        out.push_str("process_id:");
        out.push_str(&std::process::id().to_string());
        out.push_str("\r\n");
        out.push_str(&format!("run_id:{}\r\n", s.replid));
        out.push_str(&format!("tcp_port:{}\r\n", s.config.get_int("port").unwrap_or(0)));
        out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
        out.push_str(&format!("uptime_in_days:{}\r\n", uptime / 86400));
        out.push_str("\r\n");
    }
    if want("clients") {
        out.push_str("# Clients\r\n");
        out.push_str(&format!("connected_clients:{}\r\n", s.connected_clients));
        // No blocking commands in this fork (`redis_document/25_redisX.md`'s
        // scope notes) — always zero, never stale.
        out.push_str("blocked_clients:0\r\n");
        out.push_str("\r\n");
    }
    if want("memory") {
        let used = used_memory(s);
        out.push_str("# Memory\r\n");
        out.push_str(&format!("used_memory:{used}\r\n"));
        out.push_str(&format!("used_memory_human:{:.2}K\r\n", used as f64 / 1024.0));
        // Real, OS-queried fields — omitted entirely (not faked as `0`)
        // when `rss_bytes()` can't determine them on this platform/sandbox;
        // see this section's own module doc for why these two don't need
        // jemalloc despite every allocator-internals field around them
        // being out of scope.
        if let Some(rss) = rss_bytes() {
            out.push_str(&format!("used_memory_rss:{rss}\r\n"));
        }
        if let Some(ratio) = mem_fragmentation_ratio(s) {
            out.push_str(&format!("mem_fragmentation_ratio:{ratio:.2}\r\n"));
        }
        out.push_str(&format!("maxmemory:{}\r\n", s.config.get_bytes("maxmemory").unwrap_or(0)));
        out.push_str(&format!("maxmemory_policy:{}\r\n", s.config.get_str("maxmemory-policy").unwrap_or("noeviction")));
        out.push_str("mem_allocator:libc\r\n");
        out.push_str("\r\n");
    }
    if want("persistence") {
        out.push_str("# Persistence\r\n");
        out.push_str(&format!("rdb_changes_since_last_save:{}\r\n", s.dirty_since_save));
        out.push_str("rdb_bgsave_in_progress:0\r\n");
        out.push_str(&format!("rdb_last_save_time:{}\r\n", s.last_save_unix.unwrap_or(s.start_time_unix)));
        out.push_str("rdb_last_bgsave_status:ok\r\n");
        out.push_str(&format!("aof_enabled:{}\r\n", crate::aof::is_enabled(&s.config) as u8));
        out.push_str("\r\n");
    }
    if want("stats") {
        out.push_str("# Stats\r\n");
        out.push_str(&format!("total_commands_processed:{}\r\n", s.total_commands_processed));
        out.push_str(&format!("total_connections_received:{}\r\n", s.connected_clients));
        out.push_str("\r\n");
    }
    if want("replication") {
        out.push_str("# Replication\r\n");
        match &s.role {
            Role::Master => out.push_str("role:master\r\n"),
            Role::Replica { host, port } => {
                out.push_str("role:slave\r\n");
                out.push_str(&format!("master_host:{host}\r\n"));
                out.push_str(&format!("master_port:{port}\r\n"));
                out.push_str("master_link_status:up\r\n");
            }
        }
        out.push_str(&format!("connected_slaves:{}\r\n", s.connected_replicas));
        out.push_str(&format!("master_replid:{}\r\n", s.replid));
        out.push_str(&format!("master_repl_offset:{}\r\n", s.master_repl_offset));
        out.push_str("\r\n");
    }
    if want("keyspace") {
        out.push_str("# Keyspace\r\n");
        for (i, db) in s.dbs.iter().enumerate() {
            let keys = db.dict.size();
            if keys > 0 {
                out.push_str(&format!("db{i}:keys={keys},expires={},avg_ttl=0\r\n", db.expires.len()));
            }
        }
        out.push_str("\r\n");
    }
    Reply::bulk(out.into_bytes())
}

// ------------------------------------------------------------- MEMORY ---
// `object.c`/`zmalloc.c`'s territory — still scoped down hard where
// jemalloc itself would be required (see `redis_document/25_redisX.md`'s
// "메모리 할당자 전략" decision, §4-2: Rust's ordinary global allocator, no
// jemalloc). That specifically rules out per-arena allocator statistics
// (`allocator.*`/`allocator-fragmentation.*` in real Redis's `MEMORY STATS`)
// and real *active* defrag (`activedefrag`, which needs jemalloc's arena
// compaction API to move live objects without the application's
// involvement — there is no portable equivalent in Rust's global
// allocator). `MALLOC-STATS` stays unimplemented for the same reason.
//
// What *doesn't* need jemalloc, and is implemented for real: `maxmemory`
// eviction (needs "an estimate of used memory", already had one for `INFO`,
// and "a way to rank keys for eviction", the `AccessInfo` side table below)
// — and, as of this pass, real fragmentation *detection* (not mitigation):
// `rss_bytes()` asks the OS for this process's actual resident set size,
// which genuinely does include allocator/heap fragmentation, the Lua VM,
// polars, every dependency's own allocations — everything `used_memory()`'s
// keyspace-content walk doesn't and was never meant to count. The ratio of
// the two (`mem_fragmentation_ratio`) is a real, honestly-computed number,
// even without jemalloc underneath it. `MEMORY DOCTOR` uses it to recommend
// the one mitigation this fork *can* honestly offer when fragmentation is
// high: restart the process (safe, since RDB/AOF persist the keyspace) —
// not a fake no-op `activedefrag` that would claim to compact memory while
// doing nothing.

/// Total estimated memory this server's keyspace holds across every db —
/// the same per-key walk `INFO memory`'s `used_memory` field reports,
/// factored out so `enforce_maxmemory` doesn't duplicate it.
pub(crate) fn used_memory(s: &Server) -> usize {
    s.dbs
        .iter()
        .map(|db| db.dict.get_dict(SLOT).map(|d| d.iter().map(|(k, v)| k.len() + estimate_robj_content_size(v) + 16).sum::<usize>()).unwrap_or(0))
        .sum()
}

/// What `Server::lazyfree_unconditional` hands off to the one background
/// thread this fork spins up on first actual lazy-free use — either a
/// single removed value (`UNLINK`/`DEL`/an overwritten old value) or an
/// entire old `Database` (`FLUSHALL`/`FLUSHDB`, whose *size* is exactly what
/// makes dropping it inline slow — freeing one key at a time on the bg
/// thread there would miss the point and still block the caller building
/// the list).
// Neither variant's payload is ever *read* — the whole point is to move it
// onto the background thread and let it `Drop`, which the dead-code lint
// can't see counts as a use.
#[allow(dead_code)]
enum LazyFreeJob {
    Value(RObj),
    Db(Database),
}

impl Server {
    /// Routes `job` to this fork's one lazy-free background thread, spawning
    /// it on first use (see `lazyfree_sender`'s own doc comment on why
    /// that's lazy rather than always-on). Unconditional — callers that need
    /// to respect a `lazyfree-lazy-*` config flag check it themselves first
    /// (`lazyfree_if_enabled`); `UNLINK` calls this directly with no check at
    /// all, matching real Redis's own "UNLINK is always async" rule.
    fn lazyfree_unconditional(&mut self, job: LazyFreeJob) {
        let sender = self.lazyfree_sender.get_or_insert_with(|| {
            let (tx, rx) = std::sync::mpsc::channel::<LazyFreeJob>();
            std::thread::spawn(move || {
                for job in rx {
                    drop(job);
                }
            });
            tx
        });
        // `send` only fails if the background thread already exited, which
        // only happens if dropping some earlier value panicked — vanishingly
        // unlikely, and not a correctness issue even then: `SendError`
        // hands the job straight back, so it's simply dropped right here,
        // inline, instead of lost.
        drop(sender.send(job));
    }

    /// The config-gated form every lazy-free call site except `UNLINK` uses:
    /// free `job` on the background thread only if `config_flag` (one of the
    /// `lazyfree-lazy-*` keys) is currently `"yes"` — otherwise `job` is
    /// simply dropped right here, inline, which is already the correct
    /// synchronous behavior real Redis falls back to with the flag off.
    fn lazyfree_if_enabled(&mut self, config_flag: &str, job: LazyFreeJob) {
        if self.config.get_str(config_flag) == Some("yes") {
            self.lazyfree_unconditional(job);
        }
    }
}

/// `evict.c`'s `LFULogIncr`, ported verbatim: a saturating `0..=255`
/// counter incremented *probabilistically* rather than on every access —
/// the increment chance shrinks as the counter grows (`lfu-log-factor`,
/// default `10`, same meaning as real Redis), so a key accessed a million
/// times doesn't need a million-wide counter to still outrank one accessed
/// a hundred times; it just saturates slower. Uses `pseudo_random_unit_f64`
/// (`object.rs`) for the same "std's own randomness is good enough, this
/// isn't security-sensitive" reason that function's own doc comment gives.
fn lfu_log_incr(counter: u8, lfu_log_factor: f64) -> u8 {
    if counter == 255 {
        return 255;
    }
    let baseval = (counter as f64 - 5.0).max(0.0); // LFU_INIT_VAL = 5
    let p = 1.0 / (baseval * lfu_log_factor + 1.0);
    if crate::object::pseudo_random_unit_f64() < p { counter + 1 } else { counter }
}

/// `evict.c`'s `LFUDecrAndReturn`, simplified: redisX stores the last-touch
/// timestamp directly (`AccessInfo::idle_since_ms`) rather than real
/// Redis's packed 16-bit "last-decrement minute" field, so elapsed minutes
/// is a plain subtraction instead of needing `LFUTimeElapsed`'s wraparound
/// handling for a 16-bit clock that rolls over every ~45 days.
fn lfu_decay(counter: u8, idle_since_ms: i64, now_ms: i64, lfu_decay_time: i64) -> u8 {
    if lfu_decay_time <= 0 {
        return counter;
    }
    let elapsed_minutes = (now_ms - idle_since_ms) / 60_000;
    let periods = elapsed_minutes / lfu_decay_time;
    if periods <= 0 {
        return counter;
    }
    (counter as i64 - periods).max(0) as u8
}

/// Real Redis's eight `maxmemory-policy` values — `CONFIG SET maxmemory-
/// policy` validates against this (`cmd_config`), and `pick_eviction_
/// candidate`/`enforce_maxmemory` both match against it verbatim.
const MAXMEMORY_POLICIES: &[&str] =
    &["noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl"];

fn is_valid_maxmemory_policy(policy: &str) -> bool {
    MAXMEMORY_POLICIES.contains(&policy)
}

/// One full scan over `db`'s eligible keys, returning them **already sorted
/// in eviction order** for `policy` (front of the list = evict first) —
/// an **exact** ranking rather than real Redis's `performEvictions()`
/// (`maxmemory-samples` random keys sampled into a 16-slot `EvictionPool`
/// per round). Accepted trade *up*, not down, in eviction quality — same
/// "correctness over micro-optimization" call `sweep_expired` already makes
/// for active expiry. `maxmemory-samples` is accepted for `CONFIG SET`/
/// config-file compatibility but not read anywhere.
///
/// Earlier revision of this function picked just the single best candidate
/// per call, and `enforce_maxmemory` called it once per evicted key — that
/// made evicting K keys out of a db of N cost `O(N*K)`, which gets slow fast
/// once a keyspace is large and eviction pressure is real (e.g. lowering
/// `maxmemory` under an already-full cache). Scanning once and sorting is
/// `O(N log N)` **total**, independent of how many keys end up evicted from
/// the result — `enforce_maxmemory` now calls this at most once per db per
/// `enforce_maxmemory` invocation.
fn collect_eviction_order(db: &Database, policy: &str) -> Vec<Vec<u8>> {
    let Some(dict) = db.dict.get_dict(SLOT) else { return Vec::new() };
    let volatile_only = policy.starts_with("volatile-");
    let mut eligible: Vec<Vec<u8>> = dict.iter().map(|(k, _)| k.clone()).filter(|k| !volatile_only || db.expires.contains_key(k)).collect();
    match policy {
        // One whole-set Fisher-Yates shuffle instead of one `pseudo_random_
        // index` draw per pick — same "std's randomness is enough" call
        // `pseudo_random_index` itself already makes, just applied once up
        // front instead of once per candidate.
        "allkeys-random" | "volatile-random" => {
            for i in (1..eligible.len()).rev() {
                eligible.swap(i, crate::object::pseudo_random_index(i + 1));
            }
        }
        // Smallest `idle_since_ms` first = least recently used = evict
        // first. Missing an `AccessInfo` entry at all (shouldn't normally
        // happen — `touch_access` creates one on first touch — but a
        // defensive fallback costs nothing) sorts first too: "no history"
        // is treated as "maximally idle".
        "allkeys-lru" | "volatile-lru" => {
            eligible.sort_by_key(|k| db.access.get(k.as_slice()).map(|a| a.idle_since_ms).unwrap_or(i64::MIN));
        }
        "allkeys-lfu" | "volatile-lfu" => {
            eligible.sort_by_key(|k| db.access.get(k.as_slice()).map(|a| a.freq).unwrap_or(0));
        }
        "volatile-ttl" => {
            eligible.sort_by_key(|k| *db.expires.get(k.as_slice()).unwrap_or(&i64::MAX));
        }
        _ => eligible.clear(), // "noeviction" (caller never reaches here for it) or an unrecognized policy name
    }
    eligible
}

fn oom_error() -> Reply {
    Reply::Error("OOM command not allowed when used memory > 'maxmemory'.".to_string())
}

/// Real Redis's `CMD_DENYOOM` flag, approximated as a name allowlist — same
/// "good enough, not exhaustively hand-verified against every command"
/// shape this file's other command-name allowlists already use (e.g.
/// `cluster_command_keys`'s own doc comment on that exact tradeoff). Covers
/// the write commands that can *grow* a value's size, deliberately
/// excluding every purely-shrinking or metadata-only write (`DEL`/
/// `UNLINK`/the `EXPIRE` family/`LPOP`/`SREM`/`FLUSHALL`/...) — denying
/// those too under `noeviction` would make an over-the-limit server unable
/// to ever write its way back under the limit.
const DENY_OOM: &[&str] = &[
    "APPEND",
    "BITOP",
    "COPY",
    "DF.FROMCSV",
    "DF.FROMPARQUET",
    "DF.SET",
    "GEOADD",
    "GEOSEARCHSTORE",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HSET",
    "HSETNX",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "LINSERT",
    "LMOVE",
    "LPUSH",
    "MSET",
    "MSETNX",
    "PFADD",
    "PFMERGE",
    "RESTORE",
    "RPUSH",
    "RPOPLPUSH",
    "SADD",
    "SDIFFSTORE",
    "SET",
    "SETBIT",
    "SETRANGE",
    "SINTERSTORE",
    "SMOVE",
    "SUNIONSTORE",
    "XADD",
    "XGROUP",
    "ZADD",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTERSTORE",
    "ZUNIONSTORE",
];

fn command_denies_oom(name: &str) -> bool {
    DENY_OOM.contains(&name)
}

fn estimate_robj_content_size(obj: &RObj) -> usize {
    match obj {
        RObj::String(v) => v.len(),
        RObj::List(l) => l.range(0, -1).iter().map(Vec::len).sum(),
        RObj::Hash(h) => h.get_all().iter().map(|(f, v)| f.len() + v.len()).sum(),
        RObj::Set(set) => set.members().iter().map(Vec::len).sum(),
        RObj::ZSet(z) => z.range(0, -1).iter().map(|(m, _)| m.len() + 8).sum(),
        RObj::Stream(st) => st.entries().values().map(|fields| fields.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>() + 16).sum(),
        // A rough estimate only — walks the Arrow IPC encoding rather than
        // polars' own in-memory layout (chunked columnar buffers, which this
        // fork has no cheap way to size without pulling in private polars
        // internals), same "good enough, not exact" spirit as the other arms
        // above.
        #[cfg(feature = "polars")]
        RObj::DataFrame(df) => df.to_ipc_bytes().map(|b| b.len()).unwrap_or(0),
    }
}

/// This process's real resident set size, in bytes, queried from the OS —
/// not derived from `used_memory()` or any allocator API (this fork has
/// none to ask). `None` when it can't be determined: an unsupported
/// platform, or `/proc`/`ps` unavailable in a locked-down sandbox. Callers
/// degrade to omitting the RSS-derived fields entirely rather than
/// reporting a fabricated number.
fn rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest.split_whitespace().next()?.parse::<u64>().ok().map(|kb| kb * 1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        // macOS/BSD: no `/proc`, and hand-rolling `getrusage(2)`'s `rusage`
        // struct layout (and its `ru_maxrss` unit, which is bytes on macOS
        // but KB on Linux — easy to get silently wrong) isn't worth it for
        // one diagnostic field. `ps`'s own `rss=` column is always KB,
        // documented and stable across BSD/macOS/Linux `ps` alike.
        let pid = std::process::id().to_string();
        let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &pid]).output().ok()?;
        if !out.status.success() {
            return None;
        }
        String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().ok().map(|kb| kb * 1024)
    }
}

/// `used_memory_rss / used_memory` — real Redis's own `mem_fragmentation_
/// ratio` definition (`zmalloc.c`'s `get_proc_title`-adjacent `fragmentation
/// = rss / allocator_active`, simplified here since there's no allocator
/// layer between "RSS" and "what we count as used"). `None` when either
/// input is unavailable (`rss_bytes()` failed) or meaningless (an empty
/// keyspace would divide by zero).
fn mem_fragmentation_ratio(s: &Server) -> Option<f64> {
    let used = used_memory(s);
    if used == 0 {
        return None;
    }
    rss_bytes().map(|rss| rss as f64 / used as f64)
}

/// One bounded step of the active-defrag walk — scans up to `budget` keys
/// starting from `cursor` (the exact same reverse-binary cursor `KvStore::
/// scan` already gives `SCAN`, chosen for the same reason real Redis's own
/// `activeDefragCycle` reuses `dictScan`: resumable across calls, and no key
/// is ever skipped across a full lap even if the dict resizes mid-walk).
/// For each key visited, calls `RObj::defrag_in_place` to shrink whatever
/// over-allocated buffer backs its value. Returns the cursor to resume from
/// next time — `0` once a full lap completes. `budget` bounds how much work
/// one call does, so a periodic caller (`Server::maybe_run_active_defrag`)
/// can make steady, latency-bounded progress across a huge keyspace instead
/// of walking it all in one go; `MEMORY PURGE` (`force_full_defrag`) instead
/// loops this to completion on purpose, since a user calling `PURGE`
/// explicitly wants it done *now*, not spread out.
fn defrag_step(db: &mut Database, cursor: u64, budget: usize) -> u64 {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let next = db.dict.scan(cursor, Some(SLOT), |k: &Vec<u8>, _v| {
        if keys.len() < budget {
            keys.push(k.clone());
        }
    });
    for key in keys {
        if let Some(obj) = db.dict.get_mut(SLOT, &key) {
            obj.defrag_in_place();
        }
    }
    next
}

/// `MEMORY PURGE` — an immediate, unbounded defrag pass over every db (every
/// key, however long a full lap takes), plus shrinking the db-level
/// `expires`/`access` side maps themselves (plain `std::collections::
/// HashMap`s, which have the identical "never shrinks on removal alone"
/// property `ListPack`/`IntSet` do). Resets each db's defrag cursor back to
/// `0` afterward — the next periodic pass starts a fresh lap rather than
/// resuming mid-lap against a now-already-fully-defragged keyspace.
fn force_full_defrag(s: &mut Server) {
    for dbidx in 0..s.dbs.len() {
        let mut cursor = 0u64;
        loop {
            cursor = defrag_step(&mut s.dbs[dbidx], cursor, usize::MAX);
            if cursor == 0 {
                break;
            }
        }
        s.defrag_cursors[dbidx] = 0;
        s.dbs[dbidx].expires.shrink_to_fit();
        s.dbs[dbidx].access.shrink_to_fit();
    }
}

/// `MEMORY USAGE key` — key bytes + an estimate of the value's own content
/// size, plus a small fixed overhead allowance standing in for what a real
/// per-key dict entry/object header would cost. Not sampled (`SAMPLES` is
/// accepted and ignored) — this fork just walks the whole value, which is
/// fine at the scale anything in this fork's own test suite actually uses.
fn cmd_memory(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "USAGE" if argv.len() >= 3 => match s.dbs[c.db_index].dict.get(SLOT, &argv[2]) {
            None => Reply::Null,
            Some(obj) => Reply::Integer((argv[2].len() + estimate_robj_content_size(obj) + 16) as i64),
        },
        // A deliberately smaller subset of real Redis's ~30-field `MEMORY
        // STATS` — every field here is a real, honestly-computed number
        // (nothing jemalloc-shaped faked out as a constant); see this
        // section's module doc for exactly why the allocator-internals
        // fields aren't included at all rather than stubbed.
        "STATS" => {
            let used = used_memory(s) as i64;
            let keys: i64 = s.dbs.iter().map(|db| db.dict.size() as i64).sum();
            let mut out = vec![(Reply::bulk(b"used_memory".to_vec()), Reply::Integer(used)), (Reply::bulk(b"keys.count".to_vec()), Reply::Integer(keys))];
            if let Some(rss) = rss_bytes() {
                out.push((Reply::bulk(b"used_memory_rss".to_vec()), Reply::Integer(rss as i64)));
            }
            if let Some(ratio) = mem_fragmentation_ratio(s) {
                out.push((Reply::bulk(b"mem_fragmentation_ratio".to_vec()), Reply::bulk(format!("{ratio:.2}").into_bytes())));
            }
            Reply::Map(out)
        }
        "DOCTOR" => {
            let diagnosis = match mem_fragmentation_ratio(s) {
                None => {
                    "This fork uses Rust's default global allocator, not jemalloc, and this process's real RSS \
                     couldn't be determined on this platform/sandbox, so there's nothing to diagnose here."
                        .to_string()
                }
                Some(ratio) if ratio >= 1.5 => {
                    let defrag_on = s.config.get_str("activedefrag") == Some("yes");
                    let mitigation = if defrag_on {
                        "`activedefrag yes` is already on, so this fork is actively reclaiming over-allocated \
                         per-value buffer capacity in the background — run `MEMORY PURGE` for an immediate, \
                         unbounded pass instead of waiting for the next bounded step. If the ratio stays high \
                         after that, the remaining gap is likely real allocator-level fragmentation this fork's \
                         plain global allocator can't compact without jemalloc's arena API — a restart is the \
                         only remaining lever there (safe: RDB/AOF persist the keyspace across it)."
                    } else {
                        "`CONFIG SET activedefrag yes` (or a one-off `MEMORY PURGE`) will reclaim over-allocated \
                         per-value buffer capacity — a real, if more limited, mitigation than real Redis's \
                         jemalloc-arena-based active defrag, but genuinely effective for a keyspace with a lot of \
                         churn (values that grew large and shrank, e.g. via LPOP/SREM/LTRIM). If fragmentation \
                         stays high after that, a restart is the remaining lever (safe: RDB/AOF persist the \
                         keyspace across it)."
                    };
                    format!(
                        "High fragmentation detected: used_memory_rss is {ratio:.2}x the live keyspace content \
                         (used_memory). {mitigation}"
                    )
                }
                Some(ratio) => {
                    format!("Fragmentation looks healthy: used_memory_rss is {ratio:.2}x the live keyspace content.")
                }
            };
            Reply::bulk(diagnosis.into_bytes())
        }
        // Real Redis's `MEMORY PURGE` asks jemalloc to return free arena
        // pages to the OS. This fork has no arena layer to ask, so instead
        // it does the part that doesn't need one: force an immediate,
        // unbounded defrag pass (`force_full_defrag`) rather than the
        // bounded, throttled one `activedefrag yes` runs incrementally.
        // Works even with `activedefrag no` — `PURGE` is an explicit,
        // one-off request, independent of the background policy.
        "PURGE" => {
            force_full_defrag(s);
            Reply::ok()
        }
        // Real `MEMORY MALLOC-STATS` dumps jemalloc's internal arena report
        // via `malloc_stats_print`. There's no such report to dump here, so
        // this prints the honest equivalent this fork actually has (to
        // stderr, matching every other operational log line this fork
        // writes — see `bin/redisx-server.rs`'s startup banner) and still
        // returns `+OK` like the real command does, rather than erroring —
        // scripts that unconditionally call `MEMORY MALLOC-STATS` as a
        // diagnostic nicety shouldn't break just because this fork tells a
        // smaller story.
        "MALLOC-STATS" => {
            eprintln!("# redisX malloc stats (no jemalloc — Rust's default global allocator)");
            eprintln!("used_memory: {}", used_memory(s));
            if let Some(rss) = rss_bytes() {
                eprintln!("used_memory_rss: {rss}");
            }
            if let Some(ratio) = mem_fragmentation_ratio(s) {
                eprintln!("mem_fragmentation_ratio: {ratio:.2}");
            }
            eprintln!("activedefrag: {}", s.config.get_str("activedefrag").unwrap_or("no"));
            Reply::ok()
        }
        _ => Reply::Error(
            "ERR Unsupported MEMORY subcommand or wrong number of arguments (this fork implements only USAGE/STATS/DOCTOR/PURGE/MALLOC-STATS)"
                .to_string(),
        ),
    }
}

/// `DEBUG` — this fork never had any `DEBUG` subcommand at all before this
/// (`DEBUG`/`LATENCY`-class introspection was blanket out-of-scope, per
/// `redis_document/25_redisX.md`'s own scope notes — tied to C's fork-based
/// process model and jemalloc internals this fork has neither of). This is a
/// deliberately small, memory/expiry-diagnostic-only subset — not an attempt
/// at the dozens of real `DEBUG` subcommands (`JMAP`/`SEGFAULT`/`STRUCTSIZE`/
/// `CHANGE-REPL-ID`/... — most of which either don't apply to a safe-Rust
/// process at all, or are about C-specific memory layout this fork doesn't
/// have):
///
/// - **`OBJECT key`** — real Redis's single most commonly used `DEBUG`
///   subcommand for exactly this purpose: a one-line memory/encoding
///   snapshot of one value. `serializedlength` reuses `rdb.rs`'s own
///   `dump_value` (the same bytes `DUMP`/`RESTORE` round-trip), `lru_
///   seconds_idle` reads the real `AccessInfo` side table (`OBJECT
///   IDLETIME`'s own data). `ql_nodes`/`ql_avg_node` only appear for lists —
///   always `1`/the list's own length, since `list.rs` never has more than
///   one listpack node (that module's own documented simplification,
///   reported here honestly rather than omitted).
/// - **`SET-ACTIVE-EXPIRE 0|1`** — lets a diagnostic/test session freeze
///   this fork's active-expire sweep to inspect memory/TTL state without it
///   quietly changing underfoot, the same real-Redis use case. See `Server::
///   active_expire_enabled`'s own doc for the one way this fork's version is
///   stronger than real Redis's (no separate passive-expire path to fall
///   back on while it's off).
fn cmd_debug(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "OBJECT" if argv.len() >= 3 => {
            let Some(obj) = s.dbs[c.db_index].dict.get(SLOT, &argv[2]) else {
                return Reply::Error("ERR no such key".to_string());
            };
            let serializedlength = crate::rdb::dump_value(obj).len();
            let idle_seconds = s.dbs[c.db_index].access.get(argv[2].as_slice()).map(|a| (now_ms() - a.idle_since_ms) / 1000).unwrap_or(0).max(0);
            let mut line = format!(
                "Value at:0x0 refcount:1 encoding:{} serializedlength:{serializedlength} lru:0 lru_seconds_idle:{idle_seconds}",
                obj.encoding_name()
            );
            if let RObj::List(l) = obj {
                line.push_str(&format!(" ql_nodes:1 ql_avg_node:{:.2}", l.len() as f64));
            }
            Reply::SimpleString(line)
        }
        "SET-ACTIVE-EXPIRE" if argv.len() >= 3 => {
            s.active_expire_enabled = ok_bytes_as_string(&argv[2]) != "0";
            Reply::ok()
        }
        _ => Reply::Error(
            "ERR DEBUG subcommand not supported (this fork implements only OBJECT/SET-ACTIVE-EXPIRE, both memory/expiry-diagnostic — see redis_document/25_redisX.md for scope)"
                .to_string(),
        ),
    }
}

/// `COMMAND [COUNT | LIST | INFO [name ...] | GETKEYS cmd [args...] | DOCS ...]`
/// — introspects the same `COMMANDS` table dispatch itself uses, so this can
/// never drift out of sync with what's actually callable. `COMMAND INFO`'s
/// reply shape follows the modern 10-field layout (`name`, `arity`, `flags`,
/// `first_key`, `last_key`, `step`, then ACL-categories/tips/key-specs/
/// subcommands arrays), but since this fork has neither ACL categories nor a
/// real per-command key-spec table, those last four are always empty arrays
/// and `first_key`/`last_key`/`step` fall back to the same `argv[1]`-is-the-
/// key heuristic `tracked_key`/`CLIENT TRACKING` already rely on elsewhere
/// (`1/1/1` for any command that takes at least one argument after its name,
/// `0/0/0` otherwise) rather than a hand-maintained key-spec per command.
/// `DOCS` replies with an empty map for every name asked about — there's no
/// prose documentation table in this fork to serve, but an empty-but-present
/// reply lets a client that merely checks "does this command exist" (rather
/// than rendering the docs) still work.
fn cmd_command(_s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() == 1 {
        return Reply::Array(COMMANDS.iter().map(command_info_reply).collect());
    }
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "COUNT" => Reply::Integer(COMMANDS.len() as i64),
        "LIST" => Reply::Array(COMMANDS.iter().map(|spec| Reply::bulk(spec.name.to_ascii_lowercase().into_bytes())).collect()),
        "INFO" if argv.len() == 2 => Reply::Array(COMMANDS.iter().map(command_info_reply).collect()),
        "INFO" => Reply::Array(
            argv[2..]
                .iter()
                .map(|name| match find_command(&ok_bytes_as_string(name).to_ascii_uppercase()) {
                    Some(spec) => command_info_reply(spec),
                    None => Reply::NullArray,
                })
                .collect(),
        ),
        "DOCS" if argv.len() == 2 => Reply::Map(COMMANDS.iter().map(|spec| (Reply::bulk(spec.name.to_ascii_lowercase().into_bytes()), Reply::Map(vec![]))).collect()),
        "DOCS" => Reply::Map(
            argv[2..]
                .iter()
                .filter(|name| find_command(&ok_bytes_as_string(name).to_ascii_uppercase()).is_some())
                .map(|name| (Reply::bulk(ok_bytes_as_string(name).to_ascii_lowercase().into_bytes()), Reply::Map(vec![])))
                .collect(),
        ),
        "GETKEYS" if argv.len() >= 3 => match tracked_key(&argv[2..]) {
            Some(key) => Reply::Array(vec![Reply::bulk(key.clone())]),
            None => Reply::Error("ERR The command has no key arguments".to_string()),
        },
        _ => Reply::Error(format!("ERR Unknown subcommand or wrong number of arguments for '{}'", sub.to_ascii_lowercase())),
    }
}

fn command_info_reply(spec: &CommandSpec) -> Reply {
    let has_key_arg = spec.arity.unsigned_abs() >= 2;
    let flag = if spec.is_write { "write" } else { "readonly" };
    Reply::Array(vec![
        Reply::bulk(spec.name.to_ascii_lowercase().into_bytes()),
        Reply::Integer(spec.arity as i64),
        Reply::Array(vec![Reply::SimpleString(flag.to_string())]),
        Reply::Integer(if has_key_arg { 1 } else { 0 }),
        Reply::Integer(if has_key_arg { 1 } else { 0 }),
        Reply::Integer(if has_key_arg { 1 } else { 0 }),
        Reply::Array(vec![]),
        Reply::Array(vec![]),
        Reply::Array(vec![]),
        Reply::Array(vec![]),
    ])
}

fn cmd_hello(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut i = 1;
    if i < argv.len() && ok_bytes_as_string(&argv[i]).parse::<u8>().is_ok() {
        match ok_bytes_as_string(&argv[i]).parse::<u8>() {
            Ok(2) => c.resp = 2,
            Ok(3) => c.resp = 3,
            _ => return Reply::Error("NOPROTO unsupported protocol version".to_string()),
        }
        i += 1;
    }
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "AUTH" if i + 2 < argv.len() => {
                let auth_reply = cmd_auth(s, c, &[b"AUTH".to_vec(), argv[i + 1].clone(), argv[i + 2].clone()]);
                if matches!(auth_reply, Reply::Error(_)) {
                    return auth_reply;
                }
                i += 3;
            }
            "SETNAME" if i + 1 < argv.len() => {
                i += 2; // accepted, ignored — no client name storage in this fork yet
            }
            _ => return Reply::Error("ERR syntax error in HELLO".to_string()),
        }
    }
    if requires_auth_fn(s, c) {
        return Reply::Error("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time".to_string());
    }
    Reply::Map(vec![
        (Reply::bulk(b"server".to_vec()), Reply::bulk(b"redisx".to_vec())),
        (Reply::bulk(b"version".to_vec()), Reply::bulk(b"7.0.0".to_vec())),
        (Reply::bulk(b"proto".to_vec()), Reply::Integer(c.resp as i64)),
        (Reply::bulk(b"id".to_vec()), Reply::Integer(c.id)),
        (Reply::bulk(b"mode".to_vec()), Reply::bulk(b"standalone".to_vec())),
        (Reply::bulk(b"role".to_vec()), Reply::bulk(if matches!(s.role, Role::Master) { b"master".to_vec() } else { b"replica".to_vec() })),
        (Reply::bulk(b"modules".to_vec()), Reply::Array(vec![])),
    ])
}

// --------------------------------------------------------- CLIENT TRACKING ---
// `tracking.c` ported: `CLIENT TRACKING ON [REDIRECT client-id] [BCAST]
// [PREFIX prefix ...] [NOLOOP] | OFF` plus `CLIENT ID` (needed to usefully
// test tracking at all — a second connection needs to know its own id to
// look for, but it's also just a trivially cheap, independently useful
// command now that `ClientState::id` exists). `OPTIN`/`OPTOUT` and every
// other `CLIENT` subcommand (`GETNAME`/`SETNAME`/`LIST`/`KILL`/...) remain
// out of scope — see `redis_document/25_redisX.md`'s "남은 부분" section.
fn cmd_client(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "ID" if argv.len() == 2 => Reply::Integer(c.id),
        "TRACKING" if argv.len() >= 3 => match ok_bytes_as_string(&argv[2]).to_ascii_uppercase().as_str() {
            "ON" => {
                let mut bcast = false;
                let mut prefixes: Vec<Vec<u8>> = Vec::new();
                let mut redirect: Option<i64> = None;
                let mut noloop = false;
                let mut i = 3;
                while i < argv.len() {
                    match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
                        "BCAST" => {
                            bcast = true;
                            i += 1;
                        }
                        "NOLOOP" => {
                            noloop = true;
                            i += 1;
                        }
                        "PREFIX" if i + 1 < argv.len() => {
                            prefixes.push(argv[i + 1].clone());
                            i += 2;
                        }
                        "REDIRECT" if i + 1 < argv.len() => {
                            let Ok(id) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                                return Reply::Error("ERR Invalid client ID".to_string());
                            };
                            redirect = Some(id);
                            i += 2;
                        }
                        _ => return Reply::Error("ERR syntax error".to_string()),
                    }
                }
                if !prefixes.is_empty() && !bcast {
                    return Reply::Error("ERR PREFIX option requires BCAST mode to be enabled".to_string());
                }
                let has_redirect = redirect.is_some_and(|id| id != 0);
                if c.resp < 3 && !has_redirect {
                    return Reply::Error("ERR Client tracking is only supported in RESP3 mode, or when a redirect client is set.".to_string());
                }
                match redirect {
                    Some(0) | None => {
                        s.tracking_redirect.remove(&c.id);
                    }
                    Some(id) => {
                        s.tracking_redirect.insert(c.id, id);
                    }
                }
                if noloop {
                    s.tracking_noloop.insert(c.id);
                } else {
                    s.tracking_noloop.remove(&c.id);
                }
                if bcast {
                    s.bcast_tracking.insert(c.id, prefixes);
                    c.tracking = false; // BCAST doesn't use the per-key read table at all.
                } else {
                    s.bcast_tracking.remove(&c.id);
                    c.tracking = true;
                }
                Reply::ok()
            }
            "OFF" => {
                c.tracking = false;
                s.bcast_tracking.remove(&c.id);
                s.tracking_redirect.remove(&c.id);
                s.tracking_noloop.remove(&c.id);
                s.tracking_table.retain(|_, ids| {
                    ids.remove(&c.id);
                    !ids.is_empty()
                });
                Reply::ok()
            }
            _ => Reply::Error("ERR syntax error".to_string()),
        },
        _ => Reply::Error(
            "ERR Unsupported CLIENT subcommand or wrong number of arguments (this fork implements only ID and TRACKING)".to_string(),
        ),
    }
}

// ----------------------------------------------------------------- ACL ---
// `requirepass`/`AUTH default ...`'s "ACL-lite" gate (unchanged from before
// `acl.rs` existed) plus the full multi-user ACL table it now lives
// alongside — see `acl.rs`'s module doc for the split between the two and
// why `requirepass` isn't simply folded into the ACL table bidirectionally.

fn requires_auth_fn(s: &Server, client: &ClientState) -> bool {
    if client.authenticated {
        return false;
    }
    let requirepass_set = !s.config.get_str("requirepass").unwrap_or("").is_empty();
    // `ACL SETUSER default off` / `... resetpass` (with no `requirepass`
    // either) must also gate unauthenticated access — otherwise disabling or
    // depasswording the default user via ACL alone would be silently
    // ineffective.
    let default_needs_auth = s.acl_users.get("default").is_some_and(|u| !u.enabled || !u.nopass);
    requirepass_set || default_needs_auth
}

/// `ACLCheckAllPerm`'s dispatch-time gate — checked once in `Server::dispatch`,
/// the exact same single choke point (and therefore the exact same bypass
/// surface) `requires_auth_fn` already used before `acl.rs` existed; see that
/// module's doc comment for which paths (`PSYNC`/`REPLICAOF`, blocking
/// commands) never reach here at all. `None` means allowed.
fn check_acl_permission(s: &Server, client: &ClientState, name: &str, argv: &[Vec<u8>]) -> Option<Reply> {
    if matches!(name, "AUTH" | "HELLO" | "QUIT" | "RESET") {
        return None;
    }
    let no_perm_for_command = || Some(Reply::Error(format!("NOPERM User {} has no permissions to run the '{}' command", client.user, name.to_ascii_lowercase())));
    let Some(user) = s.acl_users.get(&client.user) else {
        return no_perm_for_command();
    };
    if !user.can_run(name) {
        return no_perm_for_command();
    }
    if let Some(key) = tracked_key(argv) {
        if !user.can_access_key(key) {
            return Some(Reply::Error("NOPERM No permissions to access a key".to_string()));
        }
    }
    if matches!(name, "SUBSCRIBE" | "PSUBSCRIBE" | "PUBLISH") {
        if let Some(channel) = argv.get(1) {
            if !user.can_access_channel(channel) {
                return Some(Reply::Error("NOPERM No permissions to access a channel".to_string()));
            }
        }
    }
    None
}

/// `acl.c`'s `time_independent_strcmp` — a diff-accumulating, no-early-exit
/// comparison so a timing side channel can't leak *how much* of the hash
/// matched. Only meaningful between two equal-length buffers (real Redis's
/// own comment notes this too): both sides here are always a 64-hex-char
/// `sha256_hex` digest, so the length check is just a defensive bailout, not
/// a code path this fork's own callers can actually hit.
fn time_independent_strcmp(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// `AUTH [username] password`. The implicit/explicit `default` username
/// keeps going through `requirepass` exactly as before (plus any extra
/// passwords `ACL SETUSER default >pw` added — either is accepted); any
/// other username is checked against `Server::acl_users` instead. Matches
/// `acl.c`'s actual comparison, not a plaintext one: every password is
/// hashed with `ACLHashPassword`'s algorithm (SHA-256, lowercase hex) before
/// comparing, via `time_independent_strcmp`.
fn cmd_auth(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (username, password) = match argv.len() {
        2 => ("default".to_string(), &argv[1]),
        3 => (ok_bytes_as_string(&argv[1]), &argv[2]),
        _ => return Reply::Error("ERR wrong number of arguments for 'auth' command".to_string()),
    };

    if username.eq_ignore_ascii_case("default") {
        let configured = s.config.get_str("requirepass").unwrap_or("").to_string();
        let acl_default = s.acl_users.get("default");
        // Only an *explicit* ACL password counts as "a password is set" here
        // — the freshly-initialized default user's own `nopass: true` must
        // not (that's exactly the "no requirepass, no ACL changes" starting
        // state, which has always meant "AUTH itself doesn't make sense yet").
        let acl_has_explicit_password = acl_default.is_some_and(|u| !u.nopass && !u.passwords.is_empty());
        if configured.is_empty() && !acl_has_explicit_password {
            return Reply::Error("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?".to_string());
        }
        if !acl_default.is_some_and(|u| u.enabled) {
            return Reply::Error("WRONGPASS invalid username-password pair or user is disabled.".to_string());
        }
        let attempt_hash = redisx_core::sha256::sha256_hex(password);
        let via_requirepass = !configured.is_empty() && {
            let configured_hash = redisx_core::sha256::sha256_hex(configured.as_bytes());
            time_independent_strcmp(configured_hash.as_bytes(), attempt_hash.as_bytes())
        };
        let via_acl = acl_has_explicit_password && acl_default.unwrap().passwords.iter().any(|h| time_independent_strcmp(h.as_bytes(), attempt_hash.as_bytes()));
        if via_requirepass || via_acl {
            c.authenticated = true;
            c.user = "default".to_string();
            Reply::ok()
        } else {
            Reply::Error("WRONGPASS invalid username-password pair or user is disabled.".to_string())
        }
    } else {
        match s.acl_users.get(&username) {
            Some(user) if user.enabled => {
                let attempt_hash = redisx_core::sha256::sha256_hex(password);
                if user.nopass || user.passwords.iter().any(|h| time_independent_strcmp(h.as_bytes(), attempt_hash.as_bytes())) {
                    c.authenticated = true;
                    c.user = username;
                    return Reply::ok();
                }
                Reply::Error("WRONGPASS invalid username-password pair or user is disabled.".to_string())
            }
            _ => Reply::Error("WRONGPASS invalid username-password pair or user is disabled.".to_string()),
        }
    }
}

/// `ACL WHOAMI/LIST/USERS/CAT/GETUSER/SETUSER/DELUSER/DRYRUN/SAVE/LOAD` — see
/// `acl.rs`'s module doc for the scope decisions (small category set, no
/// selectors, no subcommand-level rules, `SAVE`/`LOAD` always erroring the
/// same way real Redis does without an `aclfile` configured).
fn cmd_acl(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "WHOAMI" => Reply::bulk(c.user.clone().into_bytes()),
        "LIST" => Reply::Array(s.acl_users.iter().map(|(name, u)| Reply::bulk(u.describe(name).into_bytes())).collect()),
        "USERS" => Reply::Array(s.acl_users.keys().map(|n| Reply::bulk(n.clone().into_bytes())).collect()),
        "CAT" if argv.len() == 2 => Reply::Array(crate::acl::CATEGORIES.iter().map(|cat| Reply::bulk(cat.as_bytes().to_vec())).collect()),
        "CAT" if argv.len() == 3 => {
            let cat = ok_bytes_as_string(&argv[2]).to_ascii_lowercase();
            if !crate::acl::CATEGORIES.contains(&cat.as_str()) {
                return Reply::Error(format!("ERR Unknown ACL cat '{cat}'"));
            }
            Reply::Array(crate::acl::commands_in_category(&cat).into_iter().map(|name| Reply::bulk(name.to_ascii_lowercase().into_bytes())).collect())
        }
        "GETUSER" if argv.len() == 3 => {
            let name = ok_bytes_as_string(&argv[2]);
            match s.acl_users.get(&name) {
                None => Reply::NullArray,
                Some(u) => {
                    let mut flags = vec![Reply::bulk(if u.enabled { b"on".to_vec() } else { b"off".to_vec() })];
                    if u.nopass {
                        flags.push(Reply::bulk(b"nopass".to_vec()));
                    }
                    flags.push(Reply::bulk(b"sanitize-payload".to_vec()));
                    Reply::Map(vec![
                        (Reply::bulk(b"flags".to_vec()), Reply::Array(flags)),
                        (Reply::bulk(b"passwords".to_vec()), Reply::Array(u.passwords.iter().map(|h| Reply::bulk(h.clone().into_bytes())).collect())),
                        (Reply::bulk(b"commands".to_vec()), Reply::bulk(u.commands_string().into_bytes())),
                        (Reply::bulk(b"keys".to_vec()), Reply::bulk(u.keys_string().into_bytes())),
                        (Reply::bulk(b"channels".to_vec()), Reply::bulk(u.channels_string().into_bytes())),
                        (Reply::bulk(b"selectors".to_vec()), Reply::Array(vec![])),
                    ])
                }
            }
        }
        "SETUSER" if argv.len() >= 3 => {
            let name = ok_bytes_as_string(&argv[2]);
            let rules: Vec<String> = argv[3..].iter().map(|a| ok_bytes_as_string(a)).collect();
            let base = s.acl_users.get(&name).cloned().unwrap_or_else(crate::acl::AclUser::blank);
            match base.apply_rules(&rules) {
                Ok(user) => {
                    s.acl_users.insert(name, user);
                    Reply::ok()
                }
                Err(e) => Reply::Error(format!("ERR {e}")),
            }
        }
        "DELUSER" if argv.len() >= 3 => {
            let names: Vec<String> = argv[2..].iter().map(|a| ok_bytes_as_string(a)).collect();
            if names.iter().any(|n| n == "default") {
                return Reply::Error("ERR The 'default' user cannot be removed".to_string());
            }
            let deleted = names.iter().filter(|n| s.acl_users.remove(*n).is_some()).count();
            Reply::Integer(deleted as i64)
        }
        "DRYRUN" if argv.len() >= 4 => {
            let name = ok_bytes_as_string(&argv[2]);
            let Some(user) = s.acl_users.get(&name) else {
                return Reply::Error(format!("ERR User '{name}' not found"));
            };
            let cmd_name = ok_bytes_as_string(&argv[3]).to_ascii_uppercase();
            if find_command(&cmd_name).is_none() {
                return Reply::Error(format!("ERR Command '{}' not found", cmd_name.to_ascii_lowercase()));
            }
            if !user.can_run(&cmd_name) {
                return Reply::bulk(format!("This user has no permissions to run the '{}' command", cmd_name.to_ascii_lowercase()).into_bytes());
            }
            if let Some(key) = tracked_key(&argv[3..]) {
                if !user.can_access_key(key) {
                    return Reply::bulk(b"This user has no permissions to access the key".to_vec());
                }
            }
            Reply::ok()
        }
        "SAVE" | "LOAD" => Reply::Error(
            "ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.".to_string(),
        ),
        _ => Reply::Error(format!("ERR Unknown ACL subcommand or wrong number of arguments for '{}'", sub.to_ascii_lowercase())),
    }
}

/// `WAIT numreplicas timeout` — a real, much smaller approximation of
/// `replication.c`'s version. This fork's replicas never send `REPLCONF
/// ACK <offset>` back (see `src/bin/redisx-server.rs`'s module doc, Phase
/// 5's own scope note), so there's no acknowledged-offset to actually wait
/// for; this just reports how many replicas are connected *right now*
/// (`Server::connected_replicas`, kept in sync by the binary layer, which
/// owns the real socket list) and returns immediately rather than ever
/// blocking for `timeout` ms. Good enough to prove "is anyone replicating
/// from me at all", not good enough to mean "my last write is durably
/// replicated" — flagged here rather than left to look more authoritative
/// than it is.
fn cmd_wait(s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    Reply::Integer(s.connected_replicas as i64)
}

/// `FAILOVER [ABORT]` — honestly not implemented beyond recognizing the
/// command and explaining why, rather than faking a result. Real
/// `FAILOVER`'s job (`replication.c`'s failover state machine) is to tell
/// the *master* to pick a caught-up replica and signal it, over the
/// replication link itself, to promote; the old master then demotes to
/// replica of the new one. This fork's replication link is a one-directional
/// command stream with no such control back-channel (`REPLCONF ACK` isn't
/// even parsed — see `src/bin/redisx-server.rs`'s module doc), so there is
/// no way to actually deliver a "promote yourself now" signal to a specific
/// connected replica. `REPLICAOF NO ONE` (already implemented) remains the
/// right tool for a manually-coordinated promotion.
fn cmd_failover(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() == 2 && argv[1].eq_ignore_ascii_case(b"ABORT") {
        return Reply::Error("ERR No failover in progress.".to_string());
    }
    if !matches!(s.role, Role::Master) {
        return Reply::Error("ERR FAILOVER is only supported on a master.".to_string());
    }
    if s.connected_replicas == 0 {
        return Reply::Error("ERR FAILOVER requires connected replicas.".to_string());
    }
    Reply::Error(
        "ERR FAILOVER is not supported by this fork: it would need a control back-channel over the replication \
         link to remotely tell a specific replica to promote itself, which this fork's one-directional command \
         stream doesn't have. Use REPLICAOF NO ONE on the replica you want to promote instead."
            .to_string(),
    )
}

// ------------------------------------------------------------- Cluster ---
// `CLUSTER` subcommands, `ASKING`/`READONLY`/`READWRITE`, and the
// `MOVED`/`CROSSSLOT`/`CLUSTERDOWN` redirect check wired into `dispatch`
// right after the ACL check above. `cluster.rs` has the actual hash-slot
// math, gossip protocol, and state — see its module doc for the full scope
// (what's real vs. deliberately narrowed) before reading these handlers.
// `CLUSTER MEET`/`REPLICATE` are the two subcommands that need real network
// I/O (a gossip handshake / kicking off `REPLICAOF`'s own handshake) and so,
// exactly like `PSYNC`/`REPLICAOF` themselves, never reach here at all —
// `src/bin/redisx-server.rs` intercepts them before `dispatch` and handles
// them directly against `Server::cluster`/`Server::role`.

fn cluster_disabled_error() -> Reply {
    Reply::Error("ERR This instance has cluster support disabled".to_string())
}

fn cmd_asking(_s: &mut Server, _c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    // Accepted for client compatibility, never consulted — see
    // `ClientState::cluster_readonly`'s doc comment on why there's no
    // ASK/migration dance for this flag to matter to.
    Reply::ok()
}

fn cmd_readonly(_s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    c.cluster_readonly = true;
    Reply::ok()
}

fn cmd_readwrite(_s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    c.cluster_readonly = false;
    Reply::ok()
}

/// Parses one or more `slot` (bare) or `start end` (range) arguments for
/// `ADDSLOTS`/`DELSLOTS`/`ADDSLOTSRANGE`/`DELSLOTSRANGE`, expanding ranges
/// into individual slot numbers. Returns `Err` on anything malformed or out
/// of `0..CLUSTER_SLOTS` range, same as real Redis rejecting the whole batch
/// rather than applying a prefix of it.
fn parse_slot_args(argv: &[Vec<u8>], ranges: bool) -> Result<Vec<u16>, Reply> {
    let bad = || Reply::Error("ERR Invalid or out of range slot".to_string());
    let mut out = Vec::new();
    if ranges {
        if !argv.len().is_multiple_of(2) {
            return Err(Reply::Error("ERR wrong number of arguments".to_string()));
        }
        for pair in argv.chunks(2) {
            let start: u16 = ok_bytes_as_string(&pair[0]).parse().map_err(|_| bad())?;
            let end: u16 = ok_bytes_as_string(&pair[1]).parse().map_err(|_| bad())?;
            if start > end || end >= cluster::CLUSTER_SLOTS {
                return Err(bad());
            }
            out.extend(start..=end);
        }
    } else {
        for raw in argv {
            let slot: u16 = ok_bytes_as_string(raw).parse().map_err(|_| bad())?;
            if slot >= cluster::CLUSTER_SLOTS {
                return Err(bad());
            }
            out.push(slot);
        }
    }
    Ok(out)
}

/// `CLUSTER ADDSLOTS(RANGE)` / `DELSLOTS(RANGE)` — validates every slot in
/// the batch *before* applying any of them (matching `clusterCommand`'s own
/// all-or-nothing behavior), then assigns/unassigns each to `myself`.
fn cmd_cluster_slots_mutation(s: &mut Server, argv: &[Vec<u8>], ranges: bool, del: bool) -> Reply {
    if !s.cluster.enabled {
        return cluster_disabled_error();
    }
    let slots = match parse_slot_args(argv, ranges) {
        Ok(s) => s,
        Err(e) => return e,
    };
    for &slot in &slots {
        let owned_by_me = s.cluster.myself().covers(slot);
        if del {
            if !owned_by_me {
                return Reply::Error(format!("ERR Slot {slot} is already unassigned"));
            }
        } else if s.cluster.slot_owner(slot).is_some() {
            return Reply::Error(format!("ERR Slot {slot} is already busy"));
        }
    }
    for slot in slots {
        if del {
            s.cluster.myself_mut().remove_slot(slot);
        } else {
            s.cluster.myself_mut().add_slot(slot);
        }
    }
    Reply::ok()
}

fn cmd_cluster(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "MYID" => Reply::bulk(s.cluster.my_id.clone().into_bytes()),
        "KEYSLOT" if argv.len() == 3 => Reply::Integer(cluster::key_hash_slot(&argv[2]) as i64),
        "INFO" => {
            let require_full_coverage = s.config.get_str("cluster-require-full-coverage") != Some("no");
            Reply::bulk(cluster::info_text(&s.cluster, require_full_coverage).into_bytes())
        }
        "NODES" => {
            if !s.cluster.enabled {
                return Reply::bulk(Vec::new());
            }
            Reply::bulk(cluster::nodes_description(&s.cluster).into_bytes())
        }
        "SLOTS" => {
            if !s.cluster.enabled {
                return Reply::Array(vec![]);
            }
            Reply::Array(
                cluster::slot_ranges(&s.cluster)
                    .into_iter()
                    .map(|r| {
                        let mut entry = vec![
                            Reply::Integer(r.start as i64),
                            Reply::Integer(r.end as i64),
                            Reply::Array(vec![Reply::bulk(r.master.ip.clone().into_bytes()), Reply::Integer(r.master.port as i64), Reply::bulk(r.master.id.clone().into_bytes())]),
                        ];
                        for rep in r.replicas {
                            entry.push(Reply::Array(vec![Reply::bulk(rep.ip.clone().into_bytes()), Reply::Integer(rep.port as i64), Reply::bulk(rep.id.clone().into_bytes())]));
                        }
                        Reply::Array(entry)
                    })
                    .collect(),
            )
        }
        "SHARDS" => {
            if !s.cluster.enabled {
                return Reply::Array(vec![]);
            }
            Reply::Array(
                cluster::shards(&s.cluster)
                    .into_iter()
                    .map(|shard| {
                        let slots = Reply::Array(shard.slots.iter().flat_map(|&(a, b)| [Reply::Integer(a as i64), Reply::Integer(b as i64)]).collect());
                        let nodes = Reply::Array(
                            shard
                                .nodes
                                .iter()
                                .map(|n| {
                                    Reply::Map(vec![
                                        (Reply::bulk(b"id".to_vec()), Reply::bulk(n.id.clone().into_bytes())),
                                        (Reply::bulk(b"ip".to_vec()), Reply::bulk(n.ip.clone().into_bytes())),
                                        (Reply::bulk(b"port".to_vec()), Reply::Integer(n.port as i64)),
                                        (Reply::bulk(b"role".to_vec()), Reply::bulk(if n.role == cluster::NodeRole::Master { b"master".to_vec() } else { b"replica".to_vec() })),
                                    ])
                                })
                                .collect(),
                        );
                        Reply::Map(vec![(Reply::bulk(b"slots".to_vec()), slots), (Reply::bulk(b"nodes".to_vec()), nodes)])
                    })
                    .collect(),
            )
        }
        "COUNTKEYSINSLOT" if argv.len() == 3 => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let Ok(slot) = ok_bytes_as_string(&argv[2]).parse::<u16>() else { return Reply::Error("ERR Invalid slot".to_string()) };
            let Some(dict) = s.dbs[SLOT].dict.get_dict(SLOT) else { return Reply::Integer(0) };
            Reply::Integer(dict.iter().filter(|(k, _)| cluster::key_hash_slot(k) == slot).count() as i64)
        }
        "GETKEYSINSLOT" if argv.len() == 4 => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let Ok(slot) = ok_bytes_as_string(&argv[2]).parse::<u16>() else { return Reply::Error("ERR Invalid slot".to_string()) };
            let Ok(count) = ok_bytes_as_string(&argv[3]).parse::<usize>() else { return Reply::Error("ERR Invalid count".to_string()) };
            let Some(dict) = s.dbs[SLOT].dict.get_dict(SLOT) else { return Reply::Array(vec![]) };
            Reply::Array(dict.iter().filter(|(k, _)| cluster::key_hash_slot(k) == slot).take(count).map(|(k, _)| Reply::bulk(k.clone())).collect())
        }
        "ADDSLOTS" => cmd_cluster_slots_mutation(s, &argv[2..], false, false),
        "DELSLOTS" => cmd_cluster_slots_mutation(s, &argv[2..], false, true),
        "ADDSLOTSRANGE" => cmd_cluster_slots_mutation(s, &argv[2..], true, false),
        "DELSLOTSRANGE" => cmd_cluster_slots_mutation(s, &argv[2..], true, true),
        "SETSLOT" if argv.len() >= 4 => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let Ok(slot) = ok_bytes_as_string(&argv[2]).parse::<u16>() else { return Reply::Error("ERR Invalid slot".to_string()) };
            if slot >= cluster::CLUSTER_SLOTS {
                return Reply::Error("ERR Invalid slot".to_string());
            }
            match ok_bytes_as_string(&argv[3]).to_ascii_uppercase().as_str() {
                "NODE" if argv.len() == 5 => {
                    let node_id = ok_bytes_as_string(&argv[4]);
                    if !s.cluster.nodes.contains_key(&node_id) {
                        return Reply::Error(format!("ERR Unknown node {node_id}"));
                    }
                    let owner_ids: Vec<String> = s.cluster.nodes.values().filter(|n| n.covers(slot)).map(|n| n.id.clone()).collect();
                    for id in owner_ids {
                        if let Some(n) = s.cluster.nodes.get_mut(&id) {
                            n.remove_slot(slot);
                        }
                    }
                    s.cluster.nodes.get_mut(&node_id).expect("just checked contains_key").add_slot(slot);
                    Reply::ok()
                }
                "IMPORTING" | "MIGRATING" | "STABLE" => Reply::Error(
                    "ERR CLUSTER SETSLOT IMPORTING/MIGRATING/STABLE (the live resharding dance) is not supported by \
                     this fork; use 'CLUSTER SETSLOT <slot> NODE <node-id>' for an immediate forced reassignment \
                     instead (see cluster.rs's module doc for the scope decision)."
                        .to_string(),
                ),
                _ => Reply::Error("ERR wrong number of arguments for 'cluster|setslot' command".to_string()),
            }
        }
        "BUMPEPOCH" => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let new_epoch = s.cluster.nodes.values().map(|n| n.config_epoch).max().unwrap_or(0) + 1;
            s.cluster.myself_mut().config_epoch = new_epoch;
            s.cluster.current_epoch = s.cluster.current_epoch.max(new_epoch);
            Reply::SimpleString(format!("BUMPED {new_epoch}"))
        }
        "SET-CONFIG-EPOCH" if argv.len() == 3 => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let Ok(epoch) = ok_bytes_as_string(&argv[2]).parse::<u64>() else { return Reply::Error("ERR Invalid config epoch".to_string()) };
            if s.cluster.myself().config_epoch != 0 {
                return Reply::Error("ERR Node config epoch is already non-zero".to_string());
            }
            let my_id = s.cluster.my_id.clone();
            if s.cluster.nodes.values().any(|n| n.id != my_id && n.config_epoch == epoch) {
                return Reply::Error("ERR Node config epoch is already in use by another node".to_string());
            }
            s.cluster.myself_mut().config_epoch = epoch;
            s.cluster.current_epoch = s.cluster.current_epoch.max(epoch);
            Reply::ok()
        }
        "FORGET" if argv.len() == 3 => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let node_id = ok_bytes_as_string(&argv[2]);
            if node_id == s.cluster.my_id {
                return Reply::Error("ERR I tried hard but I can't forget myself...".to_string());
            }
            if s.cluster.nodes.remove(&node_id).is_none() {
                return Reply::Error(format!("ERR No such node ID {node_id}"));
            }
            Reply::ok()
        }
        "RESET" => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            let hard = argv.get(2).is_some_and(|a| a.eq_ignore_ascii_case(b"HARD"));
            let (ip, port, cport) = {
                let m = s.cluster.myself();
                (m.ip.clone(), m.port, m.cport)
            };
            let my_id = s.cluster.my_id.clone();
            s.cluster.nodes.retain(|id, _| *id == my_id);
            s.cluster.myself_mut().slots.clear();
            s.cluster.myself_mut().role = cluster::NodeRole::Master;
            s.cluster.myself_mut().master_id = None;
            s.cluster.myself_mut().config_epoch = 0;
            s.role = Role::Master;
            if hard {
                s.cluster.nodes.remove(&my_id);
                let new_state = {
                    let mut fresh = cluster::ClusterState::new();
                    fresh.activate(ip, port, cport);
                    fresh
                };
                s.cluster = new_state;
            }
            Reply::ok()
        }
        "FLUSHSLOTS" => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            s.cluster.myself_mut().slots.clear();
            Reply::ok()
        }
        "SAVECONFIG" => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            // No `nodes.conf` persistence in this fork — see `cluster.rs`'s
            // module doc. Accepted (not rejected) since there's nothing
            // *wrong* about the request, just nothing to actually save.
            Reply::ok()
        }
        "COUNT-FAILURE-REPORTS" if argv.len() == 3 => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            Reply::Integer(0)
        }
        "LINKS" => {
            if !s.cluster.enabled {
                return cluster_disabled_error();
            }
            Reply::Array(vec![])
        }
        "REPLICATE" | "MEET" => Reply::Error(format!(
            "ERR CLUSTER {sub} requires direct execution (it needs real network I/O — see src/bin/redisx-server.rs's \
             interception of it before dispatch) and so isn't available from inside MULTI/EXEC or scripting."
        )),
        "FAILOVER" => Reply::Error(
            "ERR CLUSTER FAILOVER is not supported by this fork, for the same reason the non-cluster FAILOVER \
             command isn't (see FAILOVER's own error message) — promote a replica manually with CLUSTER RESET + \
             CLUSTER ADDSLOTS/SETSLOT instead."
                .to_string(),
        ),
        "HELP" => Reply::Array(vec![Reply::SimpleString("CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_string())]),
        _ => Reply::Error(format!("ERR Unknown CLUSTER subcommand or wrong number of arguments for '{}'", sub.to_ascii_lowercase())),
    }
}

/// Extracts the key(s) a command touches, for `CROSSSLOT` detection and
/// `MOVED` routing. Unlike `tracked_key`'s blanket "`argv[1]`, whatever that
/// is" approximation (harmless if wrong for ACL's key check — just one extra
/// spurious permission check), a false positive here would misroute ordinary
/// non-key commands (`CONFIG GET maxmemory` would see `"GET"` as a fake key
/// and get redirected). So: an explicit per-command allowlist; anything not
/// listed here is treated as keyless (never redirected) rather than guessed
/// at — a false *negative* (a real key-bearing command this table doesn't
/// yet recognize) just means that one command type isn't cluster-redirect-
/// aware yet, a far smaller problem than misrouting admin commands.
fn cluster_command_keys<'a>(name: &str, argv: &'a [Vec<u8>]) -> Vec<&'a Vec<u8>> {
    const SINGLE_KEY_AT_1: &[&str] = &[
        "APPEND", "BITCOUNT", "BITPOS", "DECR", "DECRBY", "DUMP", "EXPIRE", "EXPIREAT", "EXPIRETIME", "GEOADD", "GEODIST", "GEOPOS", "GEOSEARCH", "GET", "GETBIT",
        "GETDEL", "GETEX", "GETRANGE", "HDEL", "HEXISTS", "HGET", "HGETALL", "HINCRBY", "HINCRBYFLOAT", "HKEYS", "HLEN", "HMGET", "HRANDFIELD", "HSCAN", "HSET",
        "HSETNX", "HVALS", "INCR", "INCRBY", "INCRBYFLOAT", "LINDEX", "LINSERT", "LLEN", "LPOP", "LPOS", "LPUSH", "LPUSHX", "LRANGE", "LREM", "LSET", "LTRIM",
        "MOVE", "PERSIST", "PEXPIRE", "PEXPIREAT", "PEXPIRETIME", "PFADD", "PTTL", "RESTORE", "RPOP", "RPUSH", "RPUSHX", "SADD", "SCARD", "SET", "SETBIT",
        "SETRANGE", "SISMEMBER", "SMISMEMBER", "SMEMBERS", "SORT", "SPOP", "SRANDMEMBER", "SREM", "SSCAN", "STRLEN", "TTL", "TYPE", "XACK", "XADD", "XAUTOCLAIM",
        "XCLAIM", "XDEL", "XLEN", "XPENDING", "XRANGE", "XREVRANGE", "XSETID", "XTRIM", "ZADD", "ZCARD", "ZCOUNT", "ZINCRBY", "ZLEXCOUNT", "ZMSCORE", "ZPOPMAX",
        "ZPOPMIN", "ZRANDMEMBER", "ZRANGE", "ZRANGEBYLEX", "ZRANGEBYSCORE", "ZRANK", "ZREM", "ZREVRANGE", "ZREVRANGEBYLEX", "ZREVRANGEBYSCORE", "ZREVRANK",
        "ZSCAN", "ZSCORE",
        // `--features polars`'s `DF.*` family (`dataframe.rs`) — harmless to
        // list unconditionally even in a non-polars build, since these names
        // just never show up as `find_command`-resolvable commands there.
        "DF.FROMCSV", "DF.FROMPARQUET", "DF.SET", "DF.GET", "DF.TOCSV", "DF.TOPARQUET", "DF.SHAPE", "DF.COLUMNS", "DF.QUERY", "DF.PRETTY",
    ];
    const MULTI_KEY_ALL_ARGS: &[&str] = &["DEL", "EXISTS", "MGET", "PFCOUNT", "SDIFF", "SINTER", "SUNION", "TOUCH", "UNLINK", "WATCH"];
    const TWO_KEYS_AT_1_2: &[&str] = &["COPY", "GEOSEARCHSTORE", "LMOVE", "RENAME", "RENAMENX", "RPOPLPUSH", "SMOVE"];

    if SINGLE_KEY_AT_1.contains(&name) {
        return argv.get(1).into_iter().collect();
    }
    if MULTI_KEY_ALL_ARGS.contains(&name) {
        return argv[1..].iter().collect();
    }
    if TWO_KEYS_AT_1_2.contains(&name) {
        return argv.iter().skip(1).take(2).collect();
    }
    match name {
        "MSET" | "MSETNX" => argv[1..].iter().step_by(2).collect(),
        "BITOP" => argv[2..].iter().collect(),
        "PFMERGE" | "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => argv[1..].iter().collect(),
        "OBJECT" | "XGROUP" | "XINFO" => argv.get(2).into_iter().collect(),
        "MEMORY" if argv.get(1).is_some_and(|a| a.eq_ignore_ascii_case(b"USAGE")) => argv.get(2).into_iter().collect(),
        "DEBUG" if argv.get(1).is_some_and(|a| a.eq_ignore_ascii_case(b"OBJECT")) => argv.get(2).into_iter().collect(),
        "EVAL" | "EVALSHA" | "FCALL" | "FCALL_RO" => numkeys_prefixed(argv, 2),
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" => {
            let mut keys: Vec<&Vec<u8>> = argv.get(1).into_iter().collect();
            keys.extend(numkeys_prefixed(argv, 2));
            keys
        }
        "ZUNION" | "ZINTER" | "ZDIFF" | "ZINTERCARD" => numkeys_prefixed(argv, 1),
        "XREAD" | "XREADGROUP" => {
            let Some(pos) = argv.iter().position(|a| a.eq_ignore_ascii_case(b"STREAMS")) else { return vec![] };
            let rest = &argv[pos + 1..];
            rest[..rest.len() / 2].iter().collect()
        }
        _ => vec![],
    }
}

/// `EVAL script numkeys key [key...] arg...` / `FCALL fn numkeys key
/// [key...] arg...` / `ZUNION numkeys key [key...]`-shaped commands: a
/// `numkeys` count sits at `argv[numkeys_pos]`, the keys immediately follow.
/// Malformed/missing `numkeys` just yields no keys (keeping with this
/// table's "unrecognized means keyless" rule above) rather than erroring —
/// the command's own handler is what actually validates and reports that.
fn numkeys_prefixed(argv: &[Vec<u8>], numkeys_pos: usize) -> Vec<&Vec<u8>> {
    let Some(numkeys) = argv.get(numkeys_pos).and_then(|v| ok_bytes_as_string(v).parse::<usize>().ok()) else { return vec![] };
    argv.iter().skip(numkeys_pos + 1).take(numkeys).collect()
}

/// The `MOVED`/`ASK`/`CROSSSLOT`/`CLUSTERDOWN` gate — checked once in
/// `Server::dispatch`, right after the ACL check (see that call site), so it
/// inherits the exact same bypass surface (`PSYNC`/`REPLICAOF`, the
/// `BLPOP`-family/`XREAD`/`XREADGROUP BLOCK` commands never reach `dispatch`
/// at all — `cluster.rs`'s module doc flags this as an accepted, inherited
/// gap, same as `acl.rs`'s). `None` means "allowed to run locally".
fn check_cluster_redirect(s: &Server, c: &ClientState, name: &str, argv: &[Vec<u8>], is_write: bool) -> Option<Reply> {
    if !s.cluster.enabled {
        return None;
    }
    let keys = cluster_command_keys(name, argv);
    if keys.is_empty() {
        return None;
    }
    let mut slot = None;
    for key in &keys {
        let key_slot = cluster::key_hash_slot(key);
        match slot {
            None => slot = Some(key_slot),
            Some(existing) if existing != key_slot => return Some(Reply::Error("CROSSSLOT Keys in request don't hash to the same slot".to_string())),
            _ => {}
        }
    }
    let slot = slot?;
    let require_full_coverage = s.config.get_str("cluster-require-full-coverage") != Some("no");
    match s.cluster.slot_owner(slot) {
        None => {
            if require_full_coverage {
                Some(Reply::Error("CLUSTERDOWN Hash slot not served".to_string()))
            } else {
                None
            }
        }
        Some(owner) if owner.id == s.cluster.my_id => None,
        Some(owner) => {
            // A replica may serve a stale read for its own master's slots
            // once the client has issued `READONLY` — real Redis Cluster's
            // read-from-replica mode, ported as-is.
            let serving_as_readonly_replica = !is_write && c.cluster_readonly && s.cluster.myself().master_id.as_deref() == Some(owner.id.as_str());
            if serving_as_readonly_replica {
                None
            } else {
                Some(Reply::Error(format!("MOVED {slot} {}:{}", owner.ip, owner.port)))
            }
        }
    }
}

/// Best-effort "what key did this command touch" heuristic for `CLIENT
/// TRACKING`'s default mode: `argv[1]`, correct for every key-bearing
/// command this fork implements. A handful of non-key-bearing, non-write
/// commands (`CONFIG GET ...`, `CLIENT ID`, `PUBSUB CHANNELS`, ...) also
/// have *something* at `argv[1]` and so get a harmless, never-invalidated
/// spurious entry in `tracking_table` if a tracking client runs them — cleaned
/// up like any other entry the moment that client disconnects or turns
/// tracking off. Accepted simplification rather than an explicit per-command
/// key-spec table (`tracking.c`'s real equivalent).
fn tracked_key(argv: &[Vec<u8>]) -> Option<&Vec<u8>> {
    argv.get(1)
}

// ------------------------------------------------------------ Scripting ---
// See `script.rs`'s module doc for the engine-abstraction design and why
// `redis.call`'s reentrancy needs a thread-local raw pointer.

fn cmd_function(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" => {
            let (replace, code_idx) =
                if argv.len() >= 4 && argv[2].eq_ignore_ascii_case(b"REPLACE") { (true, 3) } else { (false, 2) };
            if argv.len() <= code_idx {
                return Reply::Error("ERR wrong number of arguments for 'function|load' command".to_string());
            }
            s.function_load(&ok_bytes_as_string(&argv[code_idx]), replace)
        }
        "DELETE" if argv.len() == 3 => s.function_delete(&ok_bytes_as_string(&argv[2])),
        "LIST" => s.function_list(),
        // `FUNCTION FLUSH [ASYNC|SYNC]` — real Redis's modifier is accepted
        // (so a script written against real Redis doesn't error here) but
        // has no effect: see `Server::function_flush`'s own doc for why
        // this fork has nothing for it to actually change.
        "FLUSH" if argv.len() <= 3 => {
            if let Some(modifier) = argv.get(2) {
                let modifier = ok_bytes_as_string(modifier).to_ascii_uppercase();
                if modifier != "ASYNC" && modifier != "SYNC" {
                    return Reply::Error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option".to_string());
                }
            }
            s.function_flush()
        }
        _ => Reply::Error("ERR Unsupported FUNCTION subcommand or wrong number of arguments".to_string()),
    }
}

/// `FCALL`/`FCALL_RO` share this one handler (`argv[0]` still carries
/// whichever name was actually invoked, since the `CommandSpec` table just
/// points both at the same function pointer) — `read_only` is derived from
/// that, then threaded all the way down to `redis_call_impl` via
/// `script.rs`'s thread-local context, closing the gap where `FCALL_RO`
/// used to accept writes exactly like plain `FCALL`.
fn cmd_fcall(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let read_only = argv[0].eq_ignore_ascii_case(b"FCALL_RO");
    let funcname = ok_bytes_as_string(&argv[1]);
    let Ok(numkeys) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    if numkeys < 0 {
        return Reply::Error("ERR Number of keys can't be negative".to_string());
    }
    let numkeys = numkeys as usize;
    if argv.len() < 3 + numkeys {
        return Reply::Error("ERR Number of keys can't be greater than number of args".to_string());
    }
    let keys = argv[3..3 + numkeys].to_vec();
    let args = argv[3 + numkeys..].to_vec();
    s.call_function(&funcname, &keys, &args, c.db_index, &c.peer, read_only)
}

/// `script.rs`'s `redis_call_impl` uses this to reject a write command
/// attempted through `redis.call`/`redis.pcall` while running under
/// `FCALL_RO`.
pub(crate) fn is_write_command(name: &str) -> bool {
    find_command(&name.to_ascii_uppercase()).is_some_and(|spec| spec.is_write)
}

/// `(numkeys, keys, args)`, split out of an `EVAL`/`EVALSHA`-shaped argv.
type NumkeysKeysArgs = (usize, Vec<Vec<u8>>, Vec<Vec<u8>>);

/// Shared `EVAL`/`EVALSHA` numkeys/key/arg parsing — identical shape to
/// `FCALL`'s (`eval.c` and `functions.c` agree on this convention).
fn parse_numkeys_and_split(argv: &[Vec<u8>]) -> Result<NumkeysKeysArgs, Reply> {
    let Ok(numkeys) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Err(Reply::Error("ERR value is not an integer or out of range".to_string()));
    };
    if numkeys < 0 {
        return Err(Reply::Error("ERR Number of keys can't be negative".to_string()));
    }
    let numkeys = numkeys as usize;
    if argv.len() < 3 + numkeys {
        return Err(Reply::Error("ERR Number of keys can't be greater than number of args".to_string()));
    }
    Ok((numkeys, argv[3..3 + numkeys].to_vec(), argv[3 + numkeys..].to_vec()))
}

/// `EVAL script numkeys [key ...] [arg ...]` — legacy scripting (`eval.c`),
/// Lua-only even in this fork (real Redis never extended `EVAL` itself to
/// other engines; only `FUNCTION`'s newer vtable supports more than Lua).
/// Every `EVAL` also caches its script under `SHA1(script)`, exactly like
/// real Redis, so a later `EVALSHA` of the same body works without an
/// explicit `SCRIPT LOAD` first.
fn cmd_eval(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let body = ok_bytes_as_string(&argv[1]);
    let (_, keys, args) = match parse_numkeys_and_split(argv) {
        Ok(v) => v,
        Err(e) => return e,
    };
    s.script_cache.insert(redisx_core::sha1::sha1_hex(body.as_bytes()), body.clone());
    s.eval_lua_script(&body, &keys, &args, c.db_index, &c.peer)
}

fn cmd_evalsha(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sha = ok_bytes_as_string(&argv[1]).to_ascii_lowercase();
    let Some(body) = s.script_cache.get(&sha).cloned() else {
        return Reply::Error("NOSCRIPT No matching script. Please use EVAL.".to_string());
    };
    let (_, keys, args) = match parse_numkeys_and_split(argv) {
        Ok(v) => v,
        Err(e) => return e,
    };
    s.eval_lua_script(&body, &keys, &args, c.db_index, &c.peer)
}

fn cmd_script(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" if argv.len() == 3 => {
            let body = ok_bytes_as_string(&argv[2]);
            let sha = redisx_core::sha1::sha1_hex(body.as_bytes());
            s.script_cache.insert(sha.clone(), body);
            Reply::bulk(sha.into_bytes())
        }
        "EXISTS" if argv.len() >= 2 => Reply::Array(
            argv[2..]
                .iter()
                .map(|a| Reply::Integer(s.script_cache.contains_key(&ok_bytes_as_string(a).to_ascii_lowercase()) as i64))
                .collect(),
        ),
        "FLUSH" => {
            s.script_cache.clear();
            Reply::ok()
        }
        _ => Reply::Error("ERR Unknown SCRIPT subcommand or wrong number of arguments".to_string()),
    }
}

/// `KvStore` is a dict-per-cluster-slot array (see `redisx-core::kvstore`);
/// Phase 2 has no cluster slotting yet, so every `Database` is created with
/// `num_dicts_bits=0` (a single dict) and always addressed at index 0 — one
/// more dict-index bit is literally all Phase 5 (cluster) will need to add.
const SLOT: usize = 0;

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

/// `SET key value [NX | XX] [GET] [EX seconds | PX ms | EXAT unix-secs | PXAT unix-ms | KEEPTTL]`.
/// `BY`-pattern-free equivalents of real `SET`'s full option set; `IDLE`/
/// object-freshness options don't exist in this fork at all (no LRU/LFU
/// tracking), so there's nothing to port there.
fn cmd_set(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut expire_at_ms: Option<i64> = None;
    let mut keep_ttl = false;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut i = 3;
    while i < argv.len() {
        let opt = ok_bytes_as_string(&argv[i]).to_ascii_uppercase();
        match opt.as_str() {
            "KEEPTTL" => {
                keep_ttl = true;
                i += 1;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GET" => {
                get = true;
                i += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                expire_at_ms = Some(match opt.as_str() {
                    "EX" => now_ms().saturating_add(n.saturating_mul(1000)),
                    "PX" => now_ms().saturating_add(n),
                    "EXAT" => n.saturating_mul(1000),
                    "PXAT" => n,
                    _ => unreachable!(),
                });
                i += 2;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    if (expire_at_ms.is_some() && keep_ttl) || (nx && xx) {
        return Reply::Error("ERR syntax error".to_string());
    }

    let existing = s.dbs[c.db_index].dict.get(SLOT, &argv[1]);
    let old_reply = if get {
        match existing {
            None => Some(Reply::Null),
            Some(RObj::String(v)) => Some(Reply::bulk(v.clone())),
            Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        }
    } else {
        None
    };
    let key_exists_now = existing.is_some();
    if (nx && key_exists_now) || (xx && !key_exists_now) {
        return old_reply.unwrap_or(Reply::Null);
    }

    let overwritten = s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::String(argv[2].clone()));
    if let Some(old) = overwritten {
        s.lazyfree_if_enabled("lazyfree-lazy-server-del", LazyFreeJob::Value(old));
    }
    if !keep_ttl {
        s.dbs[c.db_index].expires.remove(&argv[1]);
    }
    if let Some(at) = expire_at_ms {
        s.dbs[c.db_index].expires.insert(argv[1].clone(), at);
    }
    notify_keyspace_event(s, c.db_index, 'g', "set", &argv[1]);
    old_reply.unwrap_or_else(Reply::ok)
}

fn key_exists(s: &Server, dbidx: usize, key: &[u8]) -> bool {
    s.dbs[dbidx].dict.get(SLOT, &key.to_vec()).is_some()
}

/// `NX`/`XX`/`GT`/`LT` — the optional trailing flag every `EXPIRE`-family
/// setter now accepts. A key with no existing TTL is treated as having an
/// "infinite" one for `GT`/`LT`'s comparison, matching real Redis exactly:
/// `GT` can never succeed against no-TTL (nothing beats infinite), `LT`
/// always succeeds against it (anything is less than infinite).
enum ExpireCond {
    Any,
    Nx,
    Xx,
    Gt,
    Lt,
}

fn parse_expire_cond(argv: &[Vec<u8>], idx: usize) -> Result<ExpireCond, Reply> {
    if idx >= argv.len() {
        return Ok(ExpireCond::Any);
    }
    if idx + 1 != argv.len() {
        return Err(Reply::Error("ERR syntax error".to_string()));
    }
    match ok_bytes_as_string(&argv[idx]).to_ascii_uppercase().as_str() {
        "NX" => Ok(ExpireCond::Nx),
        "XX" => Ok(ExpireCond::Xx),
        "GT" => Ok(ExpireCond::Gt),
        "LT" => Ok(ExpireCond::Lt),
        _ => Err(Reply::Error("ERR Unsupported option".to_string())),
    }
}

fn expire_cond_allows(cond: &ExpireCond, existing_ttl: Option<i64>, new_at_ms: i64) -> bool {
    match cond {
        ExpireCond::Any => true,
        ExpireCond::Nx => existing_ttl.is_none(),
        ExpireCond::Xx => existing_ttl.is_some(),
        ExpireCond::Gt => matches!(existing_ttl, Some(old) if new_at_ms > old),
        ExpireCond::Lt => existing_ttl.is_none_or(|old| new_at_ms < old),
    }
}

/// Shared tail for every `EXPIRE`-family setter, given an already-computed
/// absolute expiry in ms. Matching real Redis: a key with no value can't
/// have a TTL set on it (returns `0`), and an expiry time that's already in
/// the past deletes the key immediately rather than storing a stale TTL
/// (the active-expire sweep would catch it on the very next command anyway —
/// this just skips that one extra round trip).
fn set_expire_at(s: &mut Server, c: &mut ClientState, key: &[u8], at_ms: i64, cond: &ExpireCond) -> Reply {
    if !key_exists(s, c.db_index, key) {
        return Reply::Integer(0);
    }
    let existing_ttl = s.dbs[c.db_index].expires.get(key).copied();
    if !expire_cond_allows(cond, existing_ttl, at_ms) {
        return Reply::Integer(0);
    }
    if at_ms <= now_ms() {
        s.dbs[c.db_index].remove_key(key);
        notify_keyspace_event(s, c.db_index, 'g', "del", key);
    } else {
        s.dbs[c.db_index].expires.insert(key.to_vec(), at_ms);
        notify_keyspace_event(s, c.db_index, 'g', "expire", key);
    }
    Reply::Integer(1)
}

fn cmd_expire(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let secs = match parse_i64_arg(argv, 2) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let cond = match parse_expire_cond(argv, 3) {
        Ok(c) => c,
        Err(e) => return e,
    };
    set_expire_at(s, c, &argv[1], now_ms().saturating_add(secs.saturating_mul(1000)), &cond)
}

fn cmd_pexpire(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let ms = match parse_i64_arg(argv, 2) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let cond = match parse_expire_cond(argv, 3) {
        Ok(c) => c,
        Err(e) => return e,
    };
    set_expire_at(s, c, &argv[1], now_ms().saturating_add(ms), &cond)
}

fn cmd_expireat(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let secs = match parse_i64_arg(argv, 2) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let cond = match parse_expire_cond(argv, 3) {
        Ok(c) => c,
        Err(e) => return e,
    };
    set_expire_at(s, c, &argv[1], secs.saturating_mul(1000), &cond)
}

fn cmd_pexpireat(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let at_ms = match parse_i64_arg(argv, 2) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let cond = match parse_expire_cond(argv, 3) {
        Ok(c) => c,
        Err(e) => return e,
    };
    set_expire_at(s, c, &argv[1], at_ms, &cond)
}

fn cmd_persist(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if !key_exists(s, c.db_index, &argv[1]) {
        return Reply::Integer(0);
    }
    let removed = s.dbs[c.db_index].expires.remove(&argv[1]).is_some();
    if removed {
        notify_keyspace_event(s, c.db_index, 'g', "persist", &argv[1]);
    }
    Reply::Integer(removed as i64)
}

/// `TTL`/`EXPIRETIME` reply `-2` if the key is missing, `-1` if it exists but
/// has no TTL. Since `run_command` sweeps expired keys before any handler
/// runs (see its doc comment), a logically-expired key is already physically
/// gone by the time these ever see it — no separate "is it expired" check
/// needed here.
fn cmd_ttl(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(-2),
        Some(_) => match s.dbs[c.db_index].expires.get(&argv[1]) {
            None => Reply::Integer(-1),
            Some(&at) => Reply::Integer((at - now_ms() + 500) / 1000),
        },
    }
}

fn cmd_pttl(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(-2),
        Some(_) => match s.dbs[c.db_index].expires.get(&argv[1]) {
            None => Reply::Integer(-1),
            Some(&at) => Reply::Integer(at - now_ms()),
        },
    }
}

fn cmd_expiretime(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(-2),
        Some(_) => match s.dbs[c.db_index].expires.get(&argv[1]) {
            None => Reply::Integer(-1),
            Some(&at) => Reply::Integer(at / 1000),
        },
    }
}

fn cmd_pexpiretime(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(-2),
        Some(_) => match s.dbs[c.db_index].expires.get(&argv[1]) {
            None => Reply::Integer(-1),
            Some(&at) => Reply::Integer(at),
        },
    }
}

fn cmd_get(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::String(v)) => Reply::bulk(v.clone()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Null,
    }
}

/// `DEL` — synchronous by default, but behaves exactly like `UNLINK` when
/// `lazyfree-lazy-user-del yes` (real Redis's own rule: that flag makes the
/// *implicit* command ask for the same async treatment `UNLINK` always
/// gets explicitly).
fn cmd_del(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut n = 0i64;
    for key in &argv[1..] {
        if let Some(obj) = s.dbs[c.db_index].remove_key(key) {
            n += 1;
            notify_keyspace_event(s, c.db_index, 'g', "del", key);
            s.lazyfree_if_enabled("lazyfree-lazy-user-del", LazyFreeJob::Value(obj));
        }
    }
    Reply::Integer(n)
}

fn cmd_exists(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut n = 0i64;
    for key in &argv[1..] {
        if s.dbs[c.db_index].dict.get(SLOT, key).is_some() {
            n += 1;
        }
    }
    Reply::Integer(n)
}

fn cmd_type(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(obj) => Reply::SimpleString(obj.type_name().to_string()),
        None => Reply::SimpleString("none".to_string()),
    }
}

// ----------------------------------------------------- generic key commands ---
// "Phase 3" 재점검 패스: 데이터 타입 자체는 이미 포팅됐지만, 타입에 무관하게
// 키스페이스 전체를 다루는 이 커맨드들(`KEYS`/`SCAN`류/`RENAME`/`COPY`/
// `MOVE`/`RANDOMKEY`/`TOUCH`/`UNLINK`/`OBJECT`)은 그동안 하나도 노출되지
// 않고 있었다 — 특히 `SCAN`은 Phase 0이 `kvstore`/`dict`에 실제 Redis와
// 동일한 역방향-이진 커서 알고리즘을 정확히 그 목적으로 포팅해 뒀는데도
// 커맨드로 한 번도 연결되지 않았던 것.

fn cmd_touch(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    Reply::Integer(argv[1..].iter().filter(|k| s.dbs[c.db_index].dict.get(SLOT, k).is_some()).count() as i64)
}

/// `UNLINK` — removes the key from the keyspace immediately (so `EXISTS`/
/// `GET`/etc. see it gone right away, same as `DEL`) but always drops the
/// actual value on the background lazy-free thread rather than inline here,
/// regardless of any `lazyfree-lazy-*` flag — matching real Redis's own
/// rule that `UNLINK` is the *explicit* "I want this async" command.
fn cmd_unlink(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut n = 0i64;
    for key in &argv[1..] {
        if let Some(obj) = s.dbs[c.db_index].remove_key(key) {
            n += 1;
            notify_keyspace_event(s, c.db_index, 'g', "unlink", key);
            s.lazyfree_unconditional(LazyFreeJob::Value(obj));
        }
    }
    Reply::Integer(n)
}

fn cmd_keys(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let pattern = &argv[1];
    let Some(dict) = s.dbs[c.db_index].dict.get_dict(SLOT) else { return Reply::Array(vec![]) };
    Reply::Array(dict.iter().filter(|(k, _)| crate::config::glob_match_bytes(pattern, k)).map(|(k, _)| Reply::bulk(k.clone())).collect())
}

fn cmd_randomkey(s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    let Some(dict) = s.dbs[c.db_index].dict.get_dict(SLOT) else { return Reply::Null };
    let keys: Vec<&Vec<u8>> = dict.iter().map(|(k, _)| k).collect();
    if keys.is_empty() {
        return Reply::Null;
    }
    Reply::bulk(keys[crate::object::pseudo_random_index(keys.len())].clone())
}

fn cmd_rename(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let dbidx = c.db_index;
    let Some(val) = s.dbs[dbidx].dict.remove(SLOT, &argv[1]) else {
        return Reply::Error("ERR no such key".to_string());
    };
    let ttl = s.dbs[dbidx].expires.remove(&argv[1]);
    let overwritten = s.dbs[dbidx].dict.insert(SLOT, argv[2].clone(), val);
    if let Some(old) = overwritten {
        s.lazyfree_if_enabled("lazyfree-lazy-server-del", LazyFreeJob::Value(old));
    }
    s.dbs[dbidx].expires.remove(&argv[2]);
    if let Some(at) = ttl {
        s.dbs[dbidx].expires.insert(argv[2].clone(), at);
    }
    notify_keyspace_event(s, dbidx, 'g', "rename_from", &argv[1]);
    notify_keyspace_event(s, dbidx, 'g', "rename_to", &argv[2]);
    Reply::ok()
}

/// `RENAMENX` — only renames when the destination doesn't already exist;
/// otherwise a plain `RENAME` (whose only failure mode, a missing source
/// key, is already exactly the right error to surface here too).
fn cmd_renamenx(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if s.dbs[c.db_index].dict.get(SLOT, &argv[2]).is_some() {
        return Reply::Integer(0);
    }
    match cmd_rename(s, c, argv) {
        Reply::Error(e) => Reply::Error(e),
        _ => Reply::Integer(1),
    }
}

/// `COPY source destination [DB destdb] [REPLACE]` — deep-clones the value
/// (`rdb::clone_value`) rather than sharing it, matching real Redis's "a
/// copy is fully independent of the original" contract.
fn cmd_copy(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut dest_db = c.db_index;
    let mut replace = false;
    let mut i = 3;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "DB" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                if n >= s.dbs.len() {
                    return Reply::Error("ERR DB index is out of range".to_string());
                }
                dest_db = n;
                i += 2;
            }
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    if dest_db == c.db_index && argv[1] == argv[2] {
        return Reply::Error("ERR source and destination objects are the same".to_string());
    }
    let Some(val) = s.dbs[c.db_index].dict.get(SLOT, &argv[1]) else {
        return Reply::Integer(0);
    };
    let cloned = crate::rdb::clone_value(val);
    let ttl = s.dbs[c.db_index].expires.get(&argv[1]).copied();
    if !replace && s.dbs[dest_db].dict.get(SLOT, &argv[2]).is_some() {
        return Reply::Integer(0);
    }
    let overwritten = s.dbs[dest_db].dict.insert(SLOT, argv[2].clone(), cloned);
    if let Some(old) = overwritten {
        s.lazyfree_if_enabled("lazyfree-lazy-server-del", LazyFreeJob::Value(old));
    }
    s.dbs[dest_db].expires.remove(&argv[2]);
    if let Some(at) = ttl {
        s.dbs[dest_db].expires.insert(argv[2].clone(), at);
    }
    notify_keyspace_event(s, dest_db, 'g', "copy_to", &argv[2]);
    Reply::Integer(1)
}

/// `DUMP key` — a serialized, portable snapshot of the key's value, meant to
/// be fed back into `RESTORE` (on this instance or another). Reuses this
/// module's own RDB value encoding directly (`rdb::dump_value`) since
/// `DUMP`'s payload format and this fork's internal RDB value format are the
/// same `[type byte][value]` shape by construction.
fn cmd_dump(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(obj) => Reply::bulk(crate::rdb::dump_value(obj)),
    }
}

/// `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]`.
/// `IDLETIME`/`FREQ` are accepted (so a real `DUMP`+`RESTORE` round trip from
/// a script never errors just for including them) and ignored — this fork
/// has no LRU/LFU tracking for either to feed.
fn cmd_restore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut replace = false;
    let mut absttl = false;
    let mut i = 4;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "ABSTTL" => {
                absttl = true;
                i += 1;
            }
            "IDLETIME" | "FREQ" if i + 1 < argv.len() => i += 2,
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    if !replace && key_exists(s, c.db_index, &argv[1]) {
        return Reply::Error("BUSYKEY Target key name already exists.".to_string());
    }
    let Ok(ttl_ms) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Reply::Error("ERR Invalid TTL value, must be >= 0".to_string());
    };
    if ttl_ms < 0 {
        return Reply::Error("ERR Invalid TTL value, must be >= 0".to_string());
    }
    if !crate::rdb::verify_dump_payload(&argv[3]) {
        return Reply::Error("ERR DUMP payload version or checksum are wrong".to_string());
    }
    let obj = match crate::rdb::restore_value(&argv[3]) {
        Ok(obj) => obj,
        Err(_) => return Reply::Error("ERR Bad data format".to_string()),
    };
    let overwritten = s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), obj);
    if let Some(old) = overwritten {
        s.lazyfree_if_enabled("lazyfree-lazy-server-del", LazyFreeJob::Value(old));
    }
    s.dbs[c.db_index].expires.remove(&argv[1]);
    if ttl_ms > 0 {
        let at_ms = if absttl { ttl_ms } else { now_ms().saturating_add(ttl_ms) };
        s.dbs[c.db_index].expires.insert(argv[1].clone(), at_ms);
    }
    notify_keyspace_event(s, c.db_index, 'g', "restore", &argv[1]);
    Reply::ok()
}

/// `MOVE key db` — real Redis rejects this inside a transaction/script if
/// it would be a no-op (same db); this fork just always allows it and
/// mirrors the "already there or source missing" failure cases as `0`.
fn cmd_move(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(dest_db) = ok_bytes_as_string(&argv[2]).parse::<usize>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    if dest_db >= s.dbs.len() {
        return Reply::Error("ERR DB index is out of range".to_string());
    }
    if dest_db == c.db_index {
        return Reply::Error("ERR source and destination objects are the same".to_string());
    }
    if s.dbs[c.db_index].dict.get(SLOT, &argv[1]).is_none() || s.dbs[dest_db].dict.get(SLOT, &argv[1]).is_some() {
        return Reply::Integer(0);
    }
    let val = s.dbs[c.db_index].dict.remove(SLOT, &argv[1]).unwrap();
    let ttl = s.dbs[c.db_index].expires.remove(&argv[1]);
    s.dbs[dest_db].dict.insert(SLOT, argv[1].clone(), val);
    if let Some(at) = ttl {
        s.dbs[dest_db].expires.insert(argv[1].clone(), at);
    }
    notify_keyspace_event(s, c.db_index, 'g', "move_from", &argv[1]);
    notify_keyspace_event(s, dest_db, 'g', "move_to", &argv[1]);
    Reply::Integer(1)
}

/// Shared by `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`: `[MATCH pattern] [COUNT n]`.
/// `COUNT` is parsed (so a client supplying it doesn't get a syntax error)
/// but not actually used as a hard cap — same as real Redis, where it's
/// only ever a hint about how much work to do per call, not a guarantee of
/// exactly `n` results.
fn parse_scan_options(argv: &[Vec<u8>], start: usize) -> Result<Option<Vec<u8>>, Reply> {
    let mut pattern = None;
    let mut i = start;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "MATCH" if i + 1 < argv.len() => {
                pattern = Some(argv[i + 1].clone());
                i += 2;
            }
            "COUNT" if i + 1 < argv.len() => {
                if ok_bytes_as_string(&argv[i + 1]).parse::<i64>().is_err() {
                    return Err(Reply::Error("ERR value is not an integer or out of range".to_string()));
                }
                i += 2;
            }
            _ => return Err(Reply::Error("ERR syntax error".to_string())),
        }
    }
    Ok(pattern)
}

fn scan_cursor_reply(next_cursor: u64, items: Vec<Reply>) -> Reply {
    Reply::Array(vec![Reply::bulk(next_cursor.to_string().into_bytes()), Reply::Array(items)])
}

/// `SCAN cursor [MATCH pattern] [COUNT n]` — one real step of `kvstore`'s
/// reverse-binary cursor (`redisx_core::kvstore::scan`, ported in Phase 0
/// specifically for this and never wired to a command until now).
fn cmd_scan(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(cursor) = ok_bytes_as_string(&argv[1]).parse::<u64>() else {
        return Reply::Error("ERR invalid cursor".to_string());
    };
    let pattern = match parse_scan_options(argv, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let mut found = Vec::new();
    let next = s.dbs[c.db_index].dict.scan(cursor, Some(SLOT), |k: &Vec<u8>, _v| {
        if pattern.as_deref().is_none_or(|p| crate::config::glob_match_bytes(p, k)) {
            found.push(Reply::bulk(k.clone()));
        }
    });
    scan_cursor_reply(next, found)
}

fn cmd_hscan(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    // Parsed only to validate/reject a malformed cursor argument the same
    // way `SCAN` would — never otherwise consulted, since this fork's hash
    // scan always completes in one call (see the `HashObj` note below).
    if ok_bytes_as_string(&argv[2]).parse::<u64>().is_err() {
        return Reply::Error("ERR invalid cursor".to_string());
    }
    let pattern = match parse_scan_options(argv, 3) {
        Ok(p) => p,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => scan_cursor_reply(0, vec![]),
        Some(RObj::Hash(_)) => {
            // This fork's hash has no cursor-resumable scan of its own
            // (`HashObj` is just a listpack-or-dict, not a `kvstore`) —
            // simplified to "one call returns everything that matches,
            // cursor always 0", which is a valid (if not incremental) `SCAN`
            // implementation per the protocol's own contract.
            let Some(RObj::Hash(h)) = s.dbs[c.db_index].dict.get(SLOT, &argv[1]) else { unreachable!() };
            let mut out = Vec::new();
            for (f, v) in h.get_all() {
                if pattern.as_deref().is_none_or(|p| crate::config::glob_match_bytes(p, &f)) {
                    out.push(Reply::bulk(f));
                    out.push(Reply::bulk(v));
                }
            }
            scan_cursor_reply(0, out)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_sscan(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    // Same "one call, cursor always 0" simplification as `HSCAN` — `SetObj`
    // has no cursor-resumable scan either.
    if ok_bytes_as_string(&argv[2]).parse::<u64>().is_err() {
        return Reply::Error("ERR invalid cursor".to_string());
    }
    let pattern = match parse_scan_options(argv, 3) {
        Ok(p) => p,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => scan_cursor_reply(0, vec![]),
        Some(RObj::Set(set)) => {
            let out = set.members().into_iter().filter(|m| pattern.as_deref().is_none_or(|p| crate::config::glob_match_bytes(p, m))).map(Reply::bulk).collect();
            scan_cursor_reply(0, out)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zscan(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    // Same "one call, cursor always 0" simplification again — `ZSetObj` has
    // no cursor-resumable scan either.
    if ok_bytes_as_string(&argv[2]).parse::<u64>().is_err() {
        return Reply::Error("ERR invalid cursor".to_string());
    }
    let pattern = match parse_scan_options(argv, 3) {
        Ok(p) => p,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => scan_cursor_reply(0, vec![]),
        Some(RObj::ZSet(z)) => {
            let mut out = Vec::new();
            for (m, score) in z.range(0, -1) {
                if pattern.as_deref().is_none_or(|p| crate::config::glob_match_bytes(p, &m)) {
                    out.push(Reply::bulk(m));
                    out.push(Reply::bulk(fmt_score(score)));
                }
            }
            scan_cursor_reply(0, out)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

/// `OBJECT ENCODING key` (the one subcommand with real content — see
/// `RObj::encoding_name`'s doc). `REFCOUNT` is still stubbed to `1` (this
/// fork has no object sharing to report a real refcount for). `IDLETIME`/
/// `FREQ` read the real `AccessInfo` side table (`Database::access`) now
/// that `touch_access` maintains it — `FREQ` still errors unless an LFU
/// `maxmemory-policy` is selected, matching real Redis's own behavior.
fn cmd_object(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    if argv.len() < 3 {
        return Reply::Error("ERR wrong number of arguments for 'object' command".to_string());
    }
    let Some(obj) = s.dbs[c.db_index].dict.get(SLOT, &argv[2]) else {
        return Reply::Error("ERR no such key".to_string());
    };
    match sub.as_str() {
        "ENCODING" => Reply::bulk(obj.encoding_name().as_bytes().to_vec()),
        "REFCOUNT" => Reply::Integer(1),
        "IDLETIME" => {
            let access = s.dbs[c.db_index].access.get(argv[2].as_slice());
            let idle_ms = access.map(|a| now_ms() - a.idle_since_ms).unwrap_or(0).max(0);
            Reply::Integer(idle_ms / 1000)
        }
        "FREQ" => {
            let policy = s.config.get_str("maxmemory-policy").unwrap_or("noeviction").to_ascii_lowercase();
            if !policy.contains("lfu") {
                return Reply::Error("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.".to_string());
            }
            let freq = s.dbs[c.db_index].access.get(argv[2].as_slice()).map(|a| a.freq).unwrap_or(5);
            Reply::Integer(freq as i64)
        }
        _ => Reply::Error("ERR Unknown OBJECT subcommand or wrong number of arguments".to_string()),
    }
}

/// INCR — reuses `redisx_core`'s canonical-decimal-integer convention (the
/// same strictness `listpack.c`'s `lpStringToInt64` uses) so `"007"` is
/// correctly rejected as non-numeric just like real Redis.
fn incr_by_i64(s: &mut Server, c: &mut ClientState, key: &[u8], delta: i64) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let current = match db.get(SLOT, &key.to_vec()) {
        Some(RObj::String(v)) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => n,
            None => return Reply::Error("ERR value is not an integer or out of range".to_string()),
        },
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        None => 0,
    };
    let Some(next) = current.checked_add(delta) else {
        return Reply::Error("ERR increment or decrement would overflow".to_string());
    };
    db.insert(SLOT, key.to_vec(), RObj::String(next.to_string().into_bytes()));
    notify_keyspace_event(s, c.db_index, '$', "incrby", key);
    Reply::Integer(next)
}

fn cmd_incr(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    incr_by_i64(s, c, &argv[1], 1)
}

fn cmd_incrby(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match parse_i64_arg(argv, 2) {
        Ok(delta) => incr_by_i64(s, c, &argv[1], delta),
        Err(e) => e,
    }
}

fn cmd_decr(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    incr_by_i64(s, c, &argv[1], -1)
}

fn cmd_decrby(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let n = match parse_i64_arg(argv, 2) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let Some(delta) = n.checked_neg() else {
        return Reply::Error("ERR decrement would overflow".to_string());
    };
    incr_by_i64(s, c, &argv[1], delta)
}

/// `INCRBYFLOAT` — reuses `fmt_score`'s trailing-zero-free `f64::to_string()`
/// formatting (the same one `ZINCRBY`/`ZSCORE` use) so `10.0` renders as
/// `"10"` just like real Redis's `%.17Lg`-then-trim convention.
fn cmd_incrbyfloat(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(delta) = ok_bytes_as_string(&argv[2]).parse::<f64>() else {
        return Reply::Error("ERR value is not a valid float".to_string());
    };
    let db = &mut s.dbs[c.db_index].dict;
    let current = match db.get(SLOT, &argv[1]) {
        Some(RObj::String(v)) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<f64>().ok()) {
            Some(n) => n,
            None => return Reply::Error("ERR value is not a valid float".to_string()),
        },
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        None => 0.0,
    };
    let next = current + delta;
    if !next.is_finite() {
        return Reply::Error("ERR increment would produce NaN or Infinity".to_string());
    }
    let formatted = fmt_score(next);
    db.insert(SLOT, argv[1].clone(), RObj::String(formatted.clone()));
    notify_keyspace_event(s, c.db_index, '$', "incrbyfloat", &argv[1]);
    Reply::bulk(formatted)
}

fn cmd_append(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let new_len = match db.get_mut(SLOT, &argv[1]) {
        Some(RObj::String(v)) => {
            v.extend_from_slice(&argv[2]);
            v.len()
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        None => {
            db.insert(SLOT, argv[1].clone(), RObj::String(argv[2].clone()));
            argv[2].len()
        }
    };
    notify_keyspace_event(s, c.db_index, '$', "append", &argv[1]);
    Reply::Integer(new_len as i64)
}

fn cmd_strlen(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::String(v)) => Reply::Integer(v.len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_getrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (Ok(start), Ok(end)) = (ok_bytes_as_string(&argv[2]).parse::<i64>(), ok_bytes_as_string(&argv[3]).parse::<i64>()) else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::bulk(Vec::new()),
        Some(RObj::String(v)) => match normalize_range(start, end, v.len() as i64) {
            None => Reply::bulk(Vec::new()),
            Some((lo, hi)) => Reply::bulk(v[lo as usize..=hi as usize].to_vec()),
        },
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_setrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(offset) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    if offset < 0 {
        return Reply::Error("ERR offset is out of range".to_string());
    }
    let offset = offset as usize;
    if offset + argv[3].len() > 512 * 1024 * 1024 {
        return Reply::Error("ERR string exceeds maximum allowed size (proto-max-bulk-len)".to_string());
    }
    let db = &mut s.dbs[c.db_index].dict;
    let new_len = match db.get_mut(SLOT, &argv[1]) {
        Some(RObj::String(v)) => {
            if argv[3].is_empty() {
                return Reply::Integer(v.len() as i64);
            }
            if v.len() < offset + argv[3].len() {
                v.resize(offset + argv[3].len(), 0);
            }
            v[offset..offset + argv[3].len()].copy_from_slice(&argv[3]);
            v.len()
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        None => {
            if argv[3].is_empty() {
                return Reply::Integer(0);
            }
            let mut v = vec![0u8; offset];
            v.extend_from_slice(&argv[3]);
            let len = v.len();
            db.insert(SLOT, argv[1].clone(), RObj::String(v));
            len
        }
    };
    notify_keyspace_event(s, c.db_index, '$', "setrange", &argv[1]);
    Reply::Integer(new_len as i64)
}

fn cmd_mget(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    Reply::Array(
        argv[1..]
            .iter()
            .map(|key| match s.dbs[c.db_index].dict.get(SLOT, key) {
                Some(RObj::String(v)) => Reply::bulk(v.clone()),
                _ => Reply::Null,
            })
            .collect(),
    )
}

fn cmd_mset(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if !(argv.len() - 1).is_multiple_of(2) || argv.len() < 3 {
        return Reply::Error("ERR wrong number of arguments for 'mset' command".to_string());
    }
    for pair in argv[1..].chunks(2) {
        s.dbs[c.db_index].dict.insert(SLOT, pair[0].clone(), RObj::String(pair[1].clone()));
        s.dbs[c.db_index].expires.remove(&pair[0]);
        notify_keyspace_event(s, c.db_index, 'g', "set", &pair[0]);
    }
    Reply::ok()
}

fn cmd_msetnx(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if !(argv.len() - 1).is_multiple_of(2) || argv.len() < 3 {
        return Reply::Error("ERR wrong number of arguments for 'msetnx' command".to_string());
    }
    if argv[1..].chunks(2).any(|pair| key_exists(s, c.db_index, &pair[0])) {
        return Reply::Integer(0);
    }
    for pair in argv[1..].chunks(2) {
        s.dbs[c.db_index].dict.insert(SLOT, pair[0].clone(), RObj::String(pair[1].clone()));
        notify_keyspace_event(s, c.db_index, 'g', "set", &pair[0]);
    }
    Reply::Integer(1)
}

fn cmd_getdel(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::String(v)) => {
            let v = v.clone();
            s.dbs[c.db_index].remove_key(&argv[1]);
            notify_keyspace_event(s, c.db_index, 'g', "del", &argv[1]);
            Reply::bulk(v)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Null,
    }
}

/// `GETEX key [EX s | PX ms | EXAT ts | PXAT ts | PERSIST]` — like `GET`, but
/// can also set/clear the key's TTL in the same round trip. With no option
/// it behaves exactly like plain `GET` (TTL untouched).
fn cmd_getex(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let value = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::String(v)) => v.clone(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        None => return Reply::Null,
    };
    if argv.len() > 2 {
        let opt = ok_bytes_as_string(&argv[2]).to_ascii_uppercase();
        match opt.as_str() {
            "PERSIST" if argv.len() == 3 => {
                s.dbs[c.db_index].expires.remove(&argv[1]);
            }
            "EX" | "PX" | "EXAT" | "PXAT" if argv.len() == 4 => {
                let Ok(n) = ok_bytes_as_string(&argv[3]).parse::<i64>() else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                let at_ms = match opt.as_str() {
                    "EX" => now_ms().saturating_add(n.saturating_mul(1000)),
                    "PX" => now_ms().saturating_add(n),
                    "EXAT" => n.saturating_mul(1000),
                    "PXAT" => n,
                    _ => unreachable!(),
                };
                if at_ms <= now_ms() {
                    s.dbs[c.db_index].remove_key(&argv[1]);
                    notify_keyspace_event(s, c.db_index, 'g', "del", &argv[1]);
                    return Reply::bulk(value);
                }
                s.dbs[c.db_index].expires.insert(argv[1].clone(), at_ms);
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    Reply::bulk(value)
}

/// Removes `key` from the keyspace if the (list/hash/set/zset) value it maps
/// to has become empty, matching real Redis's "an empty container key does
/// not exist" invariant (e.g. `LPOP` emptying a list deletes the key).
fn delete_if_empty(s: &mut Server, c: &mut ClientState, key: &[u8], is_empty: bool) {
    if is_empty {
        s.dbs[c.db_index].remove_key(key);
    }
}

// ---------------------------------------------------------------- List ----

fn cmd_lpush(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let obj = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::List(_)));
    if !obj {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::List(ListObj::new()));
    }
    let RObj::List(list) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    list.push_front(&argv[2..]);
    let n = list.len() as i64;
    notify_keyspace_event(s, c.db_index, 'l', "lpush", &argv[1]);
    Reply::Integer(n)
}

fn cmd_rpush(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::List(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::List(ListObj::new()));
    }
    let RObj::List(list) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    list.push_back(&argv[2..]);
    let n = list.len() as i64;
    notify_keyspace_event(s, c.db_index, 'l', "rpush", &argv[1]);
    Reply::Integer(n)
}

fn pop_count(argv: &[Vec<u8>]) -> Result<Option<usize>, Reply> {
    if argv.len() == 2 {
        return Ok(None);
    }
    match ok_bytes_as_string(&argv[2]).parse::<i64>() {
        Ok(n) if n >= 0 => Ok(Some(n as usize)),
        _ => Err(Reply::Error("ERR value is out of range, must be positive".to_string())),
    }
}

fn cmd_lpop(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let with_count = argv.len() == 3;
    let count = match pop_count(argv) {
        Ok(n) => n.unwrap_or(1),
        Err(e) => return e,
    };
    let db = &mut s.dbs[c.db_index].dict;
    let (popped, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return if with_count { Reply::NullArray } else { Reply::Null },
        Some(RObj::List(list)) => {
            let popped = list.pop_front(count);
            (popped, list.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if !popped.is_empty() {
        notify_keyspace_event(s, c.db_index, 'l', "lpop", &argv[1]);
    }
    if with_count {
        Reply::Array(popped.into_iter().map(Reply::bulk).collect())
    } else {
        match popped.into_iter().next() {
            Some(v) => Reply::bulk(v),
            None => Reply::Null,
        }
    }
}

fn cmd_rpop(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let with_count = argv.len() == 3;
    let count = match pop_count(argv) {
        Ok(n) => n.unwrap_or(1),
        Err(e) => return e,
    };
    let db = &mut s.dbs[c.db_index].dict;
    let (popped, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return if with_count { Reply::NullArray } else { Reply::Null },
        Some(RObj::List(list)) => {
            let popped = list.pop_back(count);
            (popped, list.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if !popped.is_empty() {
        notify_keyspace_event(s, c.db_index, 'l', "rpop", &argv[1]);
    }
    if with_count {
        Reply::Array(popped.into_iter().map(Reply::bulk).collect())
    } else {
        match popped.into_iter().next() {
            Some(v) => Reply::bulk(v),
            None => Reply::Null,
        }
    }
}

fn parse_i64_arg(argv: &[Vec<u8>], idx: usize) -> Result<i64, Reply> {
    ok_bytes_as_string(&argv[idx]).parse::<i64>().map_err(|_| Reply::Error("ERR value is not an integer or out of range".to_string()))
}

fn cmd_lrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (start, stop) = match (parse_i64_arg(argv, 2), parse_i64_arg(argv, 3)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::List(list)) => Reply::Array(list.range(start, stop).into_iter().map(Reply::bulk).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_llen(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::List(list)) => Reply::Integer(list.len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_lindex(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let idx = match parse_i64_arg(argv, 2) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(RObj::List(list)) => list.index(idx).map_or(Reply::Null, Reply::bulk),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_lset(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let idx = match parse_i64_arg(argv, 2) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get_mut(SLOT, &argv[1]) {
        None => return Reply::Error("ERR no such key".to_string()),
        Some(RObj::List(list)) => {
            if !list.set_at(idx, &argv[3]) {
                return Reply::Error("ERR index out of range".to_string());
            }
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    }
    notify_keyspace_event(s, c.db_index, 'l', "lset", &argv[1]);
    Reply::ok()
}

fn cmd_linsert(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let before = match ok_bytes_as_string(&argv[2]).to_ascii_uppercase().as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Reply::Error("ERR syntax error".to_string()),
    };
    let new_len = match s.dbs[c.db_index].dict.get_mut(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::List(list)) => match list.insert_pivot(&argv[3], &argv[4], before) {
            Some(n) => n as i64,
            None => return Reply::Integer(-1),
        },
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    notify_keyspace_event(s, c.db_index, 'l', "linsert", &argv[1]);
    Reply::Integer(new_len)
}

fn cmd_lrem(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let count = match parse_i64_arg(argv, 2) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let db = &mut s.dbs[c.db_index].dict;
    let (removed, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::List(list)) => {
            let removed = list.remove_value(&argv[3], count);
            (removed, list.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if removed > 0 {
        notify_keyspace_event(s, c.db_index, 'l', "lrem", &argv[1]);
    }
    Reply::Integer(removed as i64)
}

fn cmd_ltrim(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (start, stop) = match (parse_i64_arg(argv, 2), parse_i64_arg(argv, 3)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    let db = &mut s.dbs[c.db_index].dict;
    let became_empty = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::ok(),
        Some(RObj::List(list)) => {
            list.trim(start, stop);
            list.is_empty()
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    notify_keyspace_event(s, c.db_index, 'l', "ltrim", &argv[1]);
    Reply::ok()
}

/// `LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]`. `RANK`
/// (default 1, never 0) picks which matching occurrence to start returning
/// from and its sign picks the scan direction; `COUNT` (default 1, 0 = all)
/// caps how many indices come back; `MAXLEN` (default 0 = unlimited) caps
/// how many list elements are considered, counted from whichever end `RANK`
/// scans from.
fn cmd_lpos(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut rank: i64 = 1;
    let mut count: i64 = 1;
    let mut has_count = false;
    let mut maxlen: i64 = 0;
    let mut i = 3;
    while i < argv.len() {
        let opt = ok_bytes_as_string(&argv[i]).to_ascii_uppercase();
        if i + 1 >= argv.len() {
            return Reply::Error("ERR syntax error".to_string());
        }
        let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
            return Reply::Error("ERR value is not an integer or out of range".to_string());
        };
        match opt.as_str() {
            "RANK" => {
                if n == 0 {
                    return Reply::Error("ERR RANK can't be zero".to_string());
                }
                rank = n;
            }
            "COUNT" => {
                if n < 0 {
                    return Reply::Error("ERR COUNT can't be negative".to_string());
                }
                count = n;
                has_count = true;
            }
            "MAXLEN" => {
                if n < 0 {
                    return Reply::Error("ERR MAXLEN can't be negative".to_string());
                }
                maxlen = n;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
        i += 2;
    }

    let list_len = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => return if has_count { Reply::Array(vec![]) } else { Reply::Null },
        Some(RObj::List(list)) => list.len(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    let Some(RObj::List(list)) = s.dbs[c.db_index].dict.get(SLOT, &argv[1]) else { unreachable!() };
    let mut all = list.positions(&argv[2]);
    if maxlen > 0 {
        let maxlen = maxlen as usize;
        if rank > 0 {
            all.retain(|&i| i < maxlen);
        } else {
            all.retain(|&i| i >= list_len.saturating_sub(maxlen));
        }
    }
    let matches: Vec<usize> = if rank > 0 {
        all.into_iter().skip((rank - 1) as usize).collect()
    } else {
        all.reverse();
        all.into_iter().skip((-rank - 1) as usize).collect()
    };

    if has_count {
        let take_n = if count == 0 { matches.len() } else { count as usize };
        Reply::Array(matches.into_iter().take(take_n).map(|i| Reply::Integer(i as i64)).collect())
    } else {
        match matches.first() {
            Some(&i) => Reply::Integer(i as i64),
            None => Reply::Null,
        }
    }
}

fn parse_list_side(b: &[u8]) -> Result<bool, Reply> {
    match ok_bytes_as_string(b).to_ascii_uppercase().as_str() {
        "LEFT" => Ok(true),
        "RIGHT" => Ok(false),
        _ => Err(Reply::Error("ERR syntax error".to_string())),
    }
}

/// A blocking command's trailing timeout argument — seconds, fractional,
/// never negative. `0` means "block forever", matching every blocking
/// command's own convention.
fn parse_block_timeout(b: &[u8]) -> Result<f64, ()> {
    let Ok(secs) = ok_bytes_as_string(b).parse::<f64>() else { return Err(()) };
    if !secs.is_finite() || secs < 0.0 {
        return Err(());
    }
    Ok(secs)
}

// `BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BZPOPMIN`/`BZPOPMAX`'s entries in
// `COMMANDS` below — used by every execution path *except* the binary
// layer's top-level interception (`try_blocking_command`, see its own doc
// comment): `MULTI`/`EXEC`-queued and `redis.call()`-from-a-script
// invocations both go through the ordinary `dispatch` → `run_command` path,
// and real Redis's rule for both (`CLIENT_DENY_BLOCKING`) is the same —
// these commands never actually block there, they just resolve immediately
// as if their timeout had already elapsed. Each one delegates to the exact
// same `attempt_blocking_*` logic the real blocking path uses, so there's
// only one copy of "what counts as data being available" to keep correct.
// Deliberately **not** marked `is_write` in `COMMANDS`: a successful pop
// already propagates/AOF-logs/bumps `WATCH` on its own, through the nested
// synthetic `LPOP`/`RPOP`/`ZPOPMIN`/`ZPOPMAX`/`LMOVE`/`RPOPLPUSH` call's own
// trip through `run_command` — marking the outer command `is_write` too
// would double every one of those effects for every real write, the same
// "effects replication" reasoning `FCALL` itself is exempt from `is_write`
// for.

fn cmd_blpop_or_brpop_non_blocking(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() < 3 {
        return Reply::Error(format!("ERR wrong number of arguments for '{}' command", ok_bytes_as_string(&argv[0]).to_ascii_lowercase()));
    }
    if parse_block_timeout(&argv[argv.len() - 1]).is_err() {
        return Reply::Error("ERR timeout is not a float or out of range".to_string());
    }
    let from_left = argv[0].eq_ignore_ascii_case(b"BLPOP");
    let keys = argv[1..argv.len() - 1].to_vec();
    match s.attempt_blocking_list_pop(c.id, c.db_index, &keys, from_left) {
        BlockAttempt::Immediate(reply) => reply,
        BlockAttempt::StillBlocked => Reply::NullArray,
    }
}

fn cmd_bzpopmin_or_bzpopmax_non_blocking(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() < 3 {
        return Reply::Error(format!("ERR wrong number of arguments for '{}' command", ok_bytes_as_string(&argv[0]).to_ascii_lowercase()));
    }
    if parse_block_timeout(&argv[argv.len() - 1]).is_err() {
        return Reply::Error("ERR timeout is not a float or out of range".to_string());
    }
    let from_min = argv[0].eq_ignore_ascii_case(b"BZPOPMIN");
    let keys = argv[1..argv.len() - 1].to_vec();
    match s.attempt_blocking_zpop(c.id, c.db_index, &keys, from_min) {
        BlockAttempt::Immediate(reply) => reply,
        BlockAttempt::StillBlocked => Reply::NullArray,
    }
}

fn cmd_blmove_non_blocking(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() != 6 {
        return Reply::Error("ERR wrong number of arguments for 'blmove' command".to_string());
    }
    if parse_block_timeout(&argv[5]).is_err() {
        return Reply::Error("ERR timeout is not a float or out of range".to_string());
    }
    let (Ok(from_left), Ok(to_left)) = (parse_list_side(&argv[3]), parse_list_side(&argv[4])) else {
        return Reply::Error("ERR syntax error".to_string());
    };
    let synth = vec![b"LMOVE".to_vec(), argv[1].clone(), argv[2].clone(), argv[3].clone(), argv[4].clone()];
    match s.attempt_blocking_list_move(c.id, c.db_index, &argv[1], from_left, to_left, &synth) {
        BlockAttempt::Immediate(reply) => reply,
        BlockAttempt::StillBlocked => Reply::NullBulkString,
    }
}

fn cmd_brpoplpush_non_blocking(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() != 4 {
        return Reply::Error("ERR wrong number of arguments for 'brpoplpush' command".to_string());
    }
    if parse_block_timeout(&argv[3]).is_err() {
        return Reply::Error("ERR timeout is not a float or out of range".to_string());
    }
    let synth = vec![b"RPOPLPUSH".to_vec(), argv[1].clone(), argv[2].clone()];
    match s.attempt_blocking_list_move(c.id, c.db_index, &argv[1], false, true, &synth) {
        BlockAttempt::Immediate(reply) => reply,
        BlockAttempt::StillBlocked => Reply::NullBulkString,
    }
}

/// Shared implementation for `LMOVE source destination <LEFT|RIGHT>
/// <LEFT|RIGHT>` and `RPOPLPUSH source destination` (`= LMOVE source
/// destination RIGHT LEFT`). Handles `source == destination` correctly: the
/// pop happens first (single mutable borrow), then the push reopens the dict
/// to insert/append, which is also correct when it's the very same key
/// (effectively a rotate).
fn do_lmove(s: &mut Server, c: &mut ClientState, src: &[u8], dst: &[u8], from_left: bool, to_left: bool) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    if let Some(v) = db.get(SLOT, &dst.to_vec()) {
        if !matches!(v, RObj::List(_)) {
            return Reply::Error(WRONGTYPE.to_string());
        }
    }
    let (value, src_became_empty) = match db.get_mut(SLOT, &src.to_vec()) {
        None => return Reply::Null,
        Some(RObj::List(list)) => {
            let popped = if from_left { list.pop_front(1) } else { list.pop_back(1) };
            match popped.into_iter().next() {
                Some(v) => (v, list.is_empty()),
                None => return Reply::Null,
            }
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, src, src_became_empty);

    let db = &mut s.dbs[c.db_index].dict;
    if db.get(SLOT, &dst.to_vec()).is_none() {
        db.insert(SLOT, dst.to_vec(), RObj::List(ListObj::new()));
    }
    let RObj::List(dest_list) = db.get_mut(SLOT, &dst.to_vec()).unwrap() else { unreachable!() };
    if to_left {
        dest_list.push_front(std::slice::from_ref(&value));
    } else {
        dest_list.push_back(std::slice::from_ref(&value));
    }
    notify_keyspace_event(s, c.db_index, 'l', if from_left { "lpop" } else { "rpop" }, src);
    notify_keyspace_event(s, c.db_index, 'l', if to_left { "lpush" } else { "rpush" }, dst);
    Reply::bulk(value)
}

fn cmd_lmove(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let from_left = match parse_list_side(&argv[3]) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let to_left = match parse_list_side(&argv[4]) {
        Ok(b) => b,
        Err(e) => return e,
    };
    do_lmove(s, c, &argv[1].clone(), &argv[2].clone(), from_left, to_left)
}

fn cmd_rpoplpush(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    do_lmove(s, c, &argv[1].clone(), &argv[2].clone(), false, true)
}

// ---------------------------------------------------------------- Hash ----

fn cmd_hset(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if !argv.len().is_multiple_of(2) {
        return Reply::Error("ERR wrong number of arguments for 'hset' command".to_string());
    }
    let max_entries = s.config.get_int("hash-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("hash-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Hash(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::Hash(HashObj::new()));
    }
    let RObj::Hash(hash) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    let mut added = 0i64;
    for pair in argv[2..].chunks(2) {
        if hash.set(&pair[0], &pair[1], max_entries, max_value) {
            added += 1;
        }
    }
    notify_keyspace_event(s, c.db_index, 'h', "hset", &argv[1]);
    Reply::Integer(added)
}

fn cmd_hget(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(RObj::Hash(h)) => h.get(&argv[2]).map_or(Reply::Null, Reply::bulk),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hdel(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let (removed, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::Hash(h)) => {
            let n = argv[2..].iter().filter(|f| h.del(f)).count();
            (n, h.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if removed > 0 {
        notify_keyspace_event(s, c.db_index, 'h', "hdel", &argv[1]);
    }
    Reply::Integer(removed as i64)
}

fn cmd_hgetall(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Map(vec![]),
        Some(RObj::Hash(h)) => Reply::Map(h.get_all().into_iter().map(|(f, v)| (Reply::bulk(f), Reply::bulk(v))).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hlen(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::Hash(h)) => Reply::Integer(h.len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hexists(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::Hash(h)) => Reply::Integer(h.exists(&argv[2]) as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hsetnx(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let max_entries = s.config.get_int("hash-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("hash-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Hash(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::Hash(HashObj::new()));
    }
    let RObj::Hash(hash) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    if hash.exists(&argv[2]) {
        return Reply::Integer(0);
    }
    hash.set(&argv[2], &argv[3], max_entries, max_value);
    notify_keyspace_event(s, c.db_index, 'h', "hset", &argv[1]);
    Reply::Integer(1)
}

fn cmd_hmget(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(argv[2..].iter().map(|_| Reply::Null).collect()),
        Some(RObj::Hash(h)) => Reply::Array(argv[2..].iter().map(|f| h.get(f).map_or(Reply::Null, Reply::bulk)).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hkeys(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::Hash(h)) => Reply::Array(h.get_all().into_iter().map(|(f, _)| Reply::bulk(f)).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hvals(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::Hash(h)) => Reply::Array(h.get_all().into_iter().map(|(_, v)| Reply::bulk(v)).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_hincrby(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let delta = match parse_i64_arg(argv, 3) {
        Ok(n) => n,
        Err(e) => return e,
    };
    let max_entries = s.config.get_int("hash-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("hash-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Hash(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::Hash(HashObj::new()));
    }
    let RObj::Hash(hash) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    match hash.incr_by(&argv[2], delta, max_entries, max_value) {
        Ok(next) => {
            notify_keyspace_event(s, c.db_index, 'h', "hincrby", &argv[1]);
            Reply::Integer(next)
        }
        Err(e) => Reply::Error(e),
    }
}

fn cmd_hincrbyfloat(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(delta) = ok_bytes_as_string(&argv[3]).parse::<f64>() else {
        return Reply::Error("ERR value is not a valid float".to_string());
    };
    let max_entries = s.config.get_int("hash-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("hash-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Hash(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::Hash(HashObj::new()));
    }
    let RObj::Hash(hash) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    match hash.incr_by_float(&argv[2], delta, max_entries, max_value, fmt_score) {
        Ok(formatted) => {
            notify_keyspace_event(s, c.db_index, 'h', "hincrbyfloat", &argv[1]);
            Reply::bulk(formatted)
        }
        Err(e) => Reply::Error(e),
    }
}

/// `HRANDFIELD key [count [WITHVALUES]]`. With no `count`, a single random
/// field (or nil). With `count >= 0`, up to `count` *distinct* fields; with
/// `count < 0`, exactly `-count` fields chosen independently (so the same
/// field can repeat) — matching real Redis's two sampling modes.
fn cmd_hrandfield(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let all = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Vec::new(),
        Some(RObj::Hash(h)) => h.get_all(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if argv.len() == 2 {
        if all.is_empty() {
            return Reply::Null;
        }
        let (f, _) = &all[crate::object::pseudo_random_index(all.len())];
        return Reply::bulk(f.clone());
    }
    let Ok(count) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    let with_values = match argv.len() {
        3 => false,
        4 if argv[3].eq_ignore_ascii_case(b"WITHVALUES") => true,
        _ => return Reply::Error("ERR syntax error".to_string()),
    };
    if all.is_empty() {
        return Reply::Array(vec![]);
    }
    let picked: Vec<&(Vec<u8>, Vec<u8>)> = if count >= 0 {
        let mut idx: Vec<usize> = (0..all.len()).collect();
        let n = (count as usize).min(all.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let i = crate::object::pseudo_random_index(idx.len());
            out.push(idx.remove(i));
        }
        out.into_iter().map(|i| &all[i]).collect()
    } else {
        (0..(-count) as usize).map(|_| &all[crate::object::pseudo_random_index(all.len())]).collect()
    };
    let mut out = Vec::new();
    for (f, v) in picked {
        out.push(Reply::bulk(f.clone()));
        if with_values {
            out.push(Reply::bulk(v.clone()));
        }
    }
    Reply::Array(out)
}

// ----------------------------------------------------------------- Set ----

fn cmd_sadd(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let max_intset = s.config.get_int("set-max-intset-entries").unwrap_or(512);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Set(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::Set(SetObj::new()));
    }
    let RObj::Set(set) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    let added = argv[2..].iter().filter(|m| set.add(m, max_intset)).count();
    if added > 0 {
        notify_keyspace_event(s, c.db_index, 's', "sadd", &argv[1]);
    }
    Reply::Integer(added as i64)
}

fn cmd_srem(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let (removed, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::Set(set)) => {
            let n = argv[2..].iter().filter(|m| set.remove(m)).count();
            (n, set.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if removed > 0 {
        notify_keyspace_event(s, c.db_index, 's', "srem", &argv[1]);
    }
    Reply::Integer(removed as i64)
}

fn cmd_sismember(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::Set(set)) => Reply::Integer(set.contains(&argv[2]) as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_smembers(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Set(vec![]),
        Some(RObj::Set(set)) => Reply::Set(set.members().into_iter().map(Reply::bulk).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_scard(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::Set(set)) => Reply::Integer(set.len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_smismember(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(argv[2..].iter().map(|_| Reply::Integer(0)).collect()),
        Some(RObj::Set(set)) => Reply::Array(argv[2..].iter().map(|m| Reply::Integer(set.contains(m) as i64)).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_spop(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let with_count = argv.len() == 3;
    let count = if with_count {
        match ok_bytes_as_string(&argv[2]).parse::<i64>() {
            Ok(n) if n >= 0 => n as usize,
            _ => return Reply::Error("ERR value is out of range, must be positive".to_string()),
        }
    } else {
        1
    };
    let db = &mut s.dbs[c.db_index].dict;
    let (popped, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return if with_count { Reply::Set(vec![]) } else { Reply::Null },
        Some(RObj::Set(set)) => {
            let popped = set.pop_random(count, crate::object::pseudo_random_index);
            (popped, set.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if !popped.is_empty() {
        notify_keyspace_event(s, c.db_index, 's', "spop", &argv[1]);
    }
    if with_count {
        Reply::Set(popped.into_iter().map(Reply::bulk).collect())
    } else {
        match popped.into_iter().next() {
            Some(v) => Reply::bulk(v),
            None => Reply::Null,
        }
    }
}

fn cmd_srandmember(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let with_count = argv.len() == 3;
    let count = if with_count {
        match ok_bytes_as_string(&argv[2]).parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Reply::Error("ERR value is not an integer or out of range".to_string()),
        }
    } else {
        1
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => {
            if with_count {
                Reply::Array(vec![])
            } else {
                Reply::Null
            }
        }
        Some(RObj::Set(set)) => {
            let picked = set.random_members(count, crate::object::pseudo_random_index);
            if with_count {
                Reply::Array(picked.into_iter().map(Reply::bulk).collect())
            } else {
                match picked.into_iter().next() {
                    Some(v) => Reply::bulk(v),
                    None => Reply::Null,
                }
            }
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_smove(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    if let Some(v) = db.get(SLOT, &argv[2]) {
        if !matches!(v, RObj::Set(_)) {
            return Reply::Error(WRONGTYPE.to_string());
        }
    }
    let (moved, src_became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::Set(set)) => {
            let moved = set.remove(&argv[3]);
            (moved, set.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if !moved {
        return Reply::Integer(0);
    }
    delete_if_empty(s, c, &argv[1], src_became_empty);

    let max_intset = s.config.get_int("set-max-intset-entries").unwrap_or(512);
    let db = &mut s.dbs[c.db_index].dict;
    if db.get(SLOT, &argv[2]).is_none() {
        db.insert(SLOT, argv[2].clone(), RObj::Set(SetObj::new()));
    }
    let RObj::Set(dest) = db.get_mut(SLOT, &argv[2]).unwrap() else { unreachable!() };
    dest.add(&argv[3], max_intset);
    notify_keyspace_event(s, c.db_index, 's', "srem", &argv[1]);
    notify_keyspace_event(s, c.db_index, 's', "sadd", &argv[2]);
    Reply::Integer(1)
}

/// Reads every key in `keys` as a set (treating a missing key as an empty
/// set), returning `Err` on the first non-set value found. Shared by
/// `SINTER`/`SUNION`/`SDIFF` and their `*STORE` variants.
fn read_sets_or_wrongtype(s: &Server, dbidx: usize, keys: &[Vec<u8>]) -> Result<Vec<std::collections::HashSet<Vec<u8>>>, Reply> {
    keys.iter()
        .map(|k| match s.dbs[dbidx].dict.get(SLOT, k) {
            None => Ok(std::collections::HashSet::new()),
            Some(RObj::Set(set)) => Ok(set.members().into_iter().collect()),
            Some(_) => Err(Reply::Error(WRONGTYPE.to_string())),
        })
        .collect()
}

enum SetOp {
    Inter,
    Union,
    Diff,
}

fn apply_set_op(op: &SetOp, sets: Vec<std::collections::HashSet<Vec<u8>>>) -> std::collections::HashSet<Vec<u8>> {
    let mut iter = sets.into_iter();
    let Some(first) = iter.next() else { return std::collections::HashSet::new() };
    match op {
        SetOp::Inter => iter.fold(first, |acc, s| acc.intersection(&s).cloned().collect()),
        SetOp::Union => iter.fold(first, |acc, s| acc.union(&s).cloned().collect()),
        SetOp::Diff => iter.fold(first, |acc, s| acc.difference(&s).cloned().collect()),
    }
}

fn cmd_set_op(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], op: SetOp) -> Reply {
    let sets = match read_sets_or_wrongtype(s, c.db_index, &argv[1..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    Reply::Set(apply_set_op(&op, sets).into_iter().map(Reply::bulk).collect())
}

fn cmd_sinter(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_set_op(s, c, argv, SetOp::Inter)
}

fn cmd_sunion(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_set_op(s, c, argv, SetOp::Union)
}

fn cmd_sdiff(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_set_op(s, c, argv, SetOp::Diff)
}

fn cmd_set_op_store(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], op: SetOp) -> Reply {
    let sets = match read_sets_or_wrongtype(s, c.db_index, &argv[2..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let result = apply_set_op(&op, sets);
    let n = result.len();
    if n == 0 {
        s.dbs[c.db_index].remove_key(&argv[1]);
    } else {
        let max_intset = s.config.get_int("set-max-intset-entries").unwrap_or(512);
        let mut set = SetObj::new();
        for m in result {
            set.add(&m, max_intset);
        }
        s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::Set(set));
    }
    let event = match op {
        SetOp::Inter => "sinterstore",
        SetOp::Union => "sunionstore",
        SetOp::Diff => "sdiffstore",
    };
    notify_keyspace_event(s, c.db_index, 's', event, &argv[1]);
    Reply::Integer(n as i64)
}

fn cmd_sinterstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_set_op_store(s, c, argv, SetOp::Inter)
}

fn cmd_sunionstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_set_op_store(s, c, argv, SetOp::Union)
}

fn cmd_sdiffstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_set_op_store(s, c, argv, SetOp::Diff)
}

// ---------------------------------------------------------------- ZSet ----

fn fmt_score(score: f64) -> Vec<u8> {
    score.to_string().into_bytes()
}

fn cmd_zadd(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if !argv[2..].len().is_multiple_of(2) {
        return Reply::Error("ERR wrong number of arguments for 'zadd' command".to_string());
    }
    let mut pairs = Vec::new();
    for chunk in argv[2..].chunks(2) {
        match ok_bytes_as_string(&chunk[0]).parse::<f64>() {
            Ok(score) => pairs.push((score, chunk[1].clone())),
            Err(_) => return Reply::Error("ERR value is not a valid float".to_string()),
        }
    }
    let max_entries = s.config.get_int("zset-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("zset-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::ZSet(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::ZSet(ZSetObj::new()));
    }
    let RObj::ZSet(zset) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    let added = pairs.into_iter().filter(|(score, member)| zset.add(member, *score, max_entries, max_value)).count();
    notify_keyspace_event(s, c.db_index, 'z', "zadd", &argv[1]);
    Reply::Integer(added as i64)
}

fn cmd_zscore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(RObj::ZSet(z)) => z.score(&argv[2]).map_or(Reply::Null, |s| Reply::bulk(fmt_score(s))),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrem(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let (removed, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::ZSet(z)) => {
            let n = argv[2..].iter().filter(|m| z.remove(m)).count();
            (n, z.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if removed > 0 {
        notify_keyspace_event(s, c.db_index, 'z', "zrem", &argv[1]);
    }
    Reply::Integer(removed as i64)
}

fn cmd_zcard(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::ZSet(z)) => Reply::Integer(z.len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrank(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(RObj::ZSet(z)) => z.rank(&argv[2]).map_or(Reply::Null, |r| Reply::Integer(r as i64)),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (start, stop) = match (parse_i64_arg(argv, 2), parse_i64_arg(argv, 3)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    let with_scores = argv.len() == 5 && ok_bytes_as_string(&argv[4]).eq_ignore_ascii_case("WITHSCORES");
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::ZSet(z)) => {
            let entries = z.range(start, stop);
            let mut out = Vec::with_capacity(entries.len() * if with_scores { 2 } else { 1 });
            for (member, score) in entries {
                out.push(Reply::bulk(member));
                if with_scores {
                    out.push(Reply::bulk(fmt_score(score)));
                }
            }
            Reply::Array(out)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrevrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (start, stop) = match (parse_i64_arg(argv, 2), parse_i64_arg(argv, 3)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    let with_scores = argv.len() == 5 && ok_bytes_as_string(&argv[4]).eq_ignore_ascii_case("WITHSCORES");
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::ZSet(z)) => {
            let mut entries = z.range(0, -1);
            entries.reverse();
            let sliced = match normalize_range(start, stop, entries.len() as i64) {
                None => Vec::new(),
                Some((lo, hi)) => entries[lo as usize..=hi as usize].to_vec(),
            };
            let mut out = Vec::with_capacity(sliced.len() * if with_scores { 2 } else { 1 });
            for (member, score) in sliced {
                out.push(Reply::bulk(member));
                if with_scores {
                    out.push(Reply::bulk(fmt_score(score)));
                }
            }
            Reply::Array(out)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrevrank(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(RObj::ZSet(z)) => match z.rank(&argv[2]) {
            Some(r) => Reply::Integer((z.len() - 1 - r) as i64),
            None => Reply::Null,
        },
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

/// Parses one `ZRANGEBYSCORE`/`ZCOUNT`-style bound: `-inf`/`+inf`/`inf`, or a
/// plain float optionally prefixed with `(` for "exclusive" — matching
/// `zslParseRange`'s own mini-syntax.
fn parse_score_bound(b: &[u8]) -> Result<(f64, bool), Reply> {
    let s = ok_bytes_as_string(b);
    let (excl, rest) = match s.strip_prefix('(') {
        Some(r) => (true, r),
        None => (false, s.as_str()),
    };
    match rest {
        "-inf" => Ok((f64::NEG_INFINITY, excl)),
        "+inf" | "inf" => Ok((f64::INFINITY, excl)),
        _ => rest.parse::<f64>().map(|v| (v, excl)).map_err(|_| Reply::Error("ERR min or max is not a float".to_string())),
    }
}

fn cmd_zcount(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (min, min_excl) = match parse_score_bound(&argv[2]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (max, max_excl) = match parse_score_bound(&argv[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::ZSet(z)) => Reply::Integer(z.range_by_score(min, min_excl, max, max_excl).len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

/// Shared implementation for `ZRANGEBYSCORE key min max` and
/// `ZREVRANGEBYSCORE key max min` (the same filter, with the two bound
/// arguments swapped and the result order reversed).
fn cmd_zrangebyscore_impl(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], reverse: bool) -> Reply {
    let (min_arg, max_arg) = if reverse { (&argv[3], &argv[2]) } else { (&argv[2], &argv[3]) };
    let (min, min_excl) = match parse_score_bound(min_arg) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (max, max_excl) = match parse_score_bound(max_arg) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < argv.len() {
        let opt = ok_bytes_as_string(&argv[i]).to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" if i + 2 < argv.len() => {
                let (Ok(offset), Ok(count)) = (ok_bytes_as_string(&argv[i + 1]).parse::<i64>(), ok_bytes_as_string(&argv[i + 2]).parse::<i64>()) else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                limit = Some((offset, count));
                i += 3;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::ZSet(z)) => {
            let mut entries = z.range_by_score(min, min_excl, max, max_excl);
            if reverse {
                entries.reverse();
            }
            if let Some((offset, count)) = limit {
                let entries_after_offset = entries.into_iter().skip(offset.max(0) as usize);
                entries = if count >= 0 { entries_after_offset.take(count as usize).collect() } else { entries_after_offset.collect() };
            }
            let mut out = Vec::with_capacity(entries.len() * if with_scores { 2 } else { 1 });
            for (member, score) in entries {
                out.push(Reply::bulk(member));
                if with_scores {
                    out.push(Reply::bulk(fmt_score(score)));
                }
            }
            Reply::Array(out)
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrangebyscore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zrangebyscore_impl(s, c, argv, false)
}

fn cmd_zrevrangebyscore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zrangebyscore_impl(s, c, argv, true)
}

/// Parses one `ZRANGEBYLEX`/`ZLEXCOUNT`-style bound: `-`/`+` (open-ended),
/// or a member prefixed with `[` (inclusive) or `(` (exclusive) — matching
/// `zslParseLexRangeItem`.
fn parse_lex_bound(b: &[u8]) -> Result<LexBound, Reply> {
    match b {
        b"-" => Ok(LexBound::NegInf),
        b"+" => Ok(LexBound::PosInf),
        [b'[', rest @ ..] => Ok(LexBound::Incl(rest.to_vec())),
        [b'(', rest @ ..] => Ok(LexBound::Excl(rest.to_vec())),
        _ => Err(Reply::Error("ERR min or max not valid string range item".to_string())),
    }
}

fn cmd_zlexcount(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let min = match parse_lex_bound(&argv[2]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let max = match parse_lex_bound(&argv[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::ZSet(z)) => Reply::Integer(z.range_by_lex(&min, &max).len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

/// Shared implementation for `ZRANGEBYLEX key min max` and
/// `ZREVRANGEBYLEX key max min` (the same filter, with the two bound
/// arguments swapped and the result order reversed) — no `WITHSCORES`,
/// matching real Redis (lex ranges assume every member shares one score).
fn cmd_zrangebylex_impl(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], reverse: bool) -> Reply {
    let (min_arg, max_arg) = if reverse { (&argv[3], &argv[2]) } else { (&argv[2], &argv[3]) };
    let min = match parse_lex_bound(min_arg) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let max = match parse_lex_bound(max_arg) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < argv.len() {
        let opt = ok_bytes_as_string(&argv[i]).to_ascii_uppercase();
        match opt.as_str() {
            "LIMIT" if i + 2 < argv.len() => {
                let (Ok(offset), Ok(count)) = (ok_bytes_as_string(&argv[i + 1]).parse::<i64>(), ok_bytes_as_string(&argv[i + 2]).parse::<i64>()) else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                limit = Some((offset, count));
                i += 3;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(vec![]),
        Some(RObj::ZSet(z)) => {
            let mut entries = z.range_by_lex(&min, &max);
            if reverse {
                entries.reverse();
            }
            if let Some((offset, count)) = limit {
                let entries_after_offset = entries.into_iter().skip(offset.max(0) as usize);
                entries = if count >= 0 { entries_after_offset.take(count as usize).collect() } else { entries_after_offset.collect() };
            }
            Reply::Array(entries.into_iter().map(|(member, _)| Reply::bulk(member)).collect())
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrangebylex(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zrangebylex_impl(s, c, argv, false)
}

fn cmd_zrevrangebylex(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zrangebylex_impl(s, c, argv, true)
}

fn cmd_zincrby(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(delta) = ok_bytes_as_string(&argv[2]).parse::<f64>() else {
        return Reply::Error("ERR value is not a valid float".to_string());
    };
    let max_entries = s.config.get_int("zset-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("zset-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::ZSet(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::ZSet(ZSetObj::new()));
    }
    let RObj::ZSet(zset) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    let next = zset.incr_by(&argv[3], delta, max_entries, max_value);
    notify_keyspace_event(s, c.db_index, 'z', "zincr", &argv[1]);
    Reply::bulk(fmt_score(next))
}

fn cmd_zpop(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], from_min: bool) -> Reply {
    let count = if argv.len() == 3 {
        match ok_bytes_as_string(&argv[2]).parse::<i64>() {
            Ok(n) if n >= 0 => n as usize,
            _ => return Reply::Error("ERR value is out of range, must be positive".to_string()),
        }
    } else {
        1
    };
    let db = &mut s.dbs[c.db_index].dict;
    let (popped, became_empty) = match db.get_mut(SLOT, &argv[1]) {
        None => return Reply::Array(vec![]),
        Some(RObj::ZSet(z)) => {
            let popped = z.pop(count, from_min);
            (popped, z.is_empty())
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    delete_if_empty(s, c, &argv[1], became_empty);
    if !popped.is_empty() {
        notify_keyspace_event(s, c.db_index, 'z', if from_min { "zpopmin" } else { "zpopmax" }, &argv[1]);
    }
    let mut out = Vec::with_capacity(popped.len() * 2);
    for (member, score) in popped {
        out.push(Reply::bulk(member));
        out.push(Reply::bulk(fmt_score(score)));
    }
    Reply::Array(out)
}

fn cmd_zpopmin(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zpop(s, c, argv, true)
}

fn cmd_zpopmax(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zpop(s, c, argv, false)
}

fn cmd_zmscore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Array(argv[2..].iter().map(|_| Reply::Null).collect()),
        Some(RObj::ZSet(z)) => Reply::Array(argv[2..].iter().map(|m| z.score(m).map_or(Reply::Null, |sc| Reply::bulk(fmt_score(sc)))).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn cmd_zrandmember(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let with_count = argv.len() >= 3;
    let count = if with_count {
        match ok_bytes_as_string(&argv[2]).parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Reply::Error("ERR value is not an integer or out of range".to_string()),
        }
    } else {
        1
    };
    let with_scores = match argv.len() {
        2 | 3 => false,
        4 if argv[3].eq_ignore_ascii_case(b"WITHSCORES") => true,
        _ => return Reply::Error("ERR syntax error".to_string()),
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => {
            if with_count {
                Reply::Array(vec![])
            } else {
                Reply::Null
            }
        }
        Some(RObj::ZSet(z)) => {
            let picked = z.random_members(count, crate::object::pseudo_random_index);
            if with_count {
                let mut out = Vec::with_capacity(picked.len() * if with_scores { 2 } else { 1 });
                for (member, score) in picked {
                    out.push(Reply::bulk(member));
                    if with_scores {
                        out.push(Reply::bulk(fmt_score(score)));
                    }
                }
                Reply::Array(out)
            } else {
                match picked.into_iter().next() {
                    Some((member, _)) => Reply::bulk(member),
                    None => Reply::Null,
                }
            }
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

/// Reads every key in `keys` as a zset (a missing key counts as an empty
/// one, `member -> score`), returning `Err` on the first non-zset value.
/// Shared by `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE` and their `STORE`-less
/// siblings.
fn read_zsets_or_wrongtype(s: &Server, dbidx: usize, keys: &[Vec<u8>]) -> Result<Vec<std::collections::HashMap<Vec<u8>, f64>>, Reply> {
    keys.iter()
        .map(|k| match s.dbs[dbidx].dict.get(SLOT, k) {
            None => Ok(std::collections::HashMap::new()),
            Some(RObj::ZSet(z)) => Ok(z.range(0, -1).into_iter().collect()),
            Some(_) => Err(Reply::Error(WRONGTYPE.to_string())),
        })
        .collect()
}

/// `ZUNIONSTORE`/`ZINTERSTORE`'s optional `AGGREGATE` clause — how scores
/// from multiple sources combine for a member present in more than one of
/// them. Real Redis defaults to `SUM`; `ZDIFFSTORE`/`ZDIFF` don't take this
/// option at all (a diff's surviving members keep their first set's
/// original score unchanged, so there's nothing to aggregate).
enum Aggregate {
    Sum,
    Min,
    Max,
}

impl Aggregate {
    fn combine(&self, a: f64, b: f64) -> f64 {
        match self {
            Aggregate::Sum => a + b,
            Aggregate::Min => a.min(b),
            Aggregate::Max => a.max(b),
        }
    }
}

struct ZSetOpOptions {
    weights: Vec<f64>,
    aggregate: Aggregate,
    with_scores: bool,
}

/// Shared trailing-option parser for `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE`
/// and `ZUNION`/`ZINTER`/`ZDIFF`: `[WEIGHTS w1 w2...] [AGGREGATE SUM|MIN|MAX]
/// [WITHSCORES]`. `allow_weights_aggregate` is `false` for the `DIFF`
/// variants (real Redis's grammar simply doesn't accept those two options
/// there) and `allow_withscores` is `false` for the `STORE` variants (a
/// stored zset's scores speak for themselves; there's no reply to attach
/// them to).
fn parse_zset_op_options(argv: &[Vec<u8>], start: usize, numkeys: usize, allow_weights_aggregate: bool, allow_withscores: bool) -> Result<ZSetOpOptions, Reply> {
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut with_scores = false;
    let mut i = start;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "WEIGHTS" if allow_weights_aggregate && i + numkeys < argv.len() => {
                for (j, w) in weights.iter_mut().enumerate() {
                    let Ok(parsed) = ok_bytes_as_string(&argv[i + 1 + j]).parse::<f64>() else {
                        return Err(Reply::Error("ERR weight value is not a float".to_string()));
                    };
                    *w = parsed;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" if allow_weights_aggregate && i + 1 < argv.len() => {
                aggregate = match ok_bytes_as_string(&argv[i + 1]).to_ascii_uppercase().as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(Reply::Error("ERR syntax error".to_string())),
                };
                i += 2;
            }
            "WITHSCORES" if allow_withscores => {
                with_scores = true;
                i += 1;
            }
            _ => return Err(Reply::Error("ERR syntax error".to_string())),
        }
    }
    Ok(ZSetOpOptions { weights, aggregate, with_scores })
}

/// The actual union/intersection/diff math, shared by every `Z*STORE`
/// command and its `STORE`-less sibling. `weights` apply to `Union`/`Inter`
/// only (ignored for `Diff`, which has no weighting concept — see
/// `parse_zset_op_options`'s doc comment).
fn compute_zset_op(sets: &[std::collections::HashMap<Vec<u8>, f64>], weights: &[f64], agg: &Aggregate, op: &SetOp) -> std::collections::HashMap<Vec<u8>, f64> {
    match op {
        SetOp::Diff => {
            let Some((first, rest)) = sets.split_first() else { return std::collections::HashMap::new() };
            first.iter().filter(|(member, _)| !rest.iter().any(|m| m.contains_key(*member))).map(|(m, s)| (m.clone(), *s)).collect()
        }
        SetOp::Union => {
            let mut acc: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
            for (i, m) in sets.iter().enumerate() {
                let w = weights.get(i).copied().unwrap_or(1.0);
                for (member, &score) in m {
                    let weighted = score * w;
                    acc.entry(member.clone()).and_modify(|e| *e = agg.combine(*e, weighted)).or_insert(weighted);
                }
            }
            acc
        }
        SetOp::Inter => {
            let Some((first, rest)) = sets.split_first() else { return std::collections::HashMap::new() };
            let w0 = weights.first().copied().unwrap_or(1.0);
            first
                .iter()
                .filter_map(|(member, &score)| {
                    let mut combined = score * w0;
                    for (i, m) in rest.iter().enumerate() {
                        let w = weights.get(i + 1).copied().unwrap_or(1.0);
                        let other = *m.get(member)? * w;
                        combined = agg.combine(combined, other);
                    }
                    Some((member.clone(), combined))
                })
                .collect()
        }
    }
}

/// `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE destination numkeys key [key...]
/// [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX]` (the last two only for
/// `UNION`/`INTER` — see `parse_zset_op_options`).
fn cmd_zset_op_store(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], op: SetOp) -> Reply {
    let Ok(numkeys) = ok_bytes_as_string(&argv[2]).parse::<usize>() else {
        return Reply::Error("ERR numkeys should be greater than 0".to_string());
    };
    if numkeys == 0 || argv.len() < 3 + numkeys {
        return Reply::Error("ERR numkeys should be greater than 0".to_string());
    }
    let keys = argv[3..3 + numkeys].to_vec();
    let opts = match parse_zset_op_options(argv, 3 + numkeys, numkeys, !matches!(op, SetOp::Diff), false) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let maps = match read_zsets_or_wrongtype(s, c.db_index, &keys) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let result = compute_zset_op(&maps, &opts.weights, &opts.aggregate, &op);
    let n = result.len();
    if n == 0 {
        s.dbs[c.db_index].remove_key(&argv[1]);
    } else {
        let max_entries = s.config.get_int("zset-max-listpack-entries").unwrap_or(128);
        let max_value = s.config.get_int("zset-max-listpack-value").unwrap_or(64);
        let mut zset = ZSetObj::new();
        for (member, score) in result {
            zset.add(&member, score, max_entries, max_value);
        }
        s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::ZSet(zset));
    }
    let event = match op {
        SetOp::Union => "zunionstore",
        SetOp::Inter => "zinterstore",
        SetOp::Diff => "zdiffstore",
    };
    notify_keyspace_event(s, c.db_index, 'z', event, &argv[1]);
    Reply::Integer(n as i64)
}

fn cmd_zunionstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zset_op_store(s, c, argv, SetOp::Union)
}

fn cmd_zinterstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zset_op_store(s, c, argv, SetOp::Inter)
}

fn cmd_zdiffstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zset_op_store(s, c, argv, SetOp::Diff)
}

/// `ZUNION`/`ZINTER`/`ZDIFF numkeys key [key...] [WEIGHTS w...] [AGGREGATE
/// SUM|MIN|MAX] [WITHSCORES]` — the `STORE`-less siblings of the commands
/// just above: same set math, but the result comes back as a reply (sorted
/// ascending by score, ties broken lexicographically — the usual zset
/// order) instead of being written to a destination key.
fn cmd_zset_op_reply(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], op: SetOp) -> Reply {
    let Ok(numkeys) = ok_bytes_as_string(&argv[1]).parse::<usize>() else {
        return Reply::Error("ERR numkeys should be greater than 0".to_string());
    };
    if numkeys == 0 || argv.len() < 2 + numkeys {
        return Reply::Error("ERR numkeys should be greater than 0".to_string());
    }
    let keys = argv[2..2 + numkeys].to_vec();
    let opts = match parse_zset_op_options(argv, 2 + numkeys, numkeys, !matches!(op, SetOp::Diff), true) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let maps = match read_zsets_or_wrongtype(s, c.db_index, &keys) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let result = compute_zset_op(&maps, &opts.weights, &opts.aggregate, &op);
    let mut entries: Vec<(Vec<u8>, f64)> = result.into_iter().collect();
    entries.sort_by(|(am, asc), (bm, bsc)| asc.partial_cmp(bsc).unwrap_or(std::cmp::Ordering::Equal).then_with(|| am.cmp(bm)));
    let mut out = Vec::with_capacity(entries.len() * if opts.with_scores { 2 } else { 1 });
    for (member, score) in entries {
        out.push(Reply::bulk(member));
        if opts.with_scores {
            out.push(Reply::bulk(fmt_score(score)));
        }
    }
    Reply::Array(out)
}

fn cmd_zunion(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zset_op_reply(s, c, argv, SetOp::Union)
}

fn cmd_zinter(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zset_op_reply(s, c, argv, SetOp::Inter)
}

fn cmd_zdiff(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    cmd_zset_op_reply(s, c, argv, SetOp::Diff)
}

/// `ZINTERCARD numkeys key [key...] [LIMIT limit]` — the intersection's
/// cardinality without materializing (or storing) the member list itself.
/// `LIMIT 0` (the default) means unlimited, matching real Redis.
fn cmd_zintercard(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(numkeys) = ok_bytes_as_string(&argv[1]).parse::<usize>() else {
        return Reply::Error("ERR numkeys should be greater than 0".to_string());
    };
    if numkeys == 0 || argv.len() < 2 + numkeys {
        return Reply::Error("ERR numkeys should be greater than 0".to_string());
    }
    let keys = argv[2..2 + numkeys].to_vec();
    let mut limit: usize = 0;
    let mut i = 2 + numkeys;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "LIMIT" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                    return Reply::Error("ERR LIMIT can't be negative".to_string());
                };
                if n < 0 {
                    return Reply::Error("ERR LIMIT can't be negative".to_string());
                }
                limit = n as usize;
                i += 2;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    let maps = match read_zsets_or_wrongtype(s, c.db_index, &keys) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let result = compute_zset_op(&maps, &vec![1.0; numkeys], &Aggregate::Sum, &SetOp::Inter);
    let count = if limit > 0 { result.len().min(limit) } else { result.len() };
    Reply::Integer(count as i64)
}

// -------------------------------------------------------------- Bitops ----
// `bitops.c` ported onto `RObj::String` directly (no new storage — bit
// operations are just byte-indexed views over a string's existing bytes,
// same as real Redis). `BITFIELD` isn't included (its own sub-language of
// signed/unsigned multi-width get/set/incrby ops is a project in itself);
// everything else real Redis calls "bit operations" is here.

/// Clamps a possibly-negative `[start, end]` range (same convention as
/// `LRANGE`) against `[0, len)`, returning `None` for an empty/out-of-range
/// result.
fn normalize_range(mut start: i64, mut end: i64, len: i64) -> Option<(i64, i64)> {
    if len == 0 {
        return None;
    }
    if start < 0 {
        start += len;
    }
    if end < 0 {
        end += len;
    }
    start = start.max(0);
    end = end.min(len - 1);
    if start > end || start >= len {
        return None;
    }
    Some((start, end))
}

fn cmd_setbit(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(offset) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Reply::Error("ERR bit offset is not an integer or out of range".to_string());
    };
    // Matches `proto-max-bulk-len`'s default (512MB) as the practical cap on
    // how large a single string value — and thus a bit offset into it — can
    // be; real Redis ties this to the same config value.
    if !(0..512 * 1024 * 1024 * 8).contains(&offset) {
        return Reply::Error("ERR bit offset is not an integer or out of range".to_string());
    }
    let bitval = match argv[3].as_slice() {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return Reply::Error("ERR bit is not an integer or out of range".to_string()),
    };
    let byte_idx = (offset / 8) as usize;
    let bit_idx = 7 - (offset % 8) as u32;

    let db = &mut s.dbs[c.db_index].dict;
    let mut buf = match db.get(SLOT, &argv[1]) {
        None => Vec::new(),
        Some(RObj::String(v)) => v.clone(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if buf.len() <= byte_idx {
        buf.resize(byte_idx + 1, 0);
    }
    let old = (buf[byte_idx] >> bit_idx) & 1;
    if bitval == 1 {
        buf[byte_idx] |= 1 << bit_idx;
    } else {
        buf[byte_idx] &= !(1 << bit_idx);
    }
    db.insert(SLOT, argv[1].clone(), RObj::String(buf));
    notify_keyspace_event(s, c.db_index, '$', "setbit", &argv[1]);
    Reply::Integer(old as i64)
}

fn cmd_getbit(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Ok(offset) = ok_bytes_as_string(&argv[2]).parse::<i64>() else {
        return Reply::Error("ERR bit offset is not an integer or out of range".to_string());
    };
    if offset < 0 {
        return Reply::Error("ERR bit offset is not an integer or out of range".to_string());
    }
    let byte_idx = (offset / 8) as usize;
    let bit_idx = 7 - (offset % 8) as u32;
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::String(v)) => Reply::Integer(if byte_idx >= v.len() { 0 } else { ((v[byte_idx] >> bit_idx) & 1) as i64 }),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn parse_byte_or_bit_mode(argv: &[Vec<u8>], idx: usize) -> Result<bool, Reply> {
    if idx >= argv.len() {
        return Ok(false);
    }
    match ok_bytes_as_string(&argv[idx]).to_ascii_uppercase().as_str() {
        "BYTE" => Ok(false),
        "BIT" => Ok(true),
        _ => Err(Reply::Error("ERR syntax error".to_string())),
    }
}

fn cmd_bitcount(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let bytes = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => return Reply::Integer(0),
        Some(RObj::String(v)) => v.clone(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if argv.len() == 2 {
        return Reply::Integer(bytes.iter().map(|b| b.count_ones() as i64).sum());
    }
    if argv.len() != 4 && argv.len() != 5 {
        return Reply::Error("ERR syntax error".to_string());
    }
    let (Ok(start), Ok(end)) = (ok_bytes_as_string(&argv[2]).parse::<i64>(), ok_bytes_as_string(&argv[3]).parse::<i64>()) else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    let bit_mode = match parse_byte_or_bit_mode(argv, 4) {
        Ok(m) => m,
        Err(e) => return e,
    };

    if bit_mode {
        let Some((s_idx, e_idx)) = normalize_range(start, end, bytes.len() as i64 * 8) else { return Reply::Integer(0) };
        let count = (s_idx..=e_idx)
            .filter(|&bit| {
                let byte_idx = (bit / 8) as usize;
                let bit_idx = 7 - (bit % 8) as u32;
                (bytes[byte_idx] >> bit_idx) & 1 == 1
            })
            .count();
        Reply::Integer(count as i64)
    } else {
        let Some((s_idx, e_idx)) = normalize_range(start, end, bytes.len() as i64) else { return Reply::Integer(0) };
        Reply::Integer(bytes[s_idx as usize..=e_idx as usize].iter().map(|b| b.count_ones() as i64).sum())
    }
}

/// `BITPOS key bit [start [end [BYTE|BIT]]]` — the one quirky case kept
/// faithful to real Redis: searching for a `0` bit with no explicit `end`
/// on an all-`1`-bits string reports the position *just past* the string
/// (an implicit infinite run of `0`s beyond it), not `-1`.
fn cmd_bitpos(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let bytes = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Vec::new(),
        Some(RObj::String(v)) => v.clone(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    let target_bit = match argv[2].as_slice() {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return Reply::Error("ERR The bit argument must be 1 or 0.".to_string()),
    };
    let explicit_end = argv.len() >= 5;
    let bit_mode = match parse_byte_or_bit_mode(argv, 5) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let total = if bit_mode { bytes.len() as i64 * 8 } else { bytes.len() as i64 };

    let (start, end) = match argv.len() {
        3 => (0, total - 1),
        4 => match parse_i64_arg(argv, 3) {
            Ok(s0) => (s0, total - 1),
            Err(e) => return e,
        },
        _ => match (parse_i64_arg(argv, 3), parse_i64_arg(argv, 4)) {
            (Ok(s0), Ok(e0)) => (s0, e0),
            (Err(e), _) | (_, Err(e)) => return e,
        },
    };
    let Some((s_idx, e_idx)) = normalize_range(start, end, total) else { return Reply::Integer(-1) };
    let (bit_start, bit_end) = if bit_mode { (s_idx, e_idx) } else { (s_idx * 8, e_idx * 8 + 7) };

    for bit in bit_start..=bit_end {
        let byte_idx = (bit / 8) as usize;
        if byte_idx >= bytes.len() {
            break;
        }
        let bit_idx = 7 - (bit % 8) as u32;
        if (bytes[byte_idx] >> bit_idx) & 1 == target_bit {
            return Reply::Integer(bit);
        }
    }
    if target_bit == 0 && !explicit_end {
        return Reply::Integer(bit_end + 1);
    }
    Reply::Integer(-1)
}

fn cmd_bitop(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let op = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    let destkey = argv[2].clone();
    let mut sources: Vec<Vec<u8>> = Vec::with_capacity(argv.len() - 3);
    for key in &argv[3..] {
        match s.dbs[c.db_index].dict.get(SLOT, key) {
            None => sources.push(Vec::new()),
            Some(RObj::String(v)) => sources.push(v.clone()),
            Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        }
    }
    if op == "NOT" && sources.len() != 1 {
        return Reply::Error("ERR BITOP NOT must be called with a single source key.".to_string());
    }
    let maxlen = sources.iter().map(Vec::len).max().unwrap_or(0);
    let mut result = vec![0u8; maxlen];
    match op.as_str() {
        "AND" => {
            for (i, out) in result.iter_mut().enumerate() {
                *out = sources.iter().map(|src| *src.get(i).unwrap_or(&0)).fold(0xFF, |a, b| a & b);
            }
        }
        "OR" => {
            for (i, out) in result.iter_mut().enumerate() {
                *out = sources.iter().map(|src| *src.get(i).unwrap_or(&0)).fold(0, |a, b| a | b);
            }
        }
        "XOR" => {
            for (i, out) in result.iter_mut().enumerate() {
                *out = sources.iter().map(|src| *src.get(i).unwrap_or(&0)).fold(0, |a, b| a ^ b);
            }
        }
        "NOT" => {
            for (i, out) in result.iter_mut().enumerate() {
                *out = !*sources[0].get(i).unwrap_or(&0);
            }
        }
        _ => return Reply::Error("ERR syntax error".to_string()),
    }
    let len = result.len();
    if result.is_empty() {
        s.dbs[c.db_index].remove_key(&destkey);
    } else {
        s.dbs[c.db_index].dict.insert(SLOT, destkey.clone(), RObj::String(result));
    }
    notify_keyspace_event(s, c.db_index, '$', "set", &destkey);
    Reply::Integer(len as i64)
}

// ---------------------------------------------------------------- SORT ----

/// Collects a List/Set/ZSet's elements as plain byte strings for `SORT`
/// (member order from a `Set` is its own internal order — real Redis has
/// the same "a set has no defined pre-sort order" property, so this isn't a
/// new nondeterminism `SORT` doesn't already have to live with).
fn sort_source_elements(s: &Server, dbidx: usize, key: &[u8]) -> Result<Vec<Vec<u8>>, Reply> {
    match s.dbs[dbidx].dict.get(SLOT, &key.to_vec()) {
        None => Ok(Vec::new()),
        Some(RObj::List(l)) => Ok(l.range(0, -1)),
        Some(RObj::Set(set)) => Ok(set.members()),
        Some(RObj::ZSet(z)) => Ok(z.range(0, -1).into_iter().map(|(member, _)| member).collect()),
        Some(_) => Err(Reply::Error(WRONGTYPE.to_string())),
    }
}

/// `SORT key [ASC|DESC] [ALPHA] [LIMIT offset count] [STORE destination]`.
/// Real `SORT`'s `BY`/`GET` external-key-pattern options (substituting
/// `weight_*`/other keys' contents by templating the sort key into a
/// pattern) aren't included — enough extra machinery (hash-field patterns,
/// `#` self-reference, skip-the-actual-sort-if-`BY nosort`) to be its own
/// unit of work, and nothing else in this phase depends on it.
fn cmd_sort(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut alpha = false;
    let mut desc = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut store: Option<Vec<u8>> = None;
    let mut i = 2;
    while i < argv.len() {
        let opt = ok_bytes_as_string(&argv[i]).to_ascii_uppercase();
        match opt.as_str() {
            "ALPHA" => {
                alpha = true;
                i += 1;
            }
            "ASC" => {
                desc = false;
                i += 1;
            }
            "DESC" => {
                desc = true;
                i += 1;
            }
            "LIMIT" if i + 2 < argv.len() => {
                let (Ok(off), Ok(cnt)) = (ok_bytes_as_string(&argv[i + 1]).parse::<i64>(), ok_bytes_as_string(&argv[i + 2]).parse::<i64>()) else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                limit = Some((off, cnt));
                i += 3;
            }
            "STORE" if i + 1 < argv.len() => {
                store = Some(argv[i + 1].clone());
                i += 2;
            }
            "BY" | "GET" => return Reply::Error("ERR SORT's BY/GET options aren't supported by this fork".to_string()),
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }

    let mut items = match sort_source_elements(s, c.db_index, &argv[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };

    if alpha {
        items.sort();
    } else {
        let mut parsed: Vec<(f64, Vec<u8>)> = Vec::with_capacity(items.len());
        for item in items {
            let Some(n) = std::str::from_utf8(&item).ok().and_then(|s| s.trim().parse::<f64>().ok()) else {
                return Reply::Error("ERR One or more scores can't be converted into double".to_string());
            };
            parsed.push((n, item));
        }
        parsed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        items = parsed.into_iter().map(|(_, v)| v).collect();
    }
    if desc {
        items.reverse();
    }
    if let Some((off, cnt)) = limit {
        let off = off.max(0) as usize;
        let take = if cnt < 0 { usize::MAX } else { cnt as usize };
        items = items.into_iter().skip(off).take(take).collect();
    }

    if let Some(dest) = store {
        let n = items.len();
        if items.is_empty() {
            s.dbs[c.db_index].remove_key(&dest);
        } else {
            let mut list = ListObj::new();
            list.push_back(&items);
            s.dbs[c.db_index].dict.insert(SLOT, dest.clone(), RObj::List(list));
            notify_keyspace_event(s, c.db_index, 'l', "sortstore", &dest);
        }
        // `SORT ... STORE` is the one case where this command actually
        // writes — propagate/AOF-log it (the whole command verbatim, same
        // as every other write here) and invalidate tracking on the
        // *destination* key it wrote, not `argv[1]` (its read-only source).
        s.after_write(c, argv);
        s.invalidate_tracking_for_write(&[b"SORT".to_vec(), dest], c.id);
        Reply::Integer(n as i64)
    } else {
        Reply::Array(items.into_iter().map(Reply::bulk).collect())
    }
}

// ---------------------------------------------------------- HyperLogLog ---
// `PFADD`/`PFCOUNT`/`PFMERGE` layered on `RObj::String`, same as real Redis —
// see `hll.rs`'s module doc for exactly how this port's HLL differs from
// `hyperloglog.c`'s (simpler, not byte-compatible, statistically
// equivalent). `PFDEBUG`/`PFSELFTEST` (internal diagnostics) aren't
// included — nothing in this fork's own test suite needs them.

const HLL_WRONGTYPE: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";

fn cmd_pfadd(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let db = &mut s.dbs[c.db_index].dict;
    let existed = db.get(SLOT, &argv[1]).is_some();
    let mut buf = match db.get(SLOT, &argv[1]) {
        None => crate::hll::new_empty(),
        Some(RObj::String(v)) if crate::hll::is_hll(v) => v.clone(),
        Some(RObj::String(_)) => return Reply::Error(HLL_WRONGTYPE.to_string()),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    let mut changed = !existed;
    for el in &argv[2..] {
        if crate::hll::add(&mut buf, el) {
            changed = true;
        }
    }
    db.insert(SLOT, argv[1].clone(), RObj::String(buf));
    if changed {
        notify_keyspace_event(s, c.db_index, '$', "pfadd", &argv[1]);
    }
    Reply::Integer(changed as i64)
}

/// Shared by `PFCOUNT`/`PFMERGE`: every named key must be either absent or
/// an existing HLL string — collects the ones that exist.
fn collect_hlls(s: &Server, dbidx: usize, keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, Reply> {
    let mut bufs = Vec::with_capacity(keys.len());
    for key in keys {
        match s.dbs[dbidx].dict.get(SLOT, key) {
            None => {}
            Some(RObj::String(v)) if crate::hll::is_hll(v) => bufs.push(v.clone()),
            Some(RObj::String(_)) => return Err(Reply::Error(HLL_WRONGTYPE.to_string())),
            Some(_) => return Err(Reply::Error(WRONGTYPE.to_string())),
        }
    }
    Ok(bufs)
}

fn cmd_pfcount(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let bufs = match collect_hlls(s, c.db_index, &argv[1..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match bufs.len() {
        0 => Reply::Integer(0),
        1 => Reply::Integer(crate::hll::count(&bufs[0]) as i64),
        _ => {
            let refs: Vec<&[u8]> = bufs.iter().map(Vec::as_slice).collect();
            Reply::Integer(crate::hll::count(&crate::hll::merge(&refs)) as i64)
        }
    }
}

/// `PFMERGE destkey [sourcekey ...]` — `destkey` is itself included among
/// the sources if it already exists (matching real Redis: merging replaces
/// it, but its own prior contents still count toward the result).
fn cmd_pfmerge(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let bufs = match collect_hlls(s, c.db_index, &argv[1..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let merged = if bufs.is_empty() {
        crate::hll::new_empty()
    } else {
        let refs: Vec<&[u8]> = bufs.iter().map(Vec::as_slice).collect();
        crate::hll::merge(&refs)
    };
    s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::String(merged));
    notify_keyspace_event(s, c.db_index, '$', "pfadd", &argv[1]);
    Reply::ok()
}

// ----------------------------------------------------------------- Geo ---
// `geo.c` ported onto `RObj::ZSet` directly, same as real Redis — see
// `geo.rs`'s module doc for the exact scope and the encode/decode/distance
// design.

fn geo_unit_to_meters_divisor(unit: &str) -> Result<f64, Reply> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "mi" => Ok(1609.34),
        "ft" => Ok(0.3048),
        _ => Err(Reply::Error("ERR unsupported unit provided. please use M, KM, FT, MI".to_string())),
    }
}

fn cmd_geoadd(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() < 5 || !(argv.len() - 2).is_multiple_of(3) {
        return Reply::Error("ERR wrong number of arguments for 'geoadd' command".to_string());
    }
    let mut pairs = Vec::with_capacity((argv.len() - 2) / 3);
    for chunk in argv[2..].chunks(3) {
        let (Ok(lon), Ok(lat)) = (ok_bytes_as_string(&chunk[0]).parse::<f64>(), ok_bytes_as_string(&chunk[1]).parse::<f64>()) else {
            return Reply::Error("ERR value is not a valid float".to_string());
        };
        if !crate::geo::valid_coords(lon, lat) {
            return Reply::Error(format!("ERR invalid longitude,latitude pair {lon:.6},{lat:.6}"));
        }
        pairs.push((crate::geo::encode(lon, lat) as f64, chunk[2].clone()));
    }

    let max_entries = s.config.get_int("zset-max-listpack-entries").unwrap_or(128);
    let max_value = s.config.get_int("zset-max-listpack-value").unwrap_or(64);
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::ZSet(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::ZSet(ZSetObj::new()));
    }
    let RObj::ZSet(zset) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    let added = pairs.into_iter().filter(|(score, member)| zset.add(member, *score, max_entries, max_value)).count();
    notify_keyspace_event(s, c.db_index, 'z', "geoadd", &argv[1]);
    Reply::Integer(added as i64)
}

fn cmd_geopos(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let positions: Vec<Reply> = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => argv[2..].iter().map(|_| Reply::NullArray).collect(),
        Some(RObj::ZSet(z)) => argv[2..]
            .iter()
            .map(|m| match z.score(m) {
                None => Reply::NullArray,
                Some(score) => {
                    let (lon, lat) = crate::geo::decode(score.round() as u64);
                    Reply::Array(vec![Reply::bulk(format!("{lon:.17}").into_bytes()), Reply::bulk(format!("{lat:.17}").into_bytes())])
                }
            })
            .collect(),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    Reply::Array(positions)
}

fn cmd_geodist(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let unit_divisor = if argv.len() == 5 {
        match geo_unit_to_meters_divisor(&ok_bytes_as_string(&argv[4])) {
            Ok(d) => d,
            Err(e) => return e,
        }
    } else {
        1.0
    };
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Null,
        Some(RObj::ZSet(z)) => {
            let (Some(s1), Some(s2)) = (z.score(&argv[2]), z.score(&argv[3])) else { return Reply::Null };
            let (lon1, lat1) = crate::geo::decode(s1.round() as u64);
            let (lon2, lat2) = crate::geo::decode(s2.round() as u64);
            let meters = crate::geo::distance_m(lon1, lat1, lon2, lat2);
            Reply::bulk(format!("{:.4}", meters / unit_divisor).into_bytes())
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

struct GeoSearchOptions {
    from: (f64, f64),
    by_radius_m: Option<f64>,
    by_box_m: Option<(f64, f64)>,
    asc: bool,
    count: Option<usize>,
    withcoord: bool,
    withdist: bool,
    unit_divisor: f64,
    storedist: bool,
}

/// Shared option grammar for `GEOSEARCH`/`GEOSEARCHSTORE`: `<FROMMEMBER
/// member | FROMLONLAT lon lat> <BYRADIUS r unit | BYBOX w h unit> [ASC|DESC]
/// [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [STOREDIST]`. `FROMMEMBER`
/// resolves to that member's own stored geohash (an error if it's missing or
/// `key` isn't a zset) — the same `key`/db `GEOSEARCH`/`GEOSEARCHSTORE`
/// reads from either way, just called out for `FROMMEMBER` specifically
/// since it's the one option that needs to look anything up. `WITHCOORD`/
/// `WITHDIST` are accepted even for `GEOSEARCHSTORE` (real Redis's grammar
/// doesn't offer them there, since there's no reply to attach them to) —
/// harmlessly ignored by the store path rather than rejected, the same
/// permissive-superset spirit `REPLCONF` already takes with unrecognized
/// options.
fn parse_geosearch_options(s: &Server, dbidx: usize, key: &Vec<u8>, argv: &[Vec<u8>], start: usize) -> Result<GeoSearchOptions, Reply> {
    let mut from: Option<(f64, f64)> = None;
    let mut by_radius_m: Option<f64> = None;
    let mut by_box_m: Option<(f64, f64)> = None;
    let mut asc = true;
    let mut count: Option<usize> = None;
    let mut withcoord = false;
    let mut withdist = false;
    let mut storedist = false;
    let mut unit_divisor = 1.0;

    let mut i = start;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "FROMLONLAT" if i + 2 < argv.len() => {
                let (Ok(lon), Ok(lat)) = (ok_bytes_as_string(&argv[i + 1]).parse::<f64>(), ok_bytes_as_string(&argv[i + 2]).parse::<f64>()) else {
                    return Err(Reply::Error("ERR value is not a valid float".to_string()));
                };
                from = Some((lon, lat));
                i += 3;
            }
            "FROMMEMBER" if i + 1 < argv.len() => {
                let score = match s.dbs[dbidx].dict.get(SLOT, key) {
                    Some(RObj::ZSet(z)) => z.score(&argv[i + 1]),
                    Some(_) => return Err(Reply::Error(WRONGTYPE.to_string())),
                    None => None,
                };
                let Some(score) = score else {
                    return Err(Reply::Error("ERR could not decode requested zset member".to_string()));
                };
                from = Some(crate::geo::decode(score.round() as u64));
                i += 2;
            }
            "BYRADIUS" if i + 2 < argv.len() => {
                let Ok(r) = ok_bytes_as_string(&argv[i + 1]).parse::<f64>() else {
                    return Err(Reply::Error("ERR value is not a valid float".to_string()));
                };
                let d = geo_unit_to_meters_divisor(&ok_bytes_as_string(&argv[i + 2]))?;
                by_radius_m = Some(r * d);
                unit_divisor = d;
                i += 3;
            }
            "BYBOX" if i + 3 < argv.len() => {
                let (Ok(w), Ok(h)) = (ok_bytes_as_string(&argv[i + 1]).parse::<f64>(), ok_bytes_as_string(&argv[i + 2]).parse::<f64>()) else {
                    return Err(Reply::Error("ERR value is not a valid float".to_string()));
                };
                let d = geo_unit_to_meters_divisor(&ok_bytes_as_string(&argv[i + 3]))?;
                by_box_m = Some((w * d, h * d));
                unit_divisor = d;
                i += 4;
            }
            "ASC" => {
                asc = true;
                i += 1;
            }
            "DESC" => {
                asc = false;
                i += 1;
            }
            "COUNT" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                    return Err(Reply::Error("ERR COUNT must be > 0".to_string()));
                };
                count = Some(n);
                i += 2;
                if i < argv.len() && argv[i].eq_ignore_ascii_case(b"ANY") {
                    i += 1; // accepted, ignored: this fork always scans every member anyway
                }
            }
            "WITHCOORD" => {
                withcoord = true;
                i += 1;
            }
            "WITHDIST" => {
                withdist = true;
                i += 1;
            }
            "STOREDIST" => {
                storedist = true;
                i += 1;
            }
            _ => return Err(Reply::Error("ERR syntax error".to_string())),
        }
    }
    let Some(from) = from else {
        return Err(Reply::Error("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH".to_string()));
    };
    if by_radius_m.is_none() && by_box_m.is_none() {
        return Err(Reply::Error("ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH".to_string()));
    }
    Ok(GeoSearchOptions { from, by_radius_m, by_box_m, asc, count, withcoord, withdist, unit_divisor, storedist })
}

/// `(member, lon, lat, distance_m, original_geohash_score)` — the last field
/// only `GEOSEARCHSTORE` needs (to preserve the original score unless
/// `STOREDIST` asks for the distance instead).
type GeoMatch = (Vec<u8>, f64, f64, f64, f64);

/// The actual search: every `(member, score)` in `key` within `opts`'s
/// radius/box of `opts.from`, sorted/limited per `opts`.
fn geosearch_run(s: &Server, dbidx: usize, key: &Vec<u8>, opts: &GeoSearchOptions) -> Result<Vec<GeoMatch>, Reply> {
    let entries = match s.dbs[dbidx].dict.get(SLOT, key) {
        None => Vec::new(),
        Some(RObj::ZSet(z)) => z.range(0, -1),
        Some(_) => return Err(Reply::Error(WRONGTYPE.to_string())),
    };
    let (flon, flat) = opts.from;
    let mut matched: Vec<GeoMatch> = Vec::new();
    for (member, score) in entries {
        let (lon, lat) = crate::geo::decode(score.round() as u64);
        let dist = crate::geo::distance_m(flon, flat, lon, lat);
        let inside = if let Some(r) = opts.by_radius_m {
            dist <= r
        } else if let Some((w, h)) = opts.by_box_m {
            let dlat_m = (lat - flat).to_radians().abs() * crate::geo::EARTH_RADIUS_M;
            let dlon_m = (lon - flon).to_radians().abs() * crate::geo::EARTH_RADIUS_M * flat.to_radians().cos();
            dlon_m <= w / 2.0 && dlat_m <= h / 2.0
        } else {
            unreachable!("parse_geosearch_options guarantees exactly one of BYRADIUS/BYBOX is set")
        };
        if inside {
            matched.push((member, lon, lat, dist, score));
        }
    }

    matched.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal));
    if !opts.asc {
        matched.reverse();
    }
    if let Some(n) = opts.count {
        matched.truncate(n);
    }
    Ok(matched)
}

/// `GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat> <BYRADIUS r unit |
/// BYBOX w h unit> [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST]` —
/// see `geo.rs`'s module doc for what's still not included (the legacy
/// `GEORADIUS` family).
fn cmd_geosearch(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let opts = match parse_geosearch_options(s, c.db_index, &argv[1], argv, 2) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let matched = match geosearch_run(s, c.db_index, &argv[1], &opts) {
        Ok(m) => m,
        Err(e) => return e,
    };
    Reply::Array(
        matched
            .into_iter()
            .map(|(member, lon, lat, dist, _score)| {
                if !opts.withcoord && !opts.withdist {
                    return Reply::bulk(member);
                }
                let mut fields = vec![Reply::bulk(member)];
                if opts.withdist {
                    fields.push(Reply::bulk(format!("{:.4}", dist / opts.unit_divisor).into_bytes()));
                }
                if opts.withcoord {
                    fields.push(Reply::Array(vec![Reply::bulk(format!("{lon:.17}").into_bytes()), Reply::bulk(format!("{lat:.17}").into_bytes())]));
                }
                Reply::Array(fields)
            })
            .collect(),
    )
}

/// `GEOSEARCHSTORE destination source <same search options as GEOSEARCH>
/// [STOREDIST]` — stores the matched members into `destination` as a zset.
/// Without `STOREDIST`, each member keeps its original geohash score
/// (`source`'s own encoding, byte-for-byte reusable); with it, the score
/// becomes the computed distance in the search's own unit — matching
/// `georadiusGeneric`'s `storedist` branch.
fn cmd_geosearchstore(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if argv.len() < 3 {
        return Reply::Error("ERR wrong number of arguments for 'geosearchstore' command".to_string());
    }
    let opts = match parse_geosearch_options(s, c.db_index, &argv[2], argv, 3) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let matched = match geosearch_run(s, c.db_index, &argv[2], &opts) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let n = matched.len();
    if n == 0 {
        s.dbs[c.db_index].remove_key(&argv[1]);
    } else {
        let max_entries = s.config.get_int("zset-max-listpack-entries").unwrap_or(128);
        let max_value = s.config.get_int("zset-max-listpack-value").unwrap_or(64);
        let mut zset = ZSetObj::new();
        for (member, _lon, _lat, dist, original_score) in matched {
            let score = if opts.storedist { dist / opts.unit_divisor } else { original_score };
            zset.add(&member, score, max_entries, max_value);
        }
        s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::ZSet(zset));
    }
    notify_keyspace_event(s, c.db_index, 'z', "geosearchstore", &argv[1]);
    Reply::Integer(n as i64)
}

// -------------------------------------------------------------- Stream ---
// `t_stream.c` ported onto the new `RObj::Stream` variant — see
// `stream.rs`'s module doc for the `BTreeMap`-instead-of-`rax` design and
// the exact command scope (no consumer groups, no blocking `XREAD`).

enum TrimSpec {
    MaxLen(usize),
    MinId(crate::stream::StreamId),
}

/// Parses a trailing `MAXLEN|MINID [=|~] threshold [LIMIT count]` clause
/// starting at `argv[*i]`, advancing `*i` past it. The `~`/`=`
/// approximate-vs-exact marker and `LIMIT` are accepted but have no effect
/// — this fork always trims exactly, having no quicklist-node-granularity
/// reason to trim approximately the way real Redis's optimization does.
fn parse_trim_clause(argv: &[Vec<u8>], i: &mut usize) -> Result<TrimSpec, Reply> {
    let kind = ok_bytes_as_string(&argv[*i]).to_ascii_uppercase();
    *i += 1;
    if *i < argv.len() && (argv[*i] == b"~" || argv[*i] == b"=") {
        *i += 1;
    }
    if *i >= argv.len() {
        return Err(Reply::Error("ERR syntax error".to_string()));
    }
    let threshold = ok_bytes_as_string(&argv[*i]);
    *i += 1;
    if *i + 1 < argv.len() && argv[*i].eq_ignore_ascii_case(b"LIMIT") {
        *i += 2;
    }
    match kind.as_str() {
        "MAXLEN" => match threshold.parse::<usize>() {
            Ok(n) => Ok(TrimSpec::MaxLen(n)),
            Err(_) => Err(Reply::Error("ERR value is not an integer or out of range".to_string())),
        },
        "MINID" => match crate::stream::parse_exact_id(&threshold, 0) {
            Some(id) => Ok(TrimSpec::MinId(id)),
            None => Err(Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string())),
        },
        _ => Err(Reply::Error("ERR syntax error".to_string())),
    }
}

fn apply_trim(stream: &mut crate::stream::StreamObj, trim: TrimSpec) -> usize {
    match trim {
        TrimSpec::MaxLen(n) => stream.trim_maxlen(n),
        TrimSpec::MinId(id) => stream.trim_minid(id),
    }
}

/// `XADD key [NOMKSTREAM] [MAXLEN|MINID ... ] <ID|*> field value [field
/// value ...]`. The `<ms>-*` partial-auto-sequence ID form isn't supported
/// (use `*` or a fully explicit `<ms>-<seq>`) — a documented gap, not every
/// corner of `t_stream.c`'s ID grammar.
fn cmd_xadd(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut i = 2;
    let mut nomkstream = false;
    if i < argv.len() && argv[i].eq_ignore_ascii_case(b"NOMKSTREAM") {
        nomkstream = true;
        i += 1;
    }
    let mut trim: Option<TrimSpec> = None;
    if i < argv.len() && (argv[i].eq_ignore_ascii_case(b"MAXLEN") || argv[i].eq_ignore_ascii_case(b"MINID")) {
        trim = match parse_trim_clause(argv, &mut i) {
            Ok(t) => Some(t),
            Err(e) => return e,
        };
    }
    if i >= argv.len() {
        return Reply::Error("ERR wrong number of arguments for 'xadd' command".to_string());
    }
    let id_arg = ok_bytes_as_string(&argv[i]);
    i += 1;
    if i >= argv.len() || !(argv.len() - i).is_multiple_of(2) {
        return Reply::Error("ERR wrong number of arguments for 'xadd' command".to_string());
    }
    let explicit_id = if id_arg == "*" {
        None
    } else {
        match crate::stream::parse_exact_id(&id_arg, 0) {
            Some(id) => Some(id),
            None => return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string()),
        }
    };
    let fields: crate::stream::Fields = argv[i..].chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();

    let db = &mut s.dbs[c.db_index].dict;
    let existed = db.get(SLOT, &argv[1]).is_some();
    if !existed && nomkstream {
        return Reply::Null;
    }
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Stream(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if !existed {
        db.insert(SLOT, argv[1].clone(), RObj::Stream(crate::stream::StreamObj::new()));
    }
    let RObj::Stream(stream) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    let new_id = match stream.add(explicit_id, fields, now_ms() as u64) {
        Ok(id) => id,
        Err(e) => return Reply::Error(e),
    };
    if let Some(t) = trim {
        apply_trim(stream, t);
    }
    notify_keyspace_event(s, c.db_index, 't', "xadd", &argv[1]);
    Reply::bulk(new_id.to_bytes())
}

fn cmd_xlen(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Reply::Integer(0),
        Some(RObj::Stream(st)) => Reply::Integer(st.len() as i64),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
    }
}

fn stream_entries_to_reply(entries: Vec<(crate::stream::StreamId, crate::stream::Fields)>) -> Reply {
    Reply::Array(
        entries
            .into_iter()
            .map(|(id, fields)| {
                let mut flat = Vec::with_capacity(fields.len() * 2);
                for (f, v) in fields {
                    flat.push(Reply::bulk(f));
                    flat.push(Reply::bulk(v));
                }
                Reply::Array(vec![Reply::bulk(id.to_bytes()), Reply::Array(flat)])
            })
            .collect(),
    )
}

/// Shared by `XRANGE`/`XREVRANGE` — `reverse` swaps which argv position is
/// the low/high bound (`XRANGE key start end` vs. `XREVRANGE key end
/// start`) and reverses the output order.
fn xrange_impl(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>], reverse: bool) -> Reply {
    let (start_arg, end_arg) = if reverse { (&argv[3], &argv[2]) } else { (&argv[2], &argv[3]) };
    let Some((start, start_excl)) = crate::stream::parse_range_id(&ok_bytes_as_string(start_arg), 0) else {
        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
    };
    let Some((end, end_excl)) = crate::stream::parse_range_id(&ok_bytes_as_string(end_arg), u64::MAX) else {
        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
    };
    let count = match argv.len() {
        4 => None,
        6 if argv[4].eq_ignore_ascii_case(b"COUNT") => match ok_bytes_as_string(&argv[5]).parse::<usize>() {
            Ok(n) => Some(n),
            Err(_) => return Reply::Error("ERR value is not an integer or out of range".to_string()),
        },
        _ => return Reply::Error("ERR syntax error".to_string()),
    };

    let mut entries = match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        None => Vec::new(),
        Some(RObj::Stream(st)) => st.range(start, start_excl, end, end_excl, count),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if reverse {
        entries.reverse();
    }
    stream_entries_to_reply(entries)
}

fn cmd_xrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    xrange_impl(s, c, argv, false)
}

fn cmd_xrevrange(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    xrange_impl(s, c, argv, true)
}

fn cmd_xdel(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut n = 0i64;
    match s.dbs[c.db_index].dict.get_mut(SLOT, &argv[1]) {
        None => {}
        Some(RObj::Stream(st)) => {
            for id_arg in &argv[2..] {
                let Some(id) = crate::stream::parse_exact_id(&ok_bytes_as_string(id_arg), 0) else {
                    return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
                };
                if st.delete(id) {
                    n += 1;
                }
            }
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    }
    if n > 0 {
        notify_keyspace_event(s, c.db_index, 't', "xdel", &argv[1]);
    }
    Reply::Integer(n)
}

fn cmd_xtrim(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut i = 2;
    let trim = match parse_trim_clause(argv, &mut i) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let removed = match s.dbs[c.db_index].dict.get_mut(SLOT, &argv[1]) {
        None => 0,
        Some(RObj::Stream(st)) => apply_trim(st, trim),
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if removed > 0 {
        notify_keyspace_event(s, c.db_index, 't', "xtrim", &argv[1]);
    }
    Reply::Integer(removed as i64)
}

/// `XREAD [COUNT count] STREAMS key [key ...] id [id ...]` — non-blocking
/// only (no `BLOCK`); `$` (only entries newer than "right now") therefore
/// always reads as "nothing yet", which is exactly correct for a
/// non-blocking call made at this instant.
fn cmd_xread(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let mut i = 1;
    let mut count: Option<usize> = None;
    loop {
        if i >= argv.len() {
            return Reply::Error("ERR syntax error".to_string());
        }
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "COUNT" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                count = Some(n);
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    let remaining = &argv[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Reply::Error(
            "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.".to_string(),
        );
    }
    let n_streams = remaining.len() / 2;
    let keys = &remaining[..n_streams];
    let ids = &remaining[n_streams..];

    let mut results = Vec::new();
    for (key, id_arg) in keys.iter().zip(ids.iter()) {
        let id_str = ok_bytes_as_string(id_arg);
        let entries = match s.dbs[c.db_index].dict.get(SLOT, key) {
            None => continue,
            Some(RObj::Stream(st)) => {
                let after_id = if id_str == "$" {
                    st.last_id()
                } else {
                    match crate::stream::parse_exact_id(&id_str, u64::MAX) {
                        Some(id) => id,
                        None => return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string()),
                    }
                };
                st.range(after_id, true, crate::stream::StreamId::MAX, false, count)
            }
            Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        };
        if entries.is_empty() {
            continue;
        }
        results.push(Reply::Array(vec![Reply::bulk(key.clone()), stream_entries_to_reply(entries)]));
    }
    if results.is_empty() {
        Reply::NullArray
    } else {
        Reply::Array(results)
    }
}

/// `XSETID key id [ENTRIESADDED n] [MAXDELETEDID id]` — forcibly moves the
/// stream's last-id watermark (`StreamObj::set_last_id`). `ENTRIESADDED`/
/// `MAXDELETEDID` are accepted and ignored: this fork doesn't track a
/// cumulative entries-added counter or a max-deleted-id separately from the
/// watermark itself (see `cmd_xinfo`'s doc comment, which reports the same
/// approximation). No `FORCE` flag either — matching real Redis's own
/// *default* behavior, a new id smaller than the current watermark is
/// always rejected.
fn cmd_xsetid(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let Some(id) = crate::stream::parse_exact_id(&ok_bytes_as_string(&argv[2]), 0) else {
        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
    };
    let mut i = 3;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "ENTRIESADDED" | "MAXDELETEDID" if i + 1 < argv.len() => i += 2,
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    let db = &mut s.dbs[c.db_index].dict;
    let ok = db.get_mut(SLOT, &argv[1]).is_none_or(|o| matches!(o, RObj::Stream(_)));
    if !ok {
        return Reply::Error(WRONGTYPE.to_string());
    }
    if db.get(SLOT, &argv[1]).is_none() {
        db.insert(SLOT, argv[1].clone(), RObj::Stream(crate::stream::StreamObj::new()));
    }
    let RObj::Stream(stream) = db.get_mut(SLOT, &argv[1]).unwrap() else { unreachable!() };
    if id < stream.last_id() {
        return Reply::Error("ERR The ID specified in XSETID is smaller than the target stream top item".to_string());
    }
    stream.set_last_id(id);
    Reply::ok()
}

/// `XINFO STREAM key [FULL]` / `XINFO GROUPS key` / `XINFO CONSUMERS key
/// group`. `FULL` is accepted and ignored — same (summary-form) reply either
/// way, since adding the `FULL` form's per-entry PEL breakdown is more detail
/// than this fork's callers have needed. `entries-added`/`max-deleted-entry-id`
/// are honest approximations rather than invented precision: this fork
/// doesn't track a cumulative "ever added" counter separately from the
/// current length, or the id of the most recently deleted entry, so
/// `entries-added` reports the *current* length (exactly right until the
/// first `XDEL`/`XTRIM`) and `max-deleted-entry-id` always reads `0-0`.
fn cmd_xinfo(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "STREAM" if argv.len() >= 3 => match s.dbs[c.db_index].dict.get(SLOT, &argv[2]) {
            None => Reply::Error("ERR no such key".to_string()),
            Some(RObj::Stream(stream)) => {
                let to_entry_reply = |id: &crate::stream::StreamId, fields: &crate::stream::Fields| {
                    let flat: Vec<Reply> = fields.iter().flat_map(|(f, v)| [Reply::bulk(f.clone()), Reply::bulk(v.clone())]).collect();
                    Reply::Array(vec![Reply::bulk(id.to_bytes()), Reply::Array(flat)])
                };
                let first = stream.entries().iter().next();
                let last = stream.entries().iter().next_back();
                Reply::Map(vec![
                    (Reply::bulk(b"length".to_vec()), Reply::Integer(stream.len() as i64)),
                    (Reply::bulk(b"radix-tree-keys".to_vec()), Reply::Integer(stream.len() as i64)),
                    (Reply::bulk(b"radix-tree-nodes".to_vec()), Reply::Integer(stream.len() as i64 + 1)),
                    (Reply::bulk(b"last-generated-id".to_vec()), Reply::bulk(stream.last_id().to_bytes())),
                    (Reply::bulk(b"max-deleted-entry-id".to_vec()), Reply::bulk(b"0-0".to_vec())),
                    (Reply::bulk(b"entries-added".to_vec()), Reply::Integer(stream.len() as i64)),
                    (
                        Reply::bulk(b"recorded-first-entry-id".to_vec()),
                        Reply::bulk(first.map(|(id, _)| id.to_bytes()).unwrap_or_else(|| b"0-0".to_vec())),
                    ),
                    (Reply::bulk(b"groups".to_vec()), Reply::Integer(stream.groups().len() as i64)),
                    (Reply::bulk(b"first-entry".to_vec()), first.map(|(id, f)| to_entry_reply(id, f)).unwrap_or(Reply::Null)),
                    (Reply::bulk(b"last-entry".to_vec()), last.map(|(id, f)| to_entry_reply(id, f)).unwrap_or(Reply::Null)),
                ])
            }
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
        },
        "GROUPS" if argv.len() >= 3 => match s.dbs[c.db_index].dict.get(SLOT, &argv[2]) {
            None => Reply::Error("ERR no such key".to_string()),
            Some(RObj::Stream(stream)) => Reply::Array(
                stream
                    .groups()
                    .iter()
                    .map(|(name, g)| {
                        let lag = if g.entries_read() < 0 {
                            Reply::Null
                        } else {
                            Reply::Integer((stream.len() as i64 - g.entries_read()).max(0))
                        };
                        Reply::Map(vec![
                            (Reply::bulk(b"name".to_vec()), Reply::bulk(name.clone())),
                            (Reply::bulk(b"consumers".to_vec()), Reply::Integer(g.consumers().len() as i64)),
                            (Reply::bulk(b"pending".to_vec()), Reply::Integer(g.pel().len() as i64)),
                            (Reply::bulk(b"last-delivered-id".to_vec()), Reply::bulk(g.last_delivered_id().to_bytes())),
                            (Reply::bulk(b"entries-read".to_vec()), if g.entries_read() < 0 { Reply::Null } else { Reply::Integer(g.entries_read()) }),
                            (Reply::bulk(b"lag".to_vec()), lag),
                        ])
                    })
                    .collect(),
            ),
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
        },
        "CONSUMERS" if argv.len() >= 4 => {
            let now = now_ms() as u64;
            match s.dbs[c.db_index].dict.get(SLOT, &argv[2]) {
                None => Reply::Error("ERR no such key".to_string()),
                Some(RObj::Stream(stream)) => match stream.group(&argv[3]) {
                    None => Reply::Error(format!(
                        "NOGROUP No such consumer group '{}' for key name '{}'",
                        ok_bytes_as_string(&argv[3]),
                        ok_bytes_as_string(&argv[2])
                    )),
                    Some(group) => Reply::Array(
                        group
                            .consumers()
                            .iter()
                            .map(|(name, consumer)| {
                                let pending = group.pel().values().filter(|e| &e.consumer == name).count();
                                Reply::Map(vec![
                                    (Reply::bulk(b"name".to_vec()), Reply::bulk(name.clone())),
                                    (Reply::bulk(b"pending".to_vec()), Reply::Integer(pending as i64)),
                                    (Reply::bulk(b"idle".to_vec()), Reply::Integer(now.saturating_sub(consumer.seen_time_ms) as i64)),
                                    (Reply::bulk(b"inactive".to_vec()), Reply::Integer(now.saturating_sub(consumer.active_time_ms) as i64)),
                                ])
                            })
                            .collect(),
                    ),
                },
                Some(_) => Reply::Error(WRONGTYPE.to_string()),
            }
        }
        _ => Reply::Error("ERR syntax error".to_string()),
    }
}

/// `XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER` — the four
/// consumer-group management subcommands. `CREATE ... MKSTREAM` is the only
/// way this fork lets `XGROUP` create a key (matching real Redis: every
/// other subcommand errors with `NOGROUP`/"requires the key to exist" on a
/// missing stream rather than silently creating one).
fn cmd_xgroup(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    let no_such_key = || {
        Reply::Error(
            "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".to_string(),
        )
    };
    let no_such_group = |group: &[u8], key: &[u8]| {
        Reply::Error(format!("NOGROUP No such key '{}' or consumer group '{}'", ok_bytes_as_string(key), ok_bytes_as_string(group)))
    };
    match sub.as_str() {
        "CREATE" if argv.len() >= 5 => {
            let (key, group, id_arg) = (&argv[2], &argv[3], &argv[4]);
            let mut mkstream = false;
            let mut entries_read: i64 = 0;
            let mut i = 5;
            while i < argv.len() {
                match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
                    "MKSTREAM" => {
                        mkstream = true;
                        i += 1;
                    }
                    "ENTRIESREAD" if i + 1 < argv.len() => {
                        let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                            return Reply::Error("ERR value is not an integer or out of range".to_string());
                        };
                        entries_read = n;
                        i += 2;
                    }
                    _ => return Reply::Error("ERR syntax error".to_string()),
                }
            }
            let db = &mut s.dbs[c.db_index].dict;
            let existed = db.get(SLOT, key).is_some();
            if !existed {
                if !mkstream {
                    return no_such_key();
                }
                db.insert(SLOT, key.clone(), RObj::Stream(crate::stream::StreamObj::new()));
            }
            let Some(RObj::Stream(stream)) = db.get_mut(SLOT, key) else { return Reply::Error(WRONGTYPE.to_string()) };
            let id = if id_arg.as_slice() == b"$" {
                stream.last_id()
            } else {
                match crate::stream::parse_exact_id(&ok_bytes_as_string(id_arg), 0) {
                    Some(id) => id,
                    None => return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string()),
                }
            };
            match stream.create_group(group.clone(), id, entries_read) {
                Ok(()) => Reply::ok(),
                Err(e) => Reply::Error(e),
            }
        }
        "SETID" if argv.len() >= 5 => {
            let (key, group, id_arg) = (&argv[2], &argv[3], &argv[4]);
            let mut entries_read: Option<i64> = None;
            let mut i = 5;
            while i < argv.len() {
                match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
                    "ENTRIESREAD" if i + 1 < argv.len() => {
                        let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                            return Reply::Error("ERR value is not an integer or out of range".to_string());
                        };
                        entries_read = Some(n);
                        i += 2;
                    }
                    _ => return Reply::Error("ERR syntax error".to_string()),
                }
            }
            match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
                None => no_such_group(group, key),
                Some(RObj::Stream(stream)) => {
                    let id = if id_arg.as_slice() == b"$" {
                        stream.last_id()
                    } else {
                        match crate::stream::parse_exact_id(&ok_bytes_as_string(id_arg), 0) {
                            Some(id) => id,
                            None => return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string()),
                        }
                    };
                    match stream.group_mut(group) {
                        None => no_such_group(group, key),
                        Some(g) => {
                            g.set_last_delivered_id(id);
                            if let Some(n) = entries_read {
                                g.set_entries_read(n);
                            }
                            Reply::ok()
                        }
                    }
                }
                Some(_) => Reply::Error(WRONGTYPE.to_string()),
            }
        }
        "DESTROY" if argv.len() == 4 => {
            let (key, group) = (&argv[2], &argv[3]);
            match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
                None => Reply::Integer(0),
                Some(RObj::Stream(stream)) => Reply::Integer(stream.destroy_group(group) as i64),
                Some(_) => Reply::Error(WRONGTYPE.to_string()),
            }
        }
        "CREATECONSUMER" if argv.len() == 5 => {
            let (key, group, consumer) = (&argv[2], &argv[3], &argv[4]);
            match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
                None => no_such_group(group, key),
                Some(RObj::Stream(stream)) => match stream.group_mut(group) {
                    None => no_such_group(group, key),
                    Some(g) => Reply::Integer(g.ensure_consumer(consumer, now_ms() as u64) as i64),
                },
                Some(_) => Reply::Error(WRONGTYPE.to_string()),
            }
        }
        "DELCONSUMER" if argv.len() == 5 => {
            let (key, group, consumer) = (&argv[2], &argv[3], &argv[4]);
            match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
                None => no_such_group(group, key),
                Some(RObj::Stream(stream)) => match stream.group_mut(group) {
                    None => no_such_group(group, key),
                    Some(g) => Reply::Integer(g.delete_consumer(consumer) as i64),
                },
                Some(_) => Reply::Error(WRONGTYPE.to_string()),
            }
        }
        _ => Reply::Error("ERR syntax error".to_string()),
    }
}

/// `XREADGROUP GROUP group consumer [COUNT n] [NOACK] STREAMS key [key ...]
/// id [id ...]`. Only `>` ("give me new entries") and an explicit id ("replay
/// what's already in my PEL past this id") are both supported, matching real
/// Redis; `BLOCK` isn't — see this module's `stream.rs` doc comment.
fn cmd_xreadgroup(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if !argv[1].eq_ignore_ascii_case(b"GROUP") || argv.len() < 4 {
        return Reply::Error("ERR syntax error".to_string());
    }
    let group_name = argv[2].clone();
    let consumer_name = argv[3].clone();
    let mut i = 4;
    let mut count: Option<usize> = None;
    let mut noack = false;
    loop {
        if i >= argv.len() {
            return Reply::Error("ERR syntax error".to_string());
        }
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "COUNT" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                count = Some(n);
                i += 2;
            }
            "NOACK" => {
                noack = true;
                i += 1;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    let remaining = &argv[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Reply::Error(
            "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '$' must be specified.".to_string(),
        );
    }
    let n_streams = remaining.len() / 2;
    let keys = &remaining[..n_streams];
    let ids = &remaining[n_streams..];
    let now = now_ms() as u64;

    let mut results = Vec::new();
    for (key, id_arg) in keys.iter().zip(ids.iter()) {
        let id_str = ok_bytes_as_string(id_arg);
        let is_new = id_str == ">";
        let entries = match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
            None => {
                return Reply::Error(format!(
                    "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                    ok_bytes_as_string(key),
                    ok_bytes_as_string(&group_name)
                ));
            }
            Some(RObj::Stream(stream)) => {
                if stream.group(&group_name).is_none() {
                    return Reply::Error(format!(
                        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                        ok_bytes_as_string(key),
                        ok_bytes_as_string(&group_name)
                    ));
                }
                if is_new {
                    let after = stream.group(&group_name).unwrap().last_delivered_id();
                    let fetched = stream.range(after, true, crate::stream::StreamId::MAX, false, count);
                    let group = stream.group_mut(&group_name).unwrap();
                    for (id, _) in &fetched {
                        group.deliver(*id, &consumer_name, now, noack);
                        group.set_last_delivered_id(*id);
                        if group.entries_read() >= 0 {
                            group.set_entries_read(group.entries_read() + 1);
                        }
                    }
                    if fetched.is_empty() {
                        // Touching the group still counts as this consumer being seen.
                        stream.group_mut(&group_name).unwrap().ensure_consumer(&consumer_name, now);
                    }
                    fetched
                } else {
                    let Some(after_id) = crate::stream::parse_exact_id(&id_str, 0) else {
                        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
                    };
                    let group = stream.group(&group_name).unwrap();
                    let mut ids_for_consumer: Vec<crate::stream::StreamId> =
                        group.pel().iter().filter(|(id, e)| **id > after_id.prev() && e.consumer == consumer_name).map(|(id, _)| *id).collect();
                    ids_for_consumer.sort();
                    if let Some(n) = count {
                        ids_for_consumer.truncate(n);
                    }
                    ids_for_consumer.into_iter().map(|id| (id, stream.entries().get(&id).cloned().unwrap_or_default())).collect()
                }
            }
            Some(_) => return Reply::Error(WRONGTYPE.to_string()),
        };
        if entries.is_empty() && is_new {
            continue;
        }
        results.push(Reply::Array(vec![Reply::bulk(key.clone()), stream_entries_to_reply(entries)]));
    }
    if results.is_empty() {
        Reply::NullArray
    } else {
        Reply::Array(results)
    }
}

/// `XACK key group id [id ...]`.
fn cmd_xack(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (key, group) = (&argv[1], &argv[2]);
    let mut n = 0i64;
    match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
        None => {}
        Some(RObj::Stream(stream)) => {
            if let Some(g) = stream.group_mut(group) {
                for id_arg in &argv[3..] {
                    let Some(id) = crate::stream::parse_exact_id(&ok_bytes_as_string(id_arg), 0) else {
                        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
                    };
                    if g.ack(id) {
                        n += 1;
                    }
                }
            }
        }
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    }
    Reply::Integer(n)
}

/// `XPENDING key group` (summary form) or `XPENDING key group [IDLE
/// min-idle-time] start end count [consumer]` (extended form).
fn cmd_xpending(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (key, group) = (&argv[1], &argv[2]);
    let stream = match s.dbs[c.db_index].dict.get(SLOT, key) {
        None => {
            return Reply::Error(format!(
                "NOGROUP No such key '{}' or consumer group '{}'",
                ok_bytes_as_string(key),
                ok_bytes_as_string(group)
            ));
        }
        Some(RObj::Stream(st)) => st,
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    let Some(g) = stream.group(group) else {
        return Reply::Error(format!("NOGROUP No such key '{}' or consumer group '{}'", ok_bytes_as_string(key), ok_bytes_as_string(group)));
    };

    if argv.len() == 3 {
        if g.pel().is_empty() {
            return Reply::Array(vec![Reply::Integer(0), Reply::Null, Reply::Null, Reply::NullArray]);
        }
        let min_id = *g.pel().keys().next().unwrap();
        let max_id = *g.pel().keys().next_back().unwrap();
        let mut per_consumer: std::collections::BTreeMap<&[u8], i64> = std::collections::BTreeMap::new();
        for entry in g.pel().values() {
            *per_consumer.entry(entry.consumer.as_slice()).or_insert(0) += 1;
        }
        let consumers = per_consumer
            .into_iter()
            .map(|(name, count)| Reply::Array(vec![Reply::bulk(name.to_vec()), Reply::bulk(count.to_string().into_bytes())]))
            .collect();
        return Reply::Array(vec![Reply::Integer(g.pel().len() as i64), Reply::bulk(min_id.to_bytes()), Reply::bulk(max_id.to_bytes()), Reply::Array(consumers)]);
    }

    let mut i = 3;
    let mut min_idle_ms: u64 = 0;
    if argv[i].eq_ignore_ascii_case(b"IDLE") && i + 1 < argv.len() {
        let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<u64>() else {
            return Reply::Error("ERR value is not an integer or out of range".to_string());
        };
        min_idle_ms = n;
        i += 2;
    }
    if i + 2 >= argv.len() {
        return Reply::Error("ERR syntax error".to_string());
    }
    let Some((start, start_excl)) = crate::stream::parse_range_id(&ok_bytes_as_string(&argv[i]), 0) else {
        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
    };
    let Some((end, end_excl)) = crate::stream::parse_range_id(&ok_bytes_as_string(&argv[i + 1]), u64::MAX) else {
        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
    };
    let Ok(count) = ok_bytes_as_string(&argv[i + 2]).parse::<usize>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    i += 3;
    let consumer_filter: Option<Vec<u8>> = if i < argv.len() { Some(argv[i].clone()) } else { None };

    let lo = if start_excl { start.next() } else { start };
    let hi = if end_excl { end.prev() } else { end };
    let now = now_ms() as u64;
    let rows: Vec<Reply> = g
        .pel()
        .range(lo..=hi)
        .filter(|(_, e)| now.saturating_sub(e.delivery_time_ms) >= min_idle_ms)
        .filter(|(_, e)| consumer_filter.as_ref().is_none_or(|f| f == &e.consumer))
        .take(count)
        .map(|(id, e)| {
            Reply::Array(vec![
                Reply::bulk(id.to_bytes()),
                Reply::bulk(e.consumer.clone()),
                Reply::Integer(now.saturating_sub(e.delivery_time_ms) as i64),
                Reply::Integer(e.delivery_count as i64),
            ])
        })
        .collect();
    Reply::Array(rows)
}

/// Parses the shared `[IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]`
/// trailer used by `XCLAIM` after its id list.
struct ClaimOptions {
    idle_ms: Option<u64>,
    time_ms: Option<u64>,
    retrycount: Option<u64>,
    force: bool,
    justid: bool,
}

fn parse_claim_options(argv: &[Vec<u8>], mut i: usize) -> Result<ClaimOptions, Reply> {
    let mut opts = ClaimOptions { idle_ms: None, time_ms: None, retrycount: None, force: false, justid: false };
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "IDLE" if i + 1 < argv.len() => {
                opts.idle_ms = ok_bytes_as_string(&argv[i + 1]).parse().ok();
                i += 2;
            }
            "TIME" if i + 1 < argv.len() => {
                opts.time_ms = ok_bytes_as_string(&argv[i + 1]).parse().ok();
                i += 2;
            }
            "RETRYCOUNT" if i + 1 < argv.len() => {
                opts.retrycount = ok_bytes_as_string(&argv[i + 1]).parse().ok();
                i += 2;
            }
            "FORCE" => {
                opts.force = true;
                i += 1;
            }
            "JUSTID" => {
                opts.justid = true;
                i += 1;
            }
            "LASTID" if i + 1 < argv.len() => i += 2, // accepted, no per-consumer last-id tracking to update
            _ => return Err(Reply::Error("ERR syntax error".to_string())),
        }
    }
    Ok(opts)
}

/// `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME
/// ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]`.
fn cmd_xclaim(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (key, group_name, consumer) = (&argv[1], &argv[2], argv[3].clone());
    let Ok(min_idle_ms) = ok_bytes_as_string(&argv[4]).parse::<u64>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    let mut i = 5;
    let mut ids = Vec::new();
    while i < argv.len() {
        match crate::stream::parse_exact_id(&ok_bytes_as_string(&argv[i]), 0) {
            Some(id) => {
                ids.push(id);
                i += 1;
            }
            None => break,
        }
    }
    let opts = match parse_claim_options(argv, i) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let now = now_ms() as u64;
    let delivery_time = opts.time_ms.unwrap_or_else(|| now.saturating_sub(opts.idle_ms.unwrap_or(0)));

    let stream = match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
        None => return Reply::Error(format!("NOGROUP No such key '{}' or consumer group '{}'", ok_bytes_as_string(key), ok_bytes_as_string(group_name))),
        Some(RObj::Stream(st)) => st,
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if stream.group(group_name).is_none() {
        return Reply::Error(format!("NOGROUP No such key '{}' or consumer group '{}'", ok_bytes_as_string(key), ok_bytes_as_string(group_name)));
    }

    let mut claimed_ids = Vec::new();
    for id in ids {
        let still_exists = stream.entries().contains_key(&id);
        let group = stream.group_mut(group_name).unwrap();
        let in_pel = group.pel().contains_key(&id);
        if !(in_pel || (opts.force && still_exists)) {
            continue;
        }
        if !still_exists {
            group.pel_mut().remove(&id);
            continue;
        }
        if in_pel {
            let idle = now.saturating_sub(group.pel()[&id].delivery_time_ms);
            if idle < min_idle_ms {
                continue;
            }
        }
        group.ensure_consumer(&consumer, now);
        let delivery_count = if opts.justid {
            group.pel().get(&id).map(|e| e.delivery_count).unwrap_or(0)
        } else {
            opts.retrycount.unwrap_or_else(|| group.pel().get(&id).map(|e| e.delivery_count + 1).unwrap_or(1))
        };
        group.pel_mut().insert(id, crate::stream::PelEntry { consumer: consumer.clone(), delivery_time_ms: delivery_time, delivery_count });
        claimed_ids.push(id);
    }

    if opts.justid {
        Reply::Array(claimed_ids.into_iter().map(|id| Reply::bulk(id.to_bytes())).collect())
    } else {
        let entries: Vec<_> = claimed_ids.into_iter().map(|id| (id, stream.entries().get(&id).cloned().unwrap_or_default())).collect();
        stream_entries_to_reply(entries)
    }
}

/// `XAUTOCLAIM key group consumer min-idle-time start [COUNT count]
/// [JUSTID]`.
fn cmd_xautoclaim(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let (key, group_name, consumer) = (&argv[1], &argv[2], argv[3].clone());
    let Ok(min_idle_ms) = ok_bytes_as_string(&argv[4]).parse::<u64>() else {
        return Reply::Error("ERR value is not an integer or out of range".to_string());
    };
    let Some(start) = crate::stream::parse_exact_id(&ok_bytes_as_string(&argv[5]), 0) else {
        return Reply::Error("ERR Invalid stream ID specified as stream command argument".to_string());
    };
    let mut i = 6;
    let mut count = 100usize;
    let mut justid = false;
    while i < argv.len() {
        match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
            "COUNT" if i + 1 < argv.len() => {
                let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                    return Reply::Error("ERR value is not an integer or out of range".to_string());
                };
                count = n;
                i += 2;
            }
            "JUSTID" => {
                justid = true;
                i += 1;
            }
            _ => return Reply::Error("ERR syntax error".to_string()),
        }
    }
    let now = now_ms() as u64;

    let stream = match s.dbs[c.db_index].dict.get_mut(SLOT, key) {
        None => return Reply::Error(format!("NOGROUP No such key '{}' or consumer group '{}'", ok_bytes_as_string(key), ok_bytes_as_string(group_name))),
        Some(RObj::Stream(st)) => st,
        Some(_) => return Reply::Error(WRONGTYPE.to_string()),
    };
    if stream.group(group_name).is_none() {
        return Reply::Error(format!("NOGROUP No such key '{}' or consumer group '{}'", ok_bytes_as_string(key), ok_bytes_as_string(group_name)));
    }

    let candidates: Vec<crate::stream::StreamId> = stream.group(group_name).unwrap().pel().range(start..).map(|(id, _)| *id).collect();
    let mut claimed_ids = Vec::new();
    let mut deleted_ids = Vec::new();
    let mut next_cursor = crate::stream::StreamId::MIN;
    let mut examined = 0usize;
    for id in candidates {
        if examined >= count {
            next_cursor = id;
            break;
        }
        let still_exists = stream.entries().contains_key(&id);
        let group = stream.group_mut(group_name).unwrap();
        if !still_exists {
            group.pel_mut().remove(&id);
            deleted_ids.push(id);
            examined += 1;
            continue;
        }
        let idle = now.saturating_sub(group.pel()[&id].delivery_time_ms);
        if idle < min_idle_ms {
            continue;
        }
        group.ensure_consumer(&consumer, now);
        let delivery_count = if justid { group.pel()[&id].delivery_count } else { group.pel()[&id].delivery_count + 1 };
        group.pel_mut().insert(id, crate::stream::PelEntry { consumer: consumer.clone(), delivery_time_ms: now, delivery_count });
        claimed_ids.push(id);
        examined += 1;
    }

    let entries_reply = if justid {
        Reply::Array(claimed_ids.into_iter().map(|id| Reply::bulk(id.to_bytes())).collect())
    } else {
        let entries: Vec<_> = claimed_ids.into_iter().map(|id| (id, stream.entries().get(&id).cloned().unwrap_or_default())).collect();
        stream_entries_to_reply(entries)
    };
    Reply::Array(vec![
        Reply::bulk(next_cursor.to_bytes()),
        entries_reply,
        Reply::Array(deleted_ids.into_iter().map(|id| Reply::bulk(id.to_bytes())).collect()),
    ])
}

fn cmd_config(s: &mut Server, _c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sub = ok_bytes_as_string(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" if argv.len() == 3 => {
            let pattern = ok_bytes_as_string(&argv[2]);
            let pairs = s.config.get(&pattern);
            Reply::Map(pairs.into_iter().map(|(k, v)| (Reply::bulk(k), Reply::bulk(v))).collect())
        }
        "SET" if argv.len() == 4 => {
            let key = ok_bytes_as_string(&argv[2]);
            let val = ok_bytes_as_string(&argv[3]);
            // `maxmemory-policy` is the one config key this fork validates
            // as an enum at `SET` time — `config.rs`'s module doc defers
            // typed validation in general, but real Redis itself special-
            // cases this exact key the same way, and silently accepting a
            // typo'd policy name would make `enforce_maxmemory` fall through
            // to its `_ => None` (no eviction) arm forever without any
            // visible error.
            if key.eq_ignore_ascii_case("maxmemory-policy") && !is_valid_maxmemory_policy(&val.to_ascii_lowercase()) {
                return Reply::Error(format!("ERR Invalid maxmemory policy '{val}'"));
            }
            if s.config.set(&key, &val) {
                Reply::ok()
            } else {
                Reply::Error(format!("ERR Unknown option or number of arguments for CONFIG SET - '{key}'"))
            }
        }
        _ => Reply::Error("ERR Unsupported CONFIG subcommand or wrong number of arguments".to_string()),
    }
}

fn cmd_multi(_s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    if c.in_multi {
        return Reply::Error("ERR MULTI calls can not be nested".to_string());
    }
    c.in_multi = true;
    c.dirty_exec = false;
    c.queued.clear();
    Reply::ok()
}

fn cmd_discard(_s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    if !c.in_multi {
        return Reply::Error("ERR DISCARD without MULTI".to_string());
    }
    c.in_multi = false;
    c.dirty_exec = false;
    c.queued.clear();
    c.watching.clear();
    Reply::ok()
}

/// `WATCH key [key ...]` — records each key's *current* write version
/// (`Server::watched_key_versions`, defaulting a never-written key to `0`)
/// against this connection, so a later `EXEC` can tell whether any of them
/// changed in between. Not allowed once already inside `MULTI` (matches
/// real Redis — queuing a `WATCH` for later execution inside the
/// transaction it's supposed to guard would be meaningless), which is why
/// `WATCH` is in `bypasses_queueing`'s exemption list: it must run
/// immediately to even reach this check.
fn cmd_watch(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    if c.in_multi {
        return Reply::Error("ERR WATCH inside MULTI is not allowed".to_string());
    }
    for key in &argv[1..] {
        let version = *s.watched_key_versions.entry((c.db_index, key.clone())).or_insert(0);
        c.watching.push((c.db_index, key.clone(), version));
    }
    Reply::ok()
}

fn cmd_unwatch(_s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    c.watching.clear();
    Reply::ok()
}

fn cmd_exec(s: &mut Server, c: &mut ClientState, _argv: &[Vec<u8>]) -> Reply {
    if !c.in_multi {
        return Reply::Error("ERR EXEC without MULTI".to_string());
    }
    if c.dirty_exec {
        c.in_multi = false;
        c.dirty_exec = false;
        c.queued.clear();
        c.watching.clear();
        return Reply::Error("EXECABORT Transaction discarded because of previous errors.".to_string());
    }
    // `touchWatchedKeysInDb`'s payoff, checked lazily right here rather than
    // proactively by whichever other connection did the writing — see
    // `ClientState::watching`'s doc comment for why.
    let watch_dirty = c.watching.iter().any(|(db, key, recorded)| s.watched_key_versions.get(&(*db, key.clone())).copied().unwrap_or(0) != *recorded);
    c.watching.clear();
    if watch_dirty {
        c.in_multi = false;
        c.dirty_exec = false;
        c.queued.clear();
        return Reply::NullArray;
    }
    let queued = std::mem::take(&mut c.queued);
    c.in_multi = false;
    let results = queued.into_iter().map(|cmd_argv| s.execute_single(c, &cmd_argv)).collect();
    Reply::Array(results)
}

// ----------------------------------------------------------- DataFrame ----
// `--features polars` only — see `dataframe.rs`'s module doc for the full
// scope decision. `DF.SET`/`DF.FROMCSV` overwrite wholesale on success (same
// "no partial-write WRONGTYPE check against the old value" shape as `SET`
// itself, not the auto-vivify-or-WRONGTYPE shape `LPUSH`/`XADD` use — a
// DataFrame key is always replaced whole, never incrementally built up), and
// leave the old value untouched on a decode error rather than deleting the
// key first.

#[cfg(feature = "polars")]
fn cmd_df_fromcsv(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match crate::dataframe::DataFrameObj::from_csv(&argv[2]) {
        Ok(df) => {
            s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::DataFrame(df));
            notify_keyspace_event(s, c.db_index, 'g', "df.fromcsv", &argv[1]);
            Reply::ok()
        }
        Err(e) => Reply::Error(e),
    }
}

/// `DF.FROMPARQUET key <parquet-bytes>` — binary ingest, same shape as
/// `DF.SET` (overwrite-wholesale-on-success, untouched-on-decode-error; see
/// the comment above `cmd_df_fromcsv`), load from a file the same way:
/// `redisx-cli -x DF.FROMPARQUET key < file.parquet`.
#[cfg(feature = "polars")]
fn cmd_df_fromparquet(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match crate::dataframe::DataFrameObj::from_parquet(&argv[2]) {
        Ok(df) => {
            s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::DataFrame(df));
            notify_keyspace_event(s, c.db_index, 'g', "df.fromparquet", &argv[1]);
            Reply::ok()
        }
        Err(e) => Reply::Error(e),
    }
}

#[cfg(feature = "polars")]
fn cmd_df_set(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match crate::dataframe::DataFrameObj::from_ipc_bytes(&argv[2]) {
        Ok(df) => {
            s.dbs[c.db_index].dict.insert(SLOT, argv[1].clone(), RObj::DataFrame(df));
            notify_keyspace_event(s, c.db_index, 'g', "df.set", &argv[1]);
            Reply::ok()
        }
        Err(e) => Reply::Error(e),
    }
}

#[cfg(feature = "polars")]
fn cmd_df_get(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => match df.to_ipc_bytes() {
            Ok(bytes) => Reply::bulk(bytes),
            Err(e) => Reply::Error(e),
        },
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Null,
    }
}

#[cfg(feature = "polars")]
fn cmd_df_tocsv(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => match df.to_csv() {
            Ok(bytes) => Reply::bulk(bytes),
            Err(e) => Reply::Error(e),
        },
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Null,
    }
}

/// `DF.TOPARQUET key` — the write half of `DF.FROMPARQUET`, same binary
/// (not human-typeable) output profile as `DF.GET`.
#[cfg(feature = "polars")]
fn cmd_df_toparquet(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => match df.to_parquet() {
            Ok(bytes) => Reply::bulk(bytes),
            Err(e) => Reply::Error(e),
        },
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Null,
    }
}

#[cfg(feature = "polars")]
fn cmd_df_shape(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => {
            let (rows, cols) = df.shape();
            Reply::Array(vec![Reply::Integer(rows as i64), Reply::Integer(cols as i64)])
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Error("ERR no such key".to_string()),
    }
}

#[cfg(feature = "polars")]
fn cmd_df_columns(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => Reply::Array(df.column_names().into_iter().map(|n| Reply::bulk(n.into_bytes())).collect()),
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Error("ERR no such key".to_string()),
    }
}

/// `DF.QUERY key "SELECT ... FROM self ..."` — the whole reason this is a
/// native type instead of Python-blob-over-`FCALL`: the query runs inside
/// this process via `polars::sql::SQLContext`, not shipped out anywhere.
#[cfg(feature = "polars")]
fn cmd_df_query(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    let sql = ok_bytes_as_string(&argv[2]);
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => match df.query(&sql) {
            Ok(result) => crate::dataframe::dataframe_to_rows_reply(&result),
            Err(e) => Reply::Error(e),
        },
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Error("ERR no such key".to_string()),
    }
}

/// `DF.PRETTY key [sql]` — human-oriented box-drawing table (polars' own
/// `Display` impl via `dataframe.rs`'s `pretty_table`), the whole frame if
/// `sql` is omitted or `DF.QUERY`'s result pretty-printed instead of
/// returned as a row array if it's given. `redisx-cli` special-cases this
/// command name to print its bulk-string reply verbatim rather than quoted/
/// escaped (see that crate's module doc) — the point of this command is the
/// box-drawing characters and alignment, not a machine-parseable shape.
#[cfg(feature = "polars")]
fn cmd_df_pretty(s: &mut Server, c: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
    match s.dbs[c.db_index].dict.get(SLOT, &argv[1]) {
        Some(RObj::DataFrame(df)) => {
            if let Some(sql_arg) = argv.get(2) {
                let sql = ok_bytes_as_string(sql_arg);
                match df.query(&sql) {
                    Ok(result) => Reply::bulk(crate::dataframe::pretty_table(&result).into_bytes()),
                    Err(e) => Reply::Error(e),
                }
            } else {
                Reply::bulk(df.pretty().into_bytes())
            }
        }
        Some(_) => Reply::Error(WRONGTYPE.to_string()),
        None => Reply::Error("ERR no such key".to_string()),
    }
}

pub static COMMANDS: &[CommandSpec] = &[
    CommandSpec { name: "PING", arity: -1, handler: cmd_ping, is_write: false },
    CommandSpec { name: "ECHO", arity: 2, handler: cmd_echo, is_write: false },
    CommandSpec { name: "QUIT", arity: -1, handler: cmd_quit, is_write: false },
    CommandSpec { name: "RESET", arity: 1, handler: cmd_reset, is_write: false },
    CommandSpec { name: "SELECT", arity: 2, handler: cmd_select, is_write: false },
    CommandSpec { name: "DBSIZE", arity: 1, handler: cmd_dbsize, is_write: false },
    CommandSpec { name: "FLUSHALL", arity: -1, handler: cmd_flushall, is_write: true },
    CommandSpec { name: "FLUSHDB", arity: -1, handler: cmd_flushdb, is_write: true },
    CommandSpec { name: "SAVE", arity: 1, handler: cmd_save, is_write: false },
    CommandSpec { name: "BGSAVE", arity: -1, handler: cmd_bgsave, is_write: false },
    CommandSpec { name: "LASTSAVE", arity: 1, handler: cmd_lastsave, is_write: false },
    CommandSpec { name: "TIME", arity: 1, handler: cmd_time, is_write: false },
    CommandSpec { name: "SHUTDOWN", arity: -1, handler: cmd_shutdown, is_write: false },
    CommandSpec { name: "REPLCONF", arity: -1, handler: cmd_replconf, is_write: false },
    CommandSpec { name: "SLOWLOG", arity: -2, handler: cmd_slowlog, is_write: false },
    CommandSpec { name: "SUBSCRIBE", arity: -2, handler: cmd_subscribe, is_write: false },
    CommandSpec { name: "UNSUBSCRIBE", arity: -1, handler: cmd_unsubscribe, is_write: false },
    CommandSpec { name: "PSUBSCRIBE", arity: -2, handler: cmd_psubscribe, is_write: false },
    CommandSpec { name: "PUNSUBSCRIBE", arity: -1, handler: cmd_punsubscribe, is_write: false },
    // Not AOF-logged/replicated (see this command group's module-doc note
    // just above `cmd_subscribe`), so `is_write: false` despite being a
    // "write" of sorts in real Redis's command table.
    CommandSpec { name: "PUBLISH", arity: 3, handler: cmd_publish, is_write: false },
    CommandSpec { name: "PUBSUB", arity: -2, handler: cmd_pubsub, is_write: false },
    CommandSpec { name: "CLIENT", arity: -2, handler: cmd_client, is_write: false },
    CommandSpec { name: "AUTH", arity: -2, handler: cmd_auth, is_write: false },
    CommandSpec { name: "ACL", arity: -2, handler: cmd_acl, is_write: false },
    CommandSpec { name: "CLUSTER", arity: -2, handler: cmd_cluster, is_write: false },
    CommandSpec { name: "ASKING", arity: 1, handler: cmd_asking, is_write: false },
    CommandSpec { name: "READONLY", arity: 1, handler: cmd_readonly, is_write: false },
    CommandSpec { name: "READWRITE", arity: 1, handler: cmd_readwrite, is_write: false },
    CommandSpec { name: "WAIT", arity: 3, handler: cmd_wait, is_write: false },
    CommandSpec { name: "FAILOVER", arity: -1, handler: cmd_failover, is_write: false },
    CommandSpec { name: "HELLO", arity: -1, handler: cmd_hello, is_write: false },
    CommandSpec { name: "LOLWUT", arity: -1, handler: cmd_lolwut, is_write: false },
    CommandSpec { name: "MEMORY", arity: -2, handler: cmd_memory, is_write: false },
    CommandSpec { name: "DEBUG", arity: -2, handler: cmd_debug, is_write: false },
    CommandSpec { name: "COMMAND", arity: -1, handler: cmd_command, is_write: false },
    CommandSpec { name: "INFO", arity: -1, handler: cmd_info, is_write: false },
    CommandSpec { name: "SETBIT", arity: 4, handler: cmd_setbit, is_write: true },
    CommandSpec { name: "GETBIT", arity: 3, handler: cmd_getbit, is_write: false },
    CommandSpec { name: "BITCOUNT", arity: -2, handler: cmd_bitcount, is_write: false },
    CommandSpec { name: "BITPOS", arity: -3, handler: cmd_bitpos, is_write: false },
    CommandSpec { name: "BITOP", arity: -4, handler: cmd_bitop, is_write: true },
    // Not `is_write`: plain `SORT` (no `STORE`) is read-only — marking it a
    // write unconditionally would wrongly invalidate `CLIENT TRACKING` on
    // its *source* key (never actually written) instead of its destination.
    // `cmd_sort`'s own `STORE` branch calls `after_write`/tracking
    // invalidation itself, targeting the key it actually wrote.
    CommandSpec { name: "SORT", arity: -2, handler: cmd_sort, is_write: false },
    CommandSpec { name: "PFADD", arity: -2, handler: cmd_pfadd, is_write: true },
    CommandSpec { name: "PFCOUNT", arity: -2, handler: cmd_pfcount, is_write: false },
    CommandSpec { name: "PFMERGE", arity: -2, handler: cmd_pfmerge, is_write: true },
    CommandSpec { name: "GEOADD", arity: -5, handler: cmd_geoadd, is_write: true },
    CommandSpec { name: "GEOPOS", arity: -2, handler: cmd_geopos, is_write: false },
    CommandSpec { name: "GEODIST", arity: -4, handler: cmd_geodist, is_write: false },
    // `-7`: the shortest valid form is `GEOSEARCH key FROMMEMBER member
    // BYRADIUS r unit` (7 args) — one token shorter than the `FROMLONLAT lon
    // lat` form, since `FROMMEMBER` takes a single argument where
    // `FROMLONLAT` takes two.
    CommandSpec { name: "GEOSEARCH", arity: -7, handler: cmd_geosearch, is_write: false },
    CommandSpec { name: "GEOSEARCHSTORE", arity: -8, handler: cmd_geosearchstore, is_write: true },
    CommandSpec { name: "XADD", arity: -5, handler: cmd_xadd, is_write: true },
    CommandSpec { name: "XLEN", arity: 2, handler: cmd_xlen, is_write: false },
    CommandSpec { name: "XRANGE", arity: -4, handler: cmd_xrange, is_write: false },
    CommandSpec { name: "XREVRANGE", arity: -4, handler: cmd_xrevrange, is_write: false },
    CommandSpec { name: "XDEL", arity: -3, handler: cmd_xdel, is_write: true },
    CommandSpec { name: "XTRIM", arity: -4, handler: cmd_xtrim, is_write: true },
    CommandSpec { name: "XREAD", arity: -4, handler: cmd_xread, is_write: false },
    CommandSpec { name: "XSETID", arity: -3, handler: cmd_xsetid, is_write: true },
    CommandSpec { name: "XINFO", arity: -2, handler: cmd_xinfo, is_write: false },
    CommandSpec { name: "XGROUP", arity: -2, handler: cmd_xgroup, is_write: true },
    CommandSpec { name: "XREADGROUP", arity: -7, handler: cmd_xreadgroup, is_write: true },
    CommandSpec { name: "XACK", arity: -4, handler: cmd_xack, is_write: true },
    CommandSpec { name: "XPENDING", arity: -3, handler: cmd_xpending, is_write: false },
    CommandSpec { name: "XCLAIM", arity: -6, handler: cmd_xclaim, is_write: true },
    CommandSpec { name: "XAUTOCLAIM", arity: -6, handler: cmd_xautoclaim, is_write: true },
    // Not `is_write`: a script's individual `redis.call()` writes already
    // propagate on their own (each one re-enters `dispatch`/`after_write`),
    // matching real Redis's "effects replication" for scripts — propagating
    // `FCALL` itself too would double up every write it made.
    CommandSpec { name: "FUNCTION", arity: -2, handler: cmd_function, is_write: false },
    CommandSpec { name: "FCALL", arity: -3, handler: cmd_fcall, is_write: false },
    CommandSpec { name: "FCALL_RO", arity: -3, handler: cmd_fcall, is_write: false },
    // Not `is_write`, same reasoning as FCALL: each inner `redis.call()`
    // write already propagates on its own (effects replication).
    CommandSpec { name: "EVAL", arity: -3, handler: cmd_eval, is_write: false },
    CommandSpec { name: "EVALSHA", arity: -3, handler: cmd_evalsha, is_write: false },
    CommandSpec { name: "SCRIPT", arity: -2, handler: cmd_script, is_write: false },
    CommandSpec { name: "SET", arity: -3, handler: cmd_set, is_write: true },
    CommandSpec { name: "GET", arity: 2, handler: cmd_get, is_write: false },
    CommandSpec { name: "DEL", arity: -2, handler: cmd_del, is_write: true },
    CommandSpec { name: "EXISTS", arity: -2, handler: cmd_exists, is_write: false },
    CommandSpec { name: "TOUCH", arity: -2, handler: cmd_touch, is_write: false },
    CommandSpec { name: "UNLINK", arity: -2, handler: cmd_unlink, is_write: true },
    CommandSpec { name: "KEYS", arity: 2, handler: cmd_keys, is_write: false },
    CommandSpec { name: "RANDOMKEY", arity: 1, handler: cmd_randomkey, is_write: false },
    CommandSpec { name: "RENAME", arity: 3, handler: cmd_rename, is_write: true },
    CommandSpec { name: "RENAMENX", arity: 3, handler: cmd_renamenx, is_write: true },
    CommandSpec { name: "COPY", arity: -3, handler: cmd_copy, is_write: true },
    CommandSpec { name: "DUMP", arity: 2, handler: cmd_dump, is_write: false },
    CommandSpec { name: "RESTORE", arity: -4, handler: cmd_restore, is_write: true },
    CommandSpec { name: "MOVE", arity: 3, handler: cmd_move, is_write: true },
    CommandSpec { name: "SCAN", arity: -2, handler: cmd_scan, is_write: false },
    CommandSpec { name: "HSCAN", arity: -3, handler: cmd_hscan, is_write: false },
    CommandSpec { name: "SSCAN", arity: -3, handler: cmd_sscan, is_write: false },
    CommandSpec { name: "ZSCAN", arity: -3, handler: cmd_zscan, is_write: false },
    CommandSpec { name: "OBJECT", arity: -2, handler: cmd_object, is_write: false },
    CommandSpec { name: "TYPE", arity: 2, handler: cmd_type, is_write: false },
    CommandSpec { name: "INCR", arity: 2, handler: cmd_incr, is_write: true },
    CommandSpec { name: "INCRBY", arity: 3, handler: cmd_incrby, is_write: true },
    CommandSpec { name: "DECR", arity: 2, handler: cmd_decr, is_write: true },
    CommandSpec { name: "DECRBY", arity: 3, handler: cmd_decrby, is_write: true },
    CommandSpec { name: "INCRBYFLOAT", arity: 3, handler: cmd_incrbyfloat, is_write: true },
    CommandSpec { name: "APPEND", arity: 3, handler: cmd_append, is_write: true },
    CommandSpec { name: "STRLEN", arity: 2, handler: cmd_strlen, is_write: false },
    CommandSpec { name: "GETRANGE", arity: 4, handler: cmd_getrange, is_write: false },
    CommandSpec { name: "SETRANGE", arity: 4, handler: cmd_setrange, is_write: true },
    CommandSpec { name: "MGET", arity: -2, handler: cmd_mget, is_write: false },
    CommandSpec { name: "MSET", arity: -3, handler: cmd_mset, is_write: true },
    CommandSpec { name: "MSETNX", arity: -3, handler: cmd_msetnx, is_write: true },
    CommandSpec { name: "GETDEL", arity: 2, handler: cmd_getdel, is_write: true },
    CommandSpec { name: "GETEX", arity: -2, handler: cmd_getex, is_write: true },
    CommandSpec { name: "EXPIRE", arity: -3, handler: cmd_expire, is_write: true },
    CommandSpec { name: "PEXPIRE", arity: -3, handler: cmd_pexpire, is_write: true },
    CommandSpec { name: "EXPIREAT", arity: -3, handler: cmd_expireat, is_write: true },
    CommandSpec { name: "PEXPIREAT", arity: -3, handler: cmd_pexpireat, is_write: true },
    CommandSpec { name: "PERSIST", arity: 2, handler: cmd_persist, is_write: true },
    CommandSpec { name: "TTL", arity: 2, handler: cmd_ttl, is_write: false },
    CommandSpec { name: "PTTL", arity: 2, handler: cmd_pttl, is_write: false },
    CommandSpec { name: "EXPIRETIME", arity: 2, handler: cmd_expiretime, is_write: false },
    CommandSpec { name: "PEXPIRETIME", arity: 2, handler: cmd_pexpiretime, is_write: false },
    CommandSpec { name: "LPUSH", arity: -3, handler: cmd_lpush, is_write: true },
    CommandSpec { name: "RPUSH", arity: -3, handler: cmd_rpush, is_write: true },
    CommandSpec { name: "LPOP", arity: -2, handler: cmd_lpop, is_write: true },
    CommandSpec { name: "RPOP", arity: -2, handler: cmd_rpop, is_write: true },
    CommandSpec { name: "LRANGE", arity: 4, handler: cmd_lrange, is_write: false },
    CommandSpec { name: "LLEN", arity: 2, handler: cmd_llen, is_write: false },
    CommandSpec { name: "LINDEX", arity: 3, handler: cmd_lindex, is_write: false },
    CommandSpec { name: "LSET", arity: 4, handler: cmd_lset, is_write: true },
    CommandSpec { name: "LINSERT", arity: 5, handler: cmd_linsert, is_write: true },
    CommandSpec { name: "LREM", arity: 4, handler: cmd_lrem, is_write: true },
    CommandSpec { name: "LTRIM", arity: 4, handler: cmd_ltrim, is_write: true },
    CommandSpec { name: "LPOS", arity: -3, handler: cmd_lpos, is_write: false },
    CommandSpec { name: "LMOVE", arity: 5, handler: cmd_lmove, is_write: true },
    CommandSpec { name: "RPOPLPUSH", arity: 3, handler: cmd_rpoplpush, is_write: true },
    // Not `is_write` — see the long comment just above
    // `cmd_blpop_or_brpop_non_blocking` for why (effects replication via the
    // nested synthetic non-blocking command). These handlers are what
    // `MULTI`/`EXEC`-queued and script-invoked calls use; a direct top-level
    // client call never reaches them at all — the binary layer intercepts
    // the command name first and calls `Server::try_blocking_command`
    // instead (see `src/bin/redisx-server.rs`'s module doc).
    CommandSpec { name: "BLPOP", arity: -3, handler: cmd_blpop_or_brpop_non_blocking, is_write: false },
    CommandSpec { name: "BRPOP", arity: -3, handler: cmd_blpop_or_brpop_non_blocking, is_write: false },
    CommandSpec { name: "BLMOVE", arity: 6, handler: cmd_blmove_non_blocking, is_write: false },
    CommandSpec { name: "BRPOPLPUSH", arity: 4, handler: cmd_brpoplpush_non_blocking, is_write: false },
    CommandSpec { name: "HSET", arity: -4, handler: cmd_hset, is_write: true },
    CommandSpec { name: "HGET", arity: 3, handler: cmd_hget, is_write: false },
    CommandSpec { name: "HDEL", arity: -3, handler: cmd_hdel, is_write: true },
    CommandSpec { name: "HGETALL", arity: 2, handler: cmd_hgetall, is_write: false },
    CommandSpec { name: "HLEN", arity: 2, handler: cmd_hlen, is_write: false },
    CommandSpec { name: "HEXISTS", arity: 3, handler: cmd_hexists, is_write: false },
    CommandSpec { name: "HSETNX", arity: 4, handler: cmd_hsetnx, is_write: true },
    CommandSpec { name: "HMGET", arity: -3, handler: cmd_hmget, is_write: false },
    CommandSpec { name: "HKEYS", arity: 2, handler: cmd_hkeys, is_write: false },
    CommandSpec { name: "HVALS", arity: 2, handler: cmd_hvals, is_write: false },
    CommandSpec { name: "HINCRBY", arity: 4, handler: cmd_hincrby, is_write: true },
    CommandSpec { name: "HINCRBYFLOAT", arity: 4, handler: cmd_hincrbyfloat, is_write: true },
    CommandSpec { name: "HRANDFIELD", arity: -2, handler: cmd_hrandfield, is_write: false },
    CommandSpec { name: "SADD", arity: -3, handler: cmd_sadd, is_write: true },
    CommandSpec { name: "SREM", arity: -3, handler: cmd_srem, is_write: true },
    CommandSpec { name: "SISMEMBER", arity: 3, handler: cmd_sismember, is_write: false },
    CommandSpec { name: "SMEMBERS", arity: 2, handler: cmd_smembers, is_write: false },
    CommandSpec { name: "SCARD", arity: 2, handler: cmd_scard, is_write: false },
    CommandSpec { name: "SMISMEMBER", arity: -3, handler: cmd_smismember, is_write: false },
    CommandSpec { name: "SPOP", arity: -2, handler: cmd_spop, is_write: true },
    CommandSpec { name: "SRANDMEMBER", arity: -2, handler: cmd_srandmember, is_write: false },
    CommandSpec { name: "SMOVE", arity: 4, handler: cmd_smove, is_write: true },
    CommandSpec { name: "SINTER", arity: -2, handler: cmd_sinter, is_write: false },
    CommandSpec { name: "SUNION", arity: -2, handler: cmd_sunion, is_write: false },
    CommandSpec { name: "SDIFF", arity: -2, handler: cmd_sdiff, is_write: false },
    CommandSpec { name: "SINTERSTORE", arity: -3, handler: cmd_sinterstore, is_write: true },
    CommandSpec { name: "SUNIONSTORE", arity: -3, handler: cmd_sunionstore, is_write: true },
    CommandSpec { name: "SDIFFSTORE", arity: -3, handler: cmd_sdiffstore, is_write: true },
    CommandSpec { name: "ZADD", arity: -4, handler: cmd_zadd, is_write: true },
    CommandSpec { name: "ZSCORE", arity: 3, handler: cmd_zscore, is_write: false },
    CommandSpec { name: "ZREM", arity: -3, handler: cmd_zrem, is_write: true },
    CommandSpec { name: "ZCARD", arity: 2, handler: cmd_zcard, is_write: false },
    CommandSpec { name: "ZRANK", arity: 3, handler: cmd_zrank, is_write: false },
    CommandSpec { name: "ZRANGE", arity: -4, handler: cmd_zrange, is_write: false },
    CommandSpec { name: "ZREVRANGE", arity: -4, handler: cmd_zrevrange, is_write: false },
    CommandSpec { name: "ZREVRANK", arity: 3, handler: cmd_zrevrank, is_write: false },
    CommandSpec { name: "ZCOUNT", arity: 4, handler: cmd_zcount, is_write: false },
    CommandSpec { name: "ZRANGEBYSCORE", arity: -4, handler: cmd_zrangebyscore, is_write: false },
    CommandSpec { name: "ZREVRANGEBYSCORE", arity: -4, handler: cmd_zrevrangebyscore, is_write: false },
    CommandSpec { name: "ZLEXCOUNT", arity: 4, handler: cmd_zlexcount, is_write: false },
    CommandSpec { name: "ZRANGEBYLEX", arity: -4, handler: cmd_zrangebylex, is_write: false },
    CommandSpec { name: "ZREVRANGEBYLEX", arity: -4, handler: cmd_zrevrangebylex, is_write: false },
    CommandSpec { name: "ZINCRBY", arity: 4, handler: cmd_zincrby, is_write: true },
    CommandSpec { name: "ZPOPMIN", arity: -2, handler: cmd_zpopmin, is_write: true },
    CommandSpec { name: "ZPOPMAX", arity: -2, handler: cmd_zpopmax, is_write: true },
    CommandSpec { name: "BZPOPMIN", arity: -3, handler: cmd_bzpopmin_or_bzpopmax_non_blocking, is_write: false },
    CommandSpec { name: "BZPOPMAX", arity: -3, handler: cmd_bzpopmin_or_bzpopmax_non_blocking, is_write: false },
    CommandSpec { name: "ZMSCORE", arity: -3, handler: cmd_zmscore, is_write: false },
    CommandSpec { name: "ZRANDMEMBER", arity: -2, handler: cmd_zrandmember, is_write: false },
    CommandSpec { name: "ZUNIONSTORE", arity: -4, handler: cmd_zunionstore, is_write: true },
    CommandSpec { name: "ZINTERSTORE", arity: -4, handler: cmd_zinterstore, is_write: true },
    CommandSpec { name: "ZDIFFSTORE", arity: -4, handler: cmd_zdiffstore, is_write: true },
    CommandSpec { name: "ZUNION", arity: -3, handler: cmd_zunion, is_write: false },
    CommandSpec { name: "ZINTER", arity: -3, handler: cmd_zinter, is_write: false },
    CommandSpec { name: "ZDIFF", arity: -3, handler: cmd_zdiff, is_write: false },
    CommandSpec { name: "ZINTERCARD", arity: -3, handler: cmd_zintercard, is_write: false },
    CommandSpec { name: "CONFIG", arity: -2, handler: cmd_config, is_write: false },
    CommandSpec { name: "MULTI", arity: 1, handler: cmd_multi, is_write: false },
    CommandSpec { name: "DISCARD", arity: 1, handler: cmd_discard, is_write: false },
    CommandSpec { name: "EXEC", arity: 1, handler: cmd_exec, is_write: false },
    CommandSpec { name: "WATCH", arity: -2, handler: cmd_watch, is_write: false },
    CommandSpec { name: "UNWATCH", arity: 1, handler: cmd_unwatch, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.FROMCSV", arity: 3, handler: cmd_df_fromcsv, is_write: true },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.FROMPARQUET", arity: 3, handler: cmd_df_fromparquet, is_write: true },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.SET", arity: 3, handler: cmd_df_set, is_write: true },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.GET", arity: 2, handler: cmd_df_get, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.TOCSV", arity: 2, handler: cmd_df_tocsv, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.TOPARQUET", arity: 2, handler: cmd_df_toparquet, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.SHAPE", arity: 2, handler: cmd_df_shape, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.COLUMNS", arity: 2, handler: cmd_df_columns, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.QUERY", arity: 3, handler: cmd_df_query, is_write: false },
    #[cfg(feature = "polars")]
    CommandSpec { name: "DF.PRETTY", arity: -2, handler: cmd_df_pretty, is_write: false },
];

pub(crate) fn find_command(name: &str) -> Option<&'static CommandSpec> {
    COMMANDS.iter().find(|c| c.name == name)
}

fn check_arity(spec: &CommandSpec, argc: usize) -> bool {
    if spec.arity >= 0 {
        argc as i32 == spec.arity
    } else {
        argc as i32 >= -spec.arity
    }
}

/// Commands that must run immediately even inside a MULTI block, matching
/// `processCommand()`'s explicit exemption list (`c->cmd->proc != execCommand
/// && ... != quitCommand && ...`). `WATCH` is in the real list too — it has
/// to run immediately to produce its own "not allowed inside MULTI" error
/// rather than being queued and silently accepted. `UNWATCH` is not exempted
/// in real Redis either (queuing it is harmless, if pointless), so it's
/// deliberately left off this list.
fn bypasses_queueing(name: &str) -> bool {
    matches!(name, "MULTI" | "EXEC" | "DISCARD" | "QUIT" | "RESET" | "WATCH")
}

fn generate_replid() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut s = String::new();
    for _ in 0..3 {
        s.push_str(&format!("{:016x}", RandomState::new().build_hasher().finish()));
    }
    s.truncate(40);
    s
}

impl Server {
    pub fn new(num_dbs: usize) -> Self {
        let num_dbs = num_dbs.max(1);
        Server {
            dbs: (0..num_dbs).map(|_| Database::new()).collect(),
            config: ConfigStore::new(),
            replid: generate_replid(),
            master_repl_offset: 0,
            role: Role::Master,
            last_propagated_db: None,
            repl_backlog: Vec::new(),
            repl_backlog_start_offset: 0,
            pending_propagation: Vec::new(),
            lua: {
                let lua = mlua::Lua::new();
                crate::script_lua::install(&lua).expect("installing the redis.* Lua API must not fail");
                lua
            },
            libraries: std::collections::HashMap::new(),
            acl_users: std::collections::BTreeMap::from([("default".to_string(), crate::acl::AclUser::default_user())]),
            cluster: cluster::ClusterState::new(),
            functions: std::collections::HashMap::new(),
            compiled: std::collections::HashMap::new(),
            script_cache: std::collections::HashMap::new(),
            channel_subs: std::collections::HashMap::new(),
            pattern_subs: std::collections::HashMap::new(),
            pending_pushes: Vec::new(),
            tracking_table: std::collections::HashMap::new(),
            bcast_tracking: std::collections::HashMap::new(),
            tracking_redirect: std::collections::HashMap::new(),
            tracking_noloop: std::collections::HashSet::new(),
            connected_replicas: 0,
            watched_key_versions: std::collections::HashMap::new(),
            start_time_unix: now_ms() / 1000,
            last_save_unix: None,
            connected_clients: 0,
            total_commands_processed: 0,
            dirty_since_save: 0,
            blocked: std::collections::HashMap::new(),
            blocking_keys: std::collections::HashMap::new(),
            unblocked: Vec::new(),
            lazyfree_sender: None,
            defrag_cursors: vec![0; num_dbs],
            last_defrag_check: None,
            used_memory_cache: None,
            active_expire_enabled: true,
        }
    }

    /// Takes whatever blocked clients just got satisfied (by a write to a
    /// key they were waiting on) or timed out, for the binary layer to
    /// deliver each reply and resume normal command processing on that
    /// connection — the same drain pattern as `take_pending_pushes`.
    pub fn take_unblocked(&mut self) -> Vec<(i64, Reply)> {
        std::mem::take(&mut self.unblocked)
    }

    /// The binary layer (which owns the real connection list) calls this
    /// whenever a client connects or disconnects, so `INFO`'s
    /// `connected_clients` has something to report.
    pub fn set_connected_clients(&mut self, n: usize) {
        self.connected_clients = n;
    }

    /// Takes whatever `PUBLISH`/`CLIENT TRACKING` push messages have
    /// accumulated since the last call, for the binary layer to deliver to
    /// each named target client (see `pending_pushes`'s doc comment).
    pub fn take_pending_pushes(&mut self) -> Vec<(i64, Reply)> {
        std::mem::take(&mut self.pending_pushes)
    }

    /// Cleans up every trace of a client that just disconnected — its
    /// channel/pattern subscriptions and tracking-table entries — so
    /// `PUBLISH`/invalidation never tries to push to a dead connection, and
    /// so its old subscriptions don't keep a channel's subscriber set
    /// alive forever. The binary layer calls this exactly once, right
    /// before dropping a `Conn` (it owns the actual socket lifecycle;
    /// `Server` only knows client ids, not which ones are still connected).
    pub fn client_disconnected(&mut self, id: i64) {
        self.channel_subs.retain(|_, subs| {
            subs.remove(&id);
            !subs.is_empty()
        });
        self.pattern_subs.retain(|_, subs| {
            subs.remove(&id);
            !subs.is_empty()
        });
        self.tracking_table.retain(|_, subs| {
            subs.remove(&id);
            !subs.is_empty()
        });
        self.bcast_tracking.remove(&id);
        self.tracking_redirect.remove(&id);
        self.tracking_noloop.remove(&id);
        self.unregister_blocked(id);
    }

    /// The binary layer (which owns the real replica socket list) calls
    /// this whenever a replica connects or disconnects, so `WAIT` has
    /// something to report — see `cmd_wait`'s doc comment.
    pub fn set_connected_replicas(&mut self, n: usize) {
        self.connected_replicas = n;
    }

    /// Takes whatever write commands have accumulated since the last call,
    /// for the binary layer to fan out to replica sockets.
    pub fn take_pending_propagation(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.pending_propagation)
    }

    /// `feedReplicationBuffer` — encodes `argv` as a RESP multibulk command
    /// (prefixed with a `SELECT` if the target db changed since the last
    /// propagated command) and appends it to the pending propagation buffer
    /// and the replication backlog alike — `master_repl_offset` now counts
    /// every byte either one receives (including the `SELECT` prefix, which
    /// an earlier version of this method left uncounted; fixed here since
    /// `try_partial_resync`'s offset bookkeeping depends on them matching
    /// exactly, where before nothing did).
    fn propagate(&mut self, db_index: usize, argv: &[Vec<u8>]) {
        let before = self.pending_propagation.len();
        if self.last_propagated_db != Some(db_index) {
            let select = Reply::Array(vec![Reply::bulk(b"SELECT".to_vec()), Reply::bulk(db_index.to_string().into_bytes())]);
            select.encode(2, &mut self.pending_propagation);
            self.last_propagated_db = Some(db_index);
        }
        let cmd = Reply::Array(argv.iter().map(|a| Reply::bulk(a.clone())).collect());
        cmd.encode(2, &mut self.pending_propagation);
        let produced = self.pending_propagation[before..].to_vec();
        self.master_repl_offset += produced.len() as u64;
        self.feed_backlog(&produced);
    }

    /// `repl-backlog-size`'s fixed-at-1MiB equivalent — this fork has no
    /// `CONFIG`-settable size for it, just a flat constant.
    const REPL_BACKLOG_MAX: usize = 1024 * 1024;
    /// How far past `REPL_BACKLOG_MAX` `feed_backlog` lets the backlog grow
    /// before actually compacting it — see that method's own doc for why
    /// this needs to be nonzero at all.
    const REPL_BACKLOG_SLACK: usize = Self::REPL_BACKLOG_MAX / 4;

    /// Appends `bytes` to the backlog, trimming its front back down to
    /// `REPL_BACKLOG_MAX` only once it's drifted `REPL_BACKLOG_SLACK` past
    /// that cap — not on every single call that merely exceeds it.
    ///
    /// `Vec::drain` on a prefix range is `O(remaining length)`: it has to
    /// shift every byte after the drained range down to fill the gap. Doing
    /// that on *every* call once the backlog is full/`AppendOnlyFile`-style
    /// workloads keep it full — which a sustained write burst does almost
    /// immediately, 1MiB fills fast — turns every single propagated write
    /// into an `O(backlog size)` (~1MiB) operation. That's not hypothetical:
    /// it's exactly what made evicting many keys in one `enforce_maxmemory`
    /// pass slow before this existed (every evicted key propagates its own
    /// `DEL`, so `propagate`→`feed_backlog` ran once per eviction — `O(backlog
    /// size)` each time added up to real, measured latency at a few hundred
    /// evictions). Allowing `REPL_BACKLOG_SLACK` of slack before compacting
    /// amortizes that `O(backlog size)` cost across that many bytes of
    /// writes instead of paying it on every single one — the backlog's
    /// retained window still only ever drifts up to 25% over the configured
    /// size between compactions, then gets trimmed straight back to it, so
    /// `try_partial_resync`'s "still in the window" guarantee is still
    /// honored, just not pinned to an exact byte count at every instant.
    fn feed_backlog(&mut self, bytes: &[u8]) {
        self.repl_backlog.extend_from_slice(bytes);
        if self.repl_backlog.len() > Self::REPL_BACKLOG_MAX + Self::REPL_BACKLOG_SLACK {
            let excess = self.repl_backlog.len() - Self::REPL_BACKLOG_MAX;
            self.repl_backlog.drain(..excess);
            self.repl_backlog_start_offset += excess as u64;
        }
    }

    /// `masterTryPartialResynchronization`, simplified to this process's own
    /// single current `replid` (real Redis also remembers one prior replid
    /// across a `REPLICAOF NO ONE`/role flip via `cached_master`; this fork
    /// doesn't). Returns the exact bytes a reconnecting replica missed if
    /// `replid` matches and `offset` is still within the backlog's
    /// retained window, or `None` (meaning: fall back to a full resync)
    /// if the replid is unrecognized or `offset` has already scrolled out
    /// of the backlog.
    pub fn try_partial_resync(&self, replid: &str, offset: u64) -> Option<Vec<u8>> {
        if replid != self.replid {
            return None;
        }
        let backlog_end = self.repl_backlog_start_offset + self.repl_backlog.len() as u64;
        if offset < self.repl_backlog_start_offset || offset > backlog_end {
            return None;
        }
        let skip = (offset - self.repl_backlog_start_offset) as usize;
        Some(self.repl_backlog[skip..].to_vec())
    }

    /// Runs after any successful write command, from any of the three
    /// entry points (`dispatch`, `execute_single`, `apply_replicated_command`):
    /// feeds the replication stream (`propagate`) and, if `appendonly yes`,
    /// appends the MONITOR-style AOF line (`aof::append`) — the two
    /// `feedAppendOnlyFile`/`replicationFeedSlaves` call sites in real
    /// Redis's `call()` collapsed into one helper here since this fork's
    /// AOF has no recovery semantics to keep in sync with anything.
    fn after_write(&mut self, client: &ClientState, argv: &[Vec<u8>]) {
        let db_index = client.db_index;
        self.propagate(db_index, argv);
        if crate::aof::is_enabled(&self.config) {
            let _ = crate::aof::append(&self.config, db_index, &client.peer, argv);
        }
    }

    /// Applies one command received over a replica link from our master
    /// (`replicationFeedSlaves`'s receiving side / `call()` as invoked from
    /// the replication link in real Redis). Bypasses MULTI-queueing entirely
    /// — a replica always executes what its master sends immediately.
    /// Re-propagates to this server's own replicas/AOF (chained replication
    /// in real Redis works the same way: a sub-replica's master is this
    /// replica, not the original one).
    pub fn apply_replicated_command(&mut self, client: &mut ClientState, argv: &[Vec<u8>]) {
        if argv.is_empty() {
            return;
        }
        let name = ok_bytes_as_string(&argv[0]).to_ascii_uppercase();
        if let Some(spec) = find_command(&name) {
            if check_arity(spec, argv.len()) {
                self.run_command(client, spec, argv);
            }
        }
    }

    /// Entry point for the handful of blocking commands (`BLPOP`/`BRPOP`/
    /// `BLMOVE`/`BRPOPLPUSH`/`BZPOPMIN`/`BZPOPMAX`) — called by the binary
    /// layer *instead of* `dispatch()`, the same way it intercepts `PSYNC`/
    /// `REPLICAOF` (see `src/bin/redisx-server.rs`'s module doc): a blocking
    /// command needs to tell its caller "don't write a reply yet, and stop
    /// reading further commands from this connection until I say so", which
    /// `fn(&mut Server, &mut ClientState, &[Vec<u8>]) -> Reply` has no way to
    /// express. Every blocking command is implemented as "try the ordinary
    /// non-blocking equivalent via `run_command` (so it gets exactly the
    /// same propagation/AOF/tracking/watch bookkeeping as if a client had
    /// called `LPOP`/`ZPOPMIN`/`LMOVE` directly); if that comes back empty,
    /// register as blocked instead of answering". On a successful
    /// *immediate* run, note that what gets propagated/AOF-logged is the
    /// non-blocking command, not the original blocking one — matching real
    /// Redis's own behavior (a replica has no use for blocking semantics).
    pub fn try_blocking_command(&mut self, client: &mut ClientState, argv: &[Vec<u8>]) -> BlockingOutcome {
        if argv.is_empty() {
            return BlockingOutcome::Immediate(Reply::Error("ERR empty command".to_string()));
        }
        let name = ok_bytes_as_string(&argv[0]).to_ascii_uppercase();
        // `XREAD`/`XREADGROUP` don't fit the uniform "last argv element is a
        // seconds-float timeout" shape every `B*` command below shares
        // (their `BLOCK <ms>` option is a milliseconds integer sitting
        // mid-argv, before `STREAMS`), so they're handled as early returns
        // instead of falling into that shared tail.
        if name == "XREAD" {
            return self.try_blocking_xread(client, argv);
        }
        if name == "XREADGROUP" {
            return self.try_blocking_xreadgroup(client, argv);
        }
        let (keys, attempt) = match name.as_str() {
            "BLPOP" | "BRPOP" => {
                if argv.len() < 3 {
                    return BlockingOutcome::Immediate(Reply::Error(format!("ERR wrong number of arguments for '{}' command", name.to_ascii_lowercase())));
                }
                let Ok(_timeout) = parse_block_timeout(&argv[argv.len() - 1]) else {
                    return BlockingOutcome::Immediate(Reply::Error("ERR timeout is not a float or out of range".to_string()));
                };
                let keys = argv[1..argv.len() - 1].to_vec();
                (keys.clone(), self.attempt_blocking_list_pop(client.id, client.db_index, &keys, name == "BLPOP"))
            }
            "BZPOPMIN" | "BZPOPMAX" => {
                if argv.len() < 3 {
                    return BlockingOutcome::Immediate(Reply::Error(format!("ERR wrong number of arguments for '{}' command", name.to_ascii_lowercase())));
                }
                let Ok(_timeout) = parse_block_timeout(&argv[argv.len() - 1]) else {
                    return BlockingOutcome::Immediate(Reply::Error("ERR timeout is not a float or out of range".to_string()));
                };
                let keys = argv[1..argv.len() - 1].to_vec();
                (keys.clone(), self.attempt_blocking_zpop(client.id, client.db_index, &keys, name == "BZPOPMIN"))
            }
            "BLMOVE" => {
                if argv.len() != 6 {
                    return BlockingOutcome::Immediate(Reply::Error("ERR wrong number of arguments for 'blmove' command".to_string()));
                }
                let Ok(_timeout) = parse_block_timeout(&argv[5]) else {
                    return BlockingOutcome::Immediate(Reply::Error("ERR timeout is not a float or out of range".to_string()));
                };
                let (Ok(from_left), Ok(to_left)) = (parse_list_side(&argv[3]), parse_list_side(&argv[4])) else {
                    return BlockingOutcome::Immediate(Reply::Error("ERR syntax error".to_string()));
                };
                let keys = vec![argv[1].clone()];
                let synth = vec![b"LMOVE".to_vec(), argv[1].clone(), argv[2].clone(), argv[3].clone(), argv[4].clone()];
                (keys.clone(), self.attempt_blocking_list_move(client.id, client.db_index, &argv[1], from_left, to_left, &synth))
            }
            "BRPOPLPUSH" => {
                if argv.len() != 4 {
                    return BlockingOutcome::Immediate(Reply::Error("ERR wrong number of arguments for 'brpoplpush' command".to_string()));
                }
                let Ok(_timeout) = parse_block_timeout(&argv[3]) else {
                    return BlockingOutcome::Immediate(Reply::Error("ERR timeout is not a float or out of range".to_string()));
                };
                let keys = vec![argv[1].clone()];
                let synth = vec![b"RPOPLPUSH".to_vec(), argv[1].clone(), argv[2].clone()];
                (keys.clone(), self.attempt_blocking_list_move(client.id, client.db_index, &argv[1], false, true, &synth))
            }
            _ => return BlockingOutcome::Immediate(Reply::Error(format!("ERR unknown command '{name}'"))),
        };
        match attempt {
            BlockAttempt::Immediate(reply) => BlockingOutcome::Immediate(reply),
            BlockAttempt::StillBlocked => {
                // Re-parse the timeout just to compute the deadline — every
                // branch above already validated it, so this can't fail.
                let timeout = parse_block_timeout(&argv[argv.len() - 1]).unwrap_or(0.0);
                let deadline_ms = if timeout == 0.0 { None } else { Some(now_ms().saturating_add((timeout * 1000.0) as i64)) };
                self.register_blocked(client.id, client.db_index, argv.to_vec(), keys, deadline_ms);
                BlockingOutcome::Blocked
            }
        }
    }

    /// `XREAD [COUNT n] BLOCK ms STREAMS key [key ...] id [id ...]` — only
    /// reached when the binary layer has already confirmed `BLOCK` is
    /// present (see `redisx-server.rs`'s `wants_block_option`); without it,
    /// `XREAD` dispatches normally through `cmd_xread` like any other
    /// command. `$` is resolved to each stream's *current* last id exactly
    /// once, here, rather than re-resolved on every retry — re-resolving it
    /// later would always see "nothing newer than right now" at the moment
    /// of the retry (the very write that woke this client just became the
    /// new last id), the same reason real Redis's `streamParseStrictIDOrReply`
    /// only ever resolves `$` at the initial call.
    fn try_blocking_xread(&mut self, client: &mut ClientState, argv: &[Vec<u8>]) -> BlockingOutcome {
        let mut i = 1;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<i64> = None;
        loop {
            if i >= argv.len() {
                return BlockingOutcome::Immediate(Reply::Error("ERR syntax error".to_string()));
            }
            match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
                "COUNT" if i + 1 < argv.len() => {
                    let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                        return BlockingOutcome::Immediate(Reply::Error("ERR value is not an integer or out of range".to_string()));
                    };
                    count = Some(n);
                    i += 2;
                }
                "BLOCK" if i + 1 < argv.len() => {
                    let Ok(ms) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                        return BlockingOutcome::Immediate(Reply::Error("ERR timeout is not an integer or out of range".to_string()));
                    };
                    block_ms = Some(ms);
                    i += 2;
                }
                "STREAMS" => {
                    i += 1;
                    break;
                }
                _ => return BlockingOutcome::Immediate(Reply::Error("ERR syntax error".to_string())),
            }
        }
        let remaining = &argv[i..];
        if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
            return BlockingOutcome::Immediate(Reply::Error(
                "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.".to_string(),
            ));
        }
        let n_streams = remaining.len() / 2;
        let keys = remaining[..n_streams].to_vec();
        let raw_ids = &remaining[n_streams..];
        let mut resolved_ids = Vec::with_capacity(n_streams);
        for (key, id_arg) in keys.iter().zip(raw_ids.iter()) {
            if id_arg.as_slice() == b"$" {
                let last = match self.dbs[client.db_index].dict.get(SLOT, key) {
                    Some(RObj::Stream(st)) => st.last_id(),
                    Some(_) => return BlockingOutcome::Immediate(Reply::Error(WRONGTYPE.to_string())),
                    None => crate::stream::StreamId::MIN,
                };
                resolved_ids.push(last.to_bytes());
            } else {
                resolved_ids.push(id_arg.clone());
            }
        }
        match self.attempt_blocking_xread(client.id, client.db_index, &keys, &resolved_ids, count) {
            BlockAttempt::Immediate(reply) => BlockingOutcome::Immediate(reply),
            BlockAttempt::StillBlocked => {
                let mut retry_argv = vec![b"XREAD".to_vec()];
                if let Some(n) = count {
                    retry_argv.push(b"COUNT".to_vec());
                    retry_argv.push(n.to_string().into_bytes());
                }
                retry_argv.push(b"STREAMS".to_vec());
                retry_argv.extend(keys.iter().cloned());
                retry_argv.extend(resolved_ids.iter().cloned());
                let deadline_ms = block_ms.unwrap_or(0);
                let deadline_ms = if deadline_ms == 0 { None } else { Some(now_ms().saturating_add(deadline_ms)) };
                self.register_blocked(client.id, client.db_index, retry_argv, keys, deadline_ms);
                BlockingOutcome::Blocked
            }
        }
    }

    /// `XREADGROUP GROUP g consumer [COUNT n] BLOCK ms [NOACK] STREAMS key
    /// [key ...] id [id ...]` — same role as `try_blocking_xread`. Unlike
    /// `XREAD`'s `$`, `XREADGROUP`'s `>` needs no one-time resolution: it
    /// always means "whatever this group hasn't delivered yet", which stays
    /// correct however many times it's re-evaluated (the group's
    /// `last_delivered_id` watermark only ever moves forward as entries
    /// actually get delivered — see `StreamGroup::deliver`).
    fn try_blocking_xreadgroup(&mut self, client: &mut ClientState, argv: &[Vec<u8>]) -> BlockingOutcome {
        if argv.len() < 4 || !argv[1].eq_ignore_ascii_case(b"GROUP") {
            return BlockingOutcome::Immediate(Reply::Error("ERR syntax error".to_string()));
        }
        let group = argv[2].clone();
        let consumer = argv[3].clone();
        let mut i = 4;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<i64> = None;
        let mut noack = false;
        loop {
            if i >= argv.len() {
                return BlockingOutcome::Immediate(Reply::Error("ERR syntax error".to_string()));
            }
            match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
                "COUNT" if i + 1 < argv.len() => {
                    let Ok(n) = ok_bytes_as_string(&argv[i + 1]).parse::<usize>() else {
                        return BlockingOutcome::Immediate(Reply::Error("ERR value is not an integer or out of range".to_string()));
                    };
                    count = Some(n);
                    i += 2;
                }
                "BLOCK" if i + 1 < argv.len() => {
                    let Ok(ms) = ok_bytes_as_string(&argv[i + 1]).parse::<i64>() else {
                        return BlockingOutcome::Immediate(Reply::Error("ERR timeout is not an integer or out of range".to_string()));
                    };
                    block_ms = Some(ms);
                    i += 2;
                }
                "NOACK" => {
                    noack = true;
                    i += 1;
                }
                "STREAMS" => {
                    i += 1;
                    break;
                }
                _ => return BlockingOutcome::Immediate(Reply::Error("ERR syntax error".to_string())),
            }
        }
        let remaining = &argv[i..];
        if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
            return BlockingOutcome::Immediate(Reply::Error(
                "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '$' must be specified.".to_string(),
            ));
        }
        let n_streams = remaining.len() / 2;
        let keys = remaining[..n_streams].to_vec();
        let ids = remaining[n_streams..].to_vec();
        match self.attempt_blocking_xreadgroup(
            client.id,
            client.db_index,
            XReadGroupAttempt { group: &group, consumer: &consumer, keys: &keys, ids: &ids, count, noack },
        ) {
            BlockAttempt::Immediate(reply) => BlockingOutcome::Immediate(reply),
            BlockAttempt::StillBlocked => {
                let mut retry_argv = vec![b"XREADGROUP".to_vec(), b"GROUP".to_vec(), group.clone(), consumer.clone()];
                if let Some(n) = count {
                    retry_argv.push(b"COUNT".to_vec());
                    retry_argv.push(n.to_string().into_bytes());
                }
                if noack {
                    retry_argv.push(b"NOACK".to_vec());
                }
                retry_argv.push(b"STREAMS".to_vec());
                retry_argv.extend(keys.iter().cloned());
                retry_argv.extend(ids.iter().cloned());
                let deadline_ms = block_ms.unwrap_or(0);
                let deadline_ms = if deadline_ms == 0 { None } else { Some(now_ms().saturating_add(deadline_ms)) };
                self.register_blocked(client.id, client.db_index, retry_argv, keys, deadline_ms);
                BlockingOutcome::Blocked
            }
        }
    }

    /// Shared by `try_blocking_xread` and its own retries — runs `XREAD`'s
    /// ordinary handler directly (it's a pure read, `is_write: false`, so
    /// there's no propagation/AOF bookkeeping to gain by going through
    /// `run_command` the way the list/zset blocking attempts do).
    fn attempt_blocking_xread(&mut self, client_id: i64, db_index: usize, keys: &[Vec<u8>], ids: &[Vec<u8>], count: Option<usize>) -> BlockAttempt {
        let mut synthetic = ClientState { db_index, id: client_id, ..ClientState::default() };
        let mut argv = vec![b"XREAD".to_vec()];
        if let Some(n) = count {
            argv.push(b"COUNT".to_vec());
            argv.push(n.to_string().into_bytes());
        }
        argv.push(b"STREAMS".to_vec());
        argv.extend(keys.iter().cloned());
        argv.extend(ids.iter().cloned());
        match cmd_xread(self, &mut synthetic, &argv) {
            Reply::NullArray => BlockAttempt::StillBlocked,
            reply => BlockAttempt::Immediate(reply),
        }
    }

    /// Shared by `try_blocking_xreadgroup` and its own retries — routed
    /// through `run_command` (unlike `attempt_blocking_xread`) because
    /// `XREADGROUP` is `is_write: true` (it mutates the group's PEL/
    /// watermark) and should get the same propagation/AOF bookkeeping a
    /// non-blocking `XREADGROUP` call already gets via ordinary dispatch.
    fn attempt_blocking_xreadgroup(&mut self, client_id: i64, db_index: usize, req: XReadGroupAttempt<'_>) -> BlockAttempt {
        let mut synthetic = ClientState { db_index, id: client_id, ..ClientState::default() };
        let mut argv = vec![b"XREADGROUP".to_vec(), b"GROUP".to_vec(), req.group.to_vec(), req.consumer.to_vec()];
        if let Some(n) = req.count {
            argv.push(b"COUNT".to_vec());
            argv.push(n.to_string().into_bytes());
        }
        if req.noack {
            argv.push(b"NOACK".to_vec());
        }
        argv.push(b"STREAMS".to_vec());
        argv.extend(req.keys.iter().cloned());
        argv.extend(req.ids.iter().cloned());
        let spec = find_command("XREADGROUP").unwrap();
        match self.run_command(&mut synthetic, spec, &argv) {
            Reply::NullArray => BlockAttempt::StillBlocked,
            reply => BlockAttempt::Immediate(reply),
        }
    }

    /// A key encountered mid-scan that exists but is the wrong type always
    /// errors immediately (matching real Redis: `BLPOP` on an existing
    /// string is a `WRONGTYPE` error, not "keep waiting") — this also runs
    /// on every *retry*, not just the first attempt, so a key that was a
    /// list when a client started blocking and got overwritten with some
    /// other type by an unrelated write in the meantime is reported as an
    /// error rather than left blocking forever; real Redis instead simply
    /// never re-examines a blocked client except when a type-appropriate
    /// push actually happens, so it would keep that client waiting — a
    /// deliberate, documented difference, not an oversight.
    fn attempt_blocking_list_pop(&mut self, client_id: i64, db_index: usize, keys: &[Vec<u8>], from_left: bool) -> BlockAttempt {
        let pop_name = if from_left { "LPOP" } else { "RPOP" };
        let pop_spec = find_command(pop_name).unwrap();
        let mut synthetic = ClientState { db_index, id: client_id, ..ClientState::default() };
        for key in keys {
            match self.dbs[db_index].dict.get(SLOT, key) {
                Some(RObj::List(l)) if !l.is_empty() => {
                    let synth = vec![pop_name.as_bytes().to_vec(), key.clone()];
                    if let Reply::BulkString(val) = self.run_command(&mut synthetic, pop_spec, &synth) {
                        return BlockAttempt::Immediate(Reply::Array(vec![Reply::bulk(key.clone()), Reply::bulk(val)]));
                    }
                }
                Some(RObj::List(_)) | None => {}
                Some(_) => return BlockAttempt::Immediate(Reply::Error(WRONGTYPE.to_string())),
            }
        }
        BlockAttempt::StillBlocked
    }

    /// See `attempt_blocking_list_pop`'s doc comment for the WRONGTYPE-on-
    /// any-retry design note — identical here.
    fn attempt_blocking_zpop(&mut self, client_id: i64, db_index: usize, keys: &[Vec<u8>], from_min: bool) -> BlockAttempt {
        let pop_name = if from_min { "ZPOPMIN" } else { "ZPOPMAX" };
        let pop_spec = find_command(pop_name).unwrap();
        let mut synthetic = ClientState { db_index, id: client_id, ..ClientState::default() };
        for key in keys {
            match self.dbs[db_index].dict.get(SLOT, key) {
                Some(RObj::ZSet(z)) if !z.is_empty() => {
                    let synth = vec![pop_name.as_bytes().to_vec(), key.clone()];
                    if let Reply::Array(items) = self.run_command(&mut synthetic, pop_spec, &synth) {
                        if !items.is_empty() {
                            let mut out = vec![Reply::bulk(key.clone())];
                            out.extend(items);
                            return BlockAttempt::Immediate(Reply::Array(out));
                        }
                    }
                }
                Some(RObj::ZSet(_)) | None => {}
                Some(_) => return BlockAttempt::Immediate(Reply::Error(WRONGTYPE.to_string())),
            }
        }
        BlockAttempt::StillBlocked
    }

    /// Shared by `BLMOVE`/`BRPOPLPUSH` — `synth` is the fully-formed
    /// non-blocking `LMOVE`/`RPOPLPUSH` argv to retry with once `source`
    /// looks non-empty. See `attempt_blocking_list_pop`'s doc comment for
    /// the WRONGTYPE-on-any-retry design note.
    fn attempt_blocking_list_move(&mut self, client_id: i64, db_index: usize, source: &Vec<u8>, _from_left: bool, _to_left: bool, synth: &[Vec<u8>]) -> BlockAttempt {
        let mut synthetic = ClientState { db_index, id: client_id, ..ClientState::default() };
        match self.dbs[db_index].dict.get(SLOT, source) {
            Some(RObj::List(l)) if !l.is_empty() => {
                let spec = find_command(&ok_bytes_as_string(&synth[0])).unwrap();
                let reply = self.run_command(&mut synthetic, spec, synth);
                if !matches!(reply, Reply::Null | Reply::NullBulkString) {
                    return BlockAttempt::Immediate(reply);
                }
                BlockAttempt::StillBlocked
            }
            Some(RObj::List(_)) | None => BlockAttempt::StillBlocked,
            Some(_) => BlockAttempt::Immediate(Reply::Error(WRONGTYPE.to_string())),
        }
    }

    /// Re-attempts exactly one blocked client's stashed command — used both
    /// by `maybe_unblock_waiters` (triggered by a write) and could be reused
    /// by a future periodic sweep; today only the former calls it. Parses
    /// `argv[0]` fresh each time rather than caching a closure, since the
    /// full argv is already sitting right there in `BlockedClient`.
    fn retry_blocking_attempt(&mut self, client_id: i64, db_index: usize, argv: &[Vec<u8>]) -> BlockAttempt {
        let name = ok_bytes_as_string(&argv[0]).to_ascii_uppercase();
        match name.as_str() {
            "BLPOP" | "BRPOP" => {
                let keys = &argv[1..argv.len() - 1];
                self.attempt_blocking_list_pop(client_id, db_index, keys, name == "BLPOP")
            }
            "BZPOPMIN" | "BZPOPMAX" => {
                let keys = &argv[1..argv.len() - 1];
                self.attempt_blocking_zpop(client_id, db_index, keys, name == "BZPOPMIN")
            }
            "BLMOVE" => {
                let (from_left, to_left) = (parse_list_side(&argv[3]).unwrap_or(true), parse_list_side(&argv[4]).unwrap_or(true));
                let synth = vec![b"LMOVE".to_vec(), argv[1].clone(), argv[2].clone(), argv[3].clone(), argv[4].clone()];
                self.attempt_blocking_list_move(client_id, db_index, &argv[1], from_left, to_left, &synth)
            }
            "BRPOPLPUSH" => {
                let synth = vec![b"RPOPLPUSH".to_vec(), argv[1].clone(), argv[2].clone()];
                self.attempt_blocking_list_move(client_id, db_index, &argv[1], false, true, &synth)
            }
            "XREAD" => {
                // Stored by `try_blocking_xread` as `["XREAD", maybe "COUNT"
                // n,] "STREAMS", key..., id...` — `$` already resolved, so
                // this is just re-running the same parse.
                let mut i = 1;
                let mut count = None;
                if argv[i].eq_ignore_ascii_case(b"COUNT") {
                    count = ok_bytes_as_string(&argv[i + 1]).parse().ok();
                    i += 2;
                }
                i += 1; // "STREAMS"
                let remaining = &argv[i..];
                let n = remaining.len() / 2;
                self.attempt_blocking_xread(client_id, db_index, &remaining[..n], &remaining[n..], count)
            }
            "XREADGROUP" => {
                // Stored by `try_blocking_xreadgroup` as `["XREADGROUP",
                // "GROUP", group, consumer, maybe "COUNT" n, maybe "NOACK",]
                // "STREAMS", key..., id...`.
                let (group, consumer) = (argv[2].clone(), argv[3].clone());
                let mut i = 4;
                let mut count = None;
                let mut noack = false;
                loop {
                    match ok_bytes_as_string(&argv[i]).to_ascii_uppercase().as_str() {
                        "COUNT" => {
                            count = ok_bytes_as_string(&argv[i + 1]).parse().ok();
                            i += 2;
                        }
                        "NOACK" => {
                            noack = true;
                            i += 1;
                        }
                        _ => {
                            i += 1; // "STREAMS"
                            break;
                        }
                    }
                }
                let remaining = &argv[i..];
                let n = remaining.len() / 2;
                self.attempt_blocking_xreadgroup(
                    client_id,
                    db_index,
                    XReadGroupAttempt { group: &group, consumer: &consumer, keys: &remaining[..n], ids: &remaining[n..], count, noack },
                )
            }
            _ => BlockAttempt::StillBlocked,
        }
    }

    fn register_blocked(&mut self, client_id: i64, db_index: usize, argv: Vec<Vec<u8>>, keys: Vec<Vec<u8>>, deadline_ms: Option<i64>) {
        for key in &keys {
            self.blocking_keys.entry((db_index, key.clone())).or_default().push(client_id);
        }
        self.blocked.insert(client_id, BlockedClient { db_index, argv, keys, deadline_ms });
    }

    /// Removes a client from both `blocked` and every `blocking_keys` entry
    /// it was registered under — called once it's either satisfied, timed
    /// out, or its connection closed.
    fn unregister_blocked(&mut self, client_id: i64) {
        if let Some(entry) = self.blocked.remove(&client_id) {
            for key in &entry.keys {
                if let Some(ids) = self.blocking_keys.get_mut(&(entry.db_index, key.clone())) {
                    ids.retain(|&id| id != client_id);
                    if ids.is_empty() {
                        self.blocking_keys.remove(&(entry.db_index, key.clone()));
                    }
                }
            }
        }
    }

    /// Called right after any successful write to `(db_index, key)`: wakes
    /// every client blocked on `key`, oldest-registered first (real Redis's
    /// own FIFO fairness guarantee), stopping at the first one that's still
    /// not satisfiable — a later waiter never jumps the queue ahead of an
    /// earlier one just because the data it specifically wants showed up
    /// first. A satisfied client's reply lands in `unblocked` for the binary
    /// layer to deliver; since `retry_blocking_attempt` runs the retry
    /// through `run_command` too, a chain reaction (this write wakes a
    /// client whose own retry writes somewhere else, waking a third) is
    /// handled for free by the same recursion.
    ///
    /// **Unregisters the client *before* retrying, not after** — the retry
    /// itself runs through `run_command` and can trigger this very function
    /// again re-entrantly (e.g. `BRPOPLPUSH`'s retry is a real `RPOPLPUSH`
    /// write, which fires this same post-write hook for its own source key).
    /// Unregistering first means that re-entrant call can no longer find
    /// this client still "blocked", so it can't double-serve it — the bug
    /// this comment exists to warn against reintroducing.
    fn maybe_unblock_waiters(&mut self, db_index: usize, key: &[u8]) {
        loop {
            let Some(&client_id) = self.blocking_keys.get(&(db_index, key.to_vec())).and_then(|ids| ids.first()) else { return };
            let Some(entry) = self.blocked.get(&client_id) else {
                // Defensive: `blocked`/`blocking_keys` should always agree;
                // if they don't, drop the stale id and keep going rather
                // than looping forever on it.
                if let Some(ids) = self.blocking_keys.get_mut(&(db_index, key.to_vec())) {
                    ids.remove(0);
                }
                continue;
            };
            let argv = entry.argv.clone();
            let entry_db = entry.db_index;
            let entry_keys = entry.keys.clone();
            let deadline_ms = entry.deadline_ms;
            self.unregister_blocked(client_id);
            match self.retry_blocking_attempt(client_id, entry_db, &argv) {
                BlockAttempt::Immediate(reply) => {
                    self.unblocked.push((client_id, reply));
                }
                BlockAttempt::StillBlocked => {
                    // Shouldn't actually happen (this function only runs
                    // right after a write to `key`, and the client is
                    // registered under `key` specifically, so its retry
                    // must see that write) — but if it ever does, put the
                    // registration back rather than silently dropping a
                    // still-waiting client.
                    self.register_blocked(client_id, entry_db, argv, entry_keys, deadline_ms);
                    return;
                }
            }
        }
    }

    /// Called periodically by the binary layer (a recurring `ae` time event
    /// — see its own module for the cadence) to time out any blocked client
    /// whose deadline has passed. The timeout reply shape depends on which
    /// command is waiting: `BLMOVE`/`BRPOPLPUSH` reply with a null *bulk
    /// string* on timeout (their non-blocking success reply is a bulk
    /// string), everything else with a null *array*.
    pub fn check_blocking_timeouts(&mut self, now_ms: i64) {
        let expired: Vec<(i64, Vec<u8>)> =
            self.blocked.iter().filter(|(_, b)| b.deadline_ms.is_some_and(|d| d <= now_ms)).map(|(&id, b)| (id, b.argv[0].clone())).collect();
        for (client_id, cmd_name) in expired {
            self.unregister_blocked(client_id);
            let name = String::from_utf8_lossy(&cmd_name).to_ascii_uppercase();
            let reply = if matches!(name.as_str(), "BLMOVE" | "BRPOPLPUSH") { Reply::NullBulkString } else { Reply::NullArray };
            self.unblocked.push((client_id, reply));
        }
    }

    /// `call()`'s shared "run the handler, propagate/AOF if it was a
    /// successful write, SLOWLOG it if it was slow" tail — used by every
    /// execution path (`dispatch`'s non-queued branch, `execute_single`,
    /// `apply_replicated_command`) so timing/propagation/slowlog logic lives
    /// in exactly one place instead of three near-identical copies.
    fn run_command(&mut self, client: &mut ClientState, spec: &'static CommandSpec, argv: &[Vec<u8>]) -> Reply {
        self.sweep_expired(client);
        self.maybe_run_active_defrag();
        self.total_commands_processed += 1;
        if spec.is_write && command_denies_oom(spec.name) {
            if let Some(oom_reply) = self.enforce_maxmemory(client) {
                return oom_reply;
            }
        }
        let start = std::time::Instant::now();
        let reply = (spec.handler)(self, client, argv);
        let duration_us = start.elapsed().as_micros() as i64;

        if !matches!(reply, Reply::Error(_)) {
            if let Some(key) = tracked_key(argv) {
                self.touch_access(client.db_index, key);
            }
            if spec.is_write {
                self.dirty_since_save += 1;
                self.after_write(client, argv);
                self.invalidate_tracking_for_write(argv, client.id);
                if let Some(key) = tracked_key(argv) {
                    self.touch_watched_key(client.db_index, key);
                    self.maybe_unblock_waiters(client.db_index, key);
                }
            } else if client.tracking {
                self.record_tracking_read(client.id, argv);
            }
        }
        self.maybe_log_slow(client, argv, duration_us);
        reply
    }

    /// How long `used_memory_cached` trusts its last full-scan reading
    /// before taking a fresh one — see that method's own doc for the whole
    /// rationale.
    const USED_MEMORY_CACHE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    /// `enforce_maxmemory`'s "are we even close to the limit" gate — a
    /// throttled cache around `used_memory()` rather than a fresh full
    /// keyspace scan on every call. Real Redis's own `freeMemoryIfNeeded`
    /// gets this cheaply because jemalloc tracks a running atomic counter;
    /// this fork has no such counter (no jemalloc, `used_memory()` is a
    /// content walk over every key in every db), so without this cache,
    /// *every single* memory-growing write command — `enforce_maxmemory`
    /// runs before each one — would cost `O(total keys across all dbs)`,
    /// regardless of whether `maxmemory` is anywhere close to being hit.
    /// That's the actual latency problem a large keyspace under `maxmemory`
    /// would otherwise hit; this is what fixes it.
    ///
    /// Staleness is bounded, not unbounded: a cached reading can be up to
    /// `USED_MEMORY_CACHE_INTERVAL` (currently 50ms) old, so a write that
    /// actually pushes usage over `maxmemory` can still be let through
    /// without eviction for up to that long — the same kind of bounded-
    /// latency tradeoff a *soft* memory limit already implies, not a new
    /// correctness gap this fork invented. What the bound guarantees is
    /// recovery, not instant detection: once a cached reading's age exceeds
    /// the interval, the very next call recomputes it for real (`used_
    /// memory(self)`, an exact full scan) rather than trusting stale data
    /// any further — so a breach is never missed for longer than one
    /// interval, and once `enforce_maxmemory` *does* see a reading over the
    /// limit (whether from a fresh cache miss or this fast-path check), it
    /// always re-measures exactly before evicting a single key, so eviction
    /// itself never acts on a stale number.
    fn used_memory_cached(&mut self) -> usize {
        let now = std::time::Instant::now();
        if let Some((at, value)) = self.used_memory_cache {
            if now.duration_since(at) < Self::USED_MEMORY_CACHE_INTERVAL {
                return value;
            }
        }
        let value = used_memory(self);
        self.used_memory_cache = Some((now, value));
        value
    }

    /// `maxmemory`'s enforcement point, run once per write command that can
    /// *grow* memory usage (`command_denies_oom`), before its handler runs —
    /// real Redis's `freeMemoryIfNeededAndSafe()` call at the top of
    /// `processCommand()`. `maxmemory <= 0` (including unset/unparseable,
    /// via `ConfigStore::get_bytes`) means "unlimited", matching real Redis's
    /// own `0` sentinel. Returns `Some(reply)` to short-circuit the caller
    /// with that reply instead of running the handler at all — either the
    /// real `-OOM` error (`noeviction`, or every policy ran out of eligible
    /// keys), or `None` once enough has been freed to proceed.
    ///
    /// Once actually evicting, scans each db's eligible keys **exactly
    /// once** (`collect_eviction_order`) rather than rescanning per evicted
    /// key, and tracks freed bytes with a running counter instead of
    /// recomputing `used_memory()` from scratch after each removal — see
    /// `collect_eviction_order`'s own doc for why the old per-key-rescan
    /// design didn't scale, and `used_memory_cached`'s for the fast-path
    /// half of this same latency fix.
    fn enforce_maxmemory(&mut self, client: &ClientState) -> Option<Reply> {
        let limit = self.config.get_bytes("maxmemory").unwrap_or(0);
        if limit == 0 || self.used_memory_cached() as u64 <= limit {
            return None;
        }
        let policy = self.config.get_str("maxmemory-policy").unwrap_or("noeviction").to_ascii_lowercase();
        if policy == "noeviction" {
            return Some(oom_error());
        }
        // The cache that got us here may be stale — confirm with an exact
        // reading before evicting anything for real.
        let mut usage = used_memory(self) as i64;
        self.used_memory_cache = Some((std::time::Instant::now(), usage.max(0) as usize));
        'dbs: for dbidx in 0..self.dbs.len() {
            if usage.max(0) as u64 <= limit {
                break;
            }
            for key in collect_eviction_order(&self.dbs[dbidx], &policy) {
                if usage.max(0) as u64 <= limit {
                    break 'dbs;
                }
                let Some(obj) = self.dbs[dbidx].remove_key(&key) else { continue };
                usage -= (key.len() + estimate_robj_content_size(&obj) + 16) as i64;
                self.lazyfree_if_enabled("lazyfree-lazy-eviction", LazyFreeJob::Value(obj));
                notify_keyspace_event(self, dbidx, 'e', "evicted", &key);
                self.touch_watched_key(dbidx, &key);
                if matches!(self.role, Role::Master) {
                    let del_argv = vec![b"DEL".to_vec(), key];
                    self.propagate(dbidx, &del_argv);
                    if crate::aof::is_enabled(&self.config) {
                        let _ = crate::aof::append(&self.config, dbidx, &client.peer, &del_argv);
                    }
                }
            }
        }
        self.used_memory_cache = Some((std::time::Instant::now(), usage.max(0) as usize));
        if usage.max(0) as u64 <= limit { None } else { Some(oom_error()) }
    }

    /// Bumps `AccessInfo` on every successful command (read or write alike)
    /// that targets a key — `lookupKeyRead`/`lookupKeyWrite`'s shared
    /// `updateLFU`/LRU-stamp call in real Redis, which doesn't distinguish
    /// reads from writes either: an `allkeys-lru` policy needs "last looked
    /// at", not "last modified". A brand-new key (no existing `AccessInfo`
    /// entry yet) gets `AccessInfo::new` instead — see its own doc comment
    /// for why that's deliberately not folded into the decay+increment path.
    fn touch_access(&mut self, db_index: usize, key: &[u8]) {
        if self.dbs[db_index].dict.get(SLOT, &key.to_vec()).is_none() {
            return;
        }
        let now = now_ms();
        let lfu_log_factor = self.config.get_int("lfu-log-factor").unwrap_or(10) as f64;
        let lfu_decay_time = self.config.get_int("lfu-decay-time").unwrap_or(1);
        let db = &mut self.dbs[db_index];
        match db.access.get_mut(key) {
            Some(entry) => {
                entry.freq = lfu_decay(entry.freq, entry.idle_since_ms, now, lfu_decay_time);
                entry.freq = lfu_log_incr(entry.freq, lfu_log_factor);
                entry.idle_since_ms = now;
            }
            None => {
                db.access.insert(key.to_vec(), AccessInfo::new(now));
            }
        }
    }

    /// How often `maybe_run_active_defrag` is willing to even check
    /// `mem_fragmentation_ratio` — see `Server::last_defrag_check`'s own doc
    /// on why this needs throttling independent of `DEFRAG_STEP_BUDGET`.
    const DEFRAG_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
    /// How many keys one `defrag_step` call touches per db per check — bounds
    /// the latency this adds to whatever command happened to trigger a check,
    /// the same "small bounded unit of work per call, resumable via cursor"
    /// shape real Redis's own cron-driven `activeDefragCycle` has (just
    /// triggered inline from command processing instead of a cron thread, the
    /// same substitution `sweep_expired` already makes for active expiry).
    const DEFRAG_STEP_BUDGET: usize = 200;

    /// `activeDefragCycle`'s redisX equivalent — a no-op unless `activedefrag
    /// yes` is configured, and even then throttled to at most one real check
    /// per `DEFRAG_CHECK_INTERVAL` (see that constant's doc: the check itself
    /// calls `rss_bytes()`, not free on every platform). Once `mem_
    /// fragmentation_ratio` is confirmed to be at or past `active-defrag-
    /// threshold`, runs one bounded `defrag_step` per db — not a full pass —
    /// so no single command absorbs the cost of defragging a huge keyspace in
    /// one call; repeated commands make steady progress instead (`MEMORY
    /// PURGE`/`force_full_defrag` is the "no, do it all right now" escape
    /// hatch for when that's actually what's wanted).
    fn maybe_run_active_defrag(&mut self) {
        if self.config.get_str("activedefrag") != Some("yes") {
            return;
        }
        let now = std::time::Instant::now();
        if let Some(last) = self.last_defrag_check {
            if now.duration_since(last) < Self::DEFRAG_CHECK_INTERVAL {
                return;
            }
        }
        self.last_defrag_check = Some(now);
        let threshold = self.config.get_str("active-defrag-threshold").and_then(|s| s.parse::<f64>().ok()).unwrap_or(1.5);
        let Some(ratio) = mem_fragmentation_ratio(self) else { return };
        if ratio < threshold {
            return;
        }
        for dbidx in 0..self.dbs.len() {
            let cursor = self.defrag_cursors[dbidx];
            self.defrag_cursors[dbidx] = defrag_step(&mut self.dbs[dbidx], cursor, Self::DEFRAG_STEP_BUDGET);
        }
    }

    /// `CLIENT TRACKING ON` bookkeeping: remembers that `client_id` just read
    /// `tracked_key(argv)`, so a later write to the same key invalidates it
    /// for them.
    fn record_tracking_read(&mut self, client_id: i64, argv: &[Vec<u8>]) {
        if let Some(key) = tracked_key(argv) {
            self.tracking_table.entry(key.clone()).or_default().insert(client_id);
        }
    }

    /// Pushes an `invalidate` message (RESP3 push, matching real
    /// `trackingInvalidateKey`) to every client tracking the key this write
    /// just touched — both plain (non-`BCAST`) readers of this exact key
    /// (`tracking_table`, cleared as they're notified: they're no longer
    /// caching a valid copy, so there's nothing left to invalidate next
    /// time until they read it again) and every `BCAST` client whose prefix
    /// list matches it (never cleared: `BCAST` watches the *pattern*, not a
    /// one-shot read). `REDIRECT`-ed targets get the push instead of the
    /// tracking client itself; `NOLOOP` clients don't get a push for their
    /// own write.
    fn invalidate_tracking_for_write(&mut self, argv: &[Vec<u8>], writer_id: i64) {
        let Some(key) = tracked_key(argv) else { return };
        let mut targets: std::collections::HashSet<i64> = self.tracking_table.remove(key).unwrap_or_default();
        for (&id, prefixes) in &self.bcast_tracking {
            if prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p.as_slice())) {
                targets.insert(id);
            }
        }
        for id in targets {
            if id == writer_id && self.tracking_noloop.contains(&writer_id) {
                continue;
            }
            let deliver_to = self.tracking_redirect.get(&id).copied().unwrap_or(id);
            self.pending_pushes.push((deliver_to, Reply::Push(vec![Reply::bulk(b"invalidate".to_vec()), Reply::Array(vec![Reply::bulk(key.clone())])])));
        }
    }

    /// Bumps `(db_index, key)`'s write counter — see `watched_key_versions`'s
    /// doc comment. Called for every successful write command (keyed off the
    /// same `tracked_key` heuristic `CLIENT TRACKING` already relies on) and,
    /// directly with an exact key, from the active-expire sweep below.
    pub(crate) fn touch_watched_key(&mut self, db_index: usize, key: &[u8]) {
        *self.watched_key_versions.entry((db_index, key.to_vec())).or_insert(0) += 1;
    }

    /// `activeExpireCycle`, simplified to a synchronous full-scan sweep of
    /// just the command's target db, run before every single command
    /// (`run_command`) rather than real Redis's periodic cron pass over a
    /// random key sample — correctness-first, the same tradeoff Phase 0 made
    /// replacing the C fork's Fenwick-tree "fair random pick" with a plain
    /// linear scan. The `expires` map is normally empty or tiny, so this
    /// costs nothing in the common case.
    ///
    /// Only a master propagates the resulting synthetic `DEL`s to
    /// replicas/AOF; a replica still physically removes its own
    /// locally-expired keys too (simpler than real Redis's "mask on read,
    /// wait for the master's own `DEL`" split-brain-avoidance dance), relying
    /// on the master's `DEL` to arrive later and harmlessly no-op against an
    /// already-gone key.
    ///
    /// A full no-op while `DEBUG SET-ACTIVE-EXPIRE 0` is in effect
    /// (`active_expire_enabled`) — see that field's own doc for why that
    /// disables TTL enforcement entirely in this fork, not just the
    /// background sweep real Redis's own flag only suspends.
    fn sweep_expired(&mut self, client: &ClientState) {
        if !self.active_expire_enabled {
            return;
        }
        let dbidx = client.db_index;
        let now = now_ms();
        let expired: Vec<Vec<u8>> =
            self.dbs[dbidx].expires.iter().filter(|(_, &at)| at <= now).map(|(k, _)| k.clone()).collect();
        for key in expired {
            if let Some(obj) = self.dbs[dbidx].remove_key(&key) {
                self.lazyfree_if_enabled("lazyfree-lazy-expire", LazyFreeJob::Value(obj));
            }
            notify_keyspace_event(self, dbidx, 'x', "expired", &key);
            self.touch_watched_key(dbidx, &key);
            if matches!(self.role, Role::Master) {
                self.after_write(client, &[b"DEL".to_vec(), key]);
            }
        }
    }

    /// `slowlogPushEntryIfNeeded`'s threshold check — the write itself
    /// (`slowlog.rs`'s `write_entry`) only knows how to format and append a
    /// line, not whether this particular call qualifies as slow.
    fn maybe_log_slow(&mut self, client: &ClientState, argv: &[Vec<u8>], duration_us: i64) {
        let threshold = self.config.get_int("slowlog-log-slower-than").unwrap_or(10_000);
        if threshold < 0 || duration_us < threshold {
            return;
        }
        crate::slowlog::write_entry(&self.config, &client.peer, argv, duration_us);
    }

    /// `loadDataFromDisk()`'s RDB branch — called once at startup. Returns
    /// `Ok(true)` if a dump file existed and was loaded, `Ok(false)` if there
    /// simply wasn't one yet (a fresh install), matching real Redis's
    /// `errno != ENOENT` distinction.
    pub fn load_rdb_if_present(&mut self) -> Result<bool, String> {
        let path = rdb_path(&self.config);
        match std::fs::read(&path) {
            Ok(data) => {
                crate::rdb::load(self, &data)?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("{}: {e}", path.display())),
        }
    }

    /// processCommand()'s dispatch decision: queue it if inside MULTI (unless
    /// exempt), otherwise run it now.
    pub fn dispatch(&mut self, client: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
        if argv.is_empty() {
            return Reply::Error("ERR empty command".to_string());
        }
        let name = ok_bytes_as_string(&argv[0]).to_ascii_uppercase();

        let Some(spec) = find_command(&name) else {
            if client.in_multi {
                client.dirty_exec = true;
            }
            return Reply::Error(format!("ERR unknown command '{name}'"));
        };
        if !check_arity(spec, argv.len()) {
            if client.in_multi {
                client.dirty_exec = true;
            }
            return Reply::Error(format!("ERR wrong number of arguments for '{}' command", name.to_ascii_lowercase()));
        }

        if requires_auth_fn(self, client) && !matches!(name.as_str(), "AUTH" | "HELLO" | "QUIT" | "RESET") {
            return Reply::Error("NOAUTH Authentication required.".to_string());
        }

        if let Some(err) = check_acl_permission(self, client, &name, argv) {
            return err;
        }

        if let Some(err) = check_cluster_redirect(self, client, &name, argv, spec.is_write) {
            return err;
        }

        if client.in_multi && !bypasses_queueing(&name) {
            client.queued.push(argv.to_vec());
            return Reply::SimpleString("QUEUED".to_string());
        }

        self.run_command(client, spec, argv)
    }

    /// Runs one previously-queued command (`execCommand`'s inner loop),
    /// bypassing the queueing decision entirely since `client.in_multi` is
    /// already false by the time EXEC calls this.
    fn execute_single(&mut self, client: &mut ClientState, argv: &[Vec<u8>]) -> Reply {
        let name = ok_bytes_as_string(&argv[0]).to_ascii_uppercase();
        match find_command(&name) {
            Some(spec) if check_arity(spec, argv.len()) => self.run_command(client, spec, argv),
            _ => Reply::Error(format!("ERR unknown command or wrong arity for '{name}'")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn ping_and_echo() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["PING"])), Reply::SimpleString("PONG".into()));
        assert_eq!(s.dispatch(&mut c, &argv(&["ECHO", "hi"])), Reply::bulk(b"hi".to_vec()));
    }

    #[test]
    fn set_get_del_exists_type() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["TYPE", "k"])), Reply::SimpleString("string".into()));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k", "missing"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["DEL", "k"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);
    }

    #[test]
    fn incr_creates_and_increments_and_rejects_non_numeric() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["INCR", "counter"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["INCR", "counter"])), Reply::Integer(2));
        s.dispatch(&mut c, &argv(&["SET", "notnum", "abc"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["INCR", "notnum"])), Reply::Error(_)));
    }

    #[test]
    fn unknown_command_errors() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["NOPE"])), Reply::Error(_)));
    }

    #[test]
    fn wrong_arity_errors() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["GET"])), Reply::Error(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["SET", "onlykey"])), Reply::Error(_)));
    }

    #[test]
    fn config_get_and_set() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        match s.dispatch(&mut c, &argv(&["CONFIG", "GET", "maxmemory"])) {
            Reply::Map(pairs) => assert_eq!(pairs, vec![(Reply::bulk(b"maxmemory".to_vec()), Reply::bulk(b"0".to_vec()))]),
            other => panic!("expected Map, got {other:?}"),
        }
        assert_eq!(s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", "100mb"])), Reply::ok());
        match s.dispatch(&mut c, &argv(&["CONFIG", "GET", "maxmemory"])) {
            Reply::Map(pairs) => assert_eq!(pairs[0].1, Reply::bulk(b"100mb".to_vec())),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn config_set_rejects_an_unknown_maxmemory_policy() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "bogus"])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])), Reply::ok());
        // case-insensitive, matching every other value this config store accepts
        assert_eq!(s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "ALLKEYS-LFU"])), Reply::ok());
    }

    #[test]
    fn noeviction_rejects_oom_commands_but_still_allows_shrinking_writes() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", "1"]));
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]));
        match s.dispatch(&mut c, &argv(&["SET", "k2", "v2"])) {
            Reply::Error(e) => assert!(e.contains("OOM"), "expected an OOM error, got {e:?}"),
            other => panic!("expected Error, got {other:?}"),
        }
        // DEL must still work even while stuck over the limit under noeviction —
        // otherwise an operator has no escape hatch at all.
        assert_eq!(s.dispatch(&mut c, &argv(&["DEL", "k"])), Reply::Integer(1));
    }

    #[test]
    fn allkeys_random_evicts_until_back_under_the_limit() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        for i in 0..20 {
            s.dispatch(&mut c, &argv(&["SET", &format!("k{i}"), "v"]));
        }
        let before_count = s.dbs[0].dict.get_dict(SLOT).map(|d| d.len()).unwrap_or(0);
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"]));
        let limit = used_memory(&s) / 2;
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", &limit.to_string()]));
        // Any further growing write must trigger eviction down to (at or under) the limit.
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "trigger", "v"])), Reply::ok());
        assert!(used_memory(&s) <= limit + "trigger".len() + "v".len() + 16, "expected eviction to bring usage back down, got {}", used_memory(&s));
        let after_count = s.dbs[0].dict.get_dict(SLOT).map(|d| d.len()).unwrap_or(0);
        assert!(after_count < before_count + 1, "expected some keys to have been evicted, had {before_count} before and {after_count} after (plus the trigger write)");
    }

    #[test]
    fn volatile_ttl_only_evicts_keys_with_an_expiry() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "persistent", "v"]));
        s.dispatch(&mut c, &argv(&["SET", "expiring", "v"]));
        s.dispatch(&mut c, &argv(&["EXPIRE", "expiring", "1000"]));
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"]));
        let limit = used_memory(&s) - 1;
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", &limit.to_string()]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "trigger", "v"])), Reply::ok());
        // the only TTL'd key must be the one that got evicted, not the persistent one
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "expiring"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "persistent"])), Reply::Integer(1));
    }

    #[test]
    fn volatile_ttl_errors_once_no_volatile_keys_remain() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "persistent", "v"]));
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"]));
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", "1"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["SET", "trigger", "v"])), Reply::Error(_)));
    }

    #[test]
    fn a_single_enforce_maxmemory_call_evicts_in_correct_lru_order_across_many_keys() {
        // Regression guard for `collect_eviction_order`: one call that needs
        // to evict *many* keys in the same pass (not just one) must still
        // consume them in the right order, not just get the first pick right.
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        for i in 0..200 {
            s.dispatch(&mut c, &argv(&["SET", &format!("k{i}"), "vvvvvvvvvv"]));
        }
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]));
        // Touch the most-recently-written half so they're clearly newer than
        // the untouched half, then force eviction down to a tiny budget that
        // only the touched half can possibly survive.
        for i in 100..200 {
            s.dispatch(&mut c, &argv(&["GET", &format!("k{i}")]));
        }
        let survivors_budget = used_memory(&s) / 3;
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", &survivors_budget.to_string()]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "trigger", "v"])), Reply::ok());

        let untouched_survivors = (0..100).filter(|i| s.dispatch(&mut c, &argv(&["EXISTS", &format!("k{i}")])) == Reply::Integer(1)).count();
        let touched_survivors = (100..200).filter(|i| s.dispatch(&mut c, &argv(&["EXISTS", &format!("k{i}")])) == Reply::Integer(1)).count();
        assert!(touched_survivors >= untouched_survivors, "recently-touched keys must survive eviction preferentially over untouched ones (touched: {touched_survivors}, untouched: {untouched_survivors})");
        assert!(used_memory(&s) as u64 <= survivors_budget as u64 + "trigger".len() as u64 + "v".len() as u64 + 16, "usage must actually be back under the limit");
    }

    #[test]
    fn enforce_maxmemory_stays_fast_evicting_many_keys_from_a_large_keyspace() {
        // Before `collect_eviction_order` replaced the old "rescan the whole
        // db once per evicted key" design, evicting K keys out of N cost
        // O(N*K) — forcing a large cache down to near-empty would have taken
        // seconds here, not milliseconds. This is a regression guard, not a
        // tight benchmark: a generous bound that the old design would have
        // blown through by orders of magnitude, not a precise timing target.
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        for i in 0..5000 {
            s.dispatch(&mut c, &argv(&["SET", &format!("key:{i}"), "some-moderately-sized-value-padding"]));
        }
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"]));
        // Force eviction of nearly everything in one call.
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", "100"]));
        let start = std::time::Instant::now();
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "trigger", "v"])), Reply::ok());
        let elapsed = start.elapsed();
        assert!(elapsed < std::time::Duration::from_millis(500), "evicting most of a 5000-key db took {elapsed:?}, expected well under 500ms");
        assert!(s.dbs[0].dict.get_dict(SLOT).map(|d| d.len()).unwrap_or(0) < 100, "almost everything should have been evicted");
    }

    #[test]
    fn maxmemory_zero_means_unlimited() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]));
        for i in 0..50 {
            assert_eq!(s.dispatch(&mut c, &argv(&["SET", &format!("k{i}"), "v"])), Reply::ok());
        }
    }

    #[test]
    fn used_memory_cache_only_delays_catching_a_breach_by_one_interval_not_forever() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory", "1"]));
        // Force a wrong, stale-low cached reading (as if an old scan had
        // seen `0` bytes used), but backdate its timestamp past `USED_
        // MEMORY_CACHE_INTERVAL` — this is the realistic case `used_memory_
        // cached` is actually built to handle: once a cached reading is old
        // enough, the very next lookup recomputes it for real rather than
        // trusting the stale value, so a breach is never missed for longer
        // than one interval. (A cache that's both wrong *and* still fresh
        // would indeed be trusted — that's the documented, bounded staleness
        // window, not a bug; it is not what this test is about.)
        let stale_at = std::time::Instant::now() - Server::USED_MEMORY_CACHE_INTERVAL - std::time::Duration::from_millis(1);
        s.used_memory_cache = Some((stale_at, 0));
        match s.dispatch(&mut c, &argv(&["SET", "k2", "v2"])) {
            Reply::Error(e) => assert!(e.contains("OOM"), "expected an OOM error once the stale cache expired, got {e:?}"),
            other => panic!("expected an OOM error, got {other:?}"),
        }
    }

    #[test]
    fn object_idletime_and_freq_report_real_values() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        match s.dispatch(&mut c, &argv(&["OBJECT", "IDLETIME", "k"])) {
            Reply::Integer(n) => assert!(n >= 0),
            other => panic!("expected Integer, got {other:?}"),
        }
        assert!(matches!(s.dispatch(&mut c, &argv(&["OBJECT", "FREQ", "k"])), Reply::Error(_)), "FREQ must error without an LFU policy");
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"]));
        match s.dispatch(&mut c, &argv(&["OBJECT", "FREQ", "k"])) {
            Reply::Integer(n) => assert_eq!(n, 5, "a freshly-touched key starts at LFU_INIT_VAL"),
            other => panic!("expected Integer, got {other:?}"),
        }
        for _ in 0..50 {
            s.dispatch(&mut c, &argv(&["GET", "k"]));
        }
        match s.dispatch(&mut c, &argv(&["OBJECT", "FREQ", "k"])) {
            Reply::Integer(n) => assert!(n >= 5, "repeated access should never decrease freq without decay time passing"),
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn unlink_removes_the_key_immediately_and_always_spawns_the_lazyfree_thread() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert!(s.lazyfree_sender.is_none(), "no lazy-free work has happened yet");
        assert_eq!(s.dispatch(&mut c, &argv(&["UNLINK", "k"])), Reply::Integer(1));
        // the key is gone from the keyspace right away, regardless of when
        // the background thread actually drops the value
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0));
        assert!(s.lazyfree_sender.is_some(), "UNLINK must always route through the lazy-free thread, with no config flag needed");
    }

    #[test]
    fn del_stays_synchronous_unless_lazyfree_lazy_user_del_is_enabled() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k1", "v"]));
        s.dispatch(&mut c, &argv(&["DEL", "k1"]));
        assert!(s.lazyfree_sender.is_none(), "DEL must not spawn the lazy-free thread while the flag is off (the default)");

        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "lazyfree-lazy-user-del", "yes"]));
        s.dispatch(&mut c, &argv(&["SET", "k2", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["DEL", "k2"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k2"])), Reply::Integer(0));
        assert!(s.lazyfree_sender.is_some(), "DEL must route through the lazy-free thread once the flag is on");
    }

    #[test]
    fn flushall_and_flushdb_clear_every_key_regardless_of_the_lazyfree_flag() {
        let mut s = Server::new(2);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "a", "v"]));
        c.db_index = 1;
        s.dispatch(&mut c, &argv(&["SET", "b", "v"]));
        c.db_index = 0;
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "lazyfree-lazy-user-flush", "yes"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["FLUSHDB"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "a"])), Reply::Integer(0));
        c.db_index = 1;
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "b"])), Reply::Integer(1), "FLUSHDB only clears the selected db");
        assert_eq!(s.dispatch(&mut c, &argv(&["FLUSHALL"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "b"])), Reply::Integer(0));
        assert!(s.lazyfree_sender.is_some());
    }

    #[test]
    fn set_overwrite_routes_the_old_value_through_lazyfree_server_del_when_enabled() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "lazyfree-lazy-server-del", "yes"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "old"]));
        assert!(s.lazyfree_sender.is_none(), "a fresh key isn't an overwrite, nothing to lazy-free yet");
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "new"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"new".to_vec()));
        assert!(s.lazyfree_sender.is_some(), "overwriting an existing key must lazy-free the old value once the flag is on");
    }

    #[test]
    fn multi_queues_commands_and_exec_runs_them_in_order() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["MULTI"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "a", "1"])), Reply::SimpleString("QUEUED".into()));
        assert_eq!(s.dispatch(&mut c, &argv(&["INCR", "a"])), Reply::SimpleString("QUEUED".into()));
        assert!(c.in_multi);
        let result = s.dispatch(&mut c, &argv(&["EXEC"]));
        assert_eq!(result, Reply::Array(vec![Reply::ok(), Reply::Integer(2)]));
        assert!(!c.in_multi);
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "a"])), Reply::bulk(b"2".to_vec()));
    }

    #[test]
    fn nested_multi_is_rejected() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["MULTI"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["MULTI"])), Reply::Error(_)));
    }

    #[test]
    fn exec_without_multi_errors() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["EXEC"])), Reply::Error(_)));
    }

    #[test]
    fn discard_without_multi_errors() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["DISCARD"])), Reply::Error(_)));
    }

    #[test]
    fn discard_clears_queue() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["MULTI"]));
        s.dispatch(&mut c, &argv(&["SET", "a", "1"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["DISCARD"])), Reply::ok());
        assert!(!c.in_multi);
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "a"])), Reply::Null, "queued command must never have run");
    }

    #[test]
    fn unknown_command_while_queueing_dirties_transaction_and_execabort() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["MULTI"]));
        s.dispatch(&mut c, &argv(&["SET", "a", "1"]));
        // An unknown command during queueing errors immediately (not queued)
        // and dirties the transaction.
        assert!(matches!(s.dispatch(&mut c, &argv(&["BOGUS"])), Reply::Error(_)));
        let result = s.dispatch(&mut c, &argv(&["EXEC"]));
        match result {
            Reply::Error(msg) => assert!(msg.starts_with("EXECABORT")),
            other => panic!("expected EXECABORT, got {other:?}"),
        }
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "a"])), Reply::Null, "aborted transaction must not run any queued command");
    }

    // ------------------------------------------------------------- WATCH ---

    #[test]
    fn watch_aborts_exec_when_another_client_modifies_the_watched_key() {
        let mut s = Server::new(1);
        let mut c1 = ClientState::default();
        let mut c2 = ClientState::default();
        s.dispatch(&mut c1, &argv(&["SET", "k", "orig"]));
        assert_eq!(s.dispatch(&mut c1, &argv(&["WATCH", "k"])), Reply::ok());

        s.dispatch(&mut c2, &argv(&["SET", "k", "changed-by-someone-else"]));

        s.dispatch(&mut c1, &argv(&["MULTI"]));
        s.dispatch(&mut c1, &argv(&["SET", "k", "should-not-apply"]));
        assert_eq!(s.dispatch(&mut c1, &argv(&["EXEC"])), Reply::NullArray, "a dirtied WATCH makes EXEC return a null array, not EXECABORT");
        assert_eq!(s.dispatch(&mut c1, &argv(&["GET", "k"])), Reply::bulk(b"changed-by-someone-else".to_vec()), "the queued SET must never have run");
        assert!(!c1.in_multi, "EXEC always clears in_multi, dirty or not");
    }

    #[test]
    fn watch_lets_exec_through_when_nothing_changed() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["WATCH", "k"]));
        s.dispatch(&mut c, &argv(&["MULTI"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "v2"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXEC"])), Reply::Array(vec![Reply::ok()]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v2".to_vec()));
    }

    #[test]
    fn unwatch_clears_the_watch_set() {
        let mut s = Server::new(1);
        let mut c1 = ClientState::default();
        let mut c2 = ClientState::default();
        s.dispatch(&mut c1, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c1, &argv(&["WATCH", "k"]));
        assert_eq!(s.dispatch(&mut c1, &argv(&["UNWATCH"])), Reply::ok());
        s.dispatch(&mut c2, &argv(&["SET", "k", "changed"]));
        s.dispatch(&mut c1, &argv(&["MULTI"]));
        assert_eq!(s.dispatch(&mut c1, &argv(&["EXEC"])), Reply::Array(vec![]), "no longer watching, so an unrelated change doesn't abort EXEC");
    }

    #[test]
    fn watch_inside_multi_is_rejected() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["MULTI"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["WATCH", "k"])), Reply::Error(_)));
    }

    #[test]
    fn watch_is_dirtied_by_a_watched_keys_own_expiration() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["PEXPIRE", "k", "1"]));
        // WATCH only after the PEXPIRE write itself (which already bumps the
        // version on its own) has landed, so the only remaining way to dirty
        // this watch is the active-expire sweep's own explicit bump below.
        s.dispatch(&mut c, &argv(&["WATCH", "k"]));
        std::thread::sleep(std::time::Duration::from_millis(5));
        s.dispatch(&mut c, &argv(&["MULTI"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXEC"])), Reply::NullArray, "the active-expire sweep must dirty the WATCH too");
    }

    #[test]
    fn flushall_and_flushdb_dirty_every_watch() {
        let mut s = Server::new(2);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["WATCH", "k"]));
        s.dispatch(&mut c, &argv(&["FLUSHDB"]));
        s.dispatch(&mut c, &argv(&["MULTI"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXEC"])), Reply::NullArray, "FLUSHDB must dirty a watch on a key in that db");

        s.dispatch(&mut c, &argv(&["SET", "k2", "v"]));
        s.dispatch(&mut c, &argv(&["WATCH", "k2"]));
        s.dispatch(&mut c, &argv(&["FLUSHALL"]));
        s.dispatch(&mut c, &argv(&["MULTI"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXEC"])), Reply::NullArray, "FLUSHALL must dirty every watch");
    }

    #[test]
    fn flushdb_only_clears_the_selected_database() {
        let mut s = Server::new(2);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "db0"]));
        s.dispatch(&mut c, &argv(&["SELECT", "1"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "db1"]));
        s.dispatch(&mut c, &argv(&["FLUSHDB"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);
        s.dispatch(&mut c, &argv(&["SELECT", "0"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"db0".to_vec()), "FLUSHDB must not touch other databases");
    }

    #[test]
    fn select_switches_database_and_keys_are_isolated() {
        let mut s = Server::new(2);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "db0"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SELECT", "1"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);
        s.dispatch(&mut c, &argv(&["SET", "k", "db1"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"db1".to_vec()));
        s.dispatch(&mut c, &argv(&["SELECT", "0"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"db0".to_vec()));
    }

    #[test]
    fn select_out_of_range_errors() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["SELECT", "5"])), Reply::Error(_)));
    }

    #[test]
    fn write_commands_are_propagated_but_reads_are_not() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["GET", "k"]));
        let prop = s.take_pending_propagation();
        // Expect a SELECT 0 (first write ever) followed by the SET, and
        // nothing for the GET.
        let select = Reply::Array(vec![Reply::bulk(b"SELECT".to_vec()), Reply::bulk(b"0".to_vec())]).to_bytes(2);
        let set = Reply::Array(vec![Reply::bulk(b"SET".to_vec()), Reply::bulk(b"k".to_vec()), Reply::bulk(b"v".to_vec())]).to_bytes(2);
        let mut expected = select;
        expected.extend_from_slice(&set);
        assert_eq!(prop, expected);
    }

    #[test]
    fn try_partial_resync_returns_exactly_the_missed_bytes() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k1", "v1"]));
        let offset_after_first_write = s.master_repl_offset;
        s.dispatch(&mut c, &argv(&["SET", "k2", "v2"]));

        let missed = s.try_partial_resync(&s.replid.clone(), offset_after_first_write).expect("should partial-resync");
        // What a replica that was caught up through the first SET, then
        // disconnected, needs to catch up: just the second SET (no SELECT —
        // the db hasn't changed since the first write already injected one).
        let expected = Reply::Array(vec![Reply::bulk(b"SET".to_vec()), Reply::bulk(b"k2".to_vec()), Reply::bulk(b"v2".to_vec())]).to_bytes(2);
        assert_eq!(missed, expected);
    }

    #[test]
    fn try_partial_resync_rejects_unknown_replid_or_stale_offset() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));

        assert!(s.try_partial_resync("0000000000000000000000000000000000000000", s.master_repl_offset).is_none(), "wrong replid");
        assert!(s.try_partial_resync(&s.replid.clone(), s.master_repl_offset + 1000).is_none(), "offset ahead of what we've produced");
    }

    #[test]
    fn try_partial_resync_fails_once_the_offset_has_scrolled_out_of_the_backlog() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k0", "v0"]));
        let early_offset = s.master_repl_offset;

        // Push well past the 1MiB backlog cap so `early_offset` falls out
        // of the retained window.
        let big_value = "x".repeat(4096);
        for i in 0..400 {
            s.dispatch(&mut c, &argv(&["SET", &format!("k{i}"), &big_value]));
        }

        assert!(s.try_partial_resync(&s.replid.clone(), early_offset).is_none(), "offset should have scrolled out of the backlog by now");
        assert!(s.try_partial_resync(&s.replid.clone(), s.master_repl_offset).is_some(), "the current offset is still always valid (zero missed bytes)");
    }

    #[test]
    fn backlog_never_drifts_past_its_slack_bound_under_sustained_writes() {
        // Regression guard for the amortized-compaction scheme in
        // `feed_backlog`: it's allowed to drift up to `REPL_BACKLOG_SLACK`
        // past `REPL_BACKLOG_MAX` between compactions, but never further —
        // the whole point of the slack is to batch the expensive `drain`
        // call, not to let the backlog grow unboundedly.
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let value = "x".repeat(100);
        for i in 0..20000 {
            s.dispatch(&mut c, &argv(&["SET", &format!("k{i}"), &value]));
            assert!(
                s.repl_backlog.len() <= Server::REPL_BACKLOG_MAX + Server::REPL_BACKLOG_SLACK,
                "backlog grew to {} bytes, past the {}-byte slack bound, after {i} writes",
                s.repl_backlog.len(),
                Server::REPL_BACKLOG_MAX + Server::REPL_BACKLOG_SLACK
            );
        }
    }

    #[test]
    fn failed_write_is_not_propagated() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        // LPUSH on a key that's already a string -> WRONGTYPE, must not propagate.
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.take_pending_propagation(); // drain the SET's own propagation
        let reply = s.dispatch(&mut c, &argv(&["LPUSH", "k", "x"]));
        assert!(matches!(reply, Reply::Error(_)));
        assert!(s.take_pending_propagation().is_empty());
    }

    #[test]
    fn select_is_only_injected_once_per_db_change() {
        let mut s = Server::new(2);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "a", "1"]));
        s.dispatch(&mut c, &argv(&["SET", "b", "2"])); // same db -> no second SELECT
        let prop = s.take_pending_propagation();
        let occurrences = prop.windows(6).filter(|w| w == b"SELECT").count();
        assert_eq!(occurrences, 1, "SELECT should only be propagated once while the db doesn't change");
    }

    #[test]
    fn apply_replicated_command_mutates_state_without_producing_a_reply_or_requiring_multi_state() {
        let mut master = Server::new(1);
        let mut mc = ClientState::default();
        master.dispatch(&mut mc, &argv(&["SET", "k", "v"]));
        let stream = master.take_pending_propagation();

        // Simulate a replica parsing that stream and applying it.
        let mut replica = Server::new(1);
        let mut rc = ClientState::default();
        let mut pos = 0;
        while pos < stream.len() {
            match redisx_net::resp::parse_request(&stream[pos..]) {
                redisx_net::resp::ParseOutcome::Command { argv, consumed } => {
                    replica.apply_replicated_command(&mut rc, &argv);
                    pos += consumed;
                }
                _ => break,
            }
        }
        let mut check = ClientState::default();
        assert_eq!(replica.dispatch(&mut check, &argv(&["GET", "k"])), Reply::bulk(b"v".to_vec()));
    }

    // ------------------------------------------------------------ EXPIRE ---

    #[test]
    fn expire_ttl_persist_basic_lifecycle() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["TTL", "k"])), Reply::Integer(-1));

        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "100"])), Reply::Integer(1));
        let Reply::Integer(ttl) = s.dispatch(&mut c, &argv(&["TTL", "k"])) else { panic!("expected integer") };
        assert!((99..=100).contains(&ttl), "got ttl={ttl}");

        assert_eq!(s.dispatch(&mut c, &argv(&["PERSIST", "k"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["TTL", "k"])), Reply::Integer(-1));
        assert_eq!(s.dispatch(&mut c, &argv(&["PERSIST", "k"])), Reply::Integer(0), "already no TTL to remove");
    }

    #[test]
    fn expire_and_ttl_on_missing_key() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "nope", "10"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["TTL", "nope"])), Reply::Integer(-2));
        assert_eq!(s.dispatch(&mut c, &argv(&["PTTL", "nope"])), Reply::Integer(-2));
        assert_eq!(s.dispatch(&mut c, &argv(&["PERSIST", "nope"])), Reply::Integer(0));
    }

    #[test]
    fn pexpire_pttl_and_absolute_expire_family_round_trip() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["PEXPIRE", "k", "100000"])), Reply::Integer(1));
        let Reply::Integer(pttl) = s.dispatch(&mut c, &argv(&["PTTL", "k"])) else { panic!("expected integer") };
        assert!((99_000..=100_000).contains(&pttl), "got pttl={pttl}");

        let future_unix = now_ms() / 1000 + 1000;
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIREAT", "k", &future_unix.to_string()])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRETIME", "k"])), Reply::Integer(future_unix));

        let future_ms = now_ms() + 2_000_000;
        assert_eq!(s.dispatch(&mut c, &argv(&["PEXPIREAT", "k", &future_ms.to_string()])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["PEXPIRETIME", "k"])), Reply::Integer(future_ms));
    }

    #[test]
    fn expire_with_past_time_deletes_key_immediately() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "-1"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0));
    }

    #[test]
    fn set_with_ex_sets_ttl_and_plain_set_clears_it_unless_keepttl() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();

        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v", "EX", "100"])), Reply::ok());
        let Reply::Integer(ttl) = s.dispatch(&mut c, &argv(&["TTL", "k"])) else { panic!("expected integer") };
        assert!((99..=100).contains(&ttl), "got ttl={ttl}");

        // KEEPTTL preserves the existing TTL across an overwrite.
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v2", "KEEPTTL"])), Reply::ok());
        let Reply::Integer(ttl2) = s.dispatch(&mut c, &argv(&["TTL", "k"])) else { panic!("expected integer") };
        assert!((99..=100).contains(&ttl2), "got ttl={ttl2}");

        // A plain SET (no KEEPTTL) clears any existing TTL.
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v3"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["TTL", "k"])), Reply::Integer(-1));

        assert!(matches!(s.dispatch(&mut c, &argv(&["SET", "k", "v", "EX", "10", "KEEPTTL"])), Reply::Error(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["SET", "k", "v", "BOGUS"])), Reply::Error(_)));
    }

    #[test]
    fn active_expire_sweep_hides_key_and_propagates_a_del() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["PEXPIRE", "k", "1"]));
        s.take_pending_propagation(); // discard the SET/PEXPIRE propagation noise

        std::thread::sleep(std::time::Duration::from_millis(5));

        // The next command of any kind sweeps `k` before running — GET on a
        // different key still triggers the sweep for this db.
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "other"])), Reply::Null);
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0));

        let prop = s.take_pending_propagation();
        let text = String::from_utf8_lossy(&prop);
        assert!(text.to_uppercase().contains("DEL") && text.contains('k'), "got: {text}");
    }

    // ------------------------------------------------------------ PubSub ---

    #[test]
    fn subscribe_to_multiple_channels_sends_one_frame_per_channel_with_increasing_count() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 1, ..ClientState::default() };
        let Reply::Multi(frames) = s.dispatch(&mut c, &argv(&["SUBSCRIBE", "a", "b"])) else { panic!("expected Multi") };
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], Reply::Push(vec![Reply::bulk(b"subscribe".to_vec()), Reply::bulk(b"a".to_vec()), Reply::Integer(1)]));
        assert_eq!(frames[1], Reply::Push(vec![Reply::bulk(b"subscribe".to_vec()), Reply::bulk(b"b".to_vec()), Reply::Integer(2)]));
        assert_eq!(c.subscribed_channels.len(), 2);
    }

    #[test]
    fn unsubscribe_with_no_args_unsubscribes_from_everything() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 1, ..ClientState::default() };
        s.dispatch(&mut c, &argv(&["SUBSCRIBE", "a", "b"]));
        let Reply::Multi(frames) = s.dispatch(&mut c, &argv(&["UNSUBSCRIBE"])) else { panic!("expected Multi") };
        assert_eq!(frames.len(), 2);
        assert!(c.subscribed_channels.is_empty());
        assert!(!s.channel_subs.contains_key(b"a".as_slice()));
        assert!(!s.channel_subs.contains_key(b"b".as_slice()));
    }

    #[test]
    fn unsubscribe_with_no_subscriptions_sends_one_nil_frame() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 1, ..ClientState::default() };
        let Reply::Multi(frames) = s.dispatch(&mut c, &argv(&["UNSUBSCRIBE"])) else { panic!("expected Multi") };
        assert_eq!(frames, vec![Reply::Push(vec![Reply::bulk(b"unsubscribe".to_vec()), Reply::Null, Reply::Integer(0)])]);
    }

    #[test]
    fn publish_delivers_to_channel_subscriber_via_pending_pushes() {
        let mut s = Server::new(1);
        let mut subscriber = ClientState { id: 42, ..ClientState::default() };
        s.dispatch(&mut subscriber, &argv(&["SUBSCRIBE", "news"]));

        let mut publisher = ClientState { id: 99, ..ClientState::default() };
        assert_eq!(s.dispatch(&mut publisher, &argv(&["PUBLISH", "news", "hello"])), Reply::Integer(1));

        let pushes = s.take_pending_pushes();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].0, 42);
        assert_eq!(pushes[0].1, Reply::Push(vec![Reply::bulk(b"message".to_vec()), Reply::bulk(b"news".to_vec()), Reply::bulk(b"hello".to_vec())]));
    }

    #[test]
    fn publish_to_unsubscribed_channel_delivers_to_nobody() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["PUBLISH", "nobody-listening", "x"])), Reply::Integer(0));
        assert!(s.take_pending_pushes().is_empty());
    }

    #[test]
    fn psubscribe_matches_glob_pattern_and_publish_sends_pmessage() {
        let mut s = Server::new(1);
        let mut subscriber = ClientState { id: 7, ..ClientState::default() };
        s.dispatch(&mut subscriber, &argv(&["PSUBSCRIBE", "news.*"]));

        let mut publisher = ClientState::default();
        assert_eq!(s.dispatch(&mut publisher, &argv(&["PUBLISH", "news.sports", "goal"])), Reply::Integer(1));

        let pushes = s.take_pending_pushes();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].0, 7);
        assert_eq!(
            pushes[0].1,
            Reply::Push(vec![
                Reply::bulk(b"pmessage".to_vec()),
                Reply::bulk(b"news.*".to_vec()),
                Reply::bulk(b"news.sports".to_vec()),
                Reply::bulk(b"goal".to_vec())
            ])
        );
    }

    #[test]
    fn pubsub_channels_numsub_and_numpat_reflect_live_subscriptions() {
        let mut s = Server::new(1);
        let mut a = ClientState { id: 1, ..ClientState::default() };
        let mut b = ClientState { id: 2, ..ClientState::default() };
        s.dispatch(&mut a, &argv(&["SUBSCRIBE", "ch1"]));
        s.dispatch(&mut b, &argv(&["SUBSCRIBE", "ch1", "ch2"]));
        s.dispatch(&mut a, &argv(&["PSUBSCRIBE", "ch*"]));

        let Reply::Array(channels) = s.dispatch(&mut a, &argv(&["PUBSUB", "CHANNELS"])) else { panic!("expected Array") };
        let mut names: Vec<_> = channels.into_iter().map(|r| if let Reply::BulkString(b) = r { b } else { panic!() }).collect();
        names.sort();
        assert_eq!(names, vec![b"ch1".to_vec(), b"ch2".to_vec()]);

        assert_eq!(
            s.dispatch(&mut a, &argv(&["PUBSUB", "NUMSUB", "ch1", "ch2", "ch3"])),
            Reply::Array(vec![
                Reply::bulk(b"ch1".to_vec()),
                Reply::Integer(2),
                Reply::bulk(b"ch2".to_vec()),
                Reply::Integer(1),
                Reply::bulk(b"ch3".to_vec()),
                Reply::Integer(0),
            ])
        );
        assert_eq!(s.dispatch(&mut a, &argv(&["PUBSUB", "NUMPAT"])), Reply::Integer(1));
    }

    #[test]
    fn client_disconnected_cleans_up_subscriptions() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 5, ..ClientState::default() };
        s.dispatch(&mut c, &argv(&["SUBSCRIBE", "a"]));
        s.dispatch(&mut c, &argv(&["PSUBSCRIBE", "p*"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["PUBSUB", "NUMPAT"])), Reply::Integer(1));

        s.client_disconnected(5);

        assert!(s.channel_subs.is_empty());
        assert!(s.pattern_subs.is_empty());
        assert_eq!(s.dispatch(&mut c, &argv(&["PUBSUB", "NUMPAT"])), Reply::Integer(0));
    }

    // -------------------------------------------------------------- HELLO ---

    #[test]
    fn hello_negotiates_resp3_and_back_to_resp2() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(c.resp, 2);

        let Reply::Map(fields) = s.dispatch(&mut c, &argv(&["HELLO", "3"])) else { panic!("expected Map") };
        assert_eq!(c.resp, 3);
        assert!(fields.contains(&(Reply::bulk(b"proto".to_vec()), Reply::Integer(3))));

        s.dispatch(&mut c, &argv(&["HELLO", "2"]));
        assert_eq!(c.resp, 2);

        assert!(matches!(s.dispatch(&mut c, &argv(&["HELLO", "99"])), Reply::Error(_)));
    }

    // ------------------------------------------------------- CLIENT TRACKING ---

    #[test]
    fn client_id_returns_its_own_id() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 777, ..ClientState::default() };
        assert_eq!(s.dispatch(&mut c, &argv(&["CLIENT", "ID"])), Reply::Integer(777));
    }

    #[test]
    fn client_tracking_requires_resp3() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 1, resp: 2, ..ClientState::default() };
        assert!(matches!(s.dispatch(&mut c, &argv(&["CLIENT", "TRACKING", "ON"])), Reply::Error(_)));
        assert!(!c.tracking);
    }

    #[test]
    fn write_invalidates_a_tracked_key_for_every_tracking_reader() {
        let mut s = Server::new(1);
        s.dispatch(&mut ClientState::default(), &argv(&["SET", "k", "v1"]));

        let mut reader = ClientState { id: 10, resp: 3, ..ClientState::default() };
        assert_eq!(s.dispatch(&mut reader, &argv(&["CLIENT", "TRACKING", "ON"])), Reply::ok());
        assert_eq!(s.dispatch(&mut reader, &argv(&["GET", "k"])), Reply::bulk(b"v1".to_vec()));

        let mut writer = ClientState::default();
        assert_eq!(s.dispatch(&mut writer, &argv(&["SET", "k", "v2"])), Reply::ok());

        let pushes = s.take_pending_pushes();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].0, 10);
        assert_eq!(pushes[0].1, Reply::Push(vec![Reply::bulk(b"invalidate".to_vec()), Reply::Array(vec![Reply::bulk(b"k".to_vec())])]));
    }

    #[test]
    fn tracking_off_stops_future_invalidations_and_clears_existing_entries() {
        let mut s = Server::new(1);
        s.dispatch(&mut ClientState::default(), &argv(&["SET", "k", "v1"]));

        let mut reader = ClientState { id: 11, resp: 3, ..ClientState::default() };
        s.dispatch(&mut reader, &argv(&["CLIENT", "TRACKING", "ON"]));
        s.dispatch(&mut reader, &argv(&["GET", "k"]));
        assert_eq!(s.dispatch(&mut reader, &argv(&["CLIENT", "TRACKING", "OFF"])), Reply::ok());

        s.dispatch(&mut ClientState::default(), &argv(&["SET", "k", "v2"]));
        assert!(s.take_pending_pushes().is_empty());
    }

    #[test]
    fn non_tracking_client_reads_generate_no_invalidation() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v1"]));
        s.dispatch(&mut c, &argv(&["GET", "k"])); // tracking is off by default
        s.dispatch(&mut c, &argv(&["SET", "k", "v2"]));
        assert!(s.take_pending_pushes().is_empty());
    }

    // --------------------------------------------------------- SET/EXPIRE flags ---

    #[test]
    fn set_nx_xx_get_options() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();

        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v1", "XX"])), Reply::Null, "XX on missing key fails, no value set");
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);

        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v1", "NX"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v2", "NX"])), Reply::Null, "NX on existing key fails");
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v1".to_vec()));

        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v3", "GET"])), Reply::bulk(b"v1".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v3".to_vec()));

        assert!(matches!(s.dispatch(&mut c, &argv(&["SET", "k", "v", "NX", "XX"])), Reply::Error(_)));
    }

    #[test]
    fn expire_nx_xx_gt_lt_conditions() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "100", "XX"])), Reply::Integer(0), "XX fails: no TTL yet");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "100", "NX"])), Reply::Integer(1), "NX succeeds: no TTL yet");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "200", "NX"])), Reply::Integer(0), "NX fails: TTL already set");

        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "50", "GT"])), Reply::Integer(0), "GT fails: 50 < 100");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "200", "GT"])), Reply::Integer(1), "GT succeeds: 200 > 100");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "300", "LT"])), Reply::Integer(0), "LT fails: 300 > 200");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "100", "LT"])), Reply::Integer(1), "LT succeeds: 100 < 200");
    }

    #[test]
    fn expire_gt_never_succeeds_and_lt_always_succeeds_against_no_ttl() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "100", "GT"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXPIRE", "k", "100", "LT"])), Reply::Integer(1));
    }

    // -------------------------------------------------------------- Bitops ---

    #[test]
    fn setbit_getbit_round_trip_and_grow_the_string() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["SETBIT", "k", "7", "1"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(vec![0x01]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SETBIT", "k", "7", "0"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETBIT", "k", "7"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETBIT", "k", "100"])), Reply::Integer(0), "out of range reads as 0");
    }

    #[test]
    fn bitcount_matches_known_examples() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "foobar"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["BITCOUNT", "k"])), Reply::Integer(26));
        assert_eq!(s.dispatch(&mut c, &argv(&["BITCOUNT", "k", "0", "0"])), Reply::Integer(4));
        assert_eq!(s.dispatch(&mut c, &argv(&["BITCOUNT", "k", "1", "1"])), Reply::Integer(6));
        assert_eq!(s.dispatch(&mut c, &argv(&["BITCOUNT", "k", "5", "30", "BIT"])), Reply::Integer(17));
    }

    #[test]
    fn bitpos_finds_first_matching_bit() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &[b"SET".to_vec(), b"k".to_vec(), vec![0xff, 0xf0, 0x00]]);
        assert_eq!(s.dispatch(&mut c, &argv(&["BITPOS", "k", "0"])), Reply::Integer(12));
        s.dispatch(&mut c, &[b"SET".to_vec(), b"k2".to_vec(), vec![0x00, 0xff, 0xff]]);
        assert_eq!(s.dispatch(&mut c, &argv(&["BITPOS", "k2", "1", "0"])), Reply::Integer(8));
    }

    #[test]
    fn bitop_and_or_xor_not() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "a", "abc"]));
        s.dispatch(&mut c, &argv(&["SET", "b", "abd"]));
        s.dispatch(&mut c, &argv(&["BITOP", "XOR", "dest", "a", "b"]));
        let Reply::BulkString(xored) = s.dispatch(&mut c, &argv(&["GET", "dest"])) else { panic!("expected bulk") };
        assert_eq!(xored, vec![0, 0, b'c' ^ b'd']);

        s.dispatch(&mut c, &argv(&["BITOP", "NOT", "notdest", "a"]));
        let Reply::BulkString(notted) = s.dispatch(&mut c, &argv(&["GET", "notdest"])) else { panic!("expected bulk") };
        assert_eq!(notted, vec![!b'a', !b'b', !b'c']);
    }

    // ---------------------------------------------------------------- SORT ---

    #[test]
    fn sort_numeric_default_and_alpha_and_desc() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "nums", "3", "1", "2"]));
        assert_eq!(
            s.dispatch(&mut c, &argv(&["SORT", "nums"])),
            Reply::Array(vec![Reply::bulk(b"1".to_vec()), Reply::bulk(b"2".to_vec()), Reply::bulk(b"3".to_vec())])
        );
        assert_eq!(
            s.dispatch(&mut c, &argv(&["SORT", "nums", "DESC"])),
            Reply::Array(vec![Reply::bulk(b"3".to_vec()), Reply::bulk(b"2".to_vec()), Reply::bulk(b"1".to_vec())])
        );

        s.dispatch(&mut c, &argv(&["RPUSH", "words", "banana", "apple", "cherry"]));
        assert_eq!(
            s.dispatch(&mut c, &argv(&["SORT", "words", "ALPHA"])),
            Reply::Array(vec![Reply::bulk(b"apple".to_vec()), Reply::bulk(b"banana".to_vec()), Reply::bulk(b"cherry".to_vec())])
        );
        assert!(matches!(s.dispatch(&mut c, &argv(&["SORT", "words"])), Reply::Error(_)), "non-numeric without ALPHA errors");
    }

    #[test]
    fn sort_limit_and_store() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "nums", "5", "3", "1", "4", "2"]));
        assert_eq!(
            s.dispatch(&mut c, &argv(&["SORT", "nums", "LIMIT", "1", "2"])),
            Reply::Array(vec![Reply::bulk(b"2".to_vec()), Reply::bulk(b"3".to_vec())])
        );
        assert_eq!(s.dispatch(&mut c, &argv(&["SORT", "nums", "STORE", "dest"])), Reply::Integer(5));
        assert_eq!(
            s.dispatch(&mut c, &argv(&["LRANGE", "dest", "0", "-1"])),
            Reply::Array((1..=5).map(|n| Reply::bulk(n.to_string().into_bytes())).collect())
        );
    }

    // ------------------------------------------------------ keyspace notifications ---

    #[test]
    fn keyspace_and_keyevent_notifications_fire_when_enabled() {
        let mut s = Server::new(1);
        s.config.set("notify-keyspace-events", "KEA");
        let mut sub = ClientState { id: 1, ..ClientState::default() };
        s.dispatch(&mut sub, &argv(&["PSUBSCRIBE", "__key*@0__:*"]));

        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));

        let pushes = s.take_pending_pushes();
        let has_keyspace = pushes.iter().any(|(_, r)| {
            matches!(r, Reply::Push(items) if items.len() == 4 && items[2] == Reply::bulk(b"__keyspace@0__:k".to_vec()) && items[3] == Reply::bulk(b"set".to_vec()))
        });
        let has_keyevent = pushes.iter().any(|(_, r)| {
            matches!(r, Reply::Push(items) if items.len() == 4 && items[2] == Reply::bulk(b"__keyevent@0__:set".to_vec()) && items[3] == Reply::bulk(b"k".to_vec()))
        });
        assert!(has_keyspace, "got: {pushes:?}");
        assert!(has_keyevent, "got: {pushes:?}");
    }

    #[test]
    fn no_notifications_fire_when_disabled_by_default() {
        let mut s = Server::new(1);
        let mut sub = ClientState { id: 1, ..ClientState::default() };
        s.dispatch(&mut sub, &argv(&["PSUBSCRIBE", "__key*@0__:*"]));
        s.dispatch(&mut ClientState::default(), &argv(&["SET", "k", "v"]));
        assert!(s.take_pending_pushes().is_empty());
    }

    // ----------------------------------------------------------- ACL-lite ---

    #[test]
    fn no_password_set_means_no_auth_required() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v"])), Reply::ok(), "no requirepass, no gate");
        assert!(matches!(s.dispatch(&mut c, &argv(&["AUTH", "whatever"])), Reply::Error(_)));
    }

    #[test]
    fn requirepass_gates_every_command_until_authenticated() {
        let mut s = Server::new(1);
        s.config.set("requirepass", "secret");
        let mut c = ClientState::default();

        assert!(matches!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Error(_)), "NOAUTH before AUTH");
        assert!(matches!(s.dispatch(&mut c, &argv(&["AUTH", "wrong"])), Reply::Error(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Error(_)), "still gated after wrong password");

        assert_eq!(s.dispatch(&mut c, &argv(&["AUTH", "secret"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null, "now allowed through");

        // A second, brand-new connection is independently gated.
        let mut other = ClientState::default();
        assert!(matches!(s.dispatch(&mut other, &argv(&["GET", "k"])), Reply::Error(_)));
    }

    #[test]
    fn auth_compares_sha256_hashes_not_the_raw_password() {
        let mut s = Server::new(1);
        s.config.set("requirepass", "secret");
        let mut c = ClientState::default();
        // The SHA-256 hex digest of "secret" itself must not authenticate —
        // proves the comparison hashes the *attempt* too, rather than
        // comparing a precomputed hash against a raw password.
        let hash_of_secret = redisx_core::sha256::sha256_hex(b"secret");
        assert!(matches!(s.dispatch(&mut c, &argv(&["AUTH", &hash_of_secret])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["AUTH", "secret"])), Reply::ok());
    }

    #[test]
    fn time_independent_strcmp_behaves_like_constant_time_equality() {
        assert!(time_independent_strcmp(b"abcdef", b"abcdef"));
        assert!(!time_independent_strcmp(b"abcdef", b"abcdeg"), "differs only in the last byte");
        assert!(!time_independent_strcmp(b"abcdef", b"zbcdef"), "differs only in the first byte");
        assert!(!time_independent_strcmp(b"abc", b"abcd"), "different lengths never match");
        assert!(time_independent_strcmp(b"", b""));
    }

    #[test]
    fn hello_with_auth_option_authenticates_and_negotiates_resp3_together() {
        let mut s = Server::new(1);
        s.config.set("requirepass", "secret");
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["HELLO", "3"])), Reply::Error(_)), "NOAUTH even for HELLO's own reply");
        assert!(matches!(s.dispatch(&mut c, &argv(&["HELLO", "3", "AUTH", "default", "secret"])), Reply::Map(_)));
        assert_eq!(c.resp, 3);
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null);
    }

    // ----------------------------------------------------------------- ACL ---

    #[test]
    fn acl_whoami_list_and_users_reflect_the_default_user() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "WHOAMI"])), Reply::bulk(b"default".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "USERS"])), Reply::Array(vec![Reply::bulk(b"default".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "LIST"])), Reply::Array(vec![Reply::bulk(b"user default on nopass sanitize-payload ~* &* +@all".to_vec())]));
    }

    #[test]
    fn acl_setuser_creates_a_restricted_user_and_auth_switches_to_it() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "bob", "on", ">pw", "~foo:*", "+@read", "+set"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["AUTH", "bob", "pw"])), Reply::ok());
        assert_eq!(c.user, "bob");

        // Allowed: a +@read command on an allowed key.
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "foo:1"])), Reply::Null);
        // Allowed: SET was individually added on top of the +@read baseline.
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "foo:1", "v"])), Reply::ok());
        // Denied: DEL was never granted.
        assert!(matches!(s.dispatch(&mut c, &argv(&["DEL", "foo:1"])), Reply::Error(e) if e.starts_with("NOPERM")));
        // Denied: key pattern doesn't cover "bar:1".
        assert!(matches!(s.dispatch(&mut c, &argv(&["GET", "bar:1"])), Reply::Error(e) if e.starts_with("NOPERM")));
    }

    #[test]
    fn acl_setuser_rejects_the_whole_batch_on_an_invalid_rule() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "bob", "on", "+nosuchcommand"])), Reply::Error(_)));
        // Never partially created.
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "GETUSER", "bob"])), Reply::NullArray);
    }

    #[test]
    fn acl_getuser_reports_structured_state() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "bob", "on", ">pw", "~foo:*", "+@read"]));
        let Reply::Map(fields) = s.dispatch(&mut c, &argv(&["ACL", "GETUSER", "bob"])) else { panic!("expected a map reply") };
        let get = |key: &str| fields.iter().find(|(k, _)| *k == Reply::bulk(key.as_bytes().to_vec())).map(|(_, v)| v.clone());
        assert_eq!(get("keys"), Some(Reply::bulk(b"~foo:*".to_vec())));
        assert_eq!(get("commands"), Some(Reply::bulk(b"-@all +@read".to_vec())));
    }

    #[test]
    fn acl_deluser_removes_a_user_but_refuses_default() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "bob", "on"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "DELUSER", "bob"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "GETUSER", "bob"])), Reply::NullArray);
        assert!(matches!(s.dispatch(&mut c, &argv(&["ACL", "DELUSER", "default"])), Reply::Error(_)));
    }

    #[test]
    fn acl_cat_lists_categories_and_commands_within_one() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::Array(cats) = s.dispatch(&mut c, &argv(&["ACL", "CAT"])) else { panic!("expected an array") };
        assert!(cats.contains(&Reply::bulk(b"admin".to_vec())));
        let Reply::Array(write_cmds) = s.dispatch(&mut c, &argv(&["ACL", "CAT", "write"])) else { panic!("expected an array") };
        assert!(write_cmds.contains(&Reply::bulk(b"set".to_vec())));
        assert!(!write_cmds.contains(&Reply::bulk(b"get".to_vec())));
    }

    #[test]
    fn acl_dryrun_reports_without_actually_running_the_command() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "bob", "on", "~foo:*", "+@read"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ACL", "DRYRUN", "bob", "GET", "foo:1"])), Reply::ok());
        let Reply::BulkString(msg) = s.dispatch(&mut c, &argv(&["ACL", "DRYRUN", "bob", "DEL", "foo:1"])) else { panic!("expected a bulk reply") };
        assert!(String::from_utf8_lossy(&msg).contains("no permissions"));
        // DRYRUN must not have actually deleted anything a real DEL would have.
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "foo:1"])), Reply::Integer(0), "key never existed in the first place, DRYRUN shouldn't create or remove it");
    }

    #[test]
    fn acl_save_and_load_error_without_an_acl_file_same_as_real_redis() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["ACL", "SAVE"])), Reply::Error(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["ACL", "LOAD"])), Reply::Error(_)));
    }

    #[test]
    fn acl_setuser_off_locks_out_default_even_without_requirepass() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v"])), Reply::ok(), "fully open until ACL says otherwise");
        s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "default", "off"]));
        let mut c2 = ClientState::default();
        assert!(matches!(s.dispatch(&mut c2, &argv(&["GET", "k"])), Reply::Error(e) if e.starts_with("NOAUTH")));
    }

    #[test]
    fn channel_permission_is_enforced_for_pubsub_commands() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ACL", "SETUSER", "bob", "on", ">pw", "allkeys", "&news.*", "+@all"]));
        s.dispatch(&mut c, &argv(&["AUTH", "bob", "pw"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["SUBSCRIBE", "news.sports"])), Reply::Multi(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["PUBLISH", "weather.today", "x"])), Reply::Error(e) if e.starts_with("NOPERM")));
    }

    // ------------------------------------------------------------- Cluster ---

    #[test]
    fn cluster_myid_and_keyslot_work_even_when_cluster_mode_is_off() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::BulkString(id) = s.dispatch(&mut c, &argv(&["CLUSTER", "MYID"])) else { panic!("expected a node id") };
        assert_eq!(id.len(), 40);
        assert_eq!(s.dispatch(&mut c, &argv(&["CLUSTER", "KEYSLOT", "foo"])), Reply::Integer(crate::cluster::key_hash_slot(b"foo") as i64));
        assert!(matches!(s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTS", "0"])), Reply::Error(e) if e.contains("disabled")));
        // A plain, non-cluster `SET` is never redirected regardless.
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v"])), Reply::ok());
    }

    #[test]
    fn cluster_addslots_rejects_an_already_busy_slot_and_delslots_rejects_unassigned() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        assert_eq!(s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTS", "0", "1", "2"])), Reply::ok());
        assert!(matches!(s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTS", "1"])), Reply::Error(e) if e.contains("already busy")));
        assert_eq!(s.dispatch(&mut c, &argv(&["CLUSTER", "DELSLOTS", "2"])), Reply::ok());
        assert!(matches!(s.dispatch(&mut c, &argv(&["CLUSTER", "DELSLOTS", "2"])), Reply::Error(e) if e.contains("already unassigned")));
        assert_eq!(s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTSRANGE", "10", "12"])), Reply::ok());
        assert_eq!(s.cluster.myself().slot_count(), 5);
    }

    #[test]
    fn a_fully_assigned_single_node_cluster_serves_every_slot_locally() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SET", "k", "v"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v".to_vec()));
    }

    #[test]
    fn cluster_moved_redirects_to_the_node_that_actually_owns_the_slot() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        let mut other = crate::cluster::ClusterNode::new("other-node".to_string(), "10.0.0.2".to_string(), 7001, 17001);
        other.add_slot(crate::cluster::key_hash_slot(b"foo"));
        s.cluster.nodes.insert("other-node".to_string(), other);

        let Reply::Error(msg) = s.dispatch(&mut c, &argv(&["GET", "foo"])) else { panic!("expected a MOVED error") };
        assert_eq!(msg, format!("MOVED {} 10.0.0.2:7001", crate::cluster::key_hash_slot(b"foo")));
    }

    #[test]
    fn cluster_clusterdown_when_a_touched_slot_has_no_owner() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        assert!(matches!(s.dispatch(&mut c, &argv(&["GET", "foo"])), Reply::Error(e) if e.starts_with("CLUSTERDOWN")));
        s.config.set("cluster-require-full-coverage", "no");
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "foo"])), Reply::Null, "an unserved slot just behaves as empty once full coverage isn't required");
    }

    #[test]
    fn cluster_crossslot_error_for_a_multi_key_command_spanning_slots() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]));
        // `{tag}`-less keys essentially never collide onto the same slot by
        // accident, so two arbitrary names reliably trigger this.
        assert!(matches!(s.dispatch(&mut c, &argv(&["MGET", "foo", "bar"])), Reply::Error(e) if e.starts_with("CROSSSLOT")));
        // Hash-tagged keys land on the same slot and are allowed together.
        assert_eq!(s.dispatch(&mut c, &argv(&["MGET", "{t}foo", "{t}bar"])), Reply::Array(vec![Reply::Null, Reply::Null]));
    }

    #[test]
    fn cluster_select_nonzero_db_is_rejected_once_cluster_mode_is_on() {
        let mut s = Server::new(4);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["SELECT", "1"])), Reply::ok(), "fine before cluster mode is on");
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        assert!(matches!(s.dispatch(&mut c, &argv(&["SELECT", "1"])), Reply::Error(e) if e.contains("cluster mode")));
        assert_eq!(s.dispatch(&mut c, &argv(&["SELECT", "0"])), Reply::ok());
    }

    #[test]
    fn cluster_readonly_lets_a_replica_serve_stale_reads_for_its_masters_slots() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        let mut master = crate::cluster::ClusterNode::new("master-node".to_string(), "10.0.0.2".to_string(), 7001, 17001);
        master.add_slot(crate::cluster::key_hash_slot(b"foo"));
        s.cluster.nodes.insert("master-node".to_string(), master);
        s.cluster.myself_mut().role = crate::cluster::NodeRole::Replica;
        s.cluster.myself_mut().master_id = Some("master-node".to_string());

        assert!(matches!(s.dispatch(&mut c, &argv(&["GET", "foo"])), Reply::Error(e) if e.starts_with("MOVED")), "no READONLY issued yet");
        s.dispatch(&mut c, &argv(&["READONLY"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "foo"])), Reply::Null, "served locally now, as a stale read");
        assert!(matches!(s.dispatch(&mut c, &argv(&["SET", "foo", "v"])), Reply::Error(e) if e.starts_with("MOVED")), "writes still redirect even in READONLY mode");
        s.dispatch(&mut c, &argv(&["READWRITE"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["GET", "foo"])), Reply::Error(e) if e.starts_with("MOVED")), "back to redirecting after READWRITE");
    }

    #[test]
    fn cluster_setslot_node_reassigns_a_slot_from_its_current_owner() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTS", "0"]));
        let other = crate::cluster::ClusterNode::new("other-node".to_string(), "10.0.0.2".to_string(), 7001, 17001);
        s.cluster.nodes.insert("other-node".to_string(), other);

        assert_eq!(s.dispatch(&mut c, &argv(&["CLUSTER", "SETSLOT", "0", "NODE", "other-node"])), Reply::ok());
        assert!(!s.cluster.myself().covers(0));
        assert_eq!(s.cluster.nodes.get("other-node").unwrap().slots, vec![(0, 0)]);
    }

    #[test]
    fn cluster_info_nodes_and_slots_reflect_assigned_state() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        let Reply::BulkString(info) = s.dispatch(&mut c, &argv(&["CLUSTER", "INFO"])) else { panic!("expected bulk") };
        let info = String::from_utf8(info).unwrap();
        assert!(info.contains("cluster_enabled:1"));
        assert!(info.contains("cluster_state:fail"), "nothing assigned yet, full coverage required by default");

        s.dispatch(&mut c, &argv(&["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]));
        let Reply::BulkString(info) = s.dispatch(&mut c, &argv(&["CLUSTER", "INFO"])) else { panic!("expected bulk") };
        assert!(String::from_utf8(info).unwrap().contains("cluster_state:ok"));

        let Reply::BulkString(nodes) = s.dispatch(&mut c, &argv(&["CLUSTER", "NODES"])) else { panic!("expected bulk") };
        let nodes = String::from_utf8(nodes).unwrap();
        assert!(nodes.contains("myself,master"));
        assert!(nodes.contains("0-16383"));

        let Reply::Array(slots) = s.dispatch(&mut c, &argv(&["CLUSTER", "SLOTS"])) else { panic!("expected array") };
        assert_eq!(slots.len(), 1);
    }

    #[test]
    fn cluster_forget_removes_a_node_but_refuses_to_forget_self() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.cluster.activate("127.0.0.1".to_string(), 7000, 17000);
        let my_id = s.cluster.my_id.clone();
        let other = crate::cluster::ClusterNode::new("other-node".to_string(), "10.0.0.2".to_string(), 7001, 17001);
        s.cluster.nodes.insert("other-node".to_string(), other);

        assert!(matches!(s.dispatch(&mut c, &argv(&["CLUSTER", "FORGET", &my_id])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["CLUSTER", "FORGET", "other-node"])), Reply::ok());
        assert!(!s.cluster.nodes.contains_key("other-node"));
    }

    // ---------------------------------------------------------------- WAIT ---

    #[test]
    fn wait_reports_connected_replica_count() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["WAIT", "0", "100"])), Reply::Integer(0));
        s.set_connected_replicas(2);
        assert_eq!(s.dispatch(&mut c, &argv(&["WAIT", "0", "100"])), Reply::Integer(2));
    }

    #[test]
    fn failover_honestly_errors_instead_of_faking_a_result() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["FAILOVER", "ABORT"])), Reply::Error(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["FAILOVER"])), Reply::Error(_)), "no replicas connected yet");
        s.set_connected_replicas(1);
        let Reply::Error(msg) = s.dispatch(&mut c, &argv(&["FAILOVER"])) else { panic!("expected error") };
        assert!(msg.contains("not supported"), "got: {msg}");

        s.role = Role::Replica { host: "x".to_string(), port: 1 };
        assert!(matches!(s.dispatch(&mut c, &argv(&["FAILOVER"])), Reply::Error(_)), "not a master");
    }

    // ------------------------------------------- CLIENT TRACKING BCAST/REDIRECT ---

    #[test]
    fn bcast_tracking_invalidates_any_matching_key_without_a_prior_read() {
        let mut s = Server::new(1);
        let mut bcast = ClientState { id: 20, resp: 3, ..ClientState::default() };
        assert_eq!(s.dispatch(&mut bcast, &argv(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:"])), Reply::ok());

        // No GET of "user:1" ever happened — BCAST doesn't need one.
        s.dispatch(&mut ClientState::default(), &argv(&["SET", "user:1", "x"]));
        let pushes = s.take_pending_pushes();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].0, 20);

        // A key outside the prefix doesn't match.
        s.dispatch(&mut ClientState::default(), &argv(&["SET", "other", "x"]));
        assert!(s.take_pending_pushes().is_empty());
    }

    #[test]
    fn tracking_redirect_sends_invalidation_to_the_other_client() {
        let mut s = Server::new(1);
        let mut reader = ClientState { id: 1, resp: 2, ..ClientState::default() }; // RESP2 is fine: REDIRECT covers it
        assert_eq!(s.dispatch(&mut reader, &argv(&["CLIENT", "TRACKING", "ON", "REDIRECT", "99"])), Reply::ok());
        s.dispatch(&mut reader, &argv(&["GET", "k"]));

        s.dispatch(&mut ClientState::default(), &argv(&["SET", "k", "v2"]));
        let pushes = s.take_pending_pushes();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].0, 99, "delivered to the redirect target, not the tracking client itself");
    }

    #[test]
    fn tracking_noloop_skips_invalidating_the_writer_itself() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 5, resp: 3, ..ClientState::default() };
        s.dispatch(&mut c, &argv(&["CLIENT", "TRACKING", "ON", "NOLOOP"]));
        s.dispatch(&mut c, &argv(&["GET", "k"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "v2"])); // the tracking client is also the writer
        assert!(s.take_pending_pushes().is_empty());
    }

    #[test]
    fn prefix_without_bcast_is_rejected() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 1, resp: 3, ..ClientState::default() };
        assert!(matches!(s.dispatch(&mut c, &argv(&["CLIENT", "TRACKING", "ON", "PREFIX", "x"])), Reply::Error(_)));
    }

    // ------------------------------------------------------------ EVAL ---

    #[test]
    fn eval_runs_a_lua_script_and_sees_keys_argv_as_globals() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(
            s.dispatch(&mut c, &argv(&["EVAL", "return {KEYS[1], ARGV[1]}", "1", "k", "v"])),
            Reply::Array(vec![Reply::bulk(b"k".to_vec()), Reply::bulk(b"v".to_vec())])
        );
    }

    #[test]
    fn eval_redis_call_reaches_the_real_keyspace() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["EVAL", "redis.call('SET', KEYS[1], ARGV[1])", "1", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v".to_vec()));
    }

    #[test]
    fn evalsha_runs_a_script_eval_already_cached() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let script = "return ARGV[1]";
        s.dispatch(&mut c, &argv(&["EVAL", script, "0", "hi"]));
        let sha = redisx_core::sha1::sha1_hex(script.as_bytes());
        assert_eq!(s.dispatch(&mut c, &argv(&["EVALSHA", &sha, "0", "hi"])), Reply::bulk(b"hi".to_vec()));
    }

    #[test]
    fn evalsha_on_unknown_sha_errors_noscript() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::Error(msg) = s.dispatch(&mut c, &argv(&["EVALSHA", "0000000000000000000000000000000000000000", "0"])) else {
            panic!("expected error")
        };
        assert!(msg.starts_with("NOSCRIPT"), "got: {msg}");
    }

    #[test]
    fn script_load_exists_and_flush() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let script = "return 1";
        let sha = redisx_core::sha1::sha1_hex(script.as_bytes());
        assert_eq!(s.dispatch(&mut c, &argv(&["SCRIPT", "LOAD", script])), Reply::bulk(sha.clone().into_bytes()));
        assert_eq!(s.dispatch(&mut c, &argv(&["SCRIPT", "EXISTS", &sha, "deadbeef"])), Reply::Array(vec![Reply::Integer(1), Reply::Integer(0)]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EVALSHA", &sha, "0"])), Reply::Integer(1));

        assert_eq!(s.dispatch(&mut c, &argv(&["SCRIPT", "FLUSH"])), Reply::ok());
        assert!(matches!(s.dispatch(&mut c, &argv(&["EVALSHA", &sha, "0"])), Reply::Error(_)));
    }

    // ------------------------------------------------------ HyperLogLog ---

    #[test]
    fn pfadd_and_pfcount_estimate_cardinality() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["PFADD", "hll", "a", "b", "c"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["PFADD", "hll", "a"])), Reply::Integer(0), "no new info");
        assert_eq!(s.dispatch(&mut c, &argv(&["PFCOUNT", "hll"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["TYPE", "hll"])), Reply::SimpleString("string".to_string()));
    }

    #[test]
    fn pfmerge_combines_two_hlls() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["PFADD", "a", "x", "y"]));
        s.dispatch(&mut c, &argv(&["PFADD", "b", "y", "z"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["PFMERGE", "dest", "a", "b"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["PFCOUNT", "dest"])), Reply::Integer(3));
    }

    #[test]
    fn pfadd_rejects_non_hll_string_and_non_string_type() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "plain", "hello"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["PFADD", "plain", "x"])), Reply::Error(_)));
        s.dispatch(&mut c, &argv(&["RPUSH", "alist", "x"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["PFADD", "alist", "x"])), Reply::Error(_)));
    }

    // -------------------------------------------------------------- Geo ---

    #[test]
    fn geoadd_and_geopos_round_trip_approximately() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo"])), Reply::Integer(1));

        let Reply::Array(positions) = s.dispatch(&mut c, &argv(&["GEOPOS", "cities", "Palermo", "Nowhere"])) else { panic!("expected array") };
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[1], Reply::NullArray);
        let Reply::Array(coords) = &positions[0] else { panic!("expected array") };
        let Reply::BulkString(lon_bytes) = &coords[0] else { panic!("expected bulk") };
        let lon: f64 = std::str::from_utf8(lon_bytes).unwrap().parse().unwrap();
        assert!((lon - 13.361389).abs() < 0.001, "got lon={lon}");
    }

    #[test]
    fn geodist_matches_known_city_pair_distance() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"]));
        let Reply::BulkString(dist_bytes) = s.dispatch(&mut c, &argv(&["GEODIST", "cities", "Palermo", "Catania", "km"])) else { panic!("expected bulk") };
        let km: f64 = std::str::from_utf8(&dist_bytes).unwrap().parse().unwrap();
        assert!((km - 166.27).abs() < 2.0, "got {km} km");
    }

    #[test]
    fn geosearch_byradius_finds_nearby_members_only() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"]));

        let Reply::Array(found) =
            s.dispatch(&mut c, &argv(&["GEOSEARCH", "cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"]))
        else {
            panic!("expected array")
        };
        let names: Vec<Vec<u8>> = found.into_iter().map(|r| if let Reply::BulkString(b) = r { b } else { panic!() }).collect();
        assert_eq!(names, vec![b"Catania".to_vec(), b"Palermo".to_vec()], "Catania is closer to (15,37)");

        let Reply::Array(found_tight) =
            s.dispatch(&mut c, &argv(&["GEOSEARCH", "cities", "FROMLONLAT", "15", "37", "BYRADIUS", "100", "km"]))
        else {
            panic!("expected array")
        };
        assert_eq!(found_tight, vec![Reply::bulk(b"Catania".to_vec())], "Palermo (~190km away) is outside a tight 100km radius");
    }

    #[test]
    fn geosearch_frommember_searches_from_an_existing_members_own_position() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"]));
        let Reply::Array(found) = s.dispatch(&mut c, &argv(&["GEOSEARCH", "cities", "FROMMEMBER", "Catania", "BYRADIUS", "100", "km"])) else { panic!("expected array") };
        assert_eq!(found, vec![Reply::bulk(b"Catania".to_vec())], "searching from Catania itself always finds Catania (distance 0)");

        assert!(matches!(s.dispatch(&mut c, &argv(&["GEOSEARCH", "cities", "FROMMEMBER", "NoSuchCity", "BYRADIUS", "100", "km"])), Reply::Error(_)));
    }

    #[test]
    fn geosearchstore_stores_matches_and_respects_storedist() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["GEOADD", "cities", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["GEOSEARCHSTORE", "dest", "cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "dest", "Palermo"])), s.dispatch(&mut c, &argv(&["ZSCORE", "cities", "Palermo"])), "without STOREDIST, the original geohash score is preserved");

        assert_eq!(s.dispatch(&mut c, &argv(&["GEOSEARCHSTORE", "dest2", "cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "STOREDIST"])), Reply::Integer(2));
        let Reply::BulkString(dist_score) = s.dispatch(&mut c, &argv(&["ZSCORE", "dest2", "Catania"])) else { panic!("expected bulk") };
        let dist_score: f64 = String::from_utf8_lossy(&dist_score).parse().unwrap();
        // Catania is within the 200km search radius of (15,37) (confirmed by
        // `geosearch_byradius_finds_nearby_members_only`'s own 100km case),
        // so STOREDIST's km score must land somewhere inside that radius.
        assert!((0.0..200.0).contains(&dist_score), "expected a plausible STOREDIST km distance within the search radius, got {dist_score}");

        assert_eq!(s.dispatch(&mut c, &argv(&["GEOSEARCHSTORE", "empty_dest", "cities", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "empty_dest"])), Reply::Integer(0));
    }

    #[test]
    fn geoadd_rejects_out_of_range_coordinates() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["GEOADD", "cities", "200", "38", "bad"])), Reply::Error(_)));
    }

    // ----------------------------------------------------------- Stream ---

    #[test]
    fn xadd_autoid_xlen_and_xrange() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::BulkString(id1) = s.dispatch(&mut c, &argv(&["XADD", "st", "*", "field1", "value1"])) else { panic!("expected bulk") };
        let Reply::BulkString(id2) = s.dispatch(&mut c, &argv(&["XADD", "st", "*", "field2", "value2"])) else { panic!("expected bulk") };
        assert!(id1 < id2, "ids should be monotonically increasing: {id1:?} vs {id2:?}");
        assert_eq!(s.dispatch(&mut c, &argv(&["XLEN", "st"])), Reply::Integer(2));

        let Reply::Array(entries) = s.dispatch(&mut c, &argv(&["XRANGE", "st", "-", "+"])) else { panic!("expected array") };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], Reply::Array(vec![Reply::bulk(id1), Reply::Array(vec![Reply::bulk(b"field1".to_vec()), Reply::bulk(b"value1".to_vec())])]));
    }

    #[test]
    fn xadd_explicit_id_must_be_increasing() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["XADD", "st", "5-0", "f", "v"])), Reply::bulk(b"5-0".to_vec()));
        assert!(matches!(s.dispatch(&mut c, &argv(&["XADD", "st", "5-0", "f", "v"])), Reply::Error(_)));
        assert!(matches!(s.dispatch(&mut c, &argv(&["XADD", "st", "4-0", "f", "v"])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["XADD", "st", "5-1", "f", "v"])), Reply::bulk(b"5-1".to_vec()));
    }

    #[test]
    fn xrevrange_reverses_order() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "f", "b"]));
        let Reply::Array(entries) = s.dispatch(&mut c, &argv(&["XREVRANGE", "st", "+", "-"])) else { panic!("expected array") };
        assert_eq!(entries[0], Reply::Array(vec![Reply::bulk(b"2-0".to_vec()), Reply::Array(vec![Reply::bulk(b"f".to_vec()), Reply::bulk(b"b".to_vec())])]));
    }

    #[test]
    fn xdel_and_xtrim() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        for i in 1..=5 {
            s.dispatch(&mut c, &argv(&["XADD", "st", &format!("{i}-0"), "f", "v"]));
        }
        assert_eq!(s.dispatch(&mut c, &argv(&["XDEL", "st", "3-0"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["XLEN", "st"])), Reply::Integer(4));

        assert_eq!(s.dispatch(&mut c, &argv(&["XTRIM", "st", "MAXLEN", "2"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["XLEN", "st"])), Reply::Integer(2));
    }

    #[test]
    fn xread_returns_only_entries_after_the_given_id() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "f", "b"]));

        let Reply::Array(streams) = s.dispatch(&mut c, &argv(&["XREAD", "STREAMS", "st", "1-0"])) else { panic!("expected array") };
        assert_eq!(streams.len(), 1);
        let Reply::Array(stream_reply) = &streams[0] else { panic!("expected array") };
        assert_eq!(stream_reply[0], Reply::bulk(b"st".to_vec()));
        let Reply::Array(entries) = &stream_reply[1] else { panic!("expected array") };
        assert_eq!(entries.len(), 1, "only the entry after 1-0");

        assert_eq!(s.dispatch(&mut c, &argv(&["XREAD", "STREAMS", "st", "$"])), Reply::NullArray, "nothing newer than right now");
    }

    #[test]
    fn xsetid_moves_the_watermark_and_rejects_going_backwards() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "5-0", "f", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["XSETID", "st", "100-0"])), Reply::ok());
        // The new watermark takes effect: an auto-id XADD must land after it.
        let Reply::BulkString(id) = s.dispatch(&mut c, &argv(&["XADD", "st", "*", "f", "v"])) else { panic!("expected bulk") };
        let id_str = String::from_utf8_lossy(&id);
        let ms: u64 = id_str.split('-').next().unwrap().parse().unwrap();
        assert!(ms >= 100, "auto-generated id must be at or after the XSETID watermark, got {id_str}");

        assert!(matches!(s.dispatch(&mut c, &argv(&["XSETID", "st", "1-0"])), Reply::Error(_)), "can't move the watermark backwards");
        assert_eq!(s.dispatch(&mut c, &argv(&["XSETID", "newstream", "50-0"])), Reply::ok(), "XSETID can create a brand-new empty stream");
        assert_eq!(s.dispatch(&mut c, &argv(&["TYPE", "newstream"])), Reply::SimpleString("stream".to_string()));
    }

    #[test]
    fn xinfo_stream_reports_length_and_first_last_entries() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["XINFO", "STREAM", "missing"])), Reply::Error(_)));

        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f1", "v1"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "f2", "v2"]));
        let Reply::Map(info) = s.dispatch(&mut c, &argv(&["XINFO", "STREAM", "st"])) else { panic!("expected map") };
        let get = |key: &[u8]| info.iter().find(|(k, _)| matches!(k, Reply::BulkString(b) if b == key)).map(|(_, v)| v.clone());
        assert_eq!(get(b"length"), Some(Reply::Integer(2)));
        assert_eq!(get(b"last-generated-id"), Some(Reply::bulk(b"2-0".to_vec())));
        assert_eq!(get(b"groups"), Some(Reply::Integer(0)));
        let Some(Reply::Array(first_entry)) = get(b"first-entry") else { panic!("expected array") };
        assert_eq!(first_entry[0], Reply::bulk(b"1-0".to_vec()));

        assert_eq!(s.dispatch(&mut c, &argv(&["XINFO", "GROUPS", "st"])), Reply::Array(vec![]));
    }

    #[test]
    fn xadd_rejects_wrong_type_and_respects_nomkstream() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "notastream", "v"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["XADD", "notastream", "*", "f", "v"])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["XADD", "missing", "NOMKSTREAM", "*", "f", "v"])), Reply::Null);
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "missing"])), Reply::Integer(0));
    }

    #[test]
    fn xgroup_create_requires_mkstream_on_a_missing_key_then_rejects_a_duplicate_name() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "$"])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "$", "MKSTREAM"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "st"])), Reply::Integer(1));
        assert!(matches!(s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "$"])), Reply::Error(e) if e.starts_with("BUSYGROUP")));
        assert_eq!(s.dispatch(&mut c, &argv(&["XGROUP", "DESTROY", "st", "g"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["XGROUP", "DESTROY", "st", "g"])), Reply::Integer(0));
    }

    #[test]
    fn xreadgroup_delivers_new_entries_once_and_tracks_them_in_the_pel() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "f", "b"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));

        let Reply::Array(streams) = s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"])) else {
            panic!("expected array")
        };
        let Reply::Array(entry_pair) = &streams[0] else { panic!("expected array") };
        let Reply::Array(entries) = &entry_pair[1] else { panic!("expected array") };
        assert_eq!(entries.len(), 2, "both entries are new");

        // A second `>` read sees nothing new.
        assert_eq!(s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"])), Reply::NullArray);

        // Both entries are now pending for cons1.
        let Reply::Array(pending) = s.dispatch(&mut c, &argv(&["XPENDING", "st", "g"])) else { panic!("expected array") };
        assert_eq!(pending[0], Reply::Integer(2));

        assert_eq!(s.dispatch(&mut c, &argv(&["XACK", "st", "g", "1-0"])), Reply::Integer(1));
        let Reply::Array(pending2) = s.dispatch(&mut c, &argv(&["XPENDING", "st", "g"])) else { panic!("expected array") };
        assert_eq!(pending2[0], Reply::Integer(1));
    }

    #[test]
    fn xreadgroup_noack_never_enters_the_pel() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "NOACK", "STREAMS", "st", ">"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["XPENDING", "st", "g"])), Reply::Array(vec![Reply::Integer(0), Reply::Null, Reply::Null, Reply::NullArray]));
    }

    #[test]
    fn xreadgroup_missing_group_errors_with_nogroup() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        assert!(matches!(
            s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "nope", "cons1", "STREAMS", "st", ">"])),
            Reply::Error(e) if e.starts_with("NOGROUP")
        ));
    }

    #[test]
    fn xclaim_requires_min_idle_time_and_moves_ownership() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"]));

        // Not idle long enough yet (min-idle-time 10000ms, just delivered).
        assert_eq!(s.dispatch(&mut c, &argv(&["XCLAIM", "st", "g", "cons2", "10000", "1-0"])), Reply::Array(vec![]));

        // min-idle-time 0 always qualifies.
        let Reply::Array(claimed) = s.dispatch(&mut c, &argv(&["XCLAIM", "st", "g", "cons2", "0", "1-0"])) else { panic!("expected array") };
        assert_eq!(claimed.len(), 1);

        let Reply::Array(pending) = s.dispatch(&mut c, &argv(&["XPENDING", "st", "g", "-", "+", "10"])) else { panic!("expected array") };
        let Reply::Array(row) = &pending[0] else { panic!("expected array") };
        assert_eq!(row[1], Reply::bulk(b"cons2".to_vec()), "ownership moved to cons2");
    }

    #[test]
    fn xclaim_justid_increments_delivery_count_and_skips_deleted_entries() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XADD", "st", "2-0", "f", "b"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"]));
        s.dispatch(&mut c, &argv(&["XDEL", "st", "2-0"]));

        let Reply::Array(ids) = s.dispatch(&mut c, &argv(&["XCLAIM", "st", "g", "cons2", "0", "1-0", "2-0", "JUSTID"])) else {
            panic!("expected array")
        };
        assert_eq!(ids, vec![Reply::bulk(b"1-0".to_vec())], "the deleted entry is dropped from the PEL, not claimed");
        let Reply::Array(pending) = s.dispatch(&mut c, &argv(&["XPENDING", "st", "g"])) else { panic!("expected array") };
        assert_eq!(pending[0], Reply::Integer(1));
    }

    #[test]
    fn xautoclaim_scans_from_a_cursor_and_reports_deleted_ids_separately() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        for i in 1..=3u64 {
            s.dispatch(&mut c, &argv(&["XADD", "st", &format!("{i}-0"), "f", "v"]));
        }
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"]));
        s.dispatch(&mut c, &argv(&["XDEL", "st", "2-0"]));

        let Reply::Array(result) = s.dispatch(&mut c, &argv(&["XAUTOCLAIM", "st", "g", "cons2", "0", "0"])) else { panic!("expected array") };
        let Reply::Array(deleted) = &result[2] else { panic!("expected array") };
        assert_eq!(deleted, &vec![Reply::bulk(b"2-0".to_vec())]);
        let Reply::Array(claimed_entries) = &result[1] else { panic!("expected array") };
        assert_eq!(claimed_entries.len(), 2, "the two still-existing entries were claimed");
    }

    #[test]
    fn xgroup_createconsumer_and_delconsumer_report_counts() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["XGROUP", "CREATECONSUMER", "st", "g", "c1"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["XGROUP", "CREATECONSUMER", "st", "g", "c1"])), Reply::Integer(0), "already existed");
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "st", ">"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["XGROUP", "DELCONSUMER", "st", "g", "c1"])), Reply::Integer(1), "one pending entry removed with it");
    }

    #[test]
    fn xinfo_groups_and_consumers_report_real_state() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        s.dispatch(&mut c, &argv(&["XGROUP", "CREATE", "st", "g", "0"]));
        s.dispatch(&mut c, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "STREAMS", "st", ">"]));

        let Reply::Array(groups) = s.dispatch(&mut c, &argv(&["XINFO", "GROUPS", "st"])) else { panic!("expected array") };
        let Reply::Map(ginfo) = &groups[0] else { panic!("expected map") };
        let get = |info: &[(Reply, Reply)], key: &[u8]| info.iter().find(|(k, _)| matches!(k, Reply::BulkString(b) if b == key)).map(|(_, v)| v.clone());
        assert_eq!(get(ginfo, b"name"), Some(Reply::bulk(b"g".to_vec())));
        assert_eq!(get(ginfo, b"consumers"), Some(Reply::Integer(1)));
        assert_eq!(get(ginfo, b"pending"), Some(Reply::Integer(1)));

        let Reply::Array(consumers) = s.dispatch(&mut c, &argv(&["XINFO", "CONSUMERS", "st", "g"])) else { panic!("expected array") };
        let Reply::Map(cinfo) = &consumers[0] else { panic!("expected map") };
        assert_eq!(get(cinfo, b"name"), Some(Reply::bulk(b"cons1".to_vec())));
        assert_eq!(get(cinfo, b"pending"), Some(Reply::Integer(1)));

        assert!(matches!(s.dispatch(&mut c, &argv(&["XINFO", "CONSUMERS", "st", "nope"])), Reply::Error(e) if e.starts_with("NOGROUP")));
    }

    #[test]
    fn memory_usage_reports_null_for_missing_key_and_a_plausible_size_otherwise() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["MEMORY", "USAGE", "missing"])), Reply::Null);
        s.dispatch(&mut c, &argv(&["SET", "k", "hello world"]));
        let Reply::Integer(size) = s.dispatch(&mut c, &argv(&["MEMORY", "USAGE", "k"])) else { panic!("expected integer") };
        assert!(size >= "hello world".len() as i64, "got {size}");
    }

    #[test]
    fn memory_stats_reports_used_memory_and_key_count() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k1", "v"]));
        s.dispatch(&mut c, &argv(&["SET", "k2", "v"]));
        match s.dispatch(&mut c, &argv(&["MEMORY", "STATS"])) {
            Reply::Map(pairs) => {
                let get = |name: &[u8]| pairs.iter().find(|(k, _)| *k == Reply::bulk(name.to_vec())).map(|(_, v)| v.clone());
                assert_eq!(get(b"keys.count"), Some(Reply::Integer(2)));
                assert!(matches!(get(b"used_memory"), Some(Reply::Integer(n)) if n > 0));
                // used_memory_rss/mem_fragmentation_ratio are only present
                // when the OS actually reports an RSS for this process —
                // not asserted on directly since that's platform-dependent,
                // but when present they must be internally consistent.
                if let (Some(Reply::Integer(rss)), Some(Reply::Integer(used))) = (get(b"used_memory_rss"), get(b"used_memory")) {
                    assert!(rss > 0 && used > 0);
                }
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn memory_doctor_never_errors_and_recommends_a_real_mitigation() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        match s.dispatch(&mut c, &argv(&["MEMORY", "DOCTOR"])) {
            // A unit test's own process has a tiny keyspace against a large
            // binary's RSS, so the "high fragmentation" branch is the one
            // that will actually fire wherever `rss_bytes()` works at all —
            // with `activedefrag` still at its default `no`, it recommends
            // turning that on (or a one-off `MEMORY PURGE`) before falling
            // back to a restart.
            Reply::BulkString(text) => {
                let text = String::from_utf8(text).unwrap();
                assert!(text.contains("restart") || text.contains("activedefrag") || text.contains("couldn't be determined"), "{text}");
            }
            other => panic!("expected a bulk string, got {other:?}"),
        }
    }

    #[test]
    fn memory_purge_defrags_without_changing_any_data() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        // Build up a listpack-backed list with a lot of churn (grow big, then
        // shrink to 1 element) — exactly the shape `defrag_in_place` exists
        // to reclaim.
        for i in 0..2000 {
            s.dispatch(&mut c, &argv(&["RPUSH", "biglist", &format!("item-{i}-padding-padding")]));
        }
        for _ in 0..1999 {
            s.dispatch(&mut c, &argv(&["LPOP", "biglist"]));
        }
        s.dispatch(&mut c, &argv(&["SET", "str", "hello"]));
        s.dispatch(&mut c, &argv(&["SADD", "intset", "1", "2", "3"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["MEMORY", "PURGE"])), Reply::ok());

        // Data must be completely unaffected by defragging it.
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "biglist", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"item-1999-padding-padding".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "str"])), Reply::bulk(b"hello".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["SCARD", "intset"])), Reply::Integer(3));
        // The defrag cursor must be reset to the start of a fresh lap, not
        // left mid-lap against a keyspace that was just fully defragged.
        assert_eq!(s.defrag_cursors[0], 0);
    }

    #[test]
    fn memory_malloc_stats_always_returns_ok() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["MEMORY", "MALLOC-STATS"])), Reply::ok());
    }

    #[test]
    fn debug_object_reports_encoding_and_idletime_for_a_real_key_and_errors_for_a_missing_one() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["DEBUG", "OBJECT", "missing"])), Reply::Error(e) if e.contains("no such key")));

        s.dispatch(&mut c, &argv(&["SET", "k", "hello"]));
        match s.dispatch(&mut c, &argv(&["DEBUG", "OBJECT", "k"])) {
            Reply::SimpleString(line) => {
                assert!(line.contains("encoding:embstr"), "{line}");
                assert!(line.contains("serializedlength:"), "{line}");
                assert!(line.contains("lru_seconds_idle:"), "{line}");
                assert!(!line.contains("ql_nodes"), "a string must not report list-only fields: {line}");
            }
            other => panic!("expected a simple string, got {other:?}"),
        }

        s.dispatch(&mut c, &argv(&["RPUSH", "l", "a", "b", "c"]));
        match s.dispatch(&mut c, &argv(&["DEBUG", "OBJECT", "l"])) {
            Reply::SimpleString(line) => {
                assert!(line.contains("encoding:listpack"), "{line}");
                assert!(line.contains("ql_nodes:1"), "this fork's lists are always one listpack node: {line}");
                assert!(line.contains("ql_avg_node:3"), "a 3-element list's single node should report length 3: {line}");
            }
            other => panic!("expected a simple string, got {other:?}"),
        }
    }

    #[test]
    fn debug_set_active_expire_off_disables_ttl_enforcement_until_turned_back_on() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        // Disable active-expire *before* the TTL is even set, so there's no
        // race window where a sweep could fire while it's still enabled —
        // a short-TTL key is inherently timing-sensitive otherwise (see the
        // similarly-shaped `active_expire_sweep_hides_key_and_propagates_a_del`
        // test's own margin for the same reason).
        assert_eq!(s.dispatch(&mut c, &argv(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"])), Reply::ok());
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["PEXPIRE", "k", "1"]));
        std::thread::sleep(std::time::Duration::from_millis(5));
        // The key's TTL is long past, but with active-expire off this fork
        // has no other mechanism to notice — it must still be there.
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(1), "TTL enforcement must be fully suspended while active-expire is off");

        assert_eq!(s.dispatch(&mut c, &argv(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0), "turning it back on must resume sweeping the now-long-expired key");
    }

    #[test]
    fn config_set_accepts_activedefrag_and_threshold() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["CONFIG", "SET", "activedefrag", "yes"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["CONFIG", "SET", "active-defrag-threshold", "1.2"])), Reply::ok());
        match s.dispatch(&mut c, &argv(&["CONFIG", "GET", "activedefrag"])) {
            Reply::Map(pairs) => assert_eq!(pairs, vec![(Reply::bulk(b"activedefrag".to_vec()), Reply::bulk(b"yes".to_vec()))]),
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn active_defrag_runs_a_bounded_step_when_enabled_and_fragmented() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert!(s.last_defrag_check.is_none(), "activedefrag is still off, no check should have run yet");

        // Enabling it is itself a command, so `maybe_run_active_defrag` runs
        // for it too — but sees the *pre*-this-command config value (`no`,
        // same ordering `sweep_expired` already relies on), so it still
        // shouldn't check yet.
        s.dispatch(&mut c, &argv(&["CONFIG", "SET", "activedefrag", "yes"]));
        assert!(s.last_defrag_check.is_none(), "the SET command that turns it on must not itself trigger a check");

        // The next command sees `activedefrag yes` already in effect, so it
        // must run (and throttle-record) exactly one check. A tiny keyspace
        // against this test binary's own RSS all but guarantees `mem_
        // fragmentation_ratio` clears the default 1.5 threshold wherever
        // `rss_bytes()` works at all on this platform/sandbox.
        s.dispatch(&mut c, &argv(&["GET", "k"]));

        if let Some(first_check) = s.last_defrag_check {
            // A second command within the same 1s throttle window must not
            // run another check.
            s.dispatch(&mut c, &argv(&["GET", "k"]));
            assert_eq!(s.last_defrag_check, Some(first_check), "the check must be throttled, not re-run on every command");
        }
        // Data must still be intact regardless of whether defrag ran.
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v".to_vec()));
    }

    #[test]
    fn active_defrag_is_a_no_op_when_disabled() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        // `activedefrag` defaults to `no` — no check should ever run.
        s.dispatch(&mut c, &argv(&["GET", "k"]));
        assert!(s.last_defrag_check.is_none());
    }

    #[test]
    fn defrag_in_place_preserves_every_rtype_shape() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "l", "a", "b", "c"]));
        s.dispatch(&mut c, &argv(&["HSET", "h", "f1", "v1", "f2", "v2"]));
        s.dispatch(&mut c, &argv(&["SADD", "si", "1", "2", "3"]));
        s.dispatch(&mut c, &argv(&["SADD", "ss", "a", "b", "c"]));
        s.dispatch(&mut c, &argv(&["ZADD", "z", "1", "a", "2", "b"]));

        force_full_defrag(&mut s);

        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "l", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"a".to_vec()), Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["HGET", "h", "f1"])), Reply::bulk(b"v1".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["SCARD", "si"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["SCARD", "ss"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "z", "b"])), Reply::bulk(b"2".to_vec()));
    }

    #[test]
    fn function_flush_dispatch_accepts_async_sync_modifier_and_rejects_garbage() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["FUNCTION", "LOAD", "#!lua name=lib1\nredis.register_function('f1', function(keys, args) return 1 end)"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["FUNCTION", "FLUSH", "GARBAGE"])), Reply::Error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option".to_string()));

        s.dispatch(&mut c, &argv(&["FUNCTION", "LOAD", "#!lua name=lib2\nredis.register_function('f2', function(keys, args) return 2 end)"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["FUNCTION", "FLUSH", "ASYNC"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["FCALL", "f2", "0"])), Reply::Error("ERR Function not found".to_string()));

        // Bare `FUNCTION FLUSH` (no modifier) must also work.
        s.dispatch(&mut c, &argv(&["FUNCTION", "LOAD", "#!lua name=lib3\nredis.register_function('f3', function(keys, args) return 3 end)"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["FUNCTION", "FLUSH"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["FCALL", "f3", "0"])), Reply::Error("ERR Function not found".to_string()));
    }

    #[test]
    fn fcall_ro_rejects_writes_but_fcall_allows_them() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let lib = "#!lua name=writelib\nredis.register_function('dowrite', function(keys, args) return redis.call('SET', keys[1], 'v') end)";
        s.dispatch(&mut c, &argv(&["FUNCTION", "LOAD", lib]));

        let Reply::Error(msg) = s.dispatch(&mut c, &argv(&["FCALL_RO", "dowrite", "1", "k"])) else { panic!("expected error") };
        assert!(msg.contains("read-only") || msg.contains("Write commands"), "got: {msg}");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0), "the write must not have gone through");

        assert_eq!(s.dispatch(&mut c, &argv(&["FCALL", "dowrite", "1", "k"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(1));
    }

    #[test]
    fn fcall_ro_still_allows_reads() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        let lib = "#!lua name=readlib\nredis.register_function('doread', function(keys, args) return redis.call('GET', keys[1]) end)";
        s.dispatch(&mut c, &argv(&["FUNCTION", "LOAD", lib]));
        assert_eq!(s.dispatch(&mut c, &argv(&["FCALL_RO", "doread", "1", "k"])), Reply::bulk(b"v".to_vec()));
    }

    #[test]
    fn reset_clears_multi_subscriptions_tracking_and_db_selection() {
        let mut s = Server::new(3);
        let mut c = ClientState { id: 1, resp: 3, ..ClientState::default() };
        s.dispatch(&mut c, &argv(&["SELECT", "2"]));
        s.dispatch(&mut c, &argv(&["MULTI"]));
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        s.dispatch(&mut c, &argv(&["SUBSCRIBE", "ch"]));
        s.dispatch(&mut c, &argv(&["CLIENT", "TRACKING", "ON"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["RESET"])), Reply::SimpleString("RESET".to_string()));

        assert_eq!(c.db_index, 0);
        assert!(!c.in_multi);
        assert!(c.queued.is_empty());
        assert!(!c.tracking);
        assert!(c.subscribed_channels.is_empty());
        assert!(s.channel_subs.is_empty(), "server-side subscription map should be scrubbed too");

        // MULTI state was discarded, not just emptied — a plain command now
        // runs immediately instead of queuing.
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::Null, "db 0 (reselected), key never actually got set");
    }

    #[test]
    fn reset_bypasses_multi_queueing_like_quit() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["MULTI"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["RESET"])), Reply::SimpleString("RESET".to_string()));
        assert!(!c.in_multi);
    }

    #[test]
    fn object_encoding_reports_the_live_encoding() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "intstr", "123"]));
        s.dispatch(&mut c, &argv(&["SET", "shortstr", "hello"]));
        s.dispatch(&mut c, &argv(&["RPUSH", "alist", "a"]));
        s.dispatch(&mut c, &argv(&["SADD", "intset", "1", "2"]));
        s.dispatch(&mut c, &argv(&["SADD", "strset", "a", "b"]));
        s.dispatch(&mut c, &argv(&["ZADD", "azset", "1", "a"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["OBJECT", "ENCODING", "intstr"])), Reply::bulk(b"int".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["OBJECT", "ENCODING", "shortstr"])), Reply::bulk(b"embstr".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["OBJECT", "ENCODING", "alist"])), Reply::bulk(b"listpack".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["OBJECT", "ENCODING", "intset"])), Reply::bulk(b"intset".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["OBJECT", "ENCODING", "strset"])), Reply::bulk(b"hashtable".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["OBJECT", "ENCODING", "azset"])), Reply::bulk(b"listpack".to_vec()));
    }

    // ------------------------------------------------- generic key commands ---

    #[test]
    fn keys_and_randomkey() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "a:1", "x"]));
        s.dispatch(&mut c, &argv(&["SET", "a:2", "y"]));
        s.dispatch(&mut c, &argv(&["SET", "b:1", "z"]));
        let Reply::Array(mut found) = s.dispatch(&mut c, &argv(&["KEYS", "a:*"])) else { panic!("expected array") };
        let mut names: Vec<Vec<u8>> = found.drain(..).map(|r| if let Reply::BulkString(b) = r { b } else { panic!() }).collect();
        names.sort();
        assert_eq!(names, vec![b"a:1".to_vec(), b"a:2".to_vec()]);

        let Reply::BulkString(rk) = s.dispatch(&mut c, &argv(&["RANDOMKEY"])) else { panic!("expected bulk") };
        assert!(["a:1", "a:2", "b:1"].iter().any(|k| k.as_bytes() == rk));

        let mut empty = Server::new(1);
        assert_eq!(empty.dispatch(&mut ClientState::default(), &argv(&["RANDOMKEY"])), Reply::Null);
    }

    #[test]
    fn touch_and_unlink() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k1", "v"]));
        s.dispatch(&mut c, &argv(&["SET", "k2", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["TOUCH", "k1", "k2", "missing"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["UNLINK", "k1", "missing"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k1"])), Reply::Integer(0));
    }

    #[test]
    fn rename_and_renamenx() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "src", "v"]));
        s.dispatch(&mut c, &argv(&["EXPIRE", "src", "100"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["RENAME", "src", "dst"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "dst"])), Reply::bulk(b"v".to_vec()));
        let Reply::Integer(ttl) = s.dispatch(&mut c, &argv(&["TTL", "dst"])) else { panic!("expected integer") };
        assert!((99..=100).contains(&ttl), "TTL should survive the rename, got {ttl}");
        assert!(matches!(s.dispatch(&mut c, &argv(&["RENAME", "nope", "other"])), Reply::Error(_)));

        s.dispatch(&mut c, &argv(&["SET", "a", "1"]));
        s.dispatch(&mut c, &argv(&["SET", "b", "2"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["RENAMENX", "a", "b"])), Reply::Integer(0), "b already exists");
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "b"])), Reply::bulk(b"2".to_vec()));
    }

    #[test]
    fn copy_deep_clones_and_respects_replace() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "src", "a", "b"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["COPY", "src", "dst"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "dst", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"a".to_vec()), Reply::bulk(b"b".to_vec())]));

        // Independent copy: mutating the destination doesn't touch the source.
        s.dispatch(&mut c, &argv(&["RPUSH", "dst", "c"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LLEN", "src"])), Reply::Integer(2));

        assert_eq!(s.dispatch(&mut c, &argv(&["COPY", "src", "dst"])), Reply::Integer(0), "dst exists, no REPLACE");
        assert_eq!(s.dispatch(&mut c, &argv(&["COPY", "src", "dst", "REPLACE"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["LLEN", "dst"])), Reply::Integer(2), "replaced with a fresh copy of src");
    }

    #[test]
    fn move_transfers_a_key_between_databases() {
        let mut s = Server::new(2);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["MOVE", "k", "1"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0));
        s.dispatch(&mut c, &argv(&["SELECT", "1"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"v".to_vec()));
    }

    #[test]
    fn scan_eventually_visits_every_key() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        for i in 0..50 {
            s.dispatch(&mut c, &argv(&["SET", &format!("k{i}"), "v"]));
        }
        let mut seen = std::collections::HashSet::new();
        let mut cursor = "0".to_string();
        loop {
            let Reply::Array(parts) = s.dispatch(&mut c, &argv(&["SCAN", &cursor])) else { panic!("expected array") };
            let Reply::BulkString(next) = &parts[0] else { panic!() };
            cursor = String::from_utf8_lossy(next).into_owned();
            let Reply::Array(items) = &parts[1] else { panic!() };
            for item in items {
                if let Reply::BulkString(k) = item {
                    seen.insert(k.clone());
                }
            }
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), 50);
    }

    #[test]
    fn hscan_sscan_zscan_support_match() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["HSET", "h", "foo", "1", "bar", "2"]));
        let Reply::Array(parts) = s.dispatch(&mut c, &argv(&["HSCAN", "h", "0", "MATCH", "foo"])) else { panic!("expected array") };
        let Reply::Array(items) = &parts[1] else { panic!() };
        assert_eq!(items, &vec![Reply::bulk(b"foo".to_vec()), Reply::bulk(b"1".to_vec())]);

        s.dispatch(&mut c, &argv(&["SADD", "myset", "x", "y"]));
        let Reply::Array(parts) = s.dispatch(&mut c, &argv(&["SSCAN", "myset", "0"])) else { panic!("expected array") };
        let Reply::Array(items) = &parts[1] else { panic!() };
        assert_eq!(items.len(), 2);

        s.dispatch(&mut c, &argv(&["ZADD", "myzset", "1", "a"]));
        let Reply::Array(parts) = s.dispatch(&mut c, &argv(&["ZSCAN", "myzset", "0"])) else { panic!("expected array") };
        let Reply::Array(items) = &parts[1] else { panic!() };
        assert_eq!(items, &vec![Reply::bulk(b"a".to_vec()), Reply::bulk(b"1".to_vec())]);
    }

    // ------------------------------------------------------ string commands ---

    #[test]
    fn incrby_decr_decrby_and_incrbyfloat() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["INCRBY", "n", "5"])), Reply::Integer(5));
        assert_eq!(s.dispatch(&mut c, &argv(&["DECR", "n"])), Reply::Integer(4));
        assert_eq!(s.dispatch(&mut c, &argv(&["DECRBY", "n", "4"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["INCRBYFLOAT", "f", "10.5"])), Reply::bulk(b"10.5".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["INCRBYFLOAT", "f", "0.1"])), Reply::bulk(b"10.6".to_vec()));
        s.dispatch(&mut c, &argv(&["SET", "notnum", "abc"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["INCRBY", "notnum", "1"])), Reply::Error(_)));
    }

    #[test]
    fn append_and_strlen() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["APPEND", "k", "hello"])), Reply::Integer(5));
        assert_eq!(s.dispatch(&mut c, &argv(&["APPEND", "k", " world"])), Reply::Integer(11));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"hello world".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["STRLEN", "k"])), Reply::Integer(11));
        assert_eq!(s.dispatch(&mut c, &argv(&["STRLEN", "missing"])), Reply::Integer(0));
    }

    #[test]
    fn getrange_and_setrange() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "Hello World"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETRANGE", "k", "0", "4"])), Reply::bulk(b"Hello".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETRANGE", "k", "-5", "-1"])), Reply::bulk(b"World".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETRANGE", "missing", "0", "-1"])), Reply::bulk(Vec::new()));

        assert_eq!(s.dispatch(&mut c, &argv(&["SETRANGE", "k", "6", "Redis"])), Reply::Integer(11));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "k"])), Reply::bulk(b"Hello Redis".to_vec()));

        assert_eq!(s.dispatch(&mut c, &argv(&["SETRANGE", "pad", "5", "xy"])), Reply::Integer(7));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "pad"])), Reply::bulk(b"\0\0\0\0\0xy".to_vec()));
    }

    #[test]
    fn mget_mset_msetnx() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["MSET", "a", "1", "b", "2"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["MGET", "a", "b", "missing"])), Reply::Array(vec![Reply::bulk(b"1".to_vec()), Reply::bulk(b"2".to_vec()), Reply::Null]));

        assert_eq!(s.dispatch(&mut c, &argv(&["MSETNX", "b", "3", "c", "4"])), Reply::Integer(0), "b already exists, nothing should be set");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "c"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["MSETNX", "c", "4", "d", "5"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["GET", "d"])), Reply::bulk(b"5".to_vec()));
    }

    #[test]
    fn getdel_and_getex() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETDEL", "k"])), Reply::bulk(b"v".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "k"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETDEL", "k"])), Reply::Null);

        s.dispatch(&mut c, &argv(&["SET", "k2", "v2"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETEX", "k2", "EX", "100"])), Reply::bulk(b"v2".to_vec()));
        let Reply::Integer(ttl) = s.dispatch(&mut c, &argv(&["TTL", "k2"])) else { panic!("expected integer") };
        assert!((99..=100).contains(&ttl));
        assert_eq!(s.dispatch(&mut c, &argv(&["GETEX", "k2", "PERSIST"])), Reply::bulk(b"v2".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["TTL", "k2"])), Reply::Integer(-1));
    }

    // -------------------------------------------------------- list commands ---

    #[test]
    fn lset_and_linsert() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "l", "a", "c"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LSET", "l", "0", "A"])), Reply::ok());
        assert!(matches!(s.dispatch(&mut c, &argv(&["LSET", "l", "99", "x"])), Reply::Error(_)));
        assert_eq!(s.dispatch(&mut c, &argv(&["LINSERT", "l", "BEFORE", "c", "b"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "l", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"A".to_vec()), Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LINSERT", "l", "AFTER", "nope", "x"])), Reply::Integer(-1));
        assert_eq!(s.dispatch(&mut c, &argv(&["LINSERT", "missing", "AFTER", "x", "y"])), Reply::Integer(0));
    }

    #[test]
    fn lrem_and_ltrim() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "l", "a", "b", "a", "c", "a"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LREM", "l", "2", "a"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "l", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec()), Reply::bulk(b"a".to_vec())]));

        s.dispatch(&mut c, &argv(&["RPUSH", "l2", "a", "b", "c", "d", "e"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LTRIM", "l2", "1", "-2"])), Reply::ok());
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "l2", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec()), Reply::bulk(b"d".to_vec())]));
    }

    #[test]
    fn lpos_rank_count_and_maxlen() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "l", "a", "b", "c", "a", "b", "c", "a"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LPOS", "l", "a"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["LPOS", "l", "a", "RANK", "2"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["LPOS", "l", "a", "RANK", "-1"])), Reply::Integer(6));
        assert_eq!(s.dispatch(&mut c, &argv(&["LPOS", "l", "a", "COUNT", "0"])), Reply::Array(vec![Reply::Integer(0), Reply::Integer(3), Reply::Integer(6)]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LPOS", "l", "z"])), Reply::Null);
        assert_eq!(s.dispatch(&mut c, &argv(&["LPOS", "l", "z", "COUNT", "0"])), Reply::Array(vec![]));
    }

    #[test]
    fn lmove_and_rpoplpush() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "src", "a", "b", "c"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LMOVE", "src", "dst", "LEFT", "RIGHT"])), Reply::bulk(b"a".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "dst", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"a".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["RPOPLPUSH", "src", "dst"])), Reply::bulk(b"c".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "dst", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"c".to_vec()), Reply::bulk(b"a".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "src", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"b".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LMOVE", "missing", "dst", "LEFT", "LEFT"])), Reply::Null);

        // Same-key rotation: RPOPLPUSH src src rotates the list.
        s.dispatch(&mut c, &argv(&["DEL", "rot"]));
        s.dispatch(&mut c, &argv(&["RPUSH", "rot", "1", "2", "3"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["RPOPLPUSH", "rot", "rot"])), Reply::bulk(b"3".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "rot", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"3".to_vec()), Reply::bulk(b"1".to_vec()), Reply::bulk(b"2".to_vec())]));
    }

    // ----------------------------------------------------- blocking commands ---
    // `try_blocking_command`/`take_unblocked`/`check_blocking_timeouts` are
    // what the binary layer actually calls (see their own doc comments for
    // why this bypasses `dispatch()`) — these tests call them directly
    // rather than through `dispatch`, the same way the real binary would.

    #[test]
    fn blpop_returns_immediately_when_data_already_exists() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "k", "a", "b"]));
        let BlockingOutcome::Immediate(reply) = s.try_blocking_command(&mut c, &argv(&["BLPOP", "k", "0"])) else { panic!("expected immediate") };
        assert_eq!(reply, Reply::Array(vec![Reply::bulk(b"k".to_vec()), Reply::bulk(b"a".to_vec())]));
    }

    #[test]
    fn blpop_checks_keys_in_argument_order() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "k2", "only-here"]));
        let BlockingOutcome::Immediate(reply) = s.try_blocking_command(&mut c, &argv(&["BLPOP", "k1", "k2", "0"])) else { panic!("expected immediate") };
        assert_eq!(reply, Reply::Array(vec![Reply::bulk(b"k2".to_vec()), Reply::bulk(b"only-here".to_vec())]));
    }

    #[test]
    fn blpop_blocks_then_wakes_on_a_push_from_another_client() {
        let mut s = Server::new(1);
        let mut blocked_client = ClientState { id: 1, ..ClientState::default() };
        let outcome = s.try_blocking_command(&mut blocked_client, &argv(&["BLPOP", "k", "0"]));
        assert!(matches!(outcome, BlockingOutcome::Blocked));
        assert!(s.take_unblocked().is_empty(), "nothing to deliver yet");

        let mut pusher = ClientState { id: 2, ..ClientState::default() };
        s.dispatch(&mut pusher, &argv(&["RPUSH", "k", "value"]));

        let unblocked = s.take_unblocked();
        assert_eq!(unblocked, vec![(1, Reply::Array(vec![Reply::bulk(b"k".to_vec()), Reply::bulk(b"value".to_vec())]))]);
        // The key must be empty again — the wake consumed the one pushed element.
        assert_eq!(s.dispatch(&mut pusher, &argv(&["LLEN", "k"])), Reply::Integer(0));
    }

    #[test]
    fn multiple_waiters_on_the_same_key_are_served_fifo() {
        let mut s = Server::new(1);
        let mut first = ClientState { id: 1, ..ClientState::default() };
        let mut second = ClientState { id: 2, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut first, &argv(&["BLPOP", "k", "0"])), BlockingOutcome::Blocked));
        assert!(matches!(s.try_blocking_command(&mut second, &argv(&["BLPOP", "k", "0"])), BlockingOutcome::Blocked));

        let mut pusher = ClientState { id: 3, ..ClientState::default() };
        s.dispatch(&mut pusher, &argv(&["RPUSH", "k", "only-one-value"]));
        let unblocked = s.take_unblocked();
        assert_eq!(unblocked, vec![(1, Reply::Array(vec![Reply::bulk(b"k".to_vec()), Reply::bulk(b"only-one-value".to_vec())]))], "the first (oldest) waiter gets it, not the second");

        s.dispatch(&mut pusher, &argv(&["RPUSH", "k", "second-value"]));
        assert_eq!(s.take_unblocked(), vec![(2, Reply::Array(vec![Reply::bulk(b"k".to_vec()), Reply::bulk(b"second-value".to_vec())]))]);
    }

    #[test]
    fn bzpopmin_blocks_then_wakes_with_member_and_score() {
        let mut s = Server::new(1);
        let mut blocked_client = ClientState { id: 1, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut blocked_client, &argv(&["BZPOPMIN", "zk", "0"])), BlockingOutcome::Blocked));

        let mut writer = ClientState { id: 2, ..ClientState::default() };
        s.dispatch(&mut writer, &argv(&["ZADD", "zk", "5", "member"]));
        assert_eq!(s.take_unblocked(), vec![(1, Reply::Array(vec![Reply::bulk(b"zk".to_vec()), Reply::bulk(b"member".to_vec()), Reply::bulk(b"5".to_vec())]))]);
    }

    #[test]
    fn blmove_blocks_then_wakes_and_moves_into_destination() {
        let mut s = Server::new(1);
        let mut blocked_client = ClientState { id: 1, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut blocked_client, &argv(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"])), BlockingOutcome::Blocked));

        let mut writer = ClientState { id: 2, ..ClientState::default() };
        s.dispatch(&mut writer, &argv(&["RPUSH", "src", "value"]));
        assert_eq!(s.take_unblocked(), vec![(1, Reply::bulk(b"value".to_vec()))]);
        assert_eq!(s.dispatch(&mut writer, &argv(&["LRANGE", "dst", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"value".to_vec())]));
    }

    #[test]
    fn brpoplpush_is_blmove_right_to_left() {
        let mut s = Server::new(1);
        let mut blocked_client = ClientState { id: 1, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut blocked_client, &argv(&["BRPOPLPUSH", "src", "dst", "0"])), BlockingOutcome::Blocked));
        let mut writer = ClientState { id: 2, ..ClientState::default() };
        s.dispatch(&mut writer, &argv(&["RPUSH", "src", "a", "b"]));
        assert_eq!(s.take_unblocked(), vec![(1, Reply::bulk(b"b".to_vec()))], "RPOPLPUSH semantics: pop from the right");
    }

    #[test]
    fn blocking_command_on_the_wrong_type_errors_immediately_without_blocking() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        let BlockingOutcome::Immediate(reply) = s.try_blocking_command(&mut c, &argv(&["BLPOP", "k", "0"])) else { panic!("expected immediate") };
        assert!(matches!(reply, Reply::Error(_)));
    }

    #[test]
    fn blocking_timeout_produces_the_right_null_shape_and_unregisters() {
        let mut s = Server::new(1);
        let mut blpop_client = ClientState { id: 1, ..ClientState::default() };
        let mut blmove_client = ClientState { id: 2, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut blpop_client, &argv(&["BLPOP", "k1", "0.01"])), BlockingOutcome::Blocked));
        assert!(matches!(s.try_blocking_command(&mut blmove_client, &argv(&["BLMOVE", "k2", "dst", "LEFT", "LEFT", "0.01"])), BlockingOutcome::Blocked));

        s.check_blocking_timeouts(now_ms() + 50);
        let mut unblocked = s.take_unblocked();
        unblocked.sort_by_key(|(id, _)| *id);
        assert_eq!(unblocked, vec![(1, Reply::NullArray), (2, Reply::NullBulkString)]);

        // Timed out clients are fully unregistered — a later write to the
        // same key must not try to wake them again.
        let mut writer = ClientState::default();
        s.dispatch(&mut writer, &argv(&["RPUSH", "k1", "late"]));
        assert!(s.take_unblocked().is_empty());
    }

    #[test]
    fn disconnecting_a_blocked_client_removes_it_from_the_waiting_list() {
        let mut s = Server::new(1);
        let mut blocked_client = ClientState { id: 42, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut blocked_client, &argv(&["BLPOP", "k", "0"])), BlockingOutcome::Blocked));
        s.client_disconnected(42);
        let mut writer = ClientState::default();
        s.dispatch(&mut writer, &argv(&["RPUSH", "k", "value"]));
        assert!(s.take_unblocked().is_empty(), "a disconnected client must never receive a late wake-up reply");
    }

    #[test]
    fn blpop_rejects_a_negative_timeout() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let BlockingOutcome::Immediate(reply) = s.try_blocking_command(&mut c, &argv(&["BLPOP", "k", "-1"])) else { panic!("expected immediate") };
        assert!(matches!(reply, Reply::Error(_)));
    }

    #[test]
    fn blocking_commands_never_actually_block_inside_multi_exec() {
        // Real Redis's rule: a blocking command queued inside MULTI/EXEC (or
        // called from a script) resolves immediately instead of blocking —
        // `dispatch`/`execute_single` never go through the binary layer's
        // `try_blocking_command` interception at all, only the registered
        // non-blocking `COMMANDS` handler.
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["MULTI"]));
        s.dispatch(&mut c, &argv(&["BLPOP", "empty-key", "0"]));
        s.dispatch(&mut c, &argv(&["BZPOPMIN", "empty-zkey", "0"]));
        s.dispatch(&mut c, &argv(&["BLMOVE", "empty-src", "dst", "LEFT", "LEFT", "0"]));
        let Reply::Array(results) = s.dispatch(&mut c, &argv(&["EXEC"])) else { panic!("expected array") };
        assert_eq!(results, vec![Reply::NullArray, Reply::NullArray, Reply::NullBulkString], "every blocking command resolves to its timeout reply immediately, not actually blocking the whole server");
        assert!(s.blocked.is_empty(), "nothing should have been registered as blocked");
    }

    #[test]
    fn blpop_inside_multi_exec_still_pops_real_data_when_available() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["RPUSH", "k", "v"]));
        s.dispatch(&mut c, &argv(&["MULTI"]));
        s.dispatch(&mut c, &argv(&["BLPOP", "k", "0"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXEC"])), Reply::Array(vec![Reply::Array(vec![Reply::bulk(b"k".to_vec()), Reply::bulk(b"v".to_vec())])]));
    }

    #[test]
    fn xread_block_returns_immediately_when_a_matching_entry_already_exists() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        let BlockingOutcome::Immediate(reply) = s.try_blocking_command(&mut c, &argv(&["XREAD", "BLOCK", "0", "STREAMS", "st", "0"])) else {
            panic!("expected immediate")
        };
        let Reply::Array(streams) = reply else { panic!("expected array") };
        assert_eq!(streams[0], Reply::Array(vec![Reply::bulk(b"st".to_vec()), stream_entries_to_reply(vec![(crate::stream::StreamId { ms: 1, seq: 0 }, vec![(b"f".to_vec(), b"a".to_vec())])])]));
    }

    #[test]
    fn xread_block_dollar_resolves_once_then_wakes_on_a_later_xadd() {
        let mut s = Server::new(1);
        let mut blocked_client = ClientState { id: 1, ..ClientState::default() };
        let mut pusher = ClientState { id: 2, ..ClientState::default() };
        s.dispatch(&mut pusher, &argv(&["XADD", "st", "1-0", "f", "already-here"]));

        // `$` resolves to "1-0" right now, so this blocks (nothing newer yet).
        assert!(matches!(s.try_blocking_command(&mut blocked_client, &argv(&["XREAD", "BLOCK", "0", "STREAMS", "st", "$"])), BlockingOutcome::Blocked));
        assert!(s.blocked.contains_key(&1));

        s.dispatch(&mut pusher, &argv(&["XADD", "st", "2-0", "f", "new"]));
        let unblocked = s.take_unblocked();
        assert_eq!(unblocked.len(), 1);
        assert_eq!(unblocked[0].0, 1);
        let Reply::Array(streams) = &unblocked[0].1 else { panic!("expected array") };
        let Reply::Array(entry_pair) = &streams[0] else { panic!("expected array") };
        let Reply::Array(entries) = &entry_pair[1] else { panic!("expected array") };
        assert_eq!(entries.len(), 1, "only the entry added after blocking, not the one that already existed");
    }

    #[test]
    fn xread_block_on_wrong_type_errors_immediately_without_blocking() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "notastream", "v"]));
        let BlockingOutcome::Immediate(reply) = s.try_blocking_command(&mut c, &argv(&["XREAD", "BLOCK", "0", "STREAMS", "notastream", "$"])) else {
            panic!("expected immediate")
        };
        assert!(matches!(reply, Reply::Error(_)));
    }

    #[test]
    fn xread_without_block_option_is_not_routed_through_the_blocking_path() {
        // `wants_block_option` (the binary layer's routing decision) isn't
        // reachable from a unit test, but `try_blocking_command` itself
        // should never be called for a plain `XREAD` in the first place —
        // this just documents that a non-blocking XREAD argv still behaves
        // sanely if it ever were (it should never actually block).
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["XREAD", "STREAMS", "missing", "0"])), Reply::NullArray);
    }

    #[test]
    fn xreadgroup_block_wakes_on_a_new_entry_and_tracks_the_pel() {
        let mut s = Server::new(1);
        s.dispatch(&mut ClientState::default(), &argv(&["XGROUP", "CREATE", "st", "g", "$", "MKSTREAM"]));
        let mut blocked_client = ClientState { id: 1, ..ClientState::default() };
        let mut pusher = ClientState { id: 2, ..ClientState::default() };

        assert!(matches!(
            s.try_blocking_command(&mut blocked_client, &argv(&["XREADGROUP", "GROUP", "g", "cons1", "BLOCK", "0", "STREAMS", "st", ">"])),
            BlockingOutcome::Blocked
        ));
        s.dispatch(&mut pusher, &argv(&["XADD", "st", "*", "f", "v"]));
        let unblocked = s.take_unblocked();
        assert_eq!(unblocked.len(), 1);
        assert_eq!(unblocked[0].0, 1);
        assert!(!matches!(unblocked[0].1, Reply::NullArray), "the new entry should have been delivered");

        let Reply::Array(pending) = s.dispatch(&mut ClientState::default(), &argv(&["XPENDING", "st", "g"])) else { panic!("expected array") };
        assert_eq!(pending[0], Reply::Integer(1), "delivered via the blocking path still lands in the PEL");
    }

    #[test]
    fn xreadgroup_block_missing_group_errors_immediately_without_blocking() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["XADD", "st", "1-0", "f", "a"]));
        let BlockingOutcome::Immediate(reply) =
            s.try_blocking_command(&mut c, &argv(&["XREADGROUP", "GROUP", "nope", "cons1", "BLOCK", "0", "STREAMS", "st", ">"]))
        else {
            panic!("expected immediate")
        };
        assert!(matches!(reply, Reply::Error(e) if e.starts_with("NOGROUP")));
    }

    #[test]
    fn xread_block_timeout_produces_a_null_array() {
        let mut s = Server::new(1);
        let mut c = ClientState { id: 1, ..ClientState::default() };
        assert!(matches!(s.try_blocking_command(&mut c, &argv(&["XREAD", "BLOCK", "10", "STREAMS", "missing", "$"])), BlockingOutcome::Blocked));
        s.check_blocking_timeouts(now_ms() + 50);
        assert_eq!(s.take_unblocked(), vec![(1, Reply::NullArray)]);
    }

    // -------------------------------------------------------- hash commands ---

    #[test]
    fn hsetnx_and_hmget() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["HSETNX", "h", "f", "1"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["HSETNX", "h", "f", "2"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["HGET", "h", "f"])), Reply::bulk(b"1".to_vec()));
        s.dispatch(&mut c, &argv(&["HSET", "h", "g", "2"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["HMGET", "h", "f", "missing", "g"])), Reply::Array(vec![Reply::bulk(b"1".to_vec()), Reply::Null, Reply::bulk(b"2".to_vec())]));
    }

    #[test]
    fn hkeys_and_hvals() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["HSET", "h", "a", "1", "b", "2"]));
        let Reply::Array(mut keys) = s.dispatch(&mut c, &argv(&["HKEYS", "h"])) else { panic!("expected array") };
        let mut keys: Vec<Vec<u8>> = keys.drain(..).map(|r| if let Reply::BulkString(b) = r { b } else { panic!() }).collect();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        let Reply::Array(mut vals) = s.dispatch(&mut c, &argv(&["HVALS", "h"])) else { panic!("expected array") };
        let mut vals: Vec<Vec<u8>> = vals.drain(..).map(|r| if let Reply::BulkString(b) = r { b } else { panic!() }).collect();
        vals.sort();
        assert_eq!(vals, vec![b"1".to_vec(), b"2".to_vec()]);
    }

    #[test]
    fn hincrby_and_hincrbyfloat() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["HINCRBY", "h", "n", "5"])), Reply::Integer(5));
        assert_eq!(s.dispatch(&mut c, &argv(&["HINCRBY", "h", "n", "-2"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["HINCRBYFLOAT", "h", "f", "10.5"])), Reply::bulk(b"10.5".to_vec()));
        s.dispatch(&mut c, &argv(&["HSET", "h", "notnum", "abc"]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["HINCRBY", "h", "notnum", "1"])), Reply::Error(_)));
    }

    #[test]
    fn hrandfield_respects_count_sign_and_withvalues() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["HSET", "h", "a", "1", "b", "2", "c", "3"]));

        let Reply::BulkString(f) = s.dispatch(&mut c, &argv(&["HRANDFIELD", "h"])) else { panic!("expected bulk") };
        assert!(["a", "b", "c"].iter().any(|k| k.as_bytes() == f));

        let Reply::Array(distinct) = s.dispatch(&mut c, &argv(&["HRANDFIELD", "h", "10"])) else { panic!("expected array") };
        assert_eq!(distinct.len(), 3, "count >= field count caps at the distinct field count");

        let Reply::Array(with_repl) = s.dispatch(&mut c, &argv(&["HRANDFIELD", "h", "-5"])) else { panic!("expected array") };
        assert_eq!(with_repl.len(), 5, "negative count samples with replacement, exact length");

        let Reply::Array(pairs) = s.dispatch(&mut c, &argv(&["HRANDFIELD", "h", "2", "WITHVALUES"])) else { panic!("expected array") };
        assert_eq!(pairs.len(), 4, "WITHVALUES doubles the array length");

        assert_eq!(s.dispatch(&mut c, &argv(&["HRANDFIELD", "missing"])), Reply::Null);
        assert_eq!(s.dispatch(&mut c, &argv(&["HRANDFIELD", "missing", "3"])), Reply::Array(vec![]));
    }

    // --------------------------------------------------------- set commands ---

    fn sorted_bulk_strings(r: Reply) -> Vec<Vec<u8>> {
        let (Reply::Array(items) | Reply::Set(items)) = r else { panic!("expected array/set, got {r:?}") };
        let mut v: Vec<Vec<u8>> = items.into_iter().map(|i| if let Reply::BulkString(b) = i { b } else { panic!() }).collect();
        v.sort();
        v
    }

    #[test]
    fn smismember_spop_and_srandmember() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SADD", "s", "a", "b", "c"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SMISMEMBER", "s", "a", "z", "c"])), Reply::Array(vec![Reply::Integer(1), Reply::Integer(0), Reply::Integer(1)]));

        let Reply::BulkString(popped) = s.dispatch(&mut c, &argv(&["SPOP", "s"])) else { panic!("expected bulk") };
        assert!(["a", "b", "c"].iter().any(|m| m.as_bytes() == popped));
        assert_eq!(s.dispatch(&mut c, &argv(&["SCARD", "s"])), Reply::Integer(2));

        let left = sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SPOP", "s", "10"])));
        assert_eq!(left.len(), 2, "popping more than available just empties the set");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "s"])), Reply::Integer(0), "an emptied set key is removed");

        s.dispatch(&mut c, &argv(&["SADD", "s2", "x", "y", "z"]));
        let Reply::BulkString(rm) = s.dispatch(&mut c, &argv(&["SRANDMEMBER", "s2"])) else { panic!("expected bulk") };
        assert!(["x", "y", "z"].iter().any(|m| m.as_bytes() == rm));
        assert_eq!(s.dispatch(&mut c, &argv(&["SCARD", "s2"])), Reply::Integer(3), "SRANDMEMBER never removes");
        let distinct = sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SRANDMEMBER", "s2", "10"])));
        assert_eq!(distinct, vec![b"x".to_vec(), b"y".to_vec(), b"z".to_vec()]);
        let Reply::Array(with_repl) = s.dispatch(&mut c, &argv(&["SRANDMEMBER", "s2", "-5"])) else { panic!("expected array") };
        assert_eq!(with_repl.len(), 5);
    }

    #[test]
    fn smove_transfers_a_member() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SADD", "src", "a", "b"]));
        s.dispatch(&mut c, &argv(&["SADD", "dst", "c"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SMOVE", "src", "dst", "a"])), Reply::Integer(1));
        assert_eq!(sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SMEMBERS", "dst"]))), vec![b"a".to_vec(), b"c".to_vec()]);
        assert_eq!(sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SMEMBERS", "src"]))), vec![b"b".to_vec()]);
        assert_eq!(s.dispatch(&mut c, &argv(&["SMOVE", "src", "dst", "nope"])), Reply::Integer(0));
    }

    #[test]
    fn sinter_sunion_sdiff_and_store_variants() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SADD", "a", "1", "2", "3"]));
        s.dispatch(&mut c, &argv(&["SADD", "b", "2", "3", "4"]));

        assert_eq!(sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SINTER", "a", "b"]))), vec![b"2".to_vec(), b"3".to_vec()]);
        assert_eq!(sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SUNION", "a", "b"]))), vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]);
        assert_eq!(sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SDIFF", "a", "b"]))), vec![b"1".to_vec()]);

        assert_eq!(s.dispatch(&mut c, &argv(&["SINTERSTORE", "dest", "a", "b"])), Reply::Integer(2));
        assert_eq!(sorted_bulk_strings(s.dispatch(&mut c, &argv(&["SMEMBERS", "dest"]))), vec![b"2".to_vec(), b"3".to_vec()]);

        assert_eq!(s.dispatch(&mut c, &argv(&["SDIFFSTORE", "empty_dest", "b", "a"])), Reply::Integer(1));
        s.dispatch(&mut c, &argv(&["SADD", "already_there", "x"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["SDIFFSTORE", "already_there", "a", "a"])), Reply::Integer(0), "an empty result deletes the destination key");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "already_there"])), Reply::Integer(0));
    }

    // -------------------------------------------------------- zset commands ---

    fn seed_zset(s: &mut Server, c: &mut ClientState, key: &str) {
        s.dispatch(c, &argv(&["ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d"]));
    }

    #[test]
    fn zrevrange_and_zrevrank() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        seed_zset(&mut s, &mut c, "z");
        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZREVRANGE", "z", "0", "-1"])),
            Reply::Array(vec![Reply::bulk(b"d".to_vec()), Reply::bulk(b"c".to_vec()), Reply::bulk(b"b".to_vec()), Reply::bulk(b"a".to_vec())])
        );
        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZREVRANGE", "z", "0", "1", "WITHSCORES"])),
            Reply::Array(vec![Reply::bulk(b"d".to_vec()), Reply::bulk(b"4".to_vec()), Reply::bulk(b"c".to_vec()), Reply::bulk(b"3".to_vec())])
        );
        assert_eq!(s.dispatch(&mut c, &argv(&["ZREVRANK", "z", "a"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZREVRANK", "z", "d"])), Reply::Integer(0));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZREVRANK", "z", "nope"])), Reply::Null);
    }

    #[test]
    fn zcount_and_zrangebyscore() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        seed_zset(&mut s, &mut c, "z");
        assert_eq!(s.dispatch(&mut c, &argv(&["ZCOUNT", "z", "2", "3"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZCOUNT", "z", "(2", "4"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZCOUNT", "z", "-inf", "+inf"])), Reply::Integer(4));

        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZRANGEBYSCORE", "z", "2", "3"])),
            Reply::Array(vec![Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec())])
        );
        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZREVRANGEBYSCORE", "z", "3", "2"])),
            Reply::Array(vec![Reply::bulk(b"c".to_vec()), Reply::bulk(b"b".to_vec())])
        );
        assert_eq!(s.dispatch(&mut c, &argv(&["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2"])), Reply::Array(vec![Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec())]));
    }

    #[test]
    fn zlexcount_and_zrangebylex() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        // Same score on every member, matching the command's precondition.
        s.dispatch(&mut c, &argv(&["ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["ZLEXCOUNT", "z", "-", "+"])), Reply::Integer(4));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZLEXCOUNT", "z", "[b", "[c"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZLEXCOUNT", "z", "(b", "(d"])), Reply::Integer(1));

        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZRANGEBYLEX", "z", "-", "[b"])),
            Reply::Array(vec![Reply::bulk(b"a".to_vec()), Reply::bulk(b"b".to_vec())])
        );
        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZREVRANGEBYLEX", "z", "[b", "-"])),
            Reply::Array(vec![Reply::bulk(b"b".to_vec()), Reply::bulk(b"a".to_vec())])
        );
        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "2"])),
            Reply::Array(vec![Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec())])
        );
        assert_eq!(s.dispatch(&mut c, &argv(&["ZRANGEBYLEX", "z", "foo", "+"])), Reply::Error("ERR min or max not valid string range item".to_string()));
    }

    #[test]
    fn zincrby_and_zpopmin_zpopmax() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["ZINCRBY", "z", "5", "a"])), Reply::bulk(b"5".to_vec()));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZINCRBY", "z", "-2", "a"])), Reply::bulk(b"3".to_vec()));

        seed_zset(&mut s, &mut c, "z2");
        assert_eq!(s.dispatch(&mut c, &argv(&["ZPOPMIN", "z2"])), Reply::Array(vec![Reply::bulk(b"a".to_vec()), Reply::bulk(b"1".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZPOPMAX", "z2", "2"])), Reply::Array(vec![Reply::bulk(b"d".to_vec()), Reply::bulk(b"4".to_vec()), Reply::bulk(b"c".to_vec()), Reply::bulk(b"3".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZCARD", "z2"])), Reply::Integer(1));
    }

    #[test]
    fn zmscore_and_zrandmember() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        seed_zset(&mut s, &mut c, "z");
        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZMSCORE", "z", "a", "missing", "c"])),
            Reply::Array(vec![Reply::bulk(b"1".to_vec()), Reply::Null, Reply::bulk(b"3".to_vec())])
        );

        let Reply::BulkString(m) = s.dispatch(&mut c, &argv(&["ZRANDMEMBER", "z"])) else { panic!("expected bulk") };
        assert!(["a", "b", "c", "d"].iter().any(|k| k.as_bytes() == m));
        let Reply::Array(distinct) = s.dispatch(&mut c, &argv(&["ZRANDMEMBER", "z", "10"])) else { panic!("expected array") };
        assert_eq!(distinct.len(), 4);
        let Reply::Array(with_repl) = s.dispatch(&mut c, &argv(&["ZRANDMEMBER", "z", "-6"])) else { panic!("expected array") };
        assert_eq!(with_repl.len(), 6);
        let Reply::Array(pairs) = s.dispatch(&mut c, &argv(&["ZRANDMEMBER", "z", "2", "WITHSCORES"])) else { panic!("expected array") };
        assert_eq!(pairs.len(), 4);
        assert_eq!(s.dispatch(&mut c, &argv(&["ZRANDMEMBER", "missing"])), Reply::Null);
    }

    #[test]
    fn zunionstore_zinterstore_zdiffstore() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ZADD", "a", "1", "x", "2", "y"]));
        s.dispatch(&mut c, &argv(&["ZADD", "b", "10", "y", "20", "z"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["ZUNIONSTORE", "dest", "2", "a", "b"])), Reply::Integer(3));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "dest", "y"])), Reply::bulk(b"12".to_vec()), "union sums scores for a member present in both sets");
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "dest", "x"])), Reply::bulk(b"1".to_vec()));

        assert_eq!(s.dispatch(&mut c, &argv(&["ZINTERSTORE", "dest2", "2", "a", "b"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "dest2", "y"])), Reply::bulk(b"12".to_vec()));

        assert_eq!(s.dispatch(&mut c, &argv(&["ZDIFFSTORE", "dest3", "2", "a", "b"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "dest3", "x"])), Reply::bulk(b"1".to_vec()), "diff keeps the first set's original score");

        s.dispatch(&mut c, &argv(&["ZADD", "already_there", "1", "w"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZDIFFSTORE", "already_there", "2", "a", "a"])), Reply::Integer(0), "an empty result deletes the destination key");
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "already_there"])), Reply::Integer(0));
    }

    #[test]
    fn zunionstore_respects_weights_and_aggregate() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ZADD", "a", "1", "x"]));
        s.dispatch(&mut c, &argv(&["ZADD", "b", "2", "x"]));

        assert_eq!(s.dispatch(&mut c, &argv(&["ZUNIONSTORE", "d1", "2", "a", "b", "WEIGHTS", "10", "1"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "d1", "x"])), Reply::bulk(b"12".to_vec()), "10*1 + 1*2 = 12");

        assert_eq!(s.dispatch(&mut c, &argv(&["ZUNIONSTORE", "d2", "2", "a", "b", "AGGREGATE", "MAX"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "d2", "x"])), Reply::bulk(b"2".to_vec()));

        assert_eq!(s.dispatch(&mut c, &argv(&["ZINTERSTORE", "d3", "2", "a", "b", "AGGREGATE", "MIN"])), Reply::Integer(1));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZSCORE", "d3", "x"])), Reply::bulk(b"1".to_vec()));

        assert!(matches!(s.dispatch(&mut c, &argv(&["ZDIFFSTORE", "d4", "2", "a", "b", "WEIGHTS", "1", "1"])), Reply::Error(_)), "ZDIFFSTORE doesn't accept WEIGHTS");
    }

    #[test]
    fn zunion_zinter_zdiff_return_sorted_results_without_storing() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ZADD", "a", "1", "x", "3", "y"]));
        s.dispatch(&mut c, &argv(&["ZADD", "b", "2", "x", "5", "z"]));

        assert_eq!(
            s.dispatch(&mut c, &argv(&["ZUNION", "2", "a", "b", "WITHSCORES"])),
            Reply::Array(vec![Reply::bulk(b"x".to_vec()), Reply::bulk(b"3".to_vec()), Reply::bulk(b"y".to_vec()), Reply::bulk(b"3".to_vec()), Reply::bulk(b"z".to_vec()), Reply::bulk(b"5".to_vec())]),
            "ascending by score, x(1+2=3) ties with y(3) broken lexicographically"
        );
        assert_eq!(s.dispatch(&mut c, &argv(&["ZINTER", "2", "a", "b"])), Reply::Array(vec![Reply::bulk(b"x".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZDIFF", "2", "a", "b"])), Reply::Array(vec![Reply::bulk(b"y".to_vec())]));
        assert_eq!(s.dispatch(&mut c, &argv(&["EXISTS", "a", "b"])), Reply::Integer(2), "STORE-less variants never write a destination key");
    }

    #[test]
    fn zintercard_counts_without_materializing_members() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["ZADD", "a", "1", "x", "2", "y", "3", "z"]));
        s.dispatch(&mut c, &argv(&["ZADD", "b", "1", "x", "2", "y"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZINTERCARD", "2", "a", "b"])), Reply::Integer(2));
        assert_eq!(s.dispatch(&mut c, &argv(&["ZINTERCARD", "2", "a", "b", "LIMIT", "1"])), Reply::Integer(1));
    }

    #[test]
    fn lolwut_returns_a_verbatim_version_string() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::VerbatimString { data, .. } = s.dispatch(&mut c, &argv(&["LOLWUT"])) else { panic!("expected verbatim string") };
        assert!(String::from_utf8_lossy(&data).contains("redisX"));
    }

    #[test]
    fn time_returns_seconds_and_microseconds() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::Array(parts) = s.dispatch(&mut c, &argv(&["TIME"])) else { panic!("expected array") };
        assert_eq!(parts.len(), 2);
        let Reply::BulkString(secs) = &parts[0] else { panic!("expected bulk") };
        let secs: i64 = String::from_utf8_lossy(secs).parse().unwrap();
        assert!(secs > 1_700_000_000, "should look like a real unix timestamp");
        let Reply::BulkString(micros) = &parts[1] else { panic!("expected bulk") };
        let micros: i64 = String::from_utf8_lossy(micros).parse().unwrap();
        assert!((0..1_000_000).contains(&micros));
    }

    #[test]
    fn lastsave_falls_back_to_start_time_until_the_first_save() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["LASTSAVE"])), Reply::Integer(s.start_time_unix));
    }

    #[test]
    fn bgsave_saves_synchronously_and_updates_lastsave() {
        let dir = std::env::temp_dir().join(format!("redisx_bgsave_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut s = Server::new(1);
        s.config.set("dir", dir.to_str().unwrap());
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        assert_eq!(s.dispatch(&mut c, &argv(&["BGSAVE"])), Reply::SimpleString("Background saving started".to_string()));
        assert!(dir.join("dump.rdb").exists());
        assert!(s.last_save_unix.is_some());
        assert_eq!(s.dispatch(&mut c, &argv(&["LASTSAVE"])), Reply::Integer(s.last_save_unix.unwrap()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn command_count_list_and_info_reflect_the_dispatch_table() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["COMMAND", "COUNT"])), Reply::Integer(COMMANDS.len() as i64));

        let Reply::Array(names) = s.dispatch(&mut c, &argv(&["COMMAND", "LIST"])) else { panic!("expected array") };
        assert_eq!(names.len(), COMMANDS.len());
        assert!(names.contains(&Reply::bulk(b"get".to_vec())));

        let Reply::Array(info) = s.dispatch(&mut c, &argv(&["COMMAND", "INFO", "GET", "bogus-command"])) else { panic!("expected array") };
        assert_eq!(info.len(), 2);
        let Reply::Array(get_info) = &info[0] else { panic!("expected array for GET") };
        assert_eq!(get_info[0], Reply::bulk(b"get".to_vec()));
        assert_eq!(get_info[1], Reply::Integer(2));
        assert_eq!(info[1], Reply::NullArray, "an unknown command name is nil");
    }

    // ------------------------------------------------------ DUMP/RESTORE ---

    /// `DUMP` payloads are arbitrary binary (CRC64 bytes, raw listpack
    /// encodings, ...) — never route them through the `argv(&[&str])` text
    /// helper (`str::as_bytes` would silently corrupt anything non-UTF8).
    /// This builds a `RESTORE` argv directly from the raw payload bytes.
    fn restore_argv(key: &str, ttl_ms: &str, payload: &[u8], extra: &[&str]) -> Vec<Vec<u8>> {
        let mut v = vec![b"RESTORE".to_vec(), key.as_bytes().to_vec(), ttl_ms.as_bytes().to_vec(), payload.to_vec()];
        v.extend(extra.iter().map(|s| s.as_bytes().to_vec()));
        v
    }

    #[test]
    fn dump_and_restore_round_trips_every_data_type() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "str", "hello"]));
        s.dispatch(&mut c, &argv(&["RPUSH", "list", "a", "b", "c"]));
        s.dispatch(&mut c, &argv(&["HSET", "hash", "f", "v"]));
        s.dispatch(&mut c, &argv(&["SADD", "set", "1", "2", "3"]));
        s.dispatch(&mut c, &argv(&["ZADD", "zset", "1", "a", "2", "b"]));

        for key in ["str", "list", "hash", "set", "zset"] {
            let Reply::BulkString(payload) = s.dispatch(&mut c, &argv(&["DUMP", key])) else { panic!("expected bulk for {key}") };
            assert_eq!(s.dispatch(&mut c, &restore_argv("restored", "0", &payload, &[])), Reply::ok(), "restoring {key}");
            s.dispatch(&mut c, &argv(&["DEL", "restored"]));
        }

        let Reply::BulkString(list_payload) = s.dispatch(&mut c, &argv(&["DUMP", "list"])) else { panic!("expected bulk") };
        s.dispatch(&mut c, &restore_argv("list2", "0", &list_payload, &[]));
        assert_eq!(s.dispatch(&mut c, &argv(&["LRANGE", "list2", "0", "-1"])), Reply::Array(vec![Reply::bulk(b"a".to_vec()), Reply::bulk(b"b".to_vec()), Reply::bulk(b"c".to_vec())]));
    }

    #[test]
    fn dump_of_a_missing_key_is_nil() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["DUMP", "missing"])), Reply::Null);
    }

    #[test]
    fn restore_refuses_to_overwrite_without_replace() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        let Reply::BulkString(payload) = s.dispatch(&mut c, &argv(&["DUMP", "k"])) else { panic!("expected bulk") };
        assert!(matches!(s.dispatch(&mut c, &restore_argv("k", "0", &payload, &[])), Reply::Error(_)), "BUSYKEY without REPLACE");
        assert_eq!(s.dispatch(&mut c, &restore_argv("k", "0", &payload, &["REPLACE"])), Reply::ok());
    }

    #[test]
    fn restore_rejects_corrupted_payloads() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert!(matches!(s.dispatch(&mut c, &argv(&["RESTORE", "k", "0", "not a real dump payload"])), Reply::Error(_)));
    }

    #[test]
    fn restore_sets_a_relative_ttl() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "k", "v"]));
        let Reply::BulkString(payload) = s.dispatch(&mut c, &argv(&["DUMP", "k"])) else { panic!("expected bulk") };
        s.dispatch(&mut c, &restore_argv("k2", "100000", &payload, &[]));
        let Reply::Integer(ttl) = s.dispatch(&mut c, &argv(&["TTL", "k2"])) else { panic!("expected integer") };
        assert!((99..=100).contains(&ttl));
    }

    #[test]
    fn command_getkeys_uses_the_first_argument_heuristic() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &argv(&["COMMAND", "GETKEYS", "GET", "mykey"])), Reply::Array(vec![Reply::bulk(b"mykey".to_vec())]));
        assert!(matches!(s.dispatch(&mut c, &argv(&["COMMAND", "GETKEYS", "PING"])), Reply::Error(_)));
    }

    // ---------------------------------------------------------------- INFO ---

    #[test]
    fn info_default_includes_every_core_section() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::BulkString(body) = s.dispatch(&mut c, &argv(&["INFO"])) else { panic!("expected bulk") };
        let text = String::from_utf8_lossy(&body);
        for section in ["# Server", "# Clients", "# Memory", "# Persistence", "# Stats", "# Replication", "# Keyspace"] {
            assert!(text.contains(section), "missing {section} in:\n{text}");
        }
        assert!(text.contains("role:master"));
        assert!(text.contains(&format!("run_id:{}", s.replid)));
    }

    #[test]
    fn info_with_a_single_section_name_returns_only_that_section() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        let Reply::BulkString(body) = s.dispatch(&mut c, &argv(&["INFO", "replication"])) else { panic!("expected bulk") };
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("# Replication"));
        assert!(!text.contains("# Server"));
    }

    #[test]
    fn info_tracks_dirty_counter_and_resets_it_on_save() {
        let dir = std::env::temp_dir().join(format!("redisx_info_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut s = Server::new(1);
        s.config.set("dir", dir.to_str().unwrap());
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "a", "1"]));
        s.dispatch(&mut c, &argv(&["SET", "b", "2"]));
        let Reply::BulkString(body) = s.dispatch(&mut c, &argv(&["INFO", "persistence"])) else { panic!("expected bulk") };
        assert!(String::from_utf8_lossy(&body).contains("rdb_changes_since_last_save:2"));

        s.dispatch(&mut c, &argv(&["SAVE"]));
        let Reply::BulkString(body) = s.dispatch(&mut c, &argv(&["INFO", "persistence"])) else { panic!("expected bulk") };
        assert!(String::from_utf8_lossy(&body).contains("rdb_changes_since_last_save:0"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn info_keyspace_reports_key_and_expire_counts_per_db() {
        let mut s = Server::new(1);
        let mut c = ClientState::default();
        s.dispatch(&mut c, &argv(&["SET", "a", "1"]));
        s.dispatch(&mut c, &argv(&["SET", "b", "2"]));
        s.dispatch(&mut c, &argv(&["EXPIRE", "a", "100"]));
        let Reply::BulkString(body) = s.dispatch(&mut c, &argv(&["INFO", "keyspace"])) else { panic!("expected bulk") };
        assert!(String::from_utf8_lossy(&body).contains("db0:keys=2,expires=1,avg_ttl=0"));
    }
}
