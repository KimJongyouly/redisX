//! Redis Cluster — ported from the shared parts of `cluster.c`/`cluster.h`
//! (hash-slot math, `CLUSTER` subcommand reply shapes) plus a from-scratch,
//! deliberately simplified replacement for `cluster_legacy.c`'s gossip bus.
//! The roadmap's own §4-5 flagged one open decision before this phase could
//! even start: "reproduce the legacy gossip protocol byte-for-byte, or design
//! a new one?" — answered here as **a new one**. `cluster_legacy.c`'s wire
//! format is a binary, versioned, checksum-bearing protocol built to also
//! interoperate with every past Redis Cluster release; reproducing it would
//! be its own multi-phase project with no payoff for a redisX-to-redisX
//! cluster, which is the only thing this fork's gossip ever needs to talk to.
//! So: a plain length-prefixed, human-readable text frame
//! (`encode_gossip`/`parse_gossip` below, framed and exchanged over TCP by
//! `src/bin/redisx-server.rs` — see its module doc for the networking half)
//! carrying the same *information* — node id/address/role/slots/config-epoch
//! — but not the bytes.
//!
//! ## What's real vs. narrowed
//!
//! - **Hash slots, `MOVED`/`CROSSSLOT`/`CLUSTERDOWN`, `CLUSTER
//!   NODES`/`INFO`/`SLOTS`/`SHARDS`/`KEYSLOT`/`(COUNT|GET)KEYSINSLOT`**
//!   (`server.rs`'s `cmd_cluster`/`check_cluster_redirect`) are real,
//!   client-visible-protocol-compatible behavior — any cluster-aware client
//!   (`redis-cli -c`, `redis-py`'s `RedisCluster`, ...) talking to a redisX
//!   cluster sees exactly what it expects.
//! - **Gossip propagation is real** — nodes do learn about each other
//!   transitively (meeting node A with B, A+B's periodic exchanges with
//!   whoever *they* already know, eventually teaches every node about every
//!   other), and slot ownership changes do converge cluster-wide — but it
//!   runs over the custom text protocol above, on a plain cron tick rather
//!   than `cluster_legacy.c`'s continuous gossip-rumor-mongering with random
//!   peer sampling. Every tick talks to every known node, which is fine at
//!   this fork's scale (a handful of nodes, not hundreds) and much simpler to
//!   reason about.
//! - **No resharding dance.** `CLUSTER SETSLOT` only supports the `NODE`
//!   variant (an immediate, forced reassignment) — no `IMPORTING`/
//!   `MIGRATING`/`STABLE`, no `ASK` redirection, no `MIGRATE` command. A slot
//!   reassignment never moves the keys already stored under it: the old
//!   owner keeps serving them locally until something else deletes or moves
//!   them (`DUMP`/`RESTORE` between nodes is the manual equivalent — there is
//!   no cluster-aware way to move them *through* this fork). This is the same
//!   scope line real Redis's own `redis-cli --cluster reshard` needs
//!   `MIGRATE`+`ASK` for, deliberately left to a client-side admin tool
//!   rather than built into this fork.
//! - **No automatic/voted failover.** `CLUSTER FAILOVER` returns the same
//!   kind of permanent "not supported" error the pre-existing, non-cluster
//!   `FAILOVER` command already returns (`server.rs`'s `cmd_failover`), for
//!   the same root cause: no control channel to remotely command a specific
//!   replica to promote itself. Promote one manually: `CLUSTER RESET` +
//!   `CLUSTER ADDSLOTS`/`SETSLOT` on the replica, then re-point the rest of
//!   the cluster at it with `CLUSTER SETSLOT ... NODE` from any master.
//! - **No `nodes.conf` persistence.** Real Redis auto-saves cluster topology
//!   to `cluster-config-file` on every change and reloads it at startup, so a
//!   restarted node resumes its role without being re-`MEET`'d. This fork
//!   keeps the whole `ClusterState` in memory only (`CLUSTER SAVECONFIG` is
//!   accepted but a no-op) — restart a node and it comes back as a lone,
//!   slotless master; re-run `CLUSTER MEET`/`ADDSLOTS` (gossip re-teaches it
//!   the rest once it's re-met) to rejoin.
//! - **Fail detection is locally measured, never trusted secondhand.** Each
//!   node pings every other node it knows about, every tick, itself —
//!   a peer's `fail`/`misses` state in *our* table is never copied from a
//!   third node's gossip reply (only identity/role/slots/config-epoch are;
//!   see `merge_remote`'s doc comment). Real Redis's `PFAIL`→`FAIL` promotion
//!   needs a quorum of *other* masters independently agreeing a node is down
//!   before marking it `FAIL` cluster-wide; this fork skips that quorum step
//!   — one node's own observation is authoritative for its own view, which
//!   can disagree with another node's view of the same peer and never
//!   converges through a vote. Fine for a demo-scale cluster, a real gap for
//!   a production one.
//! - **Cross-slot detection uses an explicit per-command key-position
//!   table** (`server.rs`'s `cluster_command_keys`), not the single-key
//!   `argv[1]` approximation `acl.rs`/`CLIENT TRACKING` already lean on
//!   elsewhere — a false positive there just means one extra, harmless
//!   authorization/tracking entry, but a false positive here would misroute
//!   ordinary non-key commands (`CONFIG GET maxmemory` would see `"GET"` as a
//!   fake key). So: an explicit allowlist, and anything not on it is treated
//!   as keyless (never redirected) rather than guessed at.

use std::collections::BTreeMap;

/// `CLUSTER_SLOTS` — the hash slot count is architectural, not configurable,
/// in real Redis Cluster too.
pub const CLUSTER_SLOTS: u16 = 16384;

// ---------------------------------------------------------------- CRC16 ---
// Ported verbatim from `redis_src/src/crc16.c` (the XMODEM/ZMODEM/CRC-16-ACORN
// variant Redis Cluster uses for slot hashing — *not* the same polynomial as
// `redisx-core`'s `crc64.rs`, a different, unrelated checksum used by RDB).

#[rustfmt::skip]
const CRC16_TAB: [u16; 256] = [
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0,
];

pub fn crc16(buf: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in buf {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ b as u16) & 0x00FF) as usize];
    }
    crc
}

/// `keyHashSlot` (`cluster.h`): the low 14 bits of the key's CRC16, or — if
/// the key contains a non-empty `{tag}` — the low 14 bits of just the tag's
/// CRC16, letting an application force related keys onto the same slot.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    if let Some(s) = key.iter().position(|&b| b == b'{') {
        if let Some(rel) = key[s + 1..].iter().position(|&b| b == b'}') {
            if rel != 0 {
                return crc16(&key[s + 1..s + 1 + rel]) & 0x3FFF;
            }
        }
    }
    crc16(key) & 0x3FFF
}

fn generate_node_id() -> String {
    // Same approach as `server.rs`'s `generate_replid` (OS-randomness via
    // `RandomState`, no need for a `rand` crate) — duplicated rather than
    // shared so this module stays self-contained like `zset.rs`/`hash.rs`.
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut s = String::new();
    for _ in 0..3 {
        s.push_str(&format!("{:016x}", RandomState::new().build_hasher().finish()));
    }
    s.truncate(40);
    s
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Master,
    Replica,
}

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    pub role: NodeRole,
    pub master_id: Option<String>,
    /// Sorted, non-overlapping, maximally-merged `(start, end)` ranges —
    /// kept in this compact form at all times (`add_slot`/`remove_slot`
    /// re-merge/re-split on every change) so `CLUSTER NODES`/`SLOTS` can
    /// print directly from it like real Redis's own `node->slot_info_pairs`.
    pub slots: Vec<(u16, u16)>,
    pub config_epoch: u64,
    /// Locally measured only — see this module's doc comment on why a
    /// remote gossip reply never sets this for anyone but the sender's own
    /// view of *itself*, which this fork doesn't even forward (we just
    /// measure every known node ourselves).
    pub fail: bool,
    pub misses: u32,
    pub pong_received: i64,
}

impl ClusterNode {
    /// `pub(crate)` so `server.rs`'s tests can build a synthetic peer node
    /// directly (there's no network handshake in a unit test) — never
    /// constructed this way outside tests in practice; a real peer's record
    /// always arrives via `parse_gossip`.
    pub(crate) fn new(id: String, ip: String, port: u16, cport: u16) -> Self {
        ClusterNode { id, ip, port, cport, role: NodeRole::Master, master_id: None, slots: Vec::new(), config_epoch: 0, fail: false, misses: 0, pong_received: 0 }
    }

    pub fn covers(&self, slot: u16) -> bool {
        self.slots.iter().any(|&(a, b)| slot >= a && slot <= b)
    }

    pub fn slot_count(&self) -> usize {
        self.slots.iter().map(|&(a, b)| (b - a + 1) as usize).sum()
    }

    pub fn add_slot(&mut self, slot: u16) {
        if self.covers(slot) {
            return;
        }
        let mut slots = std::mem::take(&mut self.slots);
        slots.push((slot, slot));
        slots.sort_unstable();
        let mut merged: Vec<(u16, u16)> = Vec::new();
        for (a, b) in slots {
            if let Some(last) = merged.last_mut() {
                if a <= last.1.saturating_add(1) {
                    last.1 = last.1.max(b);
                    continue;
                }
            }
            merged.push((a, b));
        }
        self.slots = merged;
    }

    pub fn remove_slot(&mut self, slot: u16) {
        let slots = std::mem::take(&mut self.slots);
        let mut out = Vec::new();
        for (a, b) in slots {
            if slot < a || slot > b {
                out.push((a, b));
                continue;
            }
            if a < slot {
                out.push((a, slot - 1));
            }
            if slot < b {
                out.push((slot + 1, b));
            }
        }
        self.slots = out;
    }
}

pub struct ClusterState {
    pub enabled: bool,
    /// Always set, even when `enabled` is `false` — matching real Redis,
    /// where every instance gets a stable node id at startup regardless of
    /// `cluster-enabled` (`CLUSTER MYID` works either way).
    pub my_id: String,
    pub nodes: BTreeMap<String, ClusterNode>,
    pub current_epoch: u64,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterState {
    pub fn new() -> Self {
        ClusterState { enabled: false, my_id: generate_node_id(), nodes: BTreeMap::new(), current_epoch: 0 }
    }

    /// Flips cluster mode on — called once at startup
    /// (`src/bin/redisx-server.rs`'s `main`) when `cluster-enabled yes`,
    /// after the binary knows its own bound `ip`/`port`/`cport`. There is no
    /// `CONFIG SET cluster-enabled` support, matching real Redis (a
    /// startup-only setting there too).
    pub fn activate(&mut self, ip: String, port: u16, cport: u16) {
        self.enabled = true;
        let id = self.my_id.clone();
        self.nodes.insert(id.clone(), ClusterNode::new(id, ip, port, cport));
    }

    pub fn myself(&self) -> &ClusterNode {
        self.nodes.get(&self.my_id).expect("self node always present once a ClusterState exists")
    }

    pub fn myself_mut(&mut self) -> &mut ClusterNode {
        self.nodes.get_mut(&self.my_id).expect("self node always present once a ClusterState exists")
    }

    pub fn slot_owner(&self, slot: u16) -> Option<&ClusterNode> {
        self.nodes.values().find(|n| n.role == NodeRole::Master && n.covers(slot))
    }

    pub fn assigned_slots(&self) -> usize {
        self.nodes.values().filter(|n| n.role == NodeRole::Master).map(|n| n.slot_count()).sum()
    }

    /// `server.cluster->size` — masters actually serving at least one slot.
    pub fn masters_with_slots(&self) -> usize {
        self.nodes.values().filter(|n| n.role == NodeRole::Master && n.slot_count() > 0).count()
    }

    /// `cluster_state:ok` vs `:fail` — real Redis's default
    /// `cluster-require-full-coverage yes` behavior: every one of the 16384
    /// slots must have a live (non-`fail`) master owner, or the whole
    /// cluster reports down. With `cluster-require-full-coverage no`, an
    /// unassigned/failed slot just makes requests to *that* slot error with
    /// `CLUSTERDOWN`, not the whole cluster.
    pub fn state_ok(&self, require_full_coverage: bool) -> bool {
        !require_full_coverage || (0..CLUSTER_SLOTS).all(|slot| self.slot_owner(slot).is_some_and(|n| !n.fail))
    }
}

// --------------------------------------------------------------- Gossip ---
// Pure encode/decode/merge, no sockets in sight — `src/bin/redisx-server.rs`
// adds length-prefix framing and the actual TCP exchange around these.

/// Formats this node's current view of the whole cluster as one gossip
/// frame's payload (see this module's doc comment for the wire-format
/// decision). One line per node: `NODE id ip port cport role master-id-or-
/// "-" config-epoch slots-or-"-"`, plus one leading `EPOCH <n>` line for
/// `current_epoch`.
pub fn encode_gossip(state: &ClusterState) -> String {
    let mut out = format!("EPOCH {}\n", state.current_epoch);
    for n in state.nodes.values() {
        let role = match n.role {
            NodeRole::Master => "M",
            NodeRole::Replica => "S",
        };
        let master = n.master_id.as_deref().unwrap_or("-");
        let slots = if n.slots.is_empty() {
            "-".to_string()
        } else {
            n.slots.iter().map(|&(a, b)| if a == b { a.to_string() } else { format!("{a}-{b}") }).collect::<Vec<_>>().join(",")
        };
        out.push_str(&format!("NODE {} {} {} {} {} {} {} {}\n", n.id, n.ip, n.port, n.cport, role, master, n.config_epoch, slots));
    }
    out
}

pub fn parse_gossip(text: &str) -> Result<(u64, Vec<ClusterNode>), String> {
    let mut epoch = 0u64;
    let mut nodes = Vec::new();
    for line in text.lines() {
        let mut parts = line.split(' ');
        match parts.next() {
            Some("EPOCH") => epoch = parts.next().and_then(|v| v.parse().ok()).ok_or("malformed EPOCH line")?,
            Some("NODE") => {
                let id = parts.next().ok_or("missing node id")?.to_string();
                let ip = parts.next().ok_or("missing node ip")?.to_string();
                let port = parts.next().and_then(|v| v.parse().ok()).ok_or("bad node port")?;
                let cport = parts.next().and_then(|v| v.parse().ok()).ok_or("bad node cport")?;
                let role = match parts.next() {
                    Some("M") => NodeRole::Master,
                    Some("S") => NodeRole::Replica,
                    _ => return Err("bad node role".to_string()),
                };
                let master_field = parts.next().ok_or("missing master field")?;
                let master_id = if master_field == "-" { None } else { Some(master_field.to_string()) };
                let config_epoch = parts.next().and_then(|v| v.parse().ok()).ok_or("bad config epoch")?;
                let slots_field = parts.next().unwrap_or("-");
                let mut slots = Vec::new();
                if slots_field != "-" {
                    for range in slots_field.split(',') {
                        if let Some((a, b)) = range.split_once('-') {
                            slots.push((a.parse().map_err(|_| "bad slot range")?, b.parse().map_err(|_| "bad slot range")?));
                        } else {
                            let v: u16 = range.parse().map_err(|_| "bad slot")?;
                            slots.push((v, v));
                        }
                    }
                }
                nodes.push(ClusterNode { id, ip, port, cport, role, master_id, slots, config_epoch, fail: false, misses: 0, pong_received: 0 });
            }
            Some(other) => return Err(format!("unknown gossip line type {other}")),
            None => {}
        }
    }
    Ok((epoch, nodes))
}

/// Folds a peer's full gossip snapshot (as `parse_gossip` returns it) into
/// this node's own table — the anti-entropy half of the gossip protocol.
/// Conflict rule: whichever copy of a node has the higher `config_epoch`
/// wins outright (ties keep the existing copy); this is the same
/// "config-epoch resolves ownership conflicts" principle real Redis Cluster
/// uses, just applied to the whole node record instead of per-slot.
/// Deliberately **does not** touch `fail`/`misses`/`pong_received` on any
/// surviving entry (see this module's doc comment: liveness is only ever
/// locally measured, never taken on a third party's word), and never lets a
/// remote view overwrite this node's own self-entry.
pub fn merge_remote(state: &mut ClusterState, remote_epoch: u64, remote_nodes: Vec<ClusterNode>) {
    state.current_epoch = state.current_epoch.max(remote_epoch);
    for mut rn in remote_nodes {
        if rn.id == state.my_id {
            continue;
        }
        match state.nodes.get(&rn.id) {
            Some(existing) if existing.config_epoch > rn.config_epoch => continue,
            Some(existing) => {
                rn.fail = existing.fail;
                rn.misses = existing.misses;
                rn.pong_received = existing.pong_received;
                state.nodes.insert(rn.id.clone(), rn);
            }
            None => {
                state.nodes.insert(rn.id.clone(), rn);
            }
        }
    }
}

// ------------------------------------------------------- CLUSTER replies --
// Pure string/data builders — `server.rs`'s `cmd_cluster` wraps these in the
// actual `Reply` variants.

/// `CLUSTER INFO`'s body (`clusterGenStateInfo`'s Phase-23 subset — this fork
/// tracks no per-message-type bus traffic counters, so those lines are
/// omitted rather than faked).
pub fn info_text(state: &ClusterState, require_full_coverage: bool) -> String {
    if !state.enabled {
        return "cluster_enabled:0\r\ncluster_state:ok\r\ncluster_slots_assigned:0\r\ncluster_slots_ok:0\r\n\
                cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:0\r\n\
                cluster_current_epoch:0\r\ncluster_my_epoch:0\r\n"
            .to_string();
    }
    let assigned = state.assigned_slots();
    format!(
        "cluster_enabled:1\r\ncluster_state:{}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\n\
         cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\n\
         cluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n",
        if state.state_ok(require_full_coverage) { "ok" } else { "fail" },
        state.nodes.len(),
        state.masters_with_slots(),
        state.current_epoch,
        state.myself().config_epoch,
    )
}

/// `CLUSTER NODES`'s csv-alike body (`clusterGenNodesDescription`'s
/// Phase-23 subset — no hostname/aux-field columns, since this fork tracks
/// neither).
pub fn nodes_description(state: &ClusterState) -> String {
    let mut out = String::new();
    for n in state.nodes.values() {
        let mut flags = Vec::new();
        if n.id == state.my_id {
            flags.push("myself");
        }
        flags.push(match n.role {
            NodeRole::Master => "master",
            NodeRole::Replica => "slave",
        });
        if n.fail {
            flags.push("fail");
        }
        let master = n.master_id.as_deref().unwrap_or("-");
        let link = if n.id == state.my_id || !n.fail { "connected" } else { "disconnected" };
        let slots = if n.slots.is_empty() {
            String::new()
        } else {
            format!(" {}", n.slots.iter().map(|&(a, b)| if a == b { a.to_string() } else { format!("{a}-{b}") }).collect::<Vec<_>>().join(" "))
        };
        out.push_str(&format!("{} {}:{}@{} {} {} 0 {} {} {}{}\n", n.id, n.ip, n.port, n.cport, flags.join(","), master, n.pong_received, n.config_epoch, link, slots));
    }
    out
}

pub struct SlotRangeInfo<'a> {
    pub start: u16,
    pub end: u16,
    pub master: &'a ClusterNode,
    pub replicas: Vec<&'a ClusterNode>,
}

/// `CLUSTER SLOTS`'s grouping: every maximal contiguous run of slots owned
/// by the same master becomes one entry (`clusterCommandSlots`'s loop over
/// `CLUSTER_SLOTS`, ported directly — unassigned slots are simply skipped).
pub fn slot_ranges(state: &ClusterState) -> Vec<SlotRangeInfo<'_>> {
    let mut out = Vec::new();
    let mut slot: u16 = 0;
    while slot < CLUSTER_SLOTS {
        match state.slot_owner(slot) {
            None => slot += 1,
            Some(owner) => {
                let start = slot;
                while slot + 1 < CLUSTER_SLOTS && state.slot_owner(slot + 1).map(|n| n.id.as_str()) == Some(owner.id.as_str()) {
                    slot += 1;
                }
                let replicas = state.nodes.values().filter(|n| n.role == NodeRole::Replica && n.master_id.as_deref() == Some(owner.id.as_str())).collect();
                out.push(SlotRangeInfo { start, end: slot, master: owner, replicas });
                slot += 1;
            }
        }
    }
    out
}

pub struct ShardInfo<'a> {
    pub slots: Vec<(u16, u16)>,
    /// Master first, then its replicas.
    pub nodes: Vec<&'a ClusterNode>,
}

/// `CLUSTER SHARDS`: one entry per master that owns at least one slot,
/// listing all of its (possibly non-contiguous) slot ranges plus every node
/// in its shard.
pub fn shards(state: &ClusterState) -> Vec<ShardInfo<'_>> {
    state
        .nodes
        .values()
        .filter(|n| n.role == NodeRole::Master && !n.slots.is_empty())
        .map(|master| {
            let mut nodes = vec![master];
            nodes.extend(state.nodes.values().filter(|n| n.role == NodeRole::Replica && n.master_id.as_deref() == Some(master.id.as_str())));
            ShardInfo { slots: master.slots.clone(), nodes }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_matches_known_redis_test_vectors() {
        // Values widely quoted from real Redis's own crc16 test vectors
        // (`src/crc16.c`'s `crc16Test`/community references) — pins this
        // port's table/algorithm against the real one byte-for-byte.
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(crc16(b""), 0x0000);
    }

    #[test]
    fn key_hash_slot_respects_hashtags() {
        let plain = key_hash_slot(b"foo");
        let tagged = key_hash_slot(b"{foo}suffix");
        assert_eq!(plain, tagged);
        // Empty tag `{}` hashes the whole key instead of being treated as a tag.
        assert_eq!(key_hash_slot(b"{}"), crc16(b"{}") & 0x3FFF);
        // No closing brace: hash the whole key.
        assert_eq!(key_hash_slot(b"{unterminated"), crc16(b"{unterminated") & 0x3FFF);
    }

    #[test]
    fn slots_always_land_in_range() {
        for key in [&b"a"[..], b"a-very-long-key-name-indeed", b"{tag}rest", b""] {
            assert!(key_hash_slot(key) < CLUSTER_SLOTS);
        }
    }

    #[test]
    fn add_and_remove_slot_keep_ranges_merged_and_split() {
        let mut n = ClusterNode::new("id".to_string(), "127.0.0.1".to_string(), 7000, 17000);
        n.add_slot(5);
        n.add_slot(6);
        n.add_slot(4);
        assert_eq!(n.slots, vec![(4, 6)]);
        assert_eq!(n.slot_count(), 3);
        n.remove_slot(5);
        assert_eq!(n.slots, vec![(4, 4), (6, 6)]);
        assert!(!n.covers(5));
        assert!(n.covers(4) && n.covers(6));
    }

    fn sample_state() -> ClusterState {
        let mut state = ClusterState::new();
        state.activate("127.0.0.1".to_string(), 7000, 17000);
        state
    }

    #[test]
    fn slot_owner_finds_the_assigning_master() {
        let mut state = sample_state();
        assert!(state.slot_owner(0).is_none());
        state.myself_mut().add_slot(0);
        assert_eq!(state.slot_owner(0).unwrap().id, state.my_id);
    }

    #[test]
    fn state_ok_depends_on_full_coverage_setting() {
        let state = sample_state();
        assert!(!state.state_ok(true));
        assert!(state.state_ok(false));
    }

    #[test]
    fn gossip_round_trips_through_encode_and_parse() {
        let mut state = sample_state();
        state.myself_mut().add_slot(0);
        state.myself_mut().add_slot(1);
        state.current_epoch = 7;
        state.myself_mut().config_epoch = 7;

        let payload = encode_gossip(&state);
        let (epoch, nodes) = parse_gossip(&payload).unwrap();
        assert_eq!(epoch, 7);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, state.my_id);
        assert_eq!(nodes[0].slots, vec![(0, 1)]);
        assert_eq!(nodes[0].config_epoch, 7);
    }

    #[test]
    fn merge_remote_never_overwrites_self_and_prefers_higher_epoch() {
        let mut state = sample_state();
        state.myself_mut().add_slot(100);
        let my_id = state.my_id.clone();

        // A remote claiming to BE us, with a bogus slot set, must be ignored.
        let bogus_self = ClusterNode { slots: vec![(0, 0)], ..ClusterNode::new(my_id.clone(), "evil".to_string(), 1, 1) };
        merge_remote(&mut state, 0, vec![bogus_self]);
        assert_eq!(state.myself().slots, vec![(100, 100)]);

        // A genuinely new peer gets added.
        let mut peer = ClusterNode::new("peer".to_string(), "127.0.0.1".to_string(), 7001, 17001);
        peer.add_slot(200);
        peer.config_epoch = 1;
        merge_remote(&mut state, 1, vec![peer.clone()]);
        assert_eq!(state.nodes.get("peer").unwrap().slots, vec![(200, 200)]);

        // A stale (lower-epoch) claim about that peer is ignored.
        let mut stale = peer.clone();
        stale.slots = vec![(999, 999)];
        stale.config_epoch = 0;
        merge_remote(&mut state, 0, vec![stale]);
        assert_eq!(state.nodes.get("peer").unwrap().slots, vec![(200, 200)]);

        // A higher-epoch claim replaces it, but keeps our locally-measured
        // fail/misses bookkeeping intact rather than resetting it.
        state.nodes.get_mut("peer").unwrap().misses = 2;
        let mut fresher = peer.clone();
        fresher.slots = vec![(300, 300)];
        fresher.config_epoch = 2;
        merge_remote(&mut state, 2, vec![fresher]);
        let updated = state.nodes.get("peer").unwrap();
        assert_eq!(updated.slots, vec![(300, 300)]);
        assert_eq!(updated.misses, 2);
    }

    #[test]
    fn slot_ranges_groups_contiguous_slots_by_owner() {
        let mut state = sample_state();
        state.myself_mut().add_slot(0);
        state.myself_mut().add_slot(1);
        let mut other = ClusterNode::new("other".to_string(), "127.0.0.1".to_string(), 7001, 17001);
        other.add_slot(5);
        state.nodes.insert("other".to_string(), other);

        let ranges = slot_ranges(&state);
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 1));
        assert_eq!(ranges[0].master.id, state.my_id);
        assert_eq!((ranges[1].start, ranges[1].end), (5, 5));
        assert_eq!(ranges[1].master.id, "other");
    }

    #[test]
    fn shards_groups_a_master_with_its_replicas() {
        let mut state = sample_state();
        state.myself_mut().add_slot(0);
        let my_id = state.my_id.clone();
        let mut replica = ClusterNode::new("replica".to_string(), "127.0.0.1".to_string(), 7001, 17001);
        replica.role = NodeRole::Replica;
        replica.master_id = Some(my_id.clone());
        state.nodes.insert("replica".to_string(), replica);

        let shards = shards(&state);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].nodes[0].id, my_id);
        assert_eq!(shards[0].nodes[1].id, "replica");
    }
}
