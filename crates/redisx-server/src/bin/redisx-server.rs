//! `redisx-server` binary — wires Phase 1's `ae::EventLoop` + `resp` codec to
//! Phase 2-4's `Server` (command dispatch, data types, RDB), plus Phase 5's
//! basic master/replica replication.
//!
//! ## Why replication's networking lives here, not in `redisx-server::server`
//!
//! Every other command is a pure function of `(&mut Server, argv) -> Reply`:
//! read some bytes, mutate state, write a reply, done. `PSYNC` and
//! `REPLICAOF` aren't that shape:
//!
//! - A successful `PSYNC` must send a `+FULLRESYNC <id> <offset>\r\n` line,
//!   then a raw RDB byte stream with no trailing CRLF (not a normal RESP
//!   bulk reply), and then — critically — stop treating that connection's
//!   future bytes as request/reply pairs at all; they become a one-way
//!   stream of propagated write commands with no replies expected.
//! - `REPLICAOF host port` must open a *new* outbound TCP connection, run a
//!   short handshake, and register that new socket with the event loop.
//!
//! Both need the raw `mio::net::TcpStream` and the `EventLoop` itself —
//! neither of which `Server` holds (by design: it stays a plain, testable
//! state machine). So this binary intercepts these two command names before
//! they would reach `Server::dispatch()`, exactly where real Redis's own
//! `syncCommand`/`replicaofCommand` also do far more than an ordinary
//! command (`replication.c`).
//!
//! ## Scope (`redis_document/25_redisX.md`, Phase 5 — basic replication only)
//!
//! A full resync only (no partial resync / backlog / `+CONTINUE`), one
//! replica or one master at a time, and the replica's initial handshake runs
//! **synchronously** (blocking the whole single-threaded event loop for the
//! brief duration of the TCP connect + a handful of request/reply round
//! trips) before the connection is handed to the event loop for the ongoing
//! streamed-command phase. This is a one-time, short cost on an
//! administrative action (`REPLICAOF`), not a standing concurrency-model
//! violation — the actual command stream, both directions, is fully
//! event-driven. Cluster gossip (`cluster_legacy.c`) is out of scope
//! entirely; see the roadmap's Phase 5 section for why.
//!
//! To avoid real Redis's diskless (EOF-marker-delimited) RDB transfer format
//! — which needs scanning for a 40-byte random marker instead of just
//! reading a known length — this fork's replica deliberately omits `capa eof`
//! from its `REPLCONF capa` handshake line. `replication.c`'s master-side
//! logic (`socket_target = ... && (mincapa & SLAVE_CAPA_EOF)`) only picks
//! diskless transfer when the replica advertises that capability, so this
//! reliably steers even a *real* `redis-server` master onto the simple
//! `$<len>\r\n<bytes>` disk-based path this fork already needs for its own
//! `PSYNC` implementation — one code path serves both directions.
//!
//! Run: `cargo run --bin redisx-server -- 7890`. From `redis-cli`:
//! `REPLICAOF 127.0.0.1 7890` on another instance makes it a replica of this
//! one (or vice versa — works against a real `redis-server` too, in either
//! direction).

use mio::net::TcpListener;
use redisx_net::ae::EventLoop;
use redisx_net::resp::{parse_request, ParseOutcome, Reply};
use redisx_server::cluster;
use redisx_server::server::BlockingOutcome;
use redisx_server::tls::{TlsClientStream, TlsStream};
use redisx_server::{ClientState, ConfigStore, Server};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::TcpListener as StdTcpListener;
use std::net::TcpStream as StdTcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

/// A connection's actual transport — a plain TCP socket, or one with TLS
/// (`redisx_server::tls`) layered on top. Every other call site in this file
/// (`service_connection`, `handle_psync`, the `REPLICAOF` handshake's
/// *inbound* side, ...) reads/writes `conn.stream` exactly like a plain
/// `TcpStream` — this enum's `Read`/`Write` impls are the only place that
/// knows the difference, so TLS support needed no changes anywhere else in
/// the request/reply path. Inbound connections (accepted on `tls-port`) are
/// `Tls`; this fork's own outbound `REPLICAOF` handshake is `TlsClient` when
/// `tls-replication yes` is configured, plain otherwise (see `tls.rs`'s
/// module doc for the asymmetry between the two directions).
enum ConnStream {
    Plain(mio::net::TcpStream),
    Tls(Box<TlsStream>),
    TlsClient(Box<TlsClientStream>),
}

impl Read for ConnStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            ConnStream::Plain(s) => s.read(buf),
            ConnStream::Tls(s) => s.read(buf),
            ConnStream::TlsClient(s) => s.read(buf),
        }
    }
}

impl Write for ConnStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            ConnStream::Plain(s) => s.write(buf),
            ConnStream::Tls(s) => s.write(buf),
            ConnStream::TlsClient(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            ConnStream::Plain(s) => s.flush(),
            ConnStream::Tls(s) => s.flush(),
            ConnStream::TlsClient(s) => s.flush(),
        }
    }
}

struct Conn {
    stream: ConnStream,
    inbuf: Vec<u8>,
    client: ClientState,
    /// Set once this connection has issued a successful `PSYNC`: subsequent
    /// write-command propagation gets fanned out to it, and its bytes are no
    /// longer parsed as request/reply traffic from our side (a replica does
    /// send `REPLCONF ACK` periodically in real Redis, which this fork
    /// doesn't parse — see the module doc's scope note).
    is_replica: bool,
    /// Commands parsed out of `inbuf` but not yet processed — normally
    /// drained down to empty in the same `service_connection` call that
    /// parsed them, *except* when one of them blocks (`BLPOP`-family): the
    /// rest stay queued here until `deliver_unblocked` wakes this
    /// connection back up, instead of being silently dropped. The common
    /// case (nobody pipelines a command right behind a blocking one) always
    /// finds this empty.
    pending_commands: VecDeque<Vec<Vec<u8>>>,
    /// Bytes queued for this connection that `write()` hasn't handed to the
    /// kernel yet — see `try_flush`'s doc comment for why a reply can't just
    /// be `write_all`'d straight to a **non-blocking** socket.
    pending_out: Vec<u8>,
    /// How many leading bytes of `pending_out` have already been written.
    /// Kept as a cursor instead of draining the front of `pending_out` on
    /// every partial write so a reply that trickles out over many small
    /// writes doesn't cost `O(size^2)` in memmoves — `pending_out` is only
    /// ever actually shrunk (`clear()`) once this catches up to its length.
    pending_out_sent: usize,
}

type Conns = Rc<RefCell<HashMap<RawFd, Conn>>>;
type SharedServer = Rc<RefCell<Server>>;

/// Reads everything currently available on `fd`, parses every
/// currently-complete command into `conn.pending_commands`, then hands off
/// to `drain_pending_commands`. Returns `true` if the connection should be
/// closed. A connection currently blocked on `BLPOP`-family command has its
/// readable interest deleted (see `process_one_command`'s `Blocked` arm), so
/// this never actually runs for one until `deliver_unblocked` re-registers
/// it — no special-casing needed here for that.
fn service_connection(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd) -> bool {
    let mut closed = false;
    {
        let mut map = conns.borrow_mut();
        let Some(conn) = map.get_mut(&fd) else { return false };

        let mut buf = [0u8; 16 * 1024];
        loop {
            match conn.stream.read(&mut buf) {
                Ok(0) => {
                    closed = true;
                    break;
                }
                Ok(n) => conn.inbuf.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(_) => {
                    closed = true;
                    break;
                }
            }
        }
        while !closed {
            match parse_request(&conn.inbuf) {
                ParseOutcome::NeedMore => break,
                ParseOutcome::Protocol(msg) => {
                    let reply = Reply::Error(format!("ERR Protocol error: {msg}"));
                    conn.pending_out.extend_from_slice(&reply.to_bytes(conn.client.resp));
                    closed = true;
                }
                ParseOutcome::Command { argv, consumed } => {
                    conn.inbuf.drain(..consumed);
                    if !argv.is_empty() {
                        conn.pending_commands.push_back(argv);
                    }
                }
            }
        }
    }
    try_flush(ev, conns, fd);

    if closed {
        return true;
    }
    drain_pending_commands(ev, server, conns, fd)
}

/// What processing one command left this connection in — threaded through
/// `drain_pending_commands`'s loop instead of a plain `bool` since
/// "stop, but don't close" (blocked) needs to be distinguished from "stop
/// and close" (protocol error / `QUIT` / disconnect).
enum ProcessOutcome {
    Continue,
    Blocked,
    Closed,
}

/// Whether an `XREAD`/`XREADGROUP` call actually wants to block — real
/// Redis only treats the `BLOCK` option as live when it's actually present;
/// a plain `XREAD`/`XREADGROUP` dispatches normally like any other command.
/// `BLOCK` (like `COUNT`/`NOACK`/`GROUP ...`) only ever appears before the
/// `STREAMS` keyword, so scanning up to there is enough.
fn wants_block_option(argv: &[Vec<u8>]) -> bool {
    for arg in &argv[1..] {
        if arg.eq_ignore_ascii_case(b"STREAMS") {
            return false;
        }
        if arg.eq_ignore_ascii_case(b"BLOCK") {
            return true;
        }
    }
    false
}

/// Shared by the `BLPOP`-family commands and `XREAD BLOCK`/`XREADGROUP ...
/// BLOCK`: calls `Server::try_blocking_command`, writes back an immediate
/// reply or parks the connection (deleting its readable interest so nothing
/// else is read from it until `deliver_unblocked` wakes it back up).
fn try_blocking_and_respond(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd, argv: Vec<Vec<u8>>) -> ProcessOutcome {
    let mut map = conns.borrow_mut();
    let Some(conn) = map.get_mut(&fd) else { return ProcessOutcome::Closed };
    let outcome = server.borrow_mut().try_blocking_command(&mut conn.client, &argv);
    match outcome {
        BlockingOutcome::Immediate(reply) => {
            conn.pending_out.extend_from_slice(&reply.to_bytes(conn.client.resp));
            let should_close = conn.client.should_close;
            drop(map);
            try_flush(ev, conns, fd);
            fan_out_propagation(ev, server, conns, fd);
            deliver_pending_pushes(ev, server, conns);
            deliver_unblocked(ev, server, conns);
            if should_close { ProcessOutcome::Closed } else { ProcessOutcome::Continue }
        }
        BlockingOutcome::Blocked => {
            drop(map);
            // No reply yet — stop reading further bytes from this
            // connection (anything it pipelines just accumulates in the
            // kernel's socket buffer) until it's woken up.
            let _ = ev.delete_readable(fd);
            ProcessOutcome::Blocked
        }
    }
}

/// Runs one already-parsed command against `Server`, writes its reply (if
/// any — a `Blocked` outcome writes nothing at all, by design), and fans out
/// whatever propagation/pushes that produced. `PSYNC`/`REPLICAOF` are
/// intercepted here exactly as before (see module doc); the `BLPOP` family
/// and `XREAD`/`XREADGROUP` (only when they actually carry a `BLOCK` option
/// — see `wants_block_option`) are intercepted the same way, calling
/// `Server::try_blocking_command` instead of `dispatch` (see that method's
/// own doc comment for why it can't just be a normal `CommandSpec` handler).
fn process_one_command(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd, argv: Vec<Vec<u8>>) -> ProcessOutcome {
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_uppercase();
    match name.as_str() {
        "PSYNC" | "SYNC" => {
            handle_psync(ev, server, conns, fd, &argv);
        }
        "REPLICAOF" | "SLAVEOF" => {
            // Unlike real Redis (which returns +OK immediately and syncs in
            // the background), this fork's handshake is synchronous (see
            // module doc), so the reply simply waits for it to finish
            // before telling the caller whether it worked.
            let resp_version = conns.borrow().get(&fd).map_or(2, |c| c.client.resp);
            let reply = handle_replicaof(ev, server, conns, &argv, resp_version);
            {
                let mut map = conns.borrow_mut();
                if let Some(conn) = map.get_mut(&fd) {
                    let resp = conn.client.resp;
                    conn.pending_out.extend_from_slice(&reply.to_bytes(resp));
                }
            }
            try_flush(ev, conns, fd);
        }
        "CLUSTER" if argv.len() >= 2 && argv[1].eq_ignore_ascii_case(b"MEET") => {
            let reply = handle_cluster_meet(server, &argv);
            {
                let mut map = conns.borrow_mut();
                if let Some(conn) = map.get_mut(&fd) {
                    let resp = conn.client.resp;
                    conn.pending_out.extend_from_slice(&reply.to_bytes(resp));
                }
            }
            try_flush(ev, conns, fd);
        }
        "CLUSTER" if argv.len() >= 2 && argv[1].eq_ignore_ascii_case(b"REPLICATE") => {
            let resp_version = conns.borrow().get(&fd).map_or(2, |c| c.client.resp);
            let reply = handle_cluster_replicate(ev, server, conns, &argv, resp_version);
            {
                let mut map = conns.borrow_mut();
                if let Some(conn) = map.get_mut(&fd) {
                    let resp = conn.client.resp;
                    conn.pending_out.extend_from_slice(&reply.to_bytes(resp));
                }
            }
            try_flush(ev, conns, fd);
        }
        "BLPOP" | "BRPOP" | "BLMOVE" | "BRPOPLPUSH" | "BZPOPMIN" | "BZPOPMAX" => {
            return try_blocking_and_respond(ev, server, conns, fd, argv);
        }
        "XREAD" | "XREADGROUP" if wants_block_option(&argv) => {
            return try_blocking_and_respond(ev, server, conns, fd, argv);
        }
        _ => {
            let mut map = conns.borrow_mut();
            let Some(conn) = map.get_mut(&fd) else { return ProcessOutcome::Closed };
            let reply = server.borrow_mut().dispatch(&mut conn.client, &argv);
            conn.pending_out.extend_from_slice(&reply.to_bytes(conn.client.resp));
            let should_close = conn.client.should_close;
            drop(map);
            try_flush(ev, conns, fd);
            fan_out_propagation(ev, server, conns, fd);
            if should_close {
                return ProcessOutcome::Closed;
            }
        }
    }
    deliver_pending_pushes(ev, server, conns);
    deliver_unblocked(ev, server, conns);
    ProcessOutcome::Continue
}

/// Processes `conn.pending_commands` one at a time (FIFO) until either the
/// queue empties, the connection blocks on a `BLPOP`-family command (in
/// which case any further already-parsed commands simply stay queued until
/// `deliver_unblocked` resumes this connection), or it closes. Shared by
/// `service_connection` (freshly parsed bytes) and `deliver_unblocked` (a
/// blocked connection waking back up, possibly with more commands it had
/// pipelined in before blocking — see `Conn::pending_commands`'s doc
/// comment).
fn drain_pending_commands(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd) -> bool {
    loop {
        let argv = {
            let mut map = conns.borrow_mut();
            let Some(conn) = map.get_mut(&fd) else { return false };
            match conn.pending_commands.pop_front() {
                Some(argv) => argv,
                None => return false,
            }
        };
        match process_one_command(ev, server, conns, fd, argv) {
            ProcessOutcome::Continue => {}
            ProcessOutcome::Blocked => return false,
            ProcessOutcome::Closed => return true,
        }
    }
}

/// Registers `fd`'s readable handler — the accept loop's per-connection
/// setup and `deliver_unblocked`'s "resume this connection" path both need
/// the exact same closure, so it lives here once instead of twice.
fn register_readable(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd) {
    let server = server.clone();
    let conns2 = conns.clone();
    let _ = ev.set_readable(
        fd,
        Box::new(move |ev, fd| {
            if service_connection(ev, &server, &conns2, fd) {
                close_connection(ev, &server, &conns2, fd);
            }
        }),
    );
}

/// Tears down a connection that `service_connection`/`drain_pending_commands`
/// decided should close: drops its `Conn` (closing the socket), and updates
/// every piece of server-side state that depends on the live connection
/// list (`Server` has no socket access of its own — see this file's module
/// doc — so the binary layer is the only place that can keep these in
/// sync).
fn close_connection(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd) {
    let _ = ev.delete_readable(fd);
    // Also drop any writable registration `try_flush` left behind for a
    // reply that never finished draining — otherwise mio would be left with
    // a registration against an fd number the OS can now hand to a brand
    // new connection.
    let _ = ev.delete_writable(fd);
    conns.borrow_mut().remove(&fd);
    let mut srv = server.borrow_mut();
    srv.client_disconnected(fd as i64);
    srv.set_connected_replicas(conns.borrow().values().filter(|c| c.is_replica).count());
    srv.set_connected_clients(conns.borrow().len());
    drop(srv);
    eprintln!("closed connection fd={fd}");
}

/// The only place any reply/propagation/push byte reaches a socket. Plain
/// `write_all` is unsafe to use directly here: every `conn.stream` is a
/// **non-blocking** socket (`set_nonblocking(true)` at accept time), and
/// once the kernel's send buffer fills, `write()` returns `WouldBlock` —
/// which `write_all`'s loop treats as a hard error and bails out on the
/// spot, silently discarding whatever of the reply hadn't been handed to the
/// kernel yet. For a reply small enough to always fit in one write (the
/// overwhelming common case — a `SET`/`GET`/small-array reply) that never
/// shows up; for one that doesn't (a wide/long `DF.QUERY`, a big `KEYS`/
/// `SCAN` page, a `PSYNC` full-resync RDB transfer, ...) it means the client
/// silently receives a truncated reply and hangs forever waiting for bytes
/// the server already gave up on sending — this is exactly what a
/// 80000-row × 67-column `DF.QUERY` reproduced during testing: the socket
/// write stopped partway (confirmed via a raw-socket client bypassing
/// `redisx-cli`'s own reply parsing) and the client sat there waiting.
///
/// Every call site appends the reply bytes onto `conn.pending_out` first (see
/// that field's doc comment for why a cursor, not `drain`), then calls this.
/// This drains as much as the socket will currently accept; if that's
/// everything, it's a zero-extra-syscall no-op beyond the `write()` that
/// would have happened anyway. If the socket won't take any more right now,
/// the remainder stays queued and a writable-event callback (`ae.c`'s
/// `AE_WRITABLE`, already ported in `redisx-net::ae` but unused until now)
/// gets registered to call this again the moment the kernel has room —
/// `real Redis`'s own `handleClientsWithPendingWrites`/`sendReplyToClient`
/// shape, just invoked per-fd via the event loop instead of once per
/// `beforeSleep`.
///
/// Also flushes the stream's own `flush()` even once `pending_out` is empty:
/// a TLS connection's `write()` (`tls.rs`'s `TlsStream`) can report plaintext
/// as "written" the moment `rustls` accepts it into its *own* internal
/// ciphertext buffer, before that ciphertext has actually made it onto the
/// wire (`flush_tls`'s job) — so checking `flush()` here catches backpressure
/// that already-reported-written plaintext is still stuck behind.
fn try_flush(ev: &mut EventLoop, conns: &Conns, fd: RawFd) {
    let needs_more = {
        let mut map = conns.borrow_mut();
        let Some(conn) = map.get_mut(&fd) else { return };
        let mut stuck = false;
        loop {
            if conn.pending_out_sent >= conn.pending_out.len() {
                conn.pending_out.clear();
                conn.pending_out_sent = 0;
                break;
            }
            match conn.stream.write(&conn.pending_out[conn.pending_out_sent..]) {
                Ok(0) => {
                    stuck = true;
                    break;
                }
                Ok(n) => conn.pending_out_sent += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    stuck = true;
                    break;
                }
                Err(_) => {
                    // Unrecoverable — give up on this data, same as a bare
                    // `write_all` error used to be silently swallowed
                    // everywhere else in this file. The read side will
                    // notice the broken connection on its own and run
                    // `close_connection`.
                    conn.pending_out.clear();
                    conn.pending_out_sent = 0;
                    break;
                }
            }
        }
        stuck
    } || {
        let mut map = conns.borrow_mut();
        match map.get_mut(&fd) {
            Some(conn) => matches!(conn.stream.flush(), Err(e) if e.kind() == ErrorKind::WouldBlock),
            None => false,
        }
    };
    if needs_more {
        if !ev.has_writable(fd) {
            let conns2 = conns.clone();
            let _ = ev.set_writable(fd, false, Box::new(move |ev, fd| try_flush(ev, &conns2, fd)));
        }
    } else if ev.has_writable(fd) {
        let _ = ev.delete_writable(fd);
    }
}

/// Delivers every reply `Server::take_unblocked()` has accumulated (a
/// `BLPOP`-family client either satisfied by a write or timed out — see
/// `Server::try_blocking_command`'s doc comment for the whole design) and
/// resumes normal command processing on each connection: re-registers its
/// readable interest (deleted the moment it blocked) and drains anything it
/// had pipelined in before blocking. Called after every command that might
/// have unblocked someone, and once per periodic timeout-check tick (for
/// the "nobody wrote anything, it just timed out" case that has no write to
/// hang this call off of).
fn deliver_unblocked(ev: &mut EventLoop, server: &SharedServer, conns: &Conns) {
    let unblocked = server.borrow_mut().take_unblocked();
    for (client_id, reply) in unblocked {
        let fd = client_id as RawFd;
        {
            let mut map = conns.borrow_mut();
            let Some(conn) = map.get_mut(&fd) else { continue };
            let resp = conn.client.resp;
            conn.pending_out.extend_from_slice(&reply.to_bytes(resp));
        }
        try_flush(ev, conns, fd);
        register_readable(ev, server, conns, fd);
        if drain_pending_commands(ev, server, conns, fd) {
            close_connection(ev, server, conns, fd);
        }
    }
}

/// Sends every byte `Server::take_pending_propagation()` has accumulated to
/// every other connection marked as a replica (`replicationFeedSlaves`'s
/// simplified Phase 5 counterpart).
fn fan_out_propagation(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, writer_fd: RawFd) {
    let prop = server.borrow_mut().take_pending_propagation();
    if prop.is_empty() {
        return;
    }
    let replica_fds: Vec<RawFd> = {
        let mut map = conns.borrow_mut();
        let mut fds = Vec::new();
        for (rfd, rconn) in map.iter_mut() {
            if *rfd != writer_fd && rconn.is_replica {
                rconn.pending_out.extend_from_slice(&prop);
                fds.push(*rfd);
            }
        }
        fds
    };
    for rfd in replica_fds {
        try_flush(ev, conns, rfd);
    }
}

/// Delivers every `PUBLISH`/`CLIENT TRACKING` push `Server::take_pending_pushes()`
/// has accumulated to each named target client — unlike replica fan-out
/// (one blob broadcast to every replica alike), each push names exactly
/// which client id it's for (`ClientState::id`, this binary's `RawFd`
/// widened to `i64` — see `server.rs`'s `ClientState::id` doc), so this
/// looks each one up individually and encodes it with *that* connection's
/// own negotiated RESP version. A target that's already disconnected is
/// silently dropped (`Server::client_disconnected` already removed it from
/// every subscription/tracking table the moment it closed, so this should
/// be rare — a push racing exactly against that cleanup, if ever).
fn deliver_pending_pushes(ev: &mut EventLoop, server: &SharedServer, conns: &Conns) {
    let pushes = server.borrow_mut().take_pending_pushes();
    if pushes.is_empty() {
        return;
    }
    let mut target_fds: Vec<RawFd> = Vec::new();
    {
        let mut map = conns.borrow_mut();
        for (target_id, reply) in pushes {
            let fd = target_id as RawFd;
            if let Some(conn) = map.get_mut(&fd) {
                let bytes = reply.to_bytes(conn.client.resp);
                conn.pending_out.extend_from_slice(&bytes);
                target_fds.push(fd);
            }
        }
    }
    for fd in target_fds {
        try_flush(ev, conns, fd);
    }
}

/// `PSYNC <replid> <offset>`/`SYNC` — tries a partial resync first (`남은
/// 부분` pass addition: Phase 5 originally scoped this out entirely) via
/// `Server::try_partial_resync`, falling back to the original full-resync
/// path (`+FULLRESYNC` then the RDB as `$<len>\r\n<bytes>` with no trailing
/// CRLF, matching `sendBulkToSlave`'s wire format exactly) whenever the
/// replica's `replid` is unrecognized or its `offset` has already scrolled
/// out of the backlog's retained window — including the plain `PSYNC ? -1`
/// a first-time replica always sends, which can never partial-resync by
/// definition. Either way, the connection ends up marked as a replica so
/// future writes get streamed to it.
fn handle_psync(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, fd: RawFd, argv: &[Vec<u8>]) {
    let requested = (argv.len() >= 3 && argv[1] != b"?")
        .then(|| std::str::from_utf8(&argv[2]).ok().and_then(|s| s.parse::<i64>().ok()))
        .flatten()
        .filter(|&offset| offset >= 0)
        .map(|offset| (String::from_utf8_lossy(&argv[1]).into_owned(), offset as u64));

    let partial = requested.and_then(|(replid, offset)| server.borrow().try_partial_resync(&replid, offset));

    if let Some(missing_bytes) = partial {
        let replid = server.borrow().replid.clone();
        let replica_count = {
            let mut map = conns.borrow_mut();
            if let Some(conn) = map.get_mut(&fd) {
                conn.pending_out.extend_from_slice(format!("+CONTINUE {replid}\r\n").as_bytes());
                conn.pending_out.extend_from_slice(&missing_bytes);
                conn.is_replica = true;
                eprintln!("fd={fd} is now a replica (partial resync: sent {} backlog bytes)", missing_bytes.len());
            }
            map.values().filter(|c| c.is_replica).count()
        };
        try_flush(ev, conns, fd);
        server.borrow_mut().set_connected_replicas(replica_count);
        return;
    }

    let rdb_bytes = {
        let srv = server.borrow();
        redisx_server::rdb::save(&srv)
    };
    let (replid, offset) = {
        let srv = server.borrow();
        (srv.replid.clone(), srv.master_repl_offset)
    };
    let replica_count = {
        let mut map = conns.borrow_mut();
        if let Some(conn) = map.get_mut(&fd) {
            conn.pending_out.extend_from_slice(format!("+FULLRESYNC {replid} {offset}\r\n").as_bytes());
            conn.pending_out.extend_from_slice(format!("${}\r\n", rdb_bytes.len()).as_bytes());
            conn.pending_out.extend_from_slice(&rdb_bytes);
            conn.is_replica = true;
            eprintln!("fd={fd} is now a replica (sent {} byte RDB, full resync)", rdb_bytes.len());
        }
        map.values().filter(|c| c.is_replica).count()
    };
    try_flush(ev, conns, fd);
    server.borrow_mut().set_connected_replicas(replica_count);
}

/// Reads one `\r\n`-terminated line from a blocking stream (used only during
/// the brief synchronous handshake — see module doc). Generic over the
/// transport so the same handshake logic (`replicaof_handshake_io`) drives
/// both a plain `&StdTcpStream` and a TLS-wrapped `rustls::Stream` the same
/// way.
fn read_line<R: Read>(r: &mut BufReader<R>) -> std::io::Result<String> {
    let mut line = String::new();
    r.read_line(&mut line)?;
    Ok(line.trim_end().to_string())
}

fn send_command<W: Write>(stream: &mut W, parts: &[&str]) -> std::io::Result<()> {
    let mut buf = format!("*{}\r\n", parts.len());
    for p in parts {
        buf.push_str(&format!("${}\r\n{p}\r\n", p.len()));
    }
    stream.write_all(buf.as_bytes())
}

/// The blocking part of `syncWithMaster`, generic over the transport (plain
/// `&StdTcpStream` or a TLS-wrapped `rustls::Stream` — see
/// `replicaof_handshake` below): PING, `REPLCONF listening-port`, `REPLCONF
/// capa psync2` (deliberately no `capa eof` — see module doc), `PSYNC ? -1`,
/// then read the `+FULLRESYNC` line and the RDB bulk. Returns the loaded RDB
/// bytes.
fn replicaof_handshake_io<T: Read + Write>(transport: &mut T, our_port: u16) -> Result<Vec<u8>, String> {
    send_command(transport, &["PING"]).map_err(|e| e.to_string())?;
    {
        let mut r = BufReader::new(&mut *transport);
        read_line(&mut r).map_err(|e| format!("PING reply: {e}"))?;
    }

    send_command(transport, &["REPLCONF", "listening-port", &our_port.to_string()]).map_err(|e| e.to_string())?;
    {
        let mut r = BufReader::new(&mut *transport);
        read_line(&mut r).map_err(|e| format!("REPLCONF listening-port reply: {e}"))?;
    }

    send_command(transport, &["REPLCONF", "capa", "psync2"]).map_err(|e| e.to_string())?;
    {
        let mut r = BufReader::new(&mut *transport);
        read_line(&mut r).map_err(|e| format!("REPLCONF capa reply: {e}"))?;
    }

    send_command(transport, &["PSYNC", "?", "-1"]).map_err(|e| e.to_string())?;
    let mut r = BufReader::new(&mut *transport);
    let fullresync = read_line(&mut r).map_err(|e| format!("PSYNC reply: {e}"))?;
    if !fullresync.starts_with("+FULLRESYNC") {
        return Err(format!("unexpected PSYNC reply: {fullresync}"));
    }

    let bulk_header = read_line(&mut r).map_err(|e| format!("RDB bulk header: {e}"))?;
    let Some(len_str) = bulk_header.strip_prefix('$') else {
        return Err(format!("unexpected RDB bulk header: {bulk_header}"));
    };
    if len_str.starts_with("EOF:") {
        return Err("master chose diskless (EOF-marker) RDB transfer, which this fork doesn't parse \
                     (should not happen since we never advertise `capa eof`)"
            .to_string());
    }
    let len: usize = len_str.parse().map_err(|_| format!("bad RDB bulk length: {len_str}"))?;
    let mut rdb_bytes = vec![0u8; len];
    r.read_exact(&mut rdb_bytes).map_err(|e| format!("reading {len}-byte RDB: {e}"))?;

    Ok(rdb_bytes)
}

/// Connects to the master and runs `replicaof_handshake_io` over either a
/// plain socket or (`tls-replication yes`) a TLS one — see `tls.rs`'s module
/// doc for why the TLS side never validates the master's certificate chain.
/// Returns the raw fd (read off the still-plain `mio::net::TcpStream` before
/// it's wrapped, same as `setup_accept_loop` does for inbound connections),
/// the `ConnStream` ready for the event loop (already switched to
/// non-blocking), and the loaded RDB bytes.
fn replicaof_handshake(host: &str, port: u16, our_port: u16, tls_config: Option<Arc<rustls::ClientConfig>>) -> Result<(RawFd, ConnStream, Vec<u8>), String> {
    let mut stream = StdTcpStream::connect((host, port)).map_err(|e| format!("connect: {e}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let Some(tls_config) = tls_config else {
        let rdb_bytes = replicaof_handshake_io(&mut stream, our_port)?;
        stream.set_nonblocking(true).expect("set_nonblocking");
        let mio_stream = mio::net::TcpStream::from_std(stream);
        let fd = mio_stream.as_raw_fd();
        return Ok((fd, ConnStream::Plain(mio_stream), rdb_bytes));
    };

    redisx_server::tls::install_default_crypto_provider();
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| format!("invalid TLS server name {host}: {e}"))?;
    let mut client_conn = rustls::ClientConnection::new(tls_config, server_name).map_err(|e| format!("building TLS client connection: {e}"))?;
    let rdb_bytes = {
        let mut tls_stream = rustls::Stream::new(&mut client_conn, &mut stream);
        replicaof_handshake_io(&mut tls_stream, our_port)?
    };

    stream.set_nonblocking(true).expect("set_nonblocking");
    let mio_stream = mio::net::TcpStream::from_std(stream);
    let fd = mio_stream.as_raw_fd();
    Ok((fd, ConnStream::TlsClient(Box::new(TlsClientStream::new(mio_stream, client_conn))), rdb_bytes))
}

/// `REPLICAOF host port` / `REPLICAOF NO ONE` — see module doc for why this
/// is intercepted here instead of being a normal command handler.
fn handle_replicaof(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, argv: &[Vec<u8>], resp_version: u8) -> Reply {
    if argv.len() != 3 {
        return Reply::Error("ERR wrong number of arguments for 'replicaof' command".to_string());
    }
    let host = String::from_utf8_lossy(&argv[1]).to_string();
    let port_str = String::from_utf8_lossy(&argv[2]).to_string();

    if host.eq_ignore_ascii_case("no") && port_str.eq_ignore_ascii_case("one") {
        server.borrow_mut().role = redisx_server::Role::Master;
        eprintln!("REPLICAOF NO ONE: now a master");
        return Reply::ok();
    }
    let Ok(port) = port_str.parse::<u16>() else {
        return Reply::Error("ERR Invalid master port".to_string());
    };
    let our_port: u16 = server.borrow().config.get_int("port").unwrap_or(0) as u16;
    let use_tls = server.borrow().config.get_str("tls-replication") == Some("yes");
    let tls_config = use_tls.then(redisx_server::tls::load_client_config);

    eprintln!("REPLICAOF {host} {port}: starting handshake (blocking{})...", if use_tls { ", TLS" } else { "" });
    match replicaof_handshake(&host, port, our_port, tls_config) {
        Ok((fd, conn_stream, rdb_bytes)) => {
            {
                let mut srv = server.borrow_mut();
                for db in &mut srv.dbs {
                    *db = redisx_server::Database::new();
                }
                if let Err(e) = redisx_server::rdb::load(&mut srv, &rdb_bytes) {
                    eprintln!("REPLICAOF: failed to load master's RDB: {e}");
                    return Reply::Error(format!("ERR failed to load master's RDB: {e}"));
                }
                srv.role = redisx_server::Role::Replica { host: host.clone(), port };
            }
            eprintln!("REPLICAOF {host} {port}: loaded initial {} byte RDB, streaming now", rdb_bytes.len());

            conns.borrow_mut().insert(
                fd,
                Conn {
                    stream: conn_stream,
                    inbuf: Vec::new(),
                    client: ClientState { resp: resp_version, ..ClientState::default() },
                    is_replica: false,
                    pending_commands: VecDeque::new(),
                    pending_out: Vec::new(),
                    pending_out_sent: 0,
                },
            );

            let server2 = server.clone();
            let conns2 = conns.clone();
            ev.set_readable(
                fd,
                Box::new(move |ev, fd| {
                    if service_master_link(&server2, &conns2, fd) {
                        let _ = ev.delete_readable(fd);
                        conns2.borrow_mut().remove(&fd);
                        server2.borrow_mut().client_disconnected(fd as i64);
                        eprintln!("replication link to master closed (fd={fd})");
                    }
                }),
            )
            .expect("register master link");
            Reply::ok()
        }
        Err(e) => {
            eprintln!("REPLICAOF {host} {port}: handshake failed: {e}");
            Reply::Error(format!("ERR Unable to connect to MASTER: {e}"))
        }
    }
}

/// Reads and applies commands streamed from our master. Unlike
/// `service_connection`, there is no reply to send back for each command —
/// a replication link is one-way (aside from the `REPLCONF ACK`s this fork
/// doesn't send, per the module doc's scope note).
fn service_master_link(server: &SharedServer, conns: &Conns, fd: RawFd) -> bool {
    let mut map = conns.borrow_mut();
    let Some(conn) = map.get_mut(&fd) else { return false };

    let mut buf = [0u8; 16 * 1024];
    let mut closed = false;
    loop {
        match conn.stream.read(&mut buf) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(n) => conn.inbuf.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(_) => {
                closed = true;
                break;
            }
        }
    }

    loop {
        match parse_request(&conn.inbuf) {
            ParseOutcome::NeedMore => break,
            ParseOutcome::Protocol(_) => {
                closed = true;
                break;
            }
            ParseOutcome::Command { argv, consumed } => {
                conn.inbuf.drain(..consumed);
                if !argv.is_empty() {
                    server.borrow_mut().apply_replicated_command(&mut conn.client, &argv);
                }
            }
        }
    }
    closed
}

// ------------------------------------------------------------ Cluster bus ---
// `redisx_server::cluster`'s module doc explains the wire-protocol decision
// (a custom length-prefixed text frame, not `cluster_legacy.c`'s binary
// one); this section is purely the networking half. `CLUSTER MEET`/
// `REPLICATE` are intercepted in `process_one_command` exactly like
// `PSYNC`/`REPLICAOF` (see this file's module doc) because both need real
// network I/O `Server::dispatch` has no way to perform.
//
// Gossip is **one-way pushes, not request/response** — an earlier version
// of this section did a synchronous write-then-read round-trip per peer per
// tick, which produced a reproducible livelock: this whole event loop is
// single-threaded, so while a node is blocked reading peer B's reply, it
// can't `accept()` *B's own* simultaneous incoming connection (nor can B,
// symmetrically, while it's blocked waiting on *us*) — and because both
// nodes' cron ticks start at nearly the same wall-clock moment and reschedule
// at the same fixed interval, they stay in lockstep, so this wasn't a rare
// race but a near-certain failure on every single tick between any two
// always-up nodes. Each tick now just **pushes** our current table to every
// known peer and moves on without waiting for anything back — reading a
// peer's push (via `drain_gossip_listener`) is an entirely separate event,
// decoupled from whether we're also mid-push to that same peer. Information
// still flows both ways because gossip already runs symmetrically: A pushes
// to B on A's tick, B pushes to A on B's tick, and both independently drain
// whatever's arrived at the top of their own tick. One real consequence: a
// successful `CLUSTER MEET`/push only proves the connect+write succeeded,
// not that the peer actually processed it — same as real Redis's own
// `CLUSTER MEET`, which also returns `+OK` immediately and does the actual
// handshake asynchronously.

const GOSSIP_CONNECT_TIMEOUT: Duration = Duration::from_millis(300);
const GOSSIP_WRITE_TIMEOUT: Duration = Duration::from_millis(300);
/// How long `drain_gossip_listener` waits to read one incoming push before
/// giving up on it — generous relative to the write side since a slow/
/// stalled sender shouldn't be able to wedge our own cron tick indefinitely,
/// but there's no round-trip here to create the lockstep problem the module
/// doc above describes (we never wait on the *sender* doing anything after
/// its write; we just read what it already sent).
const GOSSIP_READ_TIMEOUT: Duration = Duration::from_millis(500);
/// Sanity cap on an incoming frame's declared length — this fork's own
/// gossip payloads (even a few hundred nodes) are nowhere near this size;
/// it exists only to refuse an obviously-corrupt or hostile length prefix
/// before allocating a buffer for it.
const GOSSIP_MAX_FRAME: usize = 1 << 20;

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn write_gossip_frame<T: Write>(w: &mut T, payload: &str) -> std::io::Result<()> {
    let bytes = payload.as_bytes();
    w.write_all(&(bytes.len() as u32).to_be_bytes())?;
    w.write_all(bytes)?;
    w.flush()
}

fn read_gossip_frame<T: Read>(r: &mut T) -> std::io::Result<String> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > GOSSIP_MAX_FRAME {
        return Err(std::io::Error::new(ErrorKind::InvalidData, "gossip frame too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))
}

/// One outbound gossip push: dial `ip:cport`, send our current table, done
/// — no reply read (see this section's module doc for why). Used by both
/// `CLUSTER MEET` (one immediate push to the named address) and
/// `cluster_gossip_tick` (one push per already-known peer, every tick).
fn gossip_push(ip: &str, cport: u16, our_payload: &str) -> std::io::Result<()> {
    let addr: std::net::SocketAddr = format!("{ip}:{cport}").parse().map_err(|e| std::io::Error::new(ErrorKind::InvalidInput, format!("{e}")))?;
    let mut stream = StdTcpStream::connect_timeout(&addr, GOSSIP_CONNECT_TIMEOUT)?;
    stream.set_write_timeout(Some(GOSSIP_WRITE_TIMEOUT))?;
    write_gossip_frame(&mut stream, our_payload)
}

/// Drains every inbound cluster-bus push currently pending on the
/// (non-blocking) bus listener: read the peer's gossip frame, merge it in,
/// close. One-shot per connection, no reply written — pure one-way pushes
/// (see this section's module doc).
fn drain_gossip_listener(listener: &StdTcpListener, server: &SharedServer) {
    loop {
        match listener.accept() {
            Ok((mut stream, _peer)) => {
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_read_timeout(Some(GOSSIP_READ_TIMEOUT));
                let result: std::io::Result<()> = (|| {
                    let payload = read_gossip_frame(&mut stream)?;
                    let (epoch, nodes) = cluster::parse_gossip(&payload).map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
                    let mut srv = server.borrow_mut();
                    cluster::merge_remote(&mut srv.cluster, epoch, nodes);
                    Ok(())
                })();
                if let Err(e) = result {
                    eprintln!("cluster bus: inbound push failed: {e}");
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) => {
                eprintln!("cluster bus: accept error: {e}");
                break;
            }
        }
    }
}

/// The outbound half of the periodic gossip cron tick: pushes our table to
/// every known peer (never ourselves), tracking per-peer reachability
/// locally from whether the push itself (connect+write) succeeded
/// (`cluster.rs`'s module doc explains why `fail`/`misses` are never taken
/// from a remote gossip reply, only measured here, directly).
fn cluster_gossip_tick(server: &SharedServer) {
    let peer_addrs: Vec<(String, String, u16)> = {
        let srv = server.borrow();
        srv.cluster.nodes.values().filter(|n| n.id != srv.cluster.my_id).map(|n| (n.id.clone(), n.ip.clone(), n.cport)).collect()
    };
    let our_payload = cluster::encode_gossip(&server.borrow().cluster);
    for (id, ip, cport) in peer_addrs {
        let mut srv = server.borrow_mut();
        let Some(n) = srv.cluster.nodes.get_mut(&id) else { continue };
        match gossip_push(&ip, cport, &our_payload) {
            Ok(()) => {
                n.misses = 0;
                n.fail = false;
                n.pong_received = now_ms();
            }
            Err(e) => {
                eprintln!("cluster bus: push to {id} ({ip}:{cport}) failed: {e}");
                n.misses += 1;
                if n.misses >= 3 {
                    n.fail = true;
                }
            }
        }
    }
}

/// `CLUSTER MEET ip port [cport]` — see module doc for why this needs
/// interception (real network I/O `Server::dispatch` can't do), and this
/// section's own doc comment for why a successful `MEET` only proves the
/// push landed, not that the peer has processed it yet (matching real
/// Redis's own immediate-`+OK`-then-asynchronous-handshake behavior).
/// `cport` defaults to `port + 10000`, same as real Redis when it isn't
/// given explicitly.
fn handle_cluster_meet(server: &SharedServer, argv: &[Vec<u8>]) -> Reply {
    if !server.borrow().cluster.enabled {
        return Reply::Error("ERR This instance has cluster support disabled".to_string());
    }
    if argv.len() < 4 || argv.len() > 5 {
        return Reply::Error("ERR wrong number of arguments for 'cluster|meet' command".to_string());
    }
    let ip = String::from_utf8_lossy(&argv[2]).to_string();
    let Ok(port) = String::from_utf8_lossy(&argv[3]).parse::<u16>() else {
        return Reply::Error("ERR Invalid TCP base port specified".to_string());
    };
    let cport = if argv.len() == 5 {
        match String::from_utf8_lossy(&argv[4]).parse::<u16>() {
            Ok(p) => p,
            Err(_) => return Reply::Error("ERR Invalid cluster bus port specified".to_string()),
        }
    } else {
        port + 10000
    };
    let our_payload = cluster::encode_gossip(&server.borrow().cluster);
    match gossip_push(&ip, cport, &our_payload) {
        Ok(()) => Reply::ok(),
        Err(e) => Reply::Error(format!("ERR Unable to meet cluster node at {ip}:{cport}: {e}")),
    }
}

/// `CLUSTER REPLICATE node-id` — resolves the node id against the cluster's
/// own node table (not a bare host/port, unlike plain `REPLICAOF`) and then
/// hands off to the exact same `handle_replicaof` this file already uses
/// for non-cluster replication, so the actual sync/streaming logic isn't
/// duplicated. Refuses (matching real Redis) if this node currently owns
/// any slots — `CLUSTER RESET`/`CLUSTER DELSLOTS(RANGE)` it first.
fn handle_cluster_replicate(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, argv: &[Vec<u8>], resp_version: u8) -> Reply {
    if !server.borrow().cluster.enabled {
        return Reply::Error("ERR This instance has cluster support disabled".to_string());
    }
    if argv.len() != 3 {
        return Reply::Error("ERR wrong number of arguments for 'cluster|replicate' command".to_string());
    }
    let node_id = String::from_utf8_lossy(&argv[2]).to_string();
    let (host, port) = {
        let srv = server.borrow();
        if srv.cluster.myself().slot_count() > 0 {
            return Reply::Error("ERR To set a master the node must be empty and without assigned slots.".to_string());
        }
        match srv.cluster.nodes.get(&node_id) {
            Some(n) if n.id == srv.cluster.my_id => return Reply::Error("ERR Can't replicate myself".to_string()),
            Some(n) => (n.ip.clone(), n.port),
            None => return Reply::Error(format!("ERR Unknown node {node_id}")),
        }
    };
    let replicaof_argv = vec![b"REPLICAOF".to_vec(), host.into_bytes(), port.to_string().into_bytes()];
    let reply = handle_replicaof(ev, server, conns, &replicaof_argv, resp_version);
    if matches!(&reply, Reply::SimpleString(s) if s == "OK") {
        let mut srv = server.borrow_mut();
        srv.cluster.myself_mut().master_id = Some(node_id);
        srv.cluster.myself_mut().role = cluster::NodeRole::Replica;
    }
    reply
}

/// The config file to use when none is named on the command line: next to
/// the running binary itself, not the current working directory — so an
/// installed copy (e.g. `/usr/bin/redisX/redisx-server`) defaults to
/// `/usr/bin/redisX/redisx.conf` regardless of where it's launched *from*,
/// the same "config lives with the binary" expectation an installed tool
/// usually sets up (`install.sh` relies on exactly this). Falls back to
/// `./redisx.conf` if the running binary's own path can't be determined for
/// some reason (sandboxed environments, etc.) rather than failing outright.
fn default_config_path() -> std::path::PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("redisx.conf")))
        .unwrap_or_else(|| std::path::PathBuf::from("redisx.conf"))
}

/// Loads the config named on the command line (or `default_config_path()`
/// if none was given), generating a starter file with a loud, impossible-to-
/// miss notice if nothing exists there yet — this is `redisx-server`'s
/// answer to a fresh `redis-server` with no `redis.conf` at all: rather than
/// silently running on compiled-in defaults forever, write them down
/// somewhere the operator will actually find and can edit.
fn load_or_create_config(path: &std::path::Path) -> std::io::Result<ConfigStore> {
    match ConfigStore::load_conf_file(path) {
        Ok(cfg) => {
            eprintln!("loaded config from {}", path.display());
            Ok(cfg)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let cfg = ConfigStore::new();
            cfg.write_conf_file(path)?;
            eprintln!("==============================================================");
            eprintln!("redisX: no config file found, generated one with defaults at:");
            eprintln!("  {}", path.display());
            eprintln!("Edit that file to change settings (port, dir, maxmemory, ...),");
            eprintln!("then restart redisx-server for the changes to take effect.");
            eprintln!("==============================================================");
            Ok(cfg)
        }
        Err(e) => Err(e),
    }
}

/// `redisx-server [config-file]` (the config file defaults to
/// `default_config_path()` if omitted — generated with defaults on first
/// run) or the legacy `redisx-server <port> [num-databases]` direct form (no
/// config file at all, kept for existing scripts/tests/docs that invoke it
/// that way — `num-databases` itself defaults to `16` there too, exactly
/// like it always has) — disambiguated by whether the *first* argument
/// parses as a port number at all. A config file path is never purely
/// numeric in practice, so this never misreads a real one as a port.
///
/// `tls-port`/`tls-cert-file`/`tls-key-file` add a *second*, independent
/// listener alongside the plain one (see `tls.rs`'s module doc for why this
/// fork doesn't support a TLS-only `port 0` mode) — `setup_accept_loop` is
/// shared by both so the accept-time bookkeeping (registering the
/// connection, bumping `connected_clients`, arming its readable handler)
/// lives in exactly one place.
fn setup_accept_loop(ev: &mut EventLoop, server: &SharedServer, conns: &Conns, listener: TcpListener, tls_config: Option<Arc<rustls::ServerConfig>>) -> std::io::Result<()> {
    let listener_fd = listener.as_raw_fd();
    let conns_accept = conns.clone();
    let server_accept = server.clone();
    ev.set_readable(
        listener_fd,
        Box::new(move |ev, _fd| loop {
            match listener.accept() {
                Ok((stream, peer)) => {
                    let fd = stream.as_raw_fd();
                    // `peer.to_string()` matches `getClientPeerId`'s `ip:port`
                    // form, which `aof.rs`'s MONITOR-style log lines embed.
                    // `id`: this `fd` itself — see `server.rs`'s
                    // `ClientState::id` doc for why PUBSUB/CLIENT TRACKING
                    // need a stable per-connection identifier at all.
                    let client = ClientState { peer: peer.to_string(), id: fd as i64, ..ClientState::default() };
                    let conn_stream = match &tls_config {
                        None => {
                            eprintln!("accepted {peer}");
                            ConnStream::Plain(stream)
                        }
                        Some(cfg) => match rustls::ServerConnection::new(cfg.clone()) {
                            Ok(tls_conn) => {
                                eprintln!("accepted {peer} (TLS)");
                                ConnStream::Tls(Box::new(TlsStream::new(stream, tls_conn)))
                            }
                            Err(e) => {
                                eprintln!("TLS setup failed for {peer}: {e}");
                                continue;
                            }
                        },
                    };
                    conns_accept.borrow_mut().insert(
                        fd,
                        Conn {
                            stream: conn_stream,
                            inbuf: Vec::new(),
                            client,
                            is_replica: false,
                            pending_commands: VecDeque::new(),
                            pending_out: Vec::new(),
                            pending_out_sent: 0,
                        },
                    );
                    server_accept.borrow_mut().set_connected_clients(conns_accept.borrow().len());
                    register_readable(ev, &server_accept, &conns_accept, fd);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("accept error: {e}");
                    break;
                }
            }
        }),
    )
}

/// Startup banner — purely cosmetic (stderr, like every other startup line
/// in this file), but also the one place a human glancing at the log sees
/// every config value that actually matters for this run (port, TLS
/// listener, cluster mode, compiled-in features) summarized together
/// instead of scattered across the several individual `eprintln!`s below
/// it. The ASCII art is original artwork (a `figlet -f standard redisX`
/// render) — deliberately not real Redis's own licensed `asciilogo.h` sheep
/// logo, which this fork has no claim to reuse.
fn print_startup_banner(port: u16, num_dbs: usize, tls_port: u16, cluster_enabled: bool, cluster_port: u16) {
    let mode = if cluster_enabled { format!("cluster (bus port {cluster_port})") } else { "standalone".to_string() };
    let tls = if tls_port != 0 { format!("on (port {tls_port})") } else { "off".to_string() };
    let features = {
        let mut fs = Vec::new();
        if cfg!(feature = "polars") {
            fs.push("polars");
        }
        if cfg!(feature = "python") {
            fs.push("python");
        }
        if fs.is_empty() { "(none)".to_string() } else { fs.join(", ") }
    };
    eprintln!(
        r#"
              _ _    __  __
 _ __ ___  __| (_)___\ \/ /
| '__/ _ \/ _` | / __|\  /
| | |  __/ (_| | \__ \/  \
|_|  \___|\__,_|_|___/_/\_\

redisX v{version} — a from-scratch Rust reimplementation of Redis

  PID             {pid}
  Port            {port}
  Mode            {mode}
  Databases       {num_dbs}
  TLS             {tls}
  Features        {features}
"#,
        version = env!("CARGO_PKG_VERSION"),
        pid = std::process::id(),
    );
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let legacy_numeric = args.len() >= 2 && args[1].parse::<u16>().is_ok();

    let mut config = if legacy_numeric {
        ConfigStore::new()
    } else {
        let path = args.get(1).map(std::path::PathBuf::from).unwrap_or_else(default_config_path);
        load_or_create_config(&path)?
    };

    let port: u16 = if legacy_numeric { args[1].parse().unwrap() } else { config.get_int("port").filter(|&p| p != 0).unwrap_or(2417) as u16 };
    let num_dbs: usize =
        if legacy_numeric { args.get(2).and_then(|s| s.parse().ok()).unwrap_or(16) } else { config.get_int("databases").unwrap_or(16) as usize };
    // Read the TLS settings before `config` is moved into `initial_server`
    // below. Legacy numeric invocation never configures TLS (no config file
    // to read it from), matching how it already skips every other named
    // config option.
    let (tls_port, tls_cert, tls_key): (u16, String, String) = if legacy_numeric {
        (0, String::new(), String::new())
    } else {
        (config.get_int("tls-port").unwrap_or(0) as u16, config.get_str("tls-cert-file").unwrap_or("").to_string(), config.get_str("tls-key-file").unwrap_or("").to_string())
    };
    config.set("port", &port.to_string());

    // Same "skip it for the legacy numeric invocation" rule as the TLS
    // settings just above — there's no config file to read it from there.
    let (cluster_enabled, cluster_port): (bool, u16) = if legacy_numeric {
        (false, 0)
    } else {
        let enabled = config.get_str("cluster-enabled") == Some("yes");
        let configured_cport = config.get_int("cluster-port").unwrap_or(0) as u16;
        (enabled, if configured_cport != 0 { configured_cport } else { port + 10000 })
    };

    print_startup_banner(port, num_dbs, tls_port, cluster_enabled, cluster_port);

    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let listener = TcpListener::bind(addr)?;

    let mut ev = EventLoop::new(1024)?;
    let mut initial_server = Server::new(num_dbs);
    initial_server.config = config;
    if cluster_enabled {
        initial_server.cluster.activate("127.0.0.1".to_string(), port, cluster_port);
    }
    match initial_server.load_rdb_if_present() {
        Ok(true) => eprintln!("loaded RDB from disk"),
        Ok(false) => eprintln!("no RDB file found, starting with an empty dataset"),
        Err(e) => {
            eprintln!("fatal: error loading RDB: {e}");
            std::process::exit(1);
        }
    }
    let server: SharedServer = Rc::new(RefCell::new(initial_server));
    let conns: Conns = Rc::new(RefCell::new(HashMap::new()));

    eprintln!("redisX server (Phase 5) listening on 127.0.0.1:{port} with {num_dbs} databases");
    setup_accept_loop(&mut ev, &server, &conns, listener, None)?;

    if tls_port != 0 {
        if tls_cert.is_empty() || tls_key.is_empty() {
            eprintln!("fatal: tls-port is set but tls-cert-file/tls-key-file are not — both are required to start a TLS listener");
            std::process::exit(1);
        }
        redisx_server::tls::install_default_crypto_provider();
        let tls_config = match redisx_server::tls::load_server_config(std::path::Path::new(&tls_cert), std::path::Path::new(&tls_key)) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("fatal: failed to load TLS cert/key: {e}");
                std::process::exit(1);
            }
        };
        let tls_addr = format!("127.0.0.1:{tls_port}").parse().unwrap();
        let tls_listener = TcpListener::bind(tls_addr)?;
        eprintln!("redisX TLS listener on 127.0.0.1:{tls_port}");
        setup_accept_loop(&mut ev, &server, &conns, tls_listener, Some(tls_config))?;
    }

    if cluster_enabled {
        let bus_addr = format!("127.0.0.1:{cluster_port}");
        let bus_listener = StdTcpListener::bind(&bus_addr)?;
        bus_listener.set_nonblocking(true)?;
        eprintln!("redisX cluster bus listening on {bus_addr} (node id {})", server.borrow().cluster.my_id);

        // The gossip cron tick: drains any inbound cluster-bus connections,
        // then dials every already-known peer once (see `cluster.rs`'s
        // module doc and this file's "Cluster bus" section doc comment for
        // why a plain fixed-interval tick, not continuous async gossip, is
        // an accepted tradeoff here). 1 second is coarse enough to be cheap
        // and fine enough that topology changes converge quickly at this
        // fork's intended (small) cluster scale.
        let server_cluster = server.clone();
        ev.create_time_event(
            1000,
            Box::new(move |_ev, _id| {
                drain_gossip_listener(&bus_listener, &server_cluster);
                cluster_gossip_tick(&server_cluster);
                Some(1000)
            }),
        );
    }

    // Periodic blocking-timeout sweep (`BLPOP`-family deadlines) — matches
    // real Redis's own `handleBlockedClientsTimeouts`, which also runs off
    // a cron tick rather than a timer per blocked client. 100ms is coarse
    // enough to be cheap even with many blocked clients, fine enough that
    // nobody waits meaningfully longer than the timeout they asked for.
    let server_timer = server.clone();
    let conns_timer = conns.clone();
    ev.create_time_event(
        100,
        Box::new(move |ev, _id| {
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
            server_timer.borrow_mut().check_blocking_timeouts(now_ms);
            deliver_unblocked(ev, &server_timer, &conns_timer);
            Some(100)
        }),
    );

    ev.main_loop()
}
