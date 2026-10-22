//! redisx-cli — Phase 8 of the redis_src -> redisX migration: an interactive
//! REPL + one-shot command runner, ported from a deliberately narrow slice
//! of `redis-cli.c` (11k+ lines in the original; see
//! `redis_document/25_redisX.md`'s Phase 8 section for the full scope cut).
//!
//! In scope: connecting over TCP, an interactive REPL (line editing +
//! persistent history via `rustyline`, mirroring `~/.rediscli_history`),
//! one-shot mode (`redisx-cli -p 7890 SET foo bar`), `-x` (read the last
//! argument from stdin), `HELLO`-based RESP3 negotiation (`-3`), raw vs.
//! formatted output selected the same way real `redis-cli` does (formatted
//! when stdout is a terminal, raw when piped/redirected — `--raw`/`--no-raw`
//! force one or the other), and a streaming print loop entered automatically
//! after `SUBSCRIBE`/`PSUBSCRIBE` (exercising Phase 7's pub/sub end to end).
//!
//! Out of scope, left for later if ever needed: cluster-mode redirection,
//! `--bigkeys`/`--hotkeys`/`--memkeys`, `--latency*`, `--pipe`, RDB/replica
//! mode, the Lua debugger (`--ldb`), per-command tab completion and help
//! text, real `AUTH` (accepted and ignored — there's no ACL to check it
//! against), and `redis-benchmark`/`lolwut` entirely (separate tools; see
//! the roadmap for why they're deferred rather than folded in here).
//!
//! `SUBSCRIBE` mode's "press Ctrl-C to stop" is simpler than real
//! `redis-cli`'s (which returns to the prompt): here it just lets the
//! default `SIGINT` behavior terminate the process, since cleanly canceling
//! a blocking socket read without an extra signal-handling dependency would
//! cost more than this phase's completion criterion asks for.

mod render;

use redisx_core::sds::split_args;
use redisx_net::resp::{parse_reply, Reply, ReplyParseOutcome};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::io::{IsTerminal, Read, Write};
use std::net::TcpStream;

struct Options {
    host: String,
    port: u16,
    db: i64,
    resp3: bool,
    /// `None` = auto-detect from `stdout`'s terminal-ness (real `redis-cli`'s
    /// own default), `Some(true)`/`Some(false)` = `--raw`/`--no-raw` forced it.
    raw: Option<bool>,
    read_last_arg_from_stdin: bool,
    /// Non-empty => one-shot mode: run this command and exit instead of
    /// starting the REPL. Bytes rather than `String` so `-x` can carry
    /// arbitrary binary data (e.g. `DF.SET`'s Arrow IPC payload) — see
    /// `parse_args`'s `-x` handling.
    command: Vec<Vec<u8>>,
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "Usage: redisx-cli [-h <host>] [-p <port>] [-n <db>] [-a <password>] [-3] [--raw|--no-raw] [-x] [command [arg ...]]"
    );
    std::process::exit(1);
}

fn parse_args() -> Options {
    let mut host = "127.0.0.1".to_string();
    let mut port = 2417u16;
    let mut db = 0i64;
    let mut resp3 = false;
    let mut raw = None;
    let mut read_last_arg_from_stdin = false;

    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.peek() {
        match arg.as_str() {
            "-h" => {
                args.next();
                host = args.next().unwrap_or_else(|| print_usage_and_exit());
            }
            "-p" => {
                args.next();
                let v = args.next().unwrap_or_else(|| print_usage_and_exit());
                port = v.parse().unwrap_or_else(|_| print_usage_and_exit());
            }
            "-n" => {
                args.next();
                let v = args.next().unwrap_or_else(|| print_usage_and_exit());
                db = v.parse().unwrap_or_else(|_| print_usage_and_exit());
            }
            "-a" => {
                args.next();
                args.next(); // accepted, ignored — no ACL/AUTH in this fork to check it against
            }
            "-3" => {
                args.next();
                resp3 = true;
            }
            "--raw" => {
                args.next();
                raw = Some(true);
            }
            "--no-raw" => {
                args.next();
                raw = Some(false);
            }
            "-x" => {
                args.next();
                read_last_arg_from_stdin = true;
            }
            "--help" | "-?" => print_usage_and_exit(),
            _ => break,
        }
    }
    let mut command: Vec<Vec<u8>> = args.map(String::into_bytes).collect();
    if read_last_arg_from_stdin {
        // Binary-safe on purpose (`read_to_end`, not `read_to_string`): an
        // earlier version used `read_to_string`, which silently dropped the
        // whole argument on any stdin content that wasn't valid UTF-8 (e.g.
        // `DF.SET`'s Arrow IPC bytes) instead of erroring — the argument just
        // vanished, surfacing downstream as a confusing "wrong number of
        // arguments" from the server with no clue stdin was the cause.
        let mut buf = Vec::new();
        if std::io::stdin().read_to_end(&mut buf).is_ok() {
            if buf.last() == Some(&b'\n') {
                buf.pop();
            }
            command.push(buf);
        }
    }

    Options { host, port, db, resp3, raw, read_last_arg_from_stdin, command }
}

/// Builds the RESP multibulk request wire form for `args` — identical shape
/// to a `Reply::Array` of `Reply::BulkString`s, so this just reuses that
/// encoder rather than hand-rolling the same `*N\r\n$len\r\n...` framing
/// again.
fn encode_command(args: &[Vec<u8>]) -> Vec<u8> {
    Reply::Array(args.iter().map(|a| Reply::bulk(a.clone())).collect()).to_bytes(2)
}

/// Incrementally decodes replies off a blocking `TcpStream`, the client-side
/// mirror of how the server's own binary layer accumulates bytes and calls
/// `parse_request` (see `src/bin/redisx-server.rs`) — same "buffer what
/// arrived, try to parse, read more on `NeedMore`" shape, `parse_reply`
/// instead of `parse_request`.
struct ReplyReader<'a> {
    stream: &'a mut TcpStream,
    buf: Vec<u8>,
}

impl<'a> ReplyReader<'a> {
    fn new(stream: &'a mut TcpStream) -> Self {
        ReplyReader { stream, buf: Vec::new() }
    }

    /// Blocks until one full reply is available, the connection closes
    /// (`Ok(None)`), or an I/O/protocol error occurs.
    fn next_reply(&mut self) -> std::io::Result<Option<Reply>> {
        loop {
            match parse_reply(&self.buf) {
                ReplyParseOutcome::Reply { value, consumed } => {
                    self.buf.drain(..consumed);
                    return Ok(Some(value));
                }
                ReplyParseOutcome::Protocol(msg) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg)),
                ReplyParseOutcome::NeedMore => {
                    let mut chunk = [0u8; 16 * 1024];
                    let n = self.stream.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(None);
                    }
                    self.buf.extend_from_slice(&chunk[..n]);
                }
            }
        }
    }
}

fn send_and_read(stream: &mut TcpStream, args: &[&str]) -> std::io::Result<Option<Reply>> {
    stream.write_all(&encode_command(&args.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>()))?;
    ReplyReader::new(stream).next_reply()
}

fn render(reply: &Reply, raw: bool, out: &mut impl Write) {
    if raw {
        render::render_raw(reply, out);
    } else {
        render::render_formatted(reply, out);
    }
}

/// Commands that put the connection into pub/sub streaming mode (see the
/// module doc's note on `SUBSCRIBE` handling).
fn enters_subscribe_mode(cmd_upper: &str) -> bool {
    matches!(cmd_upper, "SUBSCRIBE" | "PSUBSCRIBE")
}

/// `DF.PRETTY`'s whole point is a polars-native box-drawing table (see
/// `redisx-server`'s `dataframe.rs`), returned over the wire as an ordinary
/// bulk string — so the formatted (`--no-raw`-style) renderer would quote it
/// and escape its embedded newlines/box-drawing characters into an
/// unreadable one-liner, same as any other multi-line text would. This
/// command is special-cased to always render like `--raw` regardless of the
/// session's actual raw/formatted setting, since there's no "formatted"
/// reading of a pretty-printed table in the first place.
fn always_renders_raw(cmd_upper: &str) -> bool {
    matches!(cmd_upper, "DF.PRETTY")
}

/// Runs one command to completion: sends it, then either prints exactly one
/// reply (the common case) or, for `SUBSCRIBE`/`PSUBSCRIBE`, keeps printing
/// every reply that arrives forever (see the module doc).
fn run_command(stream: &mut TcpStream, parts: &[Vec<u8>], raw: bool) {
    if stream.write_all(&encode_command(parts)).is_err() {
        eprintln!("Error: connection lost");
        return;
    }
    let mut reader = ReplyReader::new(stream);
    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let cmd_upper = parts.first().map(|p| String::from_utf8_lossy(p).to_ascii_uppercase()).unwrap_or_default();
    let streaming = !parts.is_empty() && enters_subscribe_mode(&cmd_upper);
    let effective_raw = raw || always_renders_raw(&cmd_upper);
    loop {
        match reader.next_reply() {
            Ok(Some(reply)) => render(&reply, effective_raw, &mut out),
            Ok(None) => {
                eprintln!("Error: server closed the connection");
                return;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                return;
            }
        }
        let _ = out.flush();
        if !streaming {
            return;
        }
    }
}

fn history_path() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|home| std::path::Path::new(&home).join(".redisx-cli_history"))
}

fn repl(stream: &mut TcpStream, host: &str, port: u16, mut db: i64, raw: bool) {
    let mut editor = match DefaultEditor::new() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Could not start the line editor: {e}");
            return;
        }
    };
    let history_path = history_path();
    if let Some(path) = &history_path {
        let _ = editor.load_history(path);
    }

    loop {
        let prompt = if db == 0 { format!("{host}:{port}> ") } else { format!("{host}:{port}[{db}]> ") };
        match editor.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(trimmed);
                let Some(tokens) = split_args(trimmed) else {
                    eprintln!("Error: unbalanced quotes in request");
                    continue;
                };
                let parts: Vec<String> = tokens.iter().map(|t| t.as_str_lossy().into_owned()).collect();
                if parts.is_empty() {
                    continue;
                }
                let upper = parts[0].to_ascii_uppercase();
                if matches!(upper.as_str(), "QUIT" | "EXIT") {
                    break;
                }
                let argv: Vec<Vec<u8>> = parts.iter().map(|s| s.as_bytes().to_vec()).collect();
                run_command(stream, &argv, raw);
                // Purely cosmetic prompt tracking — the real SELECT already
                // ran on the server; this just keeps the displayed `[db]`
                // in sync with it, same as real `redis-cli`.
                if upper == "SELECT" {
                    if let Some(n) = parts.get(1).and_then(|s| s.parse::<i64>().ok()) {
                        db = n;
                    }
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    if let Some(path) = &history_path {
        let _ = editor.save_history(path);
    }
}

fn main() {
    let opts = parse_args();
    let mut stream = match TcpStream::connect((opts.host.as_str(), opts.port)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Could not connect to Redis at {}:{}: {e}", opts.host, opts.port);
            std::process::exit(1);
        }
    };

    if opts.resp3 {
        match send_and_read(&mut stream, &["HELLO", "3"]) {
            Ok(Some(Reply::Error(e))) => eprintln!("Warning: HELLO 3 failed ({e}), staying on RESP2"),
            Ok(_) => {}
            Err(e) => eprintln!("Warning: HELLO 3 failed ({e}), staying on RESP2"),
        }
    }
    if opts.db != 0 {
        match send_and_read(&mut stream, &["SELECT", &opts.db.to_string()]) {
            Ok(Some(Reply::Error(e))) => {
                eprintln!("Error selecting db {}: {e}", opts.db);
                std::process::exit(1);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error selecting db {}: {e}", opts.db);
                let _ = e;
                std::process::exit(1);
            }
        }
    }

    let raw = opts.raw.unwrap_or_else(|| !std::io::stdout().is_terminal());

    if !opts.command.is_empty() {
        run_command(&mut stream, &opts.command, raw);
        return;
    }
    if opts.read_last_arg_from_stdin {
        // `-x` with no command at all has nothing useful to do.
        print_usage_and_exit();
    }

    repl(&mut stream, &opts.host, opts.port, opts.db, raw);
}
