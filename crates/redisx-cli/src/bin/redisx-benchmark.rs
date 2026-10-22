//! redisx-benchmark — a deliberately small slice of `redis-benchmark.c`'s
//! job: open `-c` concurrent connections, fire `-n` total synchronous
//! request/reply round trips at the server, and report throughput. "남은
//! 부분" pass addition, explicitly deferred out of Phase 8 at the time
//! (see `redis_document/25_redisX.md`'s Phase 8 section) since the roadmap's
//! actual completion criterion only named `redisx-cli`'s REPL experience.
//!
//! Not included, by scope decision: per-command latency percentile
//! histograms (real `redis-benchmark`'s main value-add over a simple
//! throughput number), pipelining (`-P`), `--csv`/`--cluster`/random-key
//! distributions matching real key-space sizes, and the full ~15-command
//! default suite — this benchmarks whatever single command you give it (or
//! a small fixed default set of `PING`/`SET`/`GET`/`INCR` if you give none),
//! not a generic command language.

use redisx_net::resp::{parse_reply, Reply, ReplyParseOutcome};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

struct Options {
    host: String,
    port: u16,
    clients: usize,
    requests: usize,
    /// Non-empty => benchmark exactly this one command instead of the
    /// default fixed suite.
    command: Vec<String>,
}

fn print_usage_and_exit() -> ! {
    eprintln!("Usage: redisx-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests>] [command [arg ...]]");
    std::process::exit(1);
}

fn parse_args() -> Options {
    let mut host = "127.0.0.1".to_string();
    let mut port = 2417u16;
    let mut clients = 50usize;
    let mut requests = 100_000usize;

    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.peek() {
        match arg.as_str() {
            "-h" => {
                args.next();
                host = args.next().unwrap_or_else(|| print_usage_and_exit());
            }
            "-p" => {
                args.next();
                port = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| print_usage_and_exit());
            }
            "-c" => {
                args.next();
                clients = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| print_usage_and_exit());
            }
            "-n" => {
                args.next();
                requests = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| print_usage_and_exit());
            }
            "--help" | "-?" => print_usage_and_exit(),
            _ => break,
        }
    }
    Options { host, port, clients, requests, command: args.collect() }
}

fn encode_command(args: &[String]) -> Vec<u8> {
    Reply::Array(args.iter().map(|a| Reply::bulk(a.as_bytes().to_vec())).collect()).to_bytes(2)
}

/// Blocks until one full reply has arrived on `stream` (we don't care what
/// it is, just that the server considers the request fully handled before
/// sending the next one — same incremental-parse shape `redisx-cli`'s own
/// `ReplyReader` uses).
fn read_one_reply(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<()> {
    loop {
        match parse_reply(buf) {
            ReplyParseOutcome::Reply { consumed, .. } => {
                buf.drain(..consumed);
                return Ok(());
            }
            ReplyParseOutcome::Protocol(msg) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg)),
            ReplyParseOutcome::NeedMore => {
                let mut chunk = [0u8; 16 * 1024];
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed"));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        }
    }
}

/// Runs one command `total_requests` times spread across `clients`
/// connections/threads, printing a one-line throughput summary — the one
/// piece of real `redis-benchmark` output this port reproduces the spirit
/// of, without the percentile histogram that goes with it there.
fn bench_one(host: &str, port: u16, label: &str, command: &[String], clients: usize, total_requests: usize) {
    let per_client = total_requests / clients;
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..clients)
        .map(|client_id| {
            let host = host.to_string();
            let command = command.to_vec();
            let completed = Arc::clone(&completed);
            std::thread::spawn(move || {
                let Ok(mut stream) = TcpStream::connect((host.as_str(), port)) else {
                    eprintln!("client {client_id}: could not connect");
                    return;
                };
                stream.set_nodelay(true).ok();
                // A distinct key per client avoids every thread hammering
                // the exact same key, which would otherwise be an
                // artificial single-key contention point no real
                // multi-client workload actually has.
                let argv: Vec<String> = command.iter().map(|a| a.replace("{client}", &client_id.to_string())).collect();
                let payload = encode_command(&argv);
                let mut buf = Vec::new();
                for _ in 0..per_client {
                    if stream.write_all(&payload).is_err() || read_one_reply(&mut stream, &mut buf).is_err() {
                        break;
                    }
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let done = completed.load(Ordering::Relaxed);
    println!("====== {label} ======");
    println!("  {done} requests completed in {elapsed:.3} seconds");
    println!("  {clients} parallel clients");
    println!("  {:.2} requests per second", done as f64 / elapsed.max(1e-9));
    println!();
}

fn main() {
    let opts = parse_args();

    if !opts.command.is_empty() {
        bench_one(&opts.host, opts.port, &opts.command.join(" "), &opts.command, opts.clients.max(1), opts.requests);
        return;
    }

    // The fixed default suite, a small stand-in for real redis-benchmark's
    // ~15-command default list — see the module doc.
    let suite: &[(&str, &[&str])] = &[
        ("PING", &["PING"]),
        ("SET", &["SET", "key:{client}", "value"]),
        ("GET", &["GET", "key:{client}"]),
        ("INCR", &["INCR", "counter:{client}"]),
    ];
    for (label, argv) in suite {
        let command: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
        bench_one(&opts.host, opts.port, label, &command, opts.clients.max(1), opts.requests);
    }
}
