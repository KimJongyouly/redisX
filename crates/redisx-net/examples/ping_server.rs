//! Phase 1 completion-criterion smoke test (`redis_document/25_redisX.md`:
//! "TCP/Unix 소켓 접속 후 RESP2/3 레벨 PING 응답"): a minimal TCP server
//! wiring `ae::EventLoop` + `resp` together, handling just enough commands
//! (PING/ECHO/QUIT) to prove the mio-based reactor and the RESP codec work
//! end-to-end against a real client. Full command dispatch, output-buffer
//! write-readiness handling, and RESP3/HELLO negotiation are Phase 2 scope —
//! this intentionally stays tiny.
//!
//! Run: `cargo run --example ping_server -- 7890`, then from another shell:
//! `redis-cli -p 7890 ping` / `redis-cli -p 7890 echo hi`.

use mio::net::TcpListener;
use redisx_net::ae::EventLoop;
use redisx_net::resp::{parse_request, ParseOutcome, Reply};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;

struct Conn {
    stream: mio::net::TcpStream,
    inbuf: Vec<u8>,
}

fn handle_command(argv: &[Vec<u8>]) -> Option<Reply> {
    if argv.is_empty() {
        return None;
    }
    let cmd = String::from_utf8_lossy(&argv[0]).to_ascii_uppercase();
    Some(match cmd.as_str() {
        "PING" => {
            if argv.len() > 1 {
                Reply::bulk(argv[1].clone())
            } else {
                Reply::SimpleString("PONG".to_string())
            }
        }
        "ECHO" if argv.len() == 2 => Reply::bulk(argv[1].clone()),
        "QUIT" => Reply::ok(),
        _ => Reply::Error(format!("ERR unknown command '{cmd}'")),
    })
}

/// Reads and processes everything currently available on `fd`. Returns
/// `true` if the connection should be closed (EOF, I/O error, protocol
/// error, or `QUIT`).
fn service_connection(conns: &Rc<RefCell<HashMap<RawFd, Conn>>>, fd: RawFd) -> bool {
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

    while !closed {
        match parse_request(&conn.inbuf) {
            ParseOutcome::NeedMore => break,
            ParseOutcome::Protocol(msg) => {
                let reply = Reply::Error(format!("ERR Protocol error: {msg}"));
                let _ = conn.stream.write_all(&reply.to_bytes(2));
                closed = true;
            }
            ParseOutcome::Command { argv, consumed } => {
                conn.inbuf.drain(..consumed);
                if let Some(reply) = handle_command(&argv) {
                    let _ = conn.stream.write_all(&reply.to_bytes(2));
                }
                if argv.first().is_some_and(|a| a.eq_ignore_ascii_case(b"QUIT")) {
                    closed = true;
                }
            }
        }
    }
    closed
}

fn main() -> std::io::Result<()> {
    let port: u16 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(7890);
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    let listener_fd = listener.as_raw_fd();

    let mut ev = EventLoop::new(1024)?;
    let conns: Rc<RefCell<HashMap<RawFd, Conn>>> = Rc::new(RefCell::new(HashMap::new()));

    eprintln!("redisX ping_server listening on 127.0.0.1:{port}");

    let conns_accept = conns.clone();
    ev.set_readable(
        listener_fd,
        Box::new(move |ev, _fd| loop {
            match listener.accept() {
                Ok((stream, peer)) => {
                    let fd = stream.as_raw_fd();
                    eprintln!("accepted {peer}");
                    conns_accept.borrow_mut().insert(fd, Conn { stream, inbuf: Vec::new() });

                    let conns_read = conns_accept.clone();
                    ev.set_readable(
                        fd,
                        Box::new(move |ev, fd| {
                            if service_connection(&conns_read, fd) {
                                let _ = ev.delete_readable(fd);
                                conns_read.borrow_mut().remove(&fd); // drop closes the socket
                                eprintln!("closed connection fd={fd}");
                            }
                        }),
                    )
                    .unwrap();
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("accept error: {e}");
                    break;
                }
            }
        }),
    )?;

    ev.main_loop()
}
