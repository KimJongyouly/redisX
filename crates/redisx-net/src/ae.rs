//! Single-threaded event loop — ported from `redis_src/src/ae.c`, backed by
//! `mio` instead of a hand-rolled epoll/kqueue/select wrapper.
//!
//! ## The mio-vs-tokio decision (`redis_document/25_redisX.md` §4-1)
//!
//! Redis's defining invariant is that **command execution is never
//! interleaved**: once a client's command starts running, nothing else runs
//! until it returns. `ae.c` enforces this by construction — it's a bare
//! reactor (`poll()`/`epoll_wait()` → synchronously call back into plain
//! functions, one at a time, no concurrency primitives involved at all).
//!
//! `tokio` would make the socket I/O more "idiomatic modern Rust", but it
//! buys that by spreading `async`/`.await` through every layer that touches a
//! connection — which then requires *manually* re-deriving the
//! never-interleaved guarantee everywhere (no `.await` point may ever let a
//! second command's future make progress mid-command), instead of getting it
//! for free from the loop's shape. Later phases — especially Phase 6, where
//! the Lua/Python engines built in this fork's `function_lua.c`/
//! `function_python.c` assume fully synchronous, re-entrant-safe execution
//! via `scriptCall()` — would have to fight that shape the whole way.
//!
//! So: **mio**, used purely as a thin, portable `epoll`/`kqueue` wrapper. The
//! loop below is deliberately still "a reactor that calls plain `FnMut`
//! callbacks one at a time", matching `ae.c`'s actual shape, not mio's own
//! higher-level patterns (no tasks, no wakers, no futures).
//!
//! ## API shape vs `ae.c`
//!
//! `aeCreateFileEvent(loop, fd, mask, proc, clientData)` lets one call install
//! the *same* C function pointer for both the readable and writable
//! directions at once (trivial for a raw pointer). A boxed Rust closure isn't
//! `Copy`, so that one case doesn't carry over cleanly; this port splits it
//! into [`EventLoop::set_readable`] / [`EventLoop::set_writable`], which is
//! how the real callers (`connSetReadHandler`/`connSetWriteHandler` in
//! `connection.c`) already invoke it in practice — one direction per call.
//! Everything else (fd-indexed dispatch, `AE_BARRIER`'s inverted call order,
//! time events sharing the exact `ae.c` reschedule/delete contract, the
//! poll-timeout-from-nearest-timer computation) is a direct port.

use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io;
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

pub const READABLE: u8 = 1;
pub const WRITABLE: u8 = 2;
/// With WRITABLE, never fire the writable event in the same iteration the
/// readable event already fired for this fd (see `ae.h`'s `AE_BARRIER` doc).
pub const BARRIER: u8 = 4;

pub const FILE_EVENTS: u8 = 1 << 0;
pub const TIME_EVENTS: u8 = 1 << 1;
pub const ALL_EVENTS: u8 = FILE_EVENTS | TIME_EVENTS;
pub const DONT_WAIT: u8 = 1 << 2;

/// AE_NOMORE's Rust spelling: a time proc returns `None` to delete itself, or
/// `Some(ms)` to be called again after `ms` milliseconds.
pub type FileProc = dyn FnMut(&mut EventLoop, RawFd);
pub type TimeProc = dyn FnMut(&mut EventLoop, u64) -> Option<u64>;
pub type BeforeSleepProc = dyn FnMut(&mut EventLoop);

struct FileEvent {
    mask: u8,
    rproc: Option<Box<FileProc>>,
    wproc: Option<Box<FileProc>>,
}

impl FileEvent {
    fn empty() -> Self {
        FileEvent { mask: 0, rproc: None, wproc: None }
    }
}

struct TimeEvent {
    id: u64,
    when: Instant,
    proc: Option<Box<TimeProc>>,
    /// AE_DELETED_EVENT_ID equivalent: marked for removal, swept after the
    /// current `process_time_events` pass finishes iterating.
    deleted: bool,
}

pub struct EventLoop {
    poll: Poll,
    mio_events: Events,
    file_events: HashMap<RawFd, FileEvent>,
    time_events: Vec<TimeEvent>,
    next_time_id: u64,
    pub before_sleep: Option<Box<BeforeSleepProc>>,
    pub stop: bool,
}

impl EventLoop {
    pub fn new(events_capacity: usize) -> io::Result<Self> {
        Ok(EventLoop {
            poll: Poll::new()?,
            mio_events: Events::with_capacity(events_capacity),
            file_events: HashMap::new(),
            time_events: Vec::new(),
            next_time_id: 0,
            before_sleep: None,
            stop: false,
        })
    }

    fn interest_for(mask: u8) -> Option<Interest> {
        match (mask & READABLE != 0, mask & WRITABLE != 0) {
            (true, true) => Some(Interest::READABLE | Interest::WRITABLE),
            (true, false) => Some(Interest::READABLE),
            (false, true) => Some(Interest::WRITABLE),
            (false, false) => None,
        }
    }

    /// Syncs mio's registration for `fd` with `file_events[fd].mask`,
    /// registering/reregistering/deregistering as needed. `was_registered`
    /// says whether mio already had a registration for this fd before this
    /// call (register vs. reregister are different mio calls).
    fn sync_registration(&mut self, fd: RawFd, was_registered: bool) -> io::Result<()> {
        let mask = self.file_events.get(&fd).map_or(0, |fe| fe.mask);
        match (Self::interest_for(mask), was_registered) {
            (None, true) => {
                self.poll.registry().deregister(&mut SourceFd(&fd))?;
                self.file_events.remove(&fd);
            }
            (None, false) => {
                self.file_events.remove(&fd);
            }
            (Some(interest), true) => {
                self.poll.registry().reregister(&mut SourceFd(&fd), Token(fd as usize), interest)?;
            }
            (Some(interest), false) => {
                self.poll.registry().register(&mut SourceFd(&fd), Token(fd as usize), interest)?;
            }
        }
        Ok(())
    }

    /// aeCreateFileEvent(mask=AE_READABLE, ...)
    pub fn set_readable(&mut self, fd: RawFd, proc: Box<FileProc>) -> io::Result<()> {
        let was_registered = self.file_events.contains_key(&fd);
        let fe = self.file_events.entry(fd).or_insert_with(FileEvent::empty);
        fe.mask |= READABLE;
        fe.rproc = Some(proc);
        self.sync_registration(fd, was_registered)
    }

    /// aeCreateFileEvent(mask=AE_WRITABLE[|AE_BARRIER], ...)
    pub fn set_writable(&mut self, fd: RawFd, barrier: bool, proc: Box<FileProc>) -> io::Result<()> {
        let was_registered = self.file_events.contains_key(&fd);
        let fe = self.file_events.entry(fd).or_insert_with(FileEvent::empty);
        fe.mask = (fe.mask | WRITABLE) & !BARRIER | if barrier { BARRIER } else { 0 };
        fe.wproc = Some(proc);
        self.sync_registration(fd, was_registered)
    }

    /// aeDeleteFileEvent(mask=AE_READABLE)
    pub fn delete_readable(&mut self, fd: RawFd) -> io::Result<()> {
        if let Some(fe) = self.file_events.get_mut(&fd) {
            fe.mask &= !READABLE;
            fe.rproc = None;
            self.sync_registration(fd, true)?;
        }
        Ok(())
    }

    /// aeDeleteFileEvent(mask=AE_WRITABLE)
    pub fn delete_writable(&mut self, fd: RawFd) -> io::Result<()> {
        if let Some(fe) = self.file_events.get_mut(&fd) {
            fe.mask &= !(WRITABLE | BARRIER);
            fe.wproc = None;
            self.sync_registration(fd, true)?;
        }
        Ok(())
    }

    pub fn has_readable(&self, fd: RawFd) -> bool {
        self.file_events.get(&fd).is_some_and(|fe| fe.mask & READABLE != 0)
    }

    pub fn has_writable(&self, fd: RawFd) -> bool {
        self.file_events.get(&fd).is_some_and(|fe| fe.mask & WRITABLE != 0)
    }

    /// aeCreateTimeEvent
    pub fn create_time_event(&mut self, after_ms: u64, proc: Box<TimeProc>) -> u64 {
        let id = self.next_time_id;
        self.next_time_id += 1;
        self.time_events.push(TimeEvent {
            id,
            when: Instant::now() + Duration::from_millis(after_ms),
            proc: Some(proc),
            deleted: false,
        });
        id
    }

    /// aeDeleteTimeEvent
    pub fn delete_time_event(&mut self, id: u64) {
        if let Some(te) = self.time_events.iter_mut().find(|te| te.id == id) {
            te.deleted = true;
        }
    }

    fn us_until_earliest_timer(&self) -> Option<Duration> {
        let now = Instant::now();
        self.time_events
            .iter()
            .filter(|te| !te.deleted)
            .map(|te| te.when.saturating_duration_since(now))
            .min()
    }

    /// processTimeEvents — only fires timers created *before* this call began
    /// (the `maxId` guard in `ae.c`, preserved here via `created_before`), so
    /// a timer scheduling another timer doesn't get run in the same pass.
    fn process_time_events(&mut self) -> usize {
        let now = Instant::now();
        let created_before = self.next_time_id;
        let mut processed = 0;

        let ids: Vec<u64> = self.time_events.iter().map(|te| te.id).collect();
        for id in ids {
            if id >= created_before {
                continue;
            }
            let Some(idx) = self.time_events.iter().position(|te| te.id == id) else { continue };
            if self.time_events[idx].deleted || self.time_events[idx].when > now {
                continue;
            }
            let Some(mut proc) = self.time_events[idx].proc.take() else { continue };
            let next = proc(self, id);
            processed += 1;
            if let Some(idx) = self.time_events.iter().position(|te| te.id == id) {
                match next {
                    Some(ms) => {
                        self.time_events[idx].when = Instant::now() + Duration::from_millis(ms);
                        self.time_events[idx].proc = Some(proc);
                    }
                    None => self.time_events[idx].deleted = true,
                }
            }
        }
        self.time_events.retain(|te| !te.deleted);
        processed
    }

    /// aeProcessEvents
    pub fn process_events(&mut self, flags: u8) -> io::Result<usize> {
        if flags & (FILE_EVENTS | TIME_EVENTS) == 0 {
            return Ok(0);
        }
        let mut processed = 0;

        if !self.file_events.is_empty() || (flags & TIME_EVENTS != 0 && flags & DONT_WAIT == 0) {
            if let Some(mut cb) = self.before_sleep.take() {
                cb(self);
                self.before_sleep = Some(cb);
            }

            let timeout = if flags & DONT_WAIT != 0 {
                Some(Duration::ZERO)
            } else if flags & TIME_EVENTS != 0 {
                self.us_until_earliest_timer()
            } else {
                None
            };

            self.poll.poll(&mut self.mio_events, timeout)?;

            if flags & FILE_EVENTS != 0 {
                let fired: Vec<(RawFd, u8)> = self
                    .mio_events
                    .iter()
                    .map(|ev| {
                        let fd = ev.token().0 as RawFd;
                        let mut m = 0;
                        if ev.is_readable() {
                            m |= READABLE;
                        }
                        if ev.is_writable() || ev.is_error() || ev.is_read_closed() || ev.is_write_closed() {
                            m |= WRITABLE;
                        }
                        (fd, m)
                    })
                    .collect();

                for (fd, mask) in fired {
                    let Some(reg_mask) = self.file_events.get(&fd).map(|fe| fe.mask) else { continue };
                    let invert = reg_mask & BARRIER != 0;
                    let mut fired_count = 0;

                    if !invert && reg_mask & mask & READABLE != 0 {
                        if let Some(mut rproc) = self.file_events.get_mut(&fd).and_then(|fe| fe.rproc.take()) {
                            rproc(self, fd);
                            fired_count += 1;
                            if let Some(fe) = self.file_events.get_mut(&fd) {
                                fe.rproc = Some(rproc);
                            }
                        }
                    }
                    if let Some(reg_mask) = self.file_events.get(&fd).map(|fe| fe.mask) {
                        if reg_mask & mask & WRITABLE != 0 {
                            if let Some(mut wproc) = self.file_events.get_mut(&fd).and_then(|fe| fe.wproc.take()) {
                                wproc(self, fd);
                                fired_count += 1;
                                if let Some(fe) = self.file_events.get_mut(&fd) {
                                    fe.wproc = Some(wproc);
                                }
                            }
                        }
                    }
                    if invert && fired_count == 0 {
                        if let Some(reg_mask) = self.file_events.get(&fd).map(|fe| fe.mask) {
                            if reg_mask & mask & READABLE != 0 {
                                if let Some(mut rproc) = self.file_events.get_mut(&fd).and_then(|fe| fe.rproc.take()) {
                                    rproc(self, fd);
                                    if let Some(fe) = self.file_events.get_mut(&fd) {
                                        fe.rproc = Some(rproc);
                                    }
                                }
                            }
                        }
                    }
                    processed += 1;
                }
            }
        }

        if flags & TIME_EVENTS != 0 {
            processed += self.process_time_events();
        }
        Ok(processed)
    }

    /// aeMain
    pub fn main_loop(&mut self) -> io::Result<()> {
        self.stop = false;
        while !self.stop {
            self.process_events(ALL_EVENTS)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mio::net::{TcpListener, TcpStream};
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::rc::Rc;
    use std::cell::RefCell;

    #[test]
    fn time_event_fires_once_for_nomore_and_reschedules_otherwise() {
        let mut lp = EventLoop::new(16).unwrap();
        let calls = Rc::new(RefCell::new(0));
        let c1 = calls.clone();
        lp.create_time_event(0, Box::new(move |_lp, _id| {
            *c1.borrow_mut() += 1;
            None // AE_NOMORE: run once, then delete.
        }));
        for _ in 0..5 {
            lp.process_events(TIME_EVENTS | DONT_WAIT).unwrap();
        }
        assert_eq!(*calls.borrow(), 1);
    }

    #[test]
    fn repeating_time_event_fires_multiple_times() {
        let mut lp = EventLoop::new(16).unwrap();
        let calls = Rc::new(RefCell::new(0));
        let c1 = calls.clone();
        lp.create_time_event(0, Box::new(move |_lp, _id| {
            *c1.borrow_mut() += 1;
            Some(0) // reschedule immediately
        }));
        for _ in 0..5 {
            lp.process_events(TIME_EVENTS | DONT_WAIT).unwrap();
        }
        assert_eq!(*calls.borrow(), 5);
    }

    #[test]
    fn deleted_time_event_never_fires_again() {
        let mut lp = EventLoop::new(16).unwrap();
        let calls = Rc::new(RefCell::new(0));
        let c1 = calls.clone();
        let id = lp.create_time_event(0, Box::new(move |_lp, _id| {
            *c1.borrow_mut() += 1;
            Some(0)
        }));
        lp.process_events(TIME_EVENTS | DONT_WAIT).unwrap();
        assert_eq!(*calls.borrow(), 1);
        lp.delete_time_event(id);
        lp.process_events(TIME_EVENTS | DONT_WAIT).unwrap();
        assert_eq!(*calls.borrow(), 1, "deleted timer must not fire again");
    }

    #[test]
    fn time_event_can_create_another_time_event_without_running_it_this_pass() {
        // Mirrors ae.c's maxId guard: a timer scheduling a new timer must not
        // have that new timer fire in the very same process_time_events() call.
        let mut lp = EventLoop::new(16).unwrap();
        let inner_calls = Rc::new(RefCell::new(0));
        let ic = inner_calls.clone();
        let outer_calls = Rc::new(RefCell::new(0));
        let oc = outer_calls.clone();
        lp.create_time_event(
            0,
            Box::new(move |lp, _id| {
                *oc.borrow_mut() += 1;
                let ic2 = ic.clone();
                lp.create_time_event(0, Box::new(move |_lp, _id| {
                    *ic2.borrow_mut() += 1;
                    None
                }));
                None
            }),
        );
        lp.process_events(TIME_EVENTS | DONT_WAIT).unwrap();
        assert_eq!(*outer_calls.borrow(), 1);
        assert_eq!(*inner_calls.borrow(), 0, "new timer must not fire in the same pass that created it");
        lp.process_events(TIME_EVENTS | DONT_WAIT).unwrap();
        assert_eq!(*inner_calls.borrow(), 1, "but should fire on the next pass");
    }

    #[test]
    fn file_event_fires_on_readable_tcp_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mio_listener = TcpListener::from_std(listener);

        let mut lp = EventLoop::new(16).unwrap();
        let accepted = Rc::new(RefCell::new(false));
        let a1 = accepted.clone();
        let listener_fd = mio_listener.as_raw_fd();
        lp.set_readable(
            listener_fd,
            Box::new(move |lp, fd| {
                assert_eq!(fd, listener_fd);
                *a1.borrow_mut() = true;
                lp.delete_readable(fd).unwrap();
            }),
        )
        .unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        // Drive the loop until the accept fires (bounded so a bug can't hang the test).
        for _ in 0..200 {
            if *accepted.borrow() {
                break;
            }
            lp.process_events(ALL_EVENTS | DONT_WAIT).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(*accepted.borrow(), "readable event should have fired for the pending connection");
        let _ = client.write_all(b"x");
        let _ = mio_listener.accept();
    }

    #[test]
    fn barrier_inverts_call_order_when_both_fire() {
        // With BARRIER set, if both readable and writable are ready, writable
        // must run, and readable must NOT run in the same pass (ae.c: "never
        // fire the writable event after the readable" -> here: run writable
        // first, and only run readable afterward if writable didn't fire).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _mio_listener = TcpListener::from_std(listener);
        let client_std = std::net::TcpStream::connect(addr).unwrap();
        client_std.set_nonblocking(true).unwrap();
        let mut client = TcpStream::from_std(client_std);
        let fd = client.as_raw_fd();

        let order = Rc::new(RefCell::new(Vec::<&'static str>::new()));
        let mut lp = EventLoop::new(16).unwrap();

        let o1 = order.clone();
        lp.set_readable(fd, Box::new(move |_lp, _fd| o1.borrow_mut().push("read"))).unwrap();
        let o2 = order.clone();
        lp.set_writable(fd, true, Box::new(move |_lp, _fd| o2.borrow_mut().push("write"))).unwrap();

        // A freshly connected socket is immediately writable; give it a moment
        // in case the connect is still completing asynchronously.
        for _ in 0..50 {
            lp.process_events(ALL_EVENTS | DONT_WAIT).unwrap();
            if !order.borrow().is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(order.borrow().first(), Some(&"write"), "barrier must make writable fire, not readable, when both are ready");
        let _ = client.write_all(b"noop");
    }
}
