//! Scripting — `FUNCTION`/`FCALL`, ported from `redis_src/src/functions.c`'s
//! engine-abstraction design: a pluggable `engine` vtable so `FUNCTION LOAD`
//! can register a library written in any supported language, and `FCALL`
//! dispatches to whichever engine compiled it. This fork keeps that same
//! shape (see [`CompiledFunction`]) with two engines: Lua via the `mlua`
//! crate (`script_lua.rs`) and, behind the `python` Cargo feature, Python via
//! `pyo3` (`script_python.rs`) — mirroring this project's own
//! `function_lua.c`/`function_python.c` pair, Lua kept fully working
//! alongside Python rather than replaced by it.
//!
//! ## The reentrancy problem, and how it's solved
//!
//! `redis.call()`/`redis.pcall()` (Lua) and `redis.call()`/`redis.pcall()`
//! (Python) need to run an ordinary Redis command *from inside* a script
//! callback — i.e. they need `&mut Server` again, while a `&mut Server` is
//! already on the stack (the `FCALL` command handler that's running the
//! script in the first place). Neither `mlua`'s nor `pyo3`'s registered
//! native functions can borrow that outer `&mut Server` directly (`'static`
//! closures, no lifetime to tie to it). This fork's own `function_python.c`
//! solved exactly this with C static globals (`current_run_ctx`); the Rust
//! equivalent is a thread-local holding a raw `*mut Server`
//! (`push_context`/`pop_context`/`with_current_server`), sound under the
//! same single-threaded-reactor invariant Phase 1 already established: the
//! pointer is only ever dereferenced synchronously, nested inside the exact
//! call that set it, never concurrently.
//!
//! ## Scope (`redis_document/25_redisX.md`, Phase 6)
//!
//! `FUNCTION LOAD`/`FCALL`/`FUNCTION DELETE`/`FUNCTION LIST` only — not the
//! legacy `EVAL`/`EVALSHA` path (`eval.c`, hardcoded to Lua in the C version
//! and orthogonal machinery to `functions.c`'s engine abstraction; `FUNCTION`
//! already exercises both engines end-to-end, which is this phase's stated
//! completion criterion). No function flags (`no-writes` etc.), no
//! `FUNCTION DUMP`/`RESTORE`, no script kill/timeout.

use crate::server::{ClientState, Server};
use redisx_net::resp::Reply;
use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineKind {
    Lua,
    Python,
}

impl EngineKind {
    fn name(self) -> &'static str {
        match self {
            EngineKind::Lua => "LUA",
            EngineKind::Python => "PYTHON",
        }
    }
}

pub enum CompiledFunction {
    Lua(mlua::RegistryKey),
    #[cfg(feature = "python")]
    Python(pyo3::Py<pyo3::PyAny>),
}

pub struct Library {
    pub name: String,
    pub engine: EngineKind,
    pub functions: Vec<String>,
    /// The library's original, unmodified source (shebang line included) —
    /// kept around purely so `rdb.rs` can persist it verbatim behind the
    /// `FUNCTION2` opcode and restore it with a plain `function_load` call
    /// on reload, the same way `FUNCTION LOAD` itself works.
    pub source: String,
}

thread_local! {
    static CURRENT_SERVER: Cell<*mut Server> = const { Cell::new(std::ptr::null_mut()) };
    static CURRENT_DBID: Cell<usize> = const { Cell::new(0) };
    /// Set while running an `FCALL_RO`/`EVAL_RO`-style read-only invocation
    /// (currently: `FCALL_RO` only — see `cmd_fcall`'s doc comment) — checked
    /// by `redis_call_impl` to reject any write command a script tries to
    /// run through `redis.call`/`redis.pcall` while this is `true`, closing
    /// the gap where `FCALL_RO` used to be a plain unenforced alias of
    /// `FCALL`.
    static CURRENT_READONLY: Cell<bool> = const { Cell::new(false) };
}
// Peer string needs non-Copy storage; kept separately since `Cell` requires `Copy`.
thread_local! {
    static CURRENT_PEER: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// Saves the previous context (for correct nesting, however unlikely nested
/// scripting is in this phase) and installs a new one. Returns a token that
/// `pop_context` consumes to restore it.
pub(crate) fn push_context(server: *mut Server, dbid: usize, peer: &str, read_only: bool) -> (*mut Server, usize, String, bool) {
    let prev = (
        CURRENT_SERVER.with(|c| c.get()),
        CURRENT_DBID.with(|c| c.get()),
        CURRENT_PEER.with(|c| c.borrow().clone()),
        CURRENT_READONLY.with(|c| c.get()),
    );
    CURRENT_SERVER.with(|c| c.set(server));
    CURRENT_DBID.with(|c| c.set(dbid));
    CURRENT_PEER.with(|c| *c.borrow_mut() = peer.to_string());
    CURRENT_READONLY.with(|c| c.set(read_only));
    prev
}

pub(crate) fn pop_context(prev: (*mut Server, usize, String, bool)) {
    CURRENT_SERVER.with(|c| c.set(prev.0));
    CURRENT_DBID.with(|c| c.set(prev.1));
    CURRENT_PEER.with(|c| *c.borrow_mut() = prev.2);
    CURRENT_READONLY.with(|c| c.set(prev.3));
}

/// `redis.call`/`redis.pcall`'s shared implementation, used by both engines.
/// Goes through the exact same `Server::dispatch` every normal client
/// command uses, so type-checking, `WRONGTYPE`, replication propagation and
/// AOF logging are all automatically correct — a script's writes are
/// indistinguishable from a client's.
pub(crate) fn redis_call_impl(argv: Vec<Vec<u8>>) -> Reply {
    let ptr = CURRENT_SERVER.with(|c| c.get());
    if ptr.is_null() {
        return Reply::Error("ERR redis.call can only be used from within a script".to_string());
    }
    if CURRENT_READONLY.with(|c| c.get()) {
        if let Some(name) = argv.first() {
            if crate::server::is_write_command(&String::from_utf8_lossy(name)) {
                return Reply::Error("ERR Write commands are not allowed when calling a function in read-only mode (FCALL_RO)".to_string());
            }
        }
    }
    let dbid = CURRENT_DBID.with(|c| c.get());
    let peer = CURRENT_PEER.with(|c| c.borrow().clone());
    // SAFETY: `ptr` was set by `push_context` from a live `&mut Server` on an
    // enclosing stack frame (the `FCALL` handler); this call is strictly
    // nested inside that frame (no concurrency, no stored/escaped pointer —
    // see this module's doc comment), so no other reference to `*ptr` can be
    // live at the same time.
    let server: &mut Server = unsafe { &mut *ptr };
    let mut client = ClientState { db_index: dbid, peer, ..ClientState::default() };
    server.dispatch(&mut client, &argv)
}

/// `functionExtractLibMetaData` — parses the mandatory first-line shebang
/// (`#!lua name=mylib` / `#!python name=mylib`) and returns
/// `(engine, library_name, body)`.
/// A RESP simple/bulk error line can't contain a literal CR or LF (it would
/// desync the protocol framing — real clients, including `redis-cli`, reject
/// it outright). Lua/Python error messages routinely contain multi-line
/// tracebacks, so every error surfaced as a `Reply::Error` from scripting
/// goes through this first.
pub(crate) fn sanitize_error(msg: &str) -> String {
    msg.replace(['\r', '\n'], " ")
}

fn parse_shebang(code: &str) -> Result<(EngineKind, String, &str), String> {
    let (first_line, rest) = code.split_once('\n').unwrap_or((code, ""));
    let first_line = first_line.trim();
    let Some(spec) = first_line.strip_prefix("#!") else {
        return Err("ERR Missing library metadata".to_string());
    };
    let mut parts = spec.split_whitespace();
    let engine_name = parts.next().ok_or("ERR Missing library engine")?;
    let engine = match engine_name.to_ascii_lowercase().as_str() {
        "lua" => EngineKind::Lua,
        "python" => EngineKind::Python,
        other => return Err(format!("ERR Could not find engine '{other}'")),
    };
    let mut libname = None;
    for part in parts {
        if let Some(n) = part.strip_prefix("name=") {
            libname = Some(n.to_string());
        }
    }
    let libname = libname.ok_or("ERR Missing library name")?;
    Ok((engine, libname, rest))
}

impl Server {
    /// `FUNCTION LOAD [REPLACE] <code>`
    pub fn function_load(&mut self, code: &str, replace: bool) -> Reply {
        let (engine, libname, body) = match parse_shebang(code) {
            Ok(v) => v,
            Err(e) => return Reply::Error(e),
        };
        if self.libraries.contains_key(&libname) && !replace {
            return Reply::Error(format!("ERR Library '{libname}' already exists"));
        }

        let result = match engine {
            EngineKind::Lua => self.load_lua_library(&libname, body),
            #[cfg(feature = "python")]
            EngineKind::Python => self.load_python_library(&libname, body),
            #[cfg(not(feature = "python"))]
            EngineKind::Python => {
                Err("ERR Python engine not compiled into this build (rebuild with `--features python`)".to_string())
            }
        };

        match result {
            Ok(compiled_functions) => {
                self.unload_library(&libname);
                let function_names: Vec<String> = compiled_functions.iter().map(|(n, _)| n.clone()).collect();
                for (fname, compiled) in compiled_functions {
                    self.functions.insert(fname.clone(), libname.clone());
                    self.compiled.insert(fname, compiled);
                }
                self.libraries.insert(libname.clone(), Library { name: libname.clone(), engine, functions: function_names, source: code.to_string() });
                Reply::bulk(libname.into_bytes())
            }
            Err(e) => {
                let e = sanitize_error(&e);
                Reply::Error(if e.starts_with("ERR") { e } else { format!("ERR Error compiling function: {e}") })
            }
        }
    }

    fn unload_library(&mut self, libname: &str) {
        if let Some(old) = self.libraries.remove(libname) {
            for fname in old.functions {
                self.functions.remove(&fname);
                self.compiled.remove(&fname);
            }
        }
    }

    pub fn function_delete(&mut self, libname: &str) -> Reply {
        if !self.libraries.contains_key(libname) {
            return Reply::Error("ERR Library not found".to_string());
        }
        self.unload_library(libname);
        Reply::ok()
    }

    /// `FUNCTION FLUSH` — every library gone in one call, always `+OK` even
    /// when nothing was loaded (matching real Redis: flushing an empty set
    /// isn't an error). Real Redis's `ASYNC`/`SYNC` distinction is about
    /// whether the freed memory is reclaimed on a background thread
    /// (`lazyfree`) or inline — moot here, since a library is just a
    /// `String` source plus a few `HashMap` entries (`libraries`/
    /// `functions`/`compiled`), nowhere near large enough for that
    /// distinction to matter the way a huge keyspace flush's would (see
    /// `cmd_flushall`'s own `lazyfree-lazy-user-flush` handling for where
    /// that size threshold actually does matter in this fork).
    pub fn function_flush(&mut self) -> Reply {
        let libnames: Vec<String> = self.libraries.keys().cloned().collect();
        for libname in libnames {
            self.unload_library(&libname);
        }
        Reply::ok()
    }

    pub fn function_list(&self) -> Reply {
        let libs = self
            .libraries
            .values()
            .map(|lib| {
                Reply::Map(vec![
                    (Reply::bulk(b"library_name".to_vec()), Reply::bulk(lib.name.clone().into_bytes())),
                    (Reply::bulk(b"engine".to_vec()), Reply::bulk(lib.engine.name().as_bytes().to_vec())),
                    (
                        Reply::bulk(b"functions".to_vec()),
                        Reply::Array(
                            lib.functions
                                .iter()
                                .map(|f| {
                                    Reply::Map(vec![
                                        (Reply::bulk(b"name".to_vec()), Reply::bulk(f.clone().into_bytes())),
                                        (Reply::bulk(b"description".to_vec()), Reply::Null),
                                        (Reply::bulk(b"flags".to_vec()), Reply::Array(vec![])),
                                    ])
                                })
                                .collect(),
                        ),
                    ),
                ])
            })
            .collect();
        Reply::Array(libs)
    }

    /// `FCALL`/`FCALL_RO funcname numkeys [key ...] [arg ...]`
    pub fn call_function(&mut self, name: &str, keys: &[Vec<u8>], args: &[Vec<u8>], dbid: usize, peer: &str, read_only: bool) -> Reply {
        let Some(libname) = self.functions.get(name).cloned() else {
            return Reply::Error("ERR Function not found".to_string());
        };
        let Some(lib) = self.libraries.get(&libname) else {
            return Reply::Error("ERR Function not found".to_string());
        };
        match lib.engine {
            EngineKind::Lua => self.call_lua_function(name, keys, args, dbid, peer, read_only),
            #[cfg(feature = "python")]
            EngineKind::Python => self.call_python_function(name, keys, args, dbid, peer, read_only),
            #[cfg(not(feature = "python"))]
            EngineKind::Python => Reply::Error("ERR Python engine not compiled into this build".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ClientState;

    const ECHO_LIB: &str = "#!lua name=mylib\nredis.register_function('myecho', function(keys, args) return args[1] end)";

    #[test]
    fn load_requires_shebang() {
        let mut s = Server::new(1);
        assert!(matches!(s.function_load("return 1", false), Reply::Error(_)));
    }

    #[test]
    fn load_requires_at_least_one_registered_function() {
        let mut s = Server::new(1);
        assert!(matches!(s.function_load("#!lua name=mylib\nreturn 1", false), Reply::Error(_)));
    }

    #[test]
    fn load_and_fcall_echo_function() {
        let mut s = Server::new(1);
        assert_eq!(s.function_load(ECHO_LIB, false), Reply::bulk(b"mylib".to_vec()));
        assert_eq!(s.call_function("myecho", &[], &[b"hello".to_vec()], 0, "internal", false), Reply::bulk(b"hello".to_vec()));
    }

    #[test]
    fn load_without_replace_rejects_duplicate_library_name() {
        let mut s = Server::new(1);
        s.function_load(ECHO_LIB, false);
        assert!(matches!(s.function_load(ECHO_LIB, false), Reply::Error(_)));
        assert_eq!(s.function_load(ECHO_LIB, true), Reply::bulk(b"mylib".to_vec()));
    }

    #[test]
    fn fcall_unknown_function_errors() {
        let mut s = Server::new(1);
        assert!(matches!(s.call_function("nope", &[], &[], 0, "internal", false), Reply::Error(_)));
    }

    #[test]
    fn function_delete_removes_library_and_its_functions() {
        let mut s = Server::new(1);
        s.function_load(ECHO_LIB, false);
        assert_eq!(s.function_delete("mylib"), Reply::ok());
        assert!(matches!(s.call_function("myecho", &[], &[b"x".to_vec()], 0, "internal", false), Reply::Error(_)));
        assert!(matches!(s.function_delete("mylib"), Reply::Error(_)), "deleting again should fail, already gone");
    }

    #[test]
    fn function_flush_removes_every_library_and_still_succeeds_when_empty() {
        let mut s = Server::new(1);
        // `flush_empty` must succeed with nothing loaded at all — matching
        // real Redis, flushing an already-empty set isn't an error.
        assert_eq!(s.function_flush(), Reply::ok());

        s.function_load(ECHO_LIB, false);
        let second_lib = "#!lua name=otherlib\nredis.register_function('other', function(keys, args) return 1 end)";
        s.function_load(second_lib, false);
        assert_eq!(s.call_function("myecho", &[], &[b"x".to_vec()], 0, "internal", false), Reply::bulk(b"x".to_vec()));

        assert_eq!(s.function_flush(), Reply::ok());

        assert!(matches!(s.call_function("myecho", &[], &[b"x".to_vec()], 0, "internal", false), Reply::Error(_)), "mylib's function must be gone");
        assert!(matches!(s.call_function("other", &[], &[], 0, "internal", false), Reply::Error(_)), "otherlib's function must be gone too");
        assert_eq!(s.libraries.len(), 0);
        assert_eq!(s.functions.len(), 0);
        assert_eq!(s.compiled.len(), 0);
        // A library name that existed before the flush must be loadable
        // again without needing REPLACE.
        assert_eq!(s.function_load(ECHO_LIB, false), Reply::bulk(b"mylib".to_vec()));
    }

    #[test]
    fn redis_call_from_lua_reaches_real_keyspace() {
        let mut s = Server::new(1);
        let setget_lib = "#!lua name=setget\n\
            redis.register_function('myset', function(keys, args) return redis.call('SET', keys[1], args[1]) end)\n\
            redis.register_function('myget', function(keys, args) return redis.call('GET', keys[1]) end)";
        s.function_load(setget_lib, false);
        assert_eq!(s.call_function("myset", &[b"k".to_vec()], &[b"v".to_vec()], 0, "internal", false), Reply::ok());

        // Prove it went through the real keyspace, not just the script's
        // local state, by reading the key back with an ordinary GET.
        let mut c = ClientState::default();
        assert_eq!(s.dispatch(&mut c, &[b"GET".to_vec(), b"k".to_vec()]), Reply::bulk(b"v".to_vec()));
        assert_eq!(s.call_function("myget", &[b"k".to_vec()], &[], 0, "internal", false), Reply::bulk(b"v".to_vec()));
    }

    #[test]
    fn redis_call_error_raises_a_lua_error_but_pcall_does_not() {
        let mut s = Server::new(1);
        s.dispatch(&mut ClientState::default(), &[b"SET".to_vec(), b"strkey".to_vec(), b"x".to_vec()]);
        let lib = "#!lua name=errlib\n\
            redis.register_function('callbad', function(keys, args) return redis.call('LPUSH', keys[1], 'x') end)\n\
            redis.register_function('pcallbad', function(keys, args) return redis.pcall('LPUSH', keys[1], 'x') end)";
        s.function_load(lib, false);
        assert!(matches!(s.call_function("callbad", &[b"strkey".to_vec()], &[], 0, "internal", false), Reply::Error(_)));
        assert!(matches!(s.call_function("pcallbad", &[b"strkey".to_vec()], &[], 0, "internal", false), Reply::Error(_)));
    }

    #[test]
    fn write_through_fcall_propagates_like_a_normal_command() {
        let mut s = Server::new(1);
        let lib = "#!lua name=setlib\nredis.register_function('doset', function(keys, args) redis.call('SET', keys[1], args[1]) end)";
        s.function_load(lib, false);
        s.call_function("doset", &[b"propkey".to_vec()], &[b"propval".to_vec()], 0, "internal", false);
        let prop = s.take_pending_propagation();
        assert!(!prop.is_empty(), "the SET inside the script should have been propagated");
        let text = String::from_utf8_lossy(&prop);
        assert!(text.contains("propkey") && text.contains("propval"), "got: {text}");
    }

    // Python engine — mirrors the Lua test block above, one-for-one, to
    // prove both engines genuinely satisfy the same `FUNCTION`/`FCALL`
    // contract (reentrancy, replication, error propagation) rather than only
    // the default engine being exercised. Python's `keys`/`args` are plain
    // 0-indexed lists (see `script_python.rs`'s module doc), unlike Lua's
    // 1-indexed tables.
    #[cfg(feature = "python")]
    mod python_tests {
        use super::*;

        const PY_ECHO_LIB: &str = "#!python name=pylib\nimport redis\ndef pyecho(keys, args):\n    return args[0]\nredis.register_function('pyecho', pyecho)";

        #[test]
        fn load_and_fcall_echo_function() {
            let mut s = Server::new(1);
            assert_eq!(s.function_load(PY_ECHO_LIB, false), Reply::bulk(b"pylib".to_vec()));
            assert_eq!(s.call_function("pyecho", &[], &[b"hello".to_vec()], 0, "internal", false), Reply::bulk(b"hello".to_vec()));
        }

        #[test]
        fn redis_call_from_python_reaches_real_keyspace() {
            let mut s = Server::new(1);
            let setget_lib = "#!python name=setget\n\
                import redis\n\
                def myset(keys, args):\n    return redis.call('SET', keys[0], args[0])\n\
                def myget(keys, args):\n    return redis.call('GET', keys[0])\n\
                redis.register_function('myset', myset)\n\
                redis.register_function('myget', myget)";
            s.function_load(setget_lib, false);
            assert_eq!(s.call_function("myset", &[b"k".to_vec()], &[b"v".to_vec()], 0, "internal", false), Reply::ok());

            let mut c = ClientState::default();
            assert_eq!(s.dispatch(&mut c, &[b"GET".to_vec(), b"k".to_vec()]), Reply::bulk(b"v".to_vec()));
            assert_eq!(s.call_function("myget", &[b"k".to_vec()], &[], 0, "internal", false), Reply::bulk(b"v".to_vec()));
        }

        #[test]
        fn redis_call_error_raises_a_python_error_but_pcall_does_not() {
            let mut s = Server::new(1);
            s.dispatch(&mut ClientState::default(), &[b"SET".to_vec(), b"strkey".to_vec(), b"x".to_vec()]);
            let lib = "#!python name=errlib\n\
                import redis\n\
                def callbad(keys, args):\n    return redis.call('LPUSH', keys[0], 'x')\n\
                def pcallbad(keys, args):\n    return redis.pcall('LPUSH', keys[0], 'x')\n\
                redis.register_function('callbad', callbad)\n\
                redis.register_function('pcallbad', pcallbad)";
            s.function_load(lib, false);
            assert!(matches!(s.call_function("callbad", &[b"strkey".to_vec()], &[], 0, "internal", false), Reply::Error(_)));
            assert!(matches!(s.call_function("pcallbad", &[b"strkey".to_vec()], &[], 0, "internal", false), Reply::Error(_)));
        }

        #[test]
        fn write_through_fcall_propagates_like_a_normal_command() {
            let mut s = Server::new(1);
            let lib = "#!python name=setlib\n\
                import redis\n\
                def doset(keys, args):\n    redis.call('SET', keys[0], args[0])\n\
                redis.register_function('doset', doset)";
            s.function_load(lib, false);
            s.call_function("doset", &[b"propkey".to_vec()], &[b"propval".to_vec()], 0, "internal", false);
            let prop = s.take_pending_propagation();
            assert!(!prop.is_empty(), "the SET inside the script should have been propagated");
            let text = String::from_utf8_lossy(&prop);
            assert!(text.contains("propkey") && text.contains("propval"), "got: {text}");
        }

        #[test]
        fn both_engines_coexist_lua_not_replaced_by_python() {
            let mut s = Server::new(1);
            s.function_load(ECHO_LIB, false);
            s.function_load(PY_ECHO_LIB, false);
            // Both libraries (different engines, different names) remain
            // independently callable — loading the Python one does not
            // disturb the Lua one.
            assert_eq!(s.call_function("myecho", &[], &[b"from-lua".to_vec()], 0, "internal", false), Reply::bulk(b"from-lua".to_vec()));
            assert_eq!(s.call_function("pyecho", &[], &[b"from-python".to_vec()], 0, "internal", false), Reply::bulk(b"from-python".to_vec()));
        }
    }
}
