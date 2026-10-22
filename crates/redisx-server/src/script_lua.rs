//! Lua engine — ported from `redis_src/src/script_lua.c`/`function_lua.c`,
//! using the `mlua` crate (vendored Lua 5.4; no system Lua install needed,
//! unlike `pyo3`'s Python dependency in `script_python.rs`) instead of
//! hand-written C-API bindings. See `script.rs`'s module doc for the overall
//! `FUNCTION`/`FCALL` design and the `redis.call` reentrancy approach shared
//! by both engines.
//!
//! RESP↔Lua conversion follows the same convention `script_lua.c` documents:
//! RESP null → Lua `false`, RESP array/set/push → a 1-indexed Lua table,
//! RESP map → a flat alternating-keys-and-values table (a simplification —
//! real Redis also exposes `map` fields set-theoretically via a `__mapfield`
//! marker that nothing in this fork's RESP decoding path consumes anyway),
//! a Lua table with an `err`/`ok` string field → RESP error/simple-string,
//! any other Lua table → a RESP array (stopping at the first `nil`, Lua's
//! own array-length convention), Lua `false`/`nil` → RESP null, `true` → `1`.

use crate::script::{push_context, redis_call_impl, sanitize_error, CompiledFunction};
use crate::server::Server;
use redisx_net::resp::Reply;
use std::cell::RefCell;

thread_local! {
    /// Collects `redis.register_function(name, callback)` calls made while
    /// `load_lua_library` is executing a library's top-level chunk. `None`
    /// outside of a `FUNCTION LOAD`, so a stray call at runtime is rejected.
    static LOADING_FUNCTIONS: RefCell<Option<Vec<(String, mlua::RegistryKey)>>> = const { RefCell::new(None) };
}

/// Installs the `redis` global table (`call`/`pcall`/`register_function`)
/// into a fresh `mlua::Lua` — called once, when `Server::new` creates its
/// persistent VM.
pub fn install(lua: &mlua::Lua) -> mlua::Result<()> {
    let redis_table = lua.create_table()?;

    redis_table.set(
        "call",
        lua.create_function(|lua, args: mlua::MultiValue| {
            let argv = lua_args_to_argv(&args)?;
            match redis_call_impl(argv) {
                Reply::Error(e) => Err(mlua::Error::RuntimeError(e)),
                other => reply_to_lua(lua, &other),
            }
        })?,
    )?;

    redis_table.set(
        "pcall",
        lua.create_function(|lua, args: mlua::MultiValue| {
            let argv = lua_args_to_argv(&args)?;
            reply_to_lua(lua, &redis_call_impl(argv))
        })?,
    )?;

    redis_table.set(
        "register_function",
        lua.create_function(|lua, args: mlua::MultiValue| {
            let mut iter = args.into_iter();
            let first = iter.next().ok_or_else(|| mlua::Error::RuntimeError("register_function: missing arguments".to_string()))?;
            let (name, callback): (String, mlua::Function) = if let mlua::Value::Table(t) = &first {
                (t.get("function_name")?, t.get("callback")?)
            } else {
                let name: String = lua.unpack(first)?;
                let callback_val = iter
                    .next()
                    .ok_or_else(|| mlua::Error::RuntimeError("register_function: missing callback".to_string()))?;
                (name, lua.unpack(callback_val)?)
            };
            let key = lua.create_registry_value(callback)?;
            LOADING_FUNCTIONS.with(|c| {
                let mut slot = c.borrow_mut();
                match slot.as_mut() {
                    Some(v) => {
                        v.push((name, key));
                        Ok(())
                    }
                    None => Err(mlua::Error::RuntimeError("redis.register_function can only be called during FUNCTION LOAD".to_string())),
                }
            })
        })?,
    )?;

    lua.globals().set("redis", redis_table)?;
    Ok(())
}

fn lua_args_to_argv(args: &mlua::MultiValue) -> mlua::Result<Vec<Vec<u8>>> {
    args.iter()
        .map(|v| match v {
            mlua::Value::String(s) => Ok(s.as_bytes().to_vec()),
            mlua::Value::Integer(i) => Ok(i.to_string().into_bytes()),
            mlua::Value::Number(n) => Ok(n.to_string().into_bytes()),
            _ => Err(mlua::Error::RuntimeError("Lua redis lib command arguments must be strings or integers".to_string())),
        })
        .collect()
}

/// RESP reply → Lua value, for what `redis.call`/`redis.pcall` return.
fn reply_to_lua<'lua>(lua: &'lua mlua::Lua, reply: &Reply) -> mlua::Result<mlua::Value<'lua>> {
    Ok(match reply {
        Reply::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", s.clone())?;
            mlua::Value::Table(t)
        }
        Reply::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", e.clone())?;
            mlua::Value::Table(t)
        }
        Reply::Integer(n) => mlua::Value::Integer(*n),
        Reply::BulkString(b) => mlua::Value::String(lua.create_string(b)?),
        Reply::Null | Reply::NullBulkString | Reply::NullArray => mlua::Value::Boolean(false),
        Reply::Boolean(b) => mlua::Value::Boolean(*b),
        Reply::Double(d) => mlua::Value::Number(*d),
        Reply::BigNumber(s) => mlua::Value::String(lua.create_string(s)?),
        Reply::VerbatimString { data, .. } => mlua::Value::String(lua.create_string(data)?),
        Reply::Array(items) | Reply::Set(items) | Reply::Push(items) | Reply::Multi(items) => {
            // `Multi` (several concatenated top-level frames — only ever
            // produced by PUBSUB commands, which `redis.call` from a script
            // has no real reason to invoke) has no multi-frame concept
            // inside a single Lua value, so it's just exposed as an array of
            // its converted frames — reasonable, if academic, fallback.
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                t.set((i + 1) as i64, reply_to_lua(lua, item)?)?;
            }
            mlua::Value::Table(t)
        }
        Reply::Map(pairs) => {
            let t = lua.create_table()?;
            let mut i = 1i64;
            for (k, v) in pairs {
                t.set(i, reply_to_lua(lua, k)?)?;
                i += 1;
                t.set(i, reply_to_lua(lua, v)?)?;
                i += 1;
            }
            mlua::Value::Table(t)
        }
    })
}

/// Lua value → RESP reply, for a function's final return value. (No `&Lua`
/// parameter needed: `mlua::Table`/`Value` already carry their own reference
/// to the VM internally.)
fn lua_value_to_reply(value: mlua::Value<'_>) -> Reply {
    match value {
        mlua::Value::Nil => Reply::Null,
        mlua::Value::Boolean(true) => Reply::Integer(1),
        mlua::Value::Boolean(false) => Reply::Null,
        mlua::Value::Integer(i) => Reply::Integer(i),
        mlua::Value::Number(n) => Reply::Integer(n as i64),
        mlua::Value::String(s) => Reply::BulkString(s.as_bytes().to_vec()),
        mlua::Value::Table(t) => {
            if let Ok(mlua::Value::String(err)) = t.get::<_, mlua::Value>("err") {
                return Reply::Error(err.to_str().map(|s| s.to_string()).unwrap_or_default());
            }
            if let Ok(mlua::Value::String(ok)) = t.get::<_, mlua::Value>("ok") {
                return Reply::SimpleString(ok.to_str().map(|s| s.to_string()).unwrap_or_default());
            }
            let mut items = Vec::new();
            let mut i = 1i64;
            loop {
                match t.get::<_, mlua::Value>(i) {
                    Ok(mlua::Value::Nil) | Err(_) => break,
                    Ok(v) => {
                        items.push(lua_value_to_reply(v));
                        i += 1;
                    }
                }
            }
            Reply::Array(items)
        }
        _ => Reply::Null,
    }
}

impl Server {
    /// `FUNCTION LOAD`'s Lua path: runs the library's top-level chunk (which
    /// is expected to call `redis.register_function` at least once) and
    /// collects the registered functions.
    pub(crate) fn load_lua_library(&mut self, libname: &str, body: &str) -> Result<Vec<(String, CompiledFunction)>, String> {
        LOADING_FUNCTIONS.with(|c| *c.borrow_mut() = Some(Vec::new()));
        let exec_result = self.lua.load(body).set_name(libname).exec();
        let collected = LOADING_FUNCTIONS.with(|c| c.borrow_mut().take()).unwrap_or_default();

        exec_result.map_err(|e| sanitize_error(&e.to_string()))?;
        if collected.is_empty() {
            return Err("ERR No functions registered".to_string());
        }
        Ok(collected.into_iter().map(|(name, key)| (name, CompiledFunction::Lua(key))).collect())
    }

    /// # Safety / soundness note
    /// `push_context` installs `self as *mut Server` so `redis.call` (invoked
    /// re-entrantly from inside `func.call(...)` below) can get `&mut Server`
    /// again (see `script.rs`'s module doc). That reentrant access happens
    /// while this function still holds a live borrow of `self.lua` (required
    /// by `mlua` 0.9's lifetime-generic `Value<'lua>`/`Function<'lua>` API —
    /// there is no borrow-free way to call a stored `Function` in this mlua
    /// version). This is sound in the sense that matters here — `dispatch`'s
    /// command handlers never touch `self.lua`/`self.compiled`/`self.libraries`/
    /// `self.functions`, so the two accesses never actually race or alias the
    /// same data — but it is not something `miri`/Stacked Borrows would call
    /// clean, since it technically holds two live paths to `*self` at once.
    /// Noted here rather than left implicit, same as Phase 1's `ae.rs`
    /// take-and-restore pattern documents its own unsafe reasoning.
    pub(crate) fn call_lua_function(&mut self, name: &str, keys: &[Vec<u8>], args: &[Vec<u8>], dbid: usize, peer: &str, read_only: bool) -> Reply {
        if !matches!(self.compiled.get(name), Some(CompiledFunction::Lua(_))) {
            return Reply::Error("ERR Function not found".to_string());
        }

        let server_ptr = self as *mut Server;
        let prev = push_context(server_ptr, dbid, peer, read_only);
        let result = (|| -> mlua::Result<Reply> {
            let Some(CompiledFunction::Lua(key)) = self.compiled.get(name) else { unreachable!() };
            let func: mlua::Function = self.lua.registry_value(key)?;
            let keys_table = self.lua.create_table()?;
            for (i, k) in keys.iter().enumerate() {
                keys_table.set((i + 1) as i64, self.lua.create_string(k)?)?;
            }
            let args_table = self.lua.create_table()?;
            for (i, a) in args.iter().enumerate() {
                args_table.set((i + 1) as i64, self.lua.create_string(a)?)?;
            }
            let ret: mlua::Value = func.call((keys_table, args_table))?;
            Ok(lua_value_to_reply(ret))
        })();
        crate::script::pop_context(prev);

        match result {
            Ok(r) => r,
            Err(e) => Reply::Error(format!("ERR {}", sanitize_error(&e.to_string()))),
        }
    }

    /// `EVAL`/`EVALSHA` — "남은 부분" pass addition, legacy `eval.c`
    /// scripting kept alongside `FUNCTION`/`FCALL` rather than replacing it
    /// (same as real Redis still supporting both). Unlike a registered
    /// `FUNCTION`, there's no stored callback to invoke with `(keys, args)`
    /// parameters — the script body is compiled fresh every call and reads
    /// `KEYS`/`ARGV` as plain Lua globals instead, matching real `eval.c`'s
    /// own convention exactly. Reentrancy and error handling are otherwise
    /// identical to `call_lua_function` above (same soundness note applies).
    pub(crate) fn eval_lua_script(&mut self, body: &str, keys: &[Vec<u8>], args: &[Vec<u8>], dbid: usize, peer: &str) -> Reply {
        let server_ptr = self as *mut Server;
        // `EVAL`/`EVALSHA` have no read-only counterpart in real Redis
        // either (there's no `EVAL_RO`) — always `false`.
        let prev = push_context(server_ptr, dbid, peer, false);
        let result = (|| -> mlua::Result<Reply> {
            let keys_table = self.lua.create_table()?;
            for (i, k) in keys.iter().enumerate() {
                keys_table.set((i + 1) as i64, self.lua.create_string(k)?)?;
            }
            let args_table = self.lua.create_table()?;
            for (i, a) in args.iter().enumerate() {
                args_table.set((i + 1) as i64, self.lua.create_string(a)?)?;
            }
            self.lua.globals().set("KEYS", keys_table)?;
            self.lua.globals().set("ARGV", args_table)?;
            let ret: mlua::Value = self.lua.load(body).eval()?;
            Ok(lua_value_to_reply(ret))
        })();
        crate::script::pop_context(prev);

        match result {
            Ok(r) => r,
            Err(e) => Reply::Error(format!("ERR {}", sanitize_error(&e.to_string()))),
        }
    }
}
