//! Python engine — ported from `redis_src/src/function_python.c`'s half of
//! the dual-engine design (see `script.rs`'s module doc for the vtable
//! picture and the shared `redis.call` reentrancy approach). Uses `pyo3`
//! (`auto-initialize` feature: finds and starts the system's already-installed
//! Python automatically, no `BUILD_PYTHON`-style configure-time detection
//! needed) instead of hand-written CPython C-API bindings.
//!
//! RESP↔Python conversion mirrors `script_lua.rs`'s conventions, adapted to
//! Python's native container types instead of Lua's single `table`: RESP
//! null → Python `None`, RESP array/set/push → a Python `list`, RESP map →
//! a real Python `dict` (unlike the Lua engine's flattened-table
//! simplification, Python actually has a dict so this one loses nothing), a
//! `dict` with an `err`/`ok` string key → RESP error/simple-string, any
//! other `dict` → RESP map, a `list`/`tuple` → RESP array, Python
//! `None`/`False` → RESP null, `True` → RESP integer 1. RESP bulk strings
//! are exposed as Python `str` (not `bytes`) for simplicity — same
//! documented-simplification spirit as the Lua engine's map-flattening — so
//! library code assuming valid UTF-8 text round-trips exactly; arbitrary
//! binary payloads get lossily decoded.
//!
//! A registered function receives `keys`/`args` as ordinary 0-indexed Python
//! `list`s (`keys[0]`, `args[0]`, ...) — unlike the Lua engine's 1-indexed
//! `table`s, matching each language's own indexing convention rather than
//! forcing one language's idiom onto the other.
//!
//! `#![allow(clippy::useless_conversion)]`: the `#[pyfunction]` macro expands
//! `call`/`pcall`/`register_function` into wrapper code that clippy flags as
//! converting `PyErr` into itself — a false positive in the macro's own
//! generated code, not this module's, and not suppressible per-function
//! since the lint fires on the generated wrapper item rather than the
//! annotated one.
#![allow(clippy::useless_conversion)]

use crate::script::{pop_context, push_context, redis_call_impl, sanitize_error, CompiledFunction};
use crate::server::Server;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyModule, PyTuple};
use redisx_net::resp::Reply;
use std::cell::RefCell;

type RegisteredFunctions = Vec<(String, Py<PyAny>)>;

thread_local! {
    /// Collects `redis.register_function(name, callback)` calls made while
    /// `load_python_library` is executing a library's module body. `None`
    /// outside of a `FUNCTION LOAD`, so a stray call at runtime is rejected.
    static LOADING_FUNCTIONS: RefCell<Option<RegisteredFunctions>> = const { RefCell::new(None) };
}

#[pyfunction]
#[pyo3(signature = (*args))]
fn call(args: Bound<'_, PyTuple>) -> PyResult<PyObject> {
    let py = args.py();
    let argv = py_tuple_to_argv(&args)?;
    match redis_call_impl(argv) {
        Reply::Error(e) => Err(PyRuntimeError::new_err(e)),
        other => reply_to_py(py, &other),
    }
}

#[pyfunction]
#[pyo3(signature = (*args))]
fn pcall(args: Bound<'_, PyTuple>) -> PyResult<PyObject> {
    let py = args.py();
    let argv = py_tuple_to_argv(&args)?;
    reply_to_py(py, &redis_call_impl(argv))
}

#[pyfunction]
fn register_function(function_name: String, callback: Py<PyAny>) -> PyResult<()> {
    LOADING_FUNCTIONS.with(|c| {
        let mut slot = c.borrow_mut();
        match slot.as_mut() {
            Some(v) => {
                v.push((function_name, callback));
                Ok(())
            }
            None => Err(PyRuntimeError::new_err("redis.register_function can only be called during FUNCTION LOAD")),
        }
    })
}

fn py_tuple_to_argv(args: &Bound<'_, PyTuple>) -> PyResult<Vec<Vec<u8>>> {
    args.iter()
        .map(|item| {
            if item.downcast::<PyBool>().is_ok() {
                Err(PyTypeError::new_err("Python redis lib command arguments must be strings or integers"))
            } else if let Ok(s) = item.extract::<String>() {
                Ok(s.into_bytes())
            } else if let Ok(i) = item.extract::<i64>() {
                Ok(i.to_string().into_bytes())
            } else if let Ok(f) = item.extract::<f64>() {
                Ok(f.to_string().into_bytes())
            } else {
                Err(PyTypeError::new_err("Python redis lib command arguments must be strings or integers"))
            }
        })
        .collect()
}

/// RESP reply → Python value, for what `redis.call`/`redis.pcall` return.
fn reply_to_py(py: Python<'_>, reply: &Reply) -> PyResult<PyObject> {
    Ok(match reply {
        Reply::SimpleString(s) => {
            let d = PyDict::new_bound(py);
            d.set_item("ok", s)?;
            d.into_py(py)
        }
        Reply::Error(e) => {
            let d = PyDict::new_bound(py);
            d.set_item("err", e)?;
            d.into_py(py)
        }
        Reply::Integer(n) => n.into_py(py),
        Reply::BulkString(b) => String::from_utf8_lossy(b).into_py(py),
        Reply::Null | Reply::NullBulkString | Reply::NullArray => py.None(),
        Reply::Boolean(b) => b.into_py(py),
        Reply::Double(d) => d.into_py(py),
        Reply::BigNumber(s) => s.clone().into_py(py),
        Reply::VerbatimString { data, .. } => String::from_utf8_lossy(data).into_py(py),
        // `Multi` (several concatenated top-level frames — only ever
        // produced by PUBSUB commands, which `redis.call` from a script has
        // no real reason to invoke) has no multi-frame concept inside a
        // single Python value, so it's just exposed as a list of its
        // converted frames — reasonable, if academic, fallback.
        Reply::Array(items) | Reply::Set(items) | Reply::Push(items) | Reply::Multi(items) => {
            let list = PyList::empty_bound(py);
            for item in items {
                list.append(reply_to_py(py, item)?)?;
            }
            list.into_py(py)
        }
        Reply::Map(pairs) => {
            let d = PyDict::new_bound(py);
            for (k, v) in pairs {
                d.set_item(reply_to_py(py, k)?, reply_to_py(py, v)?)?;
            }
            d.into_py(py)
        }
    })
}

/// Python value → RESP reply, for a function's final return value.
fn py_value_to_reply(value: &Bound<'_, PyAny>) -> Reply {
    if value.is_none() {
        return Reply::Null;
    }
    if let Ok(b) = value.downcast::<PyBool>() {
        return if b.is_true() { Reply::Integer(1) } else { Reply::Null };
    }
    if let Ok(i) = value.extract::<i64>() {
        return Reply::Integer(i);
    }
    if let Ok(f) = value.extract::<f64>() {
        return Reply::Integer(f as i64);
    }
    if let Ok(s) = value.extract::<String>() {
        return Reply::BulkString(s.into_bytes());
    }
    if let Ok(d) = value.downcast::<PyDict>() {
        if let Ok(Some(err)) = d.get_item("err") {
            if let Ok(s) = err.extract::<String>() {
                return Reply::Error(s);
            }
        }
        if let Ok(Some(ok)) = d.get_item("ok") {
            if let Ok(s) = ok.extract::<String>() {
                return Reply::SimpleString(s);
            }
        }
        return Reply::Map(d.iter().map(|(k, v)| (py_value_to_reply(&k), py_value_to_reply(&v))).collect());
    }
    if let Ok(list) = value.downcast::<PyList>() {
        return Reply::Array(list.iter().map(|v| py_value_to_reply(&v)).collect());
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        return Reply::Array(tuple.iter().map(|v| py_value_to_reply(&v)).collect());
    }
    Reply::Null
}

/// Installs the `redis` module (`call`/`pcall`/`register_function`) into
/// `sys.modules`, so a library's own `import redis` statement finds it —
/// called once, lazily, the first time a Python library is loaded (unlike
/// the Lua engine, there's no per-`Server` interpreter value to attach this
/// to: pyo3's `auto-initialize` feature owns one process-wide interpreter,
/// so the module only needs installing once no matter how many `Server`s
/// exist, e.g. across unit tests in the same process).
fn ensure_redis_module_installed(py: Python<'_>) -> PyResult<()> {
    let sys = py.import_bound("sys")?;
    let modules = sys.getattr("modules")?;
    if modules.contains("redis")? {
        return Ok(());
    }
    let redis_mod = PyModule::new_bound(py, "redis")?;
    redis_mod.add_function(wrap_pyfunction_bound!(call, &redis_mod)?)?;
    redis_mod.add_function(wrap_pyfunction_bound!(pcall, &redis_mod)?)?;
    redis_mod.add_function(wrap_pyfunction_bound!(register_function, &redis_mod)?)?;
    modules.set_item("redis", redis_mod)?;
    Ok(())
}

impl Server {
    /// `FUNCTION LOAD`'s Python path: execs the library's module body (which
    /// is expected to call `redis.register_function` at least once) and
    /// collects the registered functions.
    pub(crate) fn load_python_library(&mut self, libname: &str, body: &str) -> Result<Vec<(String, CompiledFunction)>, String> {
        LOADING_FUNCTIONS.with(|c| *c.borrow_mut() = Some(Vec::new()));
        let module_name = format!("__redisx_function_{}", libname.replace(|c: char| !c.is_ascii_alphanumeric(), "_"));
        let exec_result: PyResult<()> = Python::with_gil(|py| {
            ensure_redis_module_installed(py)?;
            PyModule::from_code_bound(py, body, &module_name, &module_name)?;
            Ok(())
        });
        let collected = LOADING_FUNCTIONS.with(|c| c.borrow_mut().take()).unwrap_or_default();

        exec_result.map_err(|e| sanitize_error(&e.to_string()))?;
        if collected.is_empty() {
            return Err("ERR No functions registered".to_string());
        }
        Ok(collected.into_iter().map(|(name, callback)| (name, CompiledFunction::Python(callback))).collect())
    }

    /// Same reentrancy tradeoff as `call_lua_function` (see its doc comment
    /// and `script.rs`'s module doc): `push_context` installs `self as *mut
    /// Server` so `redis.call`, invoked re-entrantly from inside
    /// `callback.call1(...)` below, can get `&mut Server` again, while this
    /// function still holds a live immutable borrow of `self.compiled`
    /// across that same call.
    pub(crate) fn call_python_function(&mut self, name: &str, keys: &[Vec<u8>], args: &[Vec<u8>], dbid: usize, peer: &str, read_only: bool) -> Reply {
        if !matches!(self.compiled.get(name), Some(CompiledFunction::Python(_))) {
            return Reply::Error("ERR Function not found".to_string());
        }

        let server_ptr = self as *mut Server;
        let prev = push_context(server_ptr, dbid, peer, read_only);
        let result: PyResult<Reply> = Python::with_gil(|py| {
            let Some(CompiledFunction::Python(callback)) = self.compiled.get(name) else { unreachable!() };
            let keys_list = PyList::new_bound(py, keys.iter().map(|k| String::from_utf8_lossy(k).into_owned()).collect::<Vec<_>>());
            let args_list = PyList::new_bound(py, args.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect::<Vec<_>>());
            let ret = callback.call1(py, (keys_list, args_list))?;
            Ok(py_value_to_reply(ret.bind(py)))
        });
        pop_context(prev);

        match result {
            Ok(r) => r,
            Err(e) => Reply::Error(format!("ERR {}", sanitize_error(&e.to_string()))),
        }
    }
}
