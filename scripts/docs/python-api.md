# The Python API

Reference for the `redis` module available inside Python `FUNCTION`
callbacks (built with `--features python`; see
[`programmability.md`](programmability.md#engines)). There is no Python
equivalent of `EVAL` — legacy scripting is Lua-only, see
[`eval-intro.md`](eval-intro.md). Source of truth:
`crates/redisx-server/src/script_python.rs`.

```python
import redis
```

`import redis` works because `FUNCTION LOAD` installs a small `redis` module
into `sys.modules` the first time any Python library is loaded — it is not a
real PyPI package, and nothing from the actual `redis-py` client library is
available. It exposes exactly three names: `call`, `pcall`,
`register_function`.

## `redis.call(command, *args)`

Runs a Redis command exactly as a client would, and returns its reply
converted to a Python value (conversion table below). Arguments must be
`str`, `int`, or `float` (floats are stringified); a `bool` is explicitly
rejected even though Python's `bool` is technically an `int` subclass, and
anything else (a `list`, `dict`, `None`) is also rejected:

```
TypeError: Python redis lib command arguments must be strings or integers
```

If the command itself returns an error reply (e.g. `WRONGTYPE`),
`redis.call` **raises** a `RuntimeError` with that message instead of
returning anything. An uncaught exception from `redis.call` propagates out of
your function as the whole `FCALL`'s own error reply.

```python
# strkey holds a String, so LPUSH on it is a type error
redis.call('LPUSH', 'strkey', 'x')
# the FCALL that ran this line never returns normally; the client sees
# (see "Errors" below for why the exception class name is prefixed):
# (error) ERR RuntimeError: WRONGTYPE Operation against a key holding the wrong kind of value
```

## `redis.pcall(command, *args)`

Identical to `redis.call`, except a command-level error comes back as an
ordinary Python value instead of raising — a `dict` with a single `'err'`
key:

```python
reply = redis.pcall('LPUSH', 'strkey', 'x')
if isinstance(reply, dict) and 'err' in reply:
    return {'err': 'could not push: ' + reply['err']}
```

> There is no `redis.error_reply`/`redis.status_reply` helper here either
> (same gap as the Lua engine) — return `{'err': '...'}` or `{'ok': '...'}`
> directly.

## `redis.register_function(function_name, callback)`

See [`functions-intro.md`](functions-intro.md#registering-a-function) — only
the positional two-argument form exists; there is no table/kwargs form and
no `flags` parameter at all (not even one that's silently ignored, unlike
the Lua engine's table form).

## Converting a reply to a Python value

What `redis.call`/`redis.pcall` hand back to your function:

| RESP reply | Python value |
|---|---|
| Simple string | `{'ok': '<string>'}` |
| Error | `{'err': '<message>'}` (only reachable via `pcall` — `call` raises instead) |
| Integer | `int` |
| Bulk string | `str` (see note on binary data below) |
| Null (any form) | `None` |
| Boolean (RESP3) | `bool` |
| Double (RESP3) | `float` |
| Array / Set / Push | `list` |
| Map (RESP3) | real `dict` — unlike the Lua engine, nothing is lost here; see [`lua-api.md`](lua-api.md#converting-a-reply-to-a-lua-value) for why Lua has to flatten instead |

**Bulk strings are decoded as UTF-8 `str`, lossily, not `bytes`.** A RESP
bulk string containing arbitrary binary data (not valid UTF-8) is decoded
with `String::from_utf8_lossy` before reaching your function — invalid bytes
are replaced, not preserved. If a value in your keyspace may contain
non-UTF-8 binary data, round-tripping it exactly through a Python function is
not currently possible.

This isn't theoretical — it's why
[`../lua/95_dataframe_query.lua`](../lua/95_dataframe_query.lua) has
`load_parquet`/`to_parquet` functions wrapping `DF.FROMPARQUET`/
`DF.TOPARQUET` and its Python sibling file does not: a real `.parquet` file's
bytes, round-tripped through an `FCALL`-wrapped `DF.TOPARQUET` in Python,
came back a different length than calling `DF.TOPARQUET` directly and failed
to parse as Parquet at all. Same keys/args and reply-conversion boundary as
this table describes, just with an actual binary file format making the
corruption concrete instead of abstract. Lua has no such limitation (Lua
strings are raw byte buffers, not forced UTF-8) — see
[`lua-api.md`](lua-api.md) for the Lua side.

## Converting a Python value to a reply

What a function's `return` value becomes as the `FCALL` reply:

| Python value | RESP reply |
|---|---|
| `None` | Null |
| `False` | Null |
| `True` | Integer `1` |
| `int` | Integer |
| `float` | Integer (**truncated toward zero**, not rounded — `3.9` becomes `3`) |
| `str` | Bulk string |
| `dict` with a string `'err'` key | Error, that string verbatim (no `ERR` prefix added) |
| `dict` with a string `'ok'` key | Simple string, that string verbatim |
| Any other `dict` | RESP map, recursively converting each key and value |
| `list` / `tuple` | Array, recursively converting each element |
| Anything else (a custom object, a generator, ...) | Null |

## Errors

Same two paths as the Lua engine (see [`lua-api.md`](lua-api.md#errors)),
Python-flavored — verified against a real `redisx-server` +
`redisx-cli`, not just read off the source:

1. **An explicit `{'err': '...'}` return** — passed through byte-for-byte, no
   prefix added:

   ```python
   return {'err': 'my custom message'}
   ```
   ```
   (error) my custom message
   ```

2. **An uncaught Python exception** — a `redis.call` failure, or any
   ordinary `raise` your function lets escape — comes back as `ERR
   <ExceptionClassName>: <message>`, not just the bare message:

   ```python
   redis.call('LPUSH', 'strkey', 'x')  # strkey holds a String
   ```
   ```
   (error) ERR RuntimeError: WRONGTYPE Operation against a key holding the wrong kind of value
   ```
   ```python
   raise ValueError('boom message')
   ```
   ```
   (error) ERR ValueError: boom message
   ```

   Unlike the Lua engine, there is no stack traceback appended — Python
   exceptions are stringified without one, so the message stays on one line
   even before the CR/LF sanitizing pass runs. This also means — unlike
   Lua's unconditional, sometimes-doubled `ERR ` prefix (see
   [`lua-api.md`](lua-api.md#errors)) — the Python engine's prefix is always
   exactly one `ERR `, immediately followed by the exception's class name.

Either way the message is run through the same CR/LF sanitizing step the Lua
engine uses, for the same reason (a literal newline in a RESP simple-error
line desyncs the protocol) — it rarely matters here since Python errors are
already single-line, but it applies uniformly across both engines.

## Security: no sandbox by default

Same caveat as the Lua engine, arguably more serious here: a loaded Python
function runs in the server's own interpreter with the full standard library
available — `import os`, `import subprocess`, `import socket`, arbitrary file
I/O, all of it, with the same privileges as the `redisx-server` process
itself. There is no restricted or audited subset of Python exposed to loaded
functions. Treat `FUNCTION LOAD` of a `#!python` library as equivalent to
giving someone shell access to the server process.

## Process-wide interpreter, one `redis` module

Unlike the Lua engine (one `mlua::Lua` VM per `Server`), Python's embedded
interpreter is process-wide (`pyo3`'s `auto-initialize` owns a single
interpreter no matter how many `Server` instances exist in the process —
relevant mainly if you've ever seen multiple `Server`s created in the same
process, e.g. in this project's own unit tests). The `redis` module is
installed into `sys.modules` lazily, the first time any Python library is
loaded, and then stays installed for the lifetime of the process.

## Not implemented

Beyond the full Python standard library already being available (unlike real
Redis, which has no Python engine at all to compare against — Python support
is specific to this fork), the following conveniences other engines/ports
sometimes add are absent:

- No `redis.log(...)` — nothing a function does is written to the server log.
- No `redis.setresp(...)` — replies always use the conversion table above.
- No `redis.replicate_commands()`/`redis.setrepl(...)` — effects replication
  (see [`programmability.md`](programmability.md#how-writes-replicate)) is
  always on, unconditionally; there's nothing to toggle.
- No debugger hooks of any kind — see [`debugging.md`](debugging.md).

## See also

- [`functions-intro.md`](functions-intro.md) — `FUNCTION LOAD`/`FCALL`
- [`lua-api.md`](lua-api.md) — the same API surface, Lua side
- [`../README.md`](../README.md) — worked examples using this API end to end
