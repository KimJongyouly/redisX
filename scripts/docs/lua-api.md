# The Lua API

Reference for the `redis` global table available inside Lua `FUNCTION`
callbacks and `EVAL` script bodies. Source of truth:
`crates/redisx-server/src/script_lua.rs`.

## `redis.call(command [, arg ...])`

Runs a Redis command exactly as a client would, and returns its reply
converted to a Lua value (conversion table below). Arguments must be Lua
strings, integers, or floats (floats are stringified); anything else (a
table, boolean, `nil`) raises:

```
Lua redis lib command arguments must be strings or integers
```

If the command itself returns an error reply (e.g. `WRONGTYPE`), `redis.call`
**raises** a Lua error with that message — it does not return anything. An
uncaught error from `redis.call` propagates out of your function/script as
the whole `FCALL`/`EVAL`'s own error reply.

```lua
-- strkey holds a String, so LPUSH on it is a type error
redis.call('LPUSH', 'strkey', 'x')
-- the FCALL/EVAL that ran this line never returns normally; the client sees
-- something like (see "Errors" below for why it looks like this, not just
-- a plain "WRONGTYPE ..." line):
-- (error) ERR runtime error: WRONGTYPE Operation against a key holding the
--   wrong kind of value stack traceback: [C]: in field 'call' ...
```

## `redis.pcall(command [, arg ...])`

Identical to `redis.call`, except a command-level error comes back as an
ordinary Lua value instead of raising — a table with a single `err` field:

```lua
local reply = redis.pcall('LPUSH', 'strkey', 'x')
if reply.err then
  return redis.error_reply('could not push: ' .. reply.err)  -- see note below
end
```

> There is no `redis.error_reply`/`redis.status_reply` helper in this port
> (see [Not implemented](#not-implemented) below) — return `{err = '...'}` or
> `{ok = '...'}` directly instead; it's exactly what those helpers would have
> built for you anyway.

## `redis.register_function(...)`

See [`functions-intro.md`](functions-intro.md#registering-a-function) — only
relevant inside a `FUNCTION LOAD` body, not inside `EVAL`.

## Converting a reply to a Lua value

What `redis.call`/`redis.pcall` hand back to your script:

| RESP reply | Lua value |
|---|---|
| Simple string | `{ok = "<string>"}` |
| Error | `{err = "<message>"}` (only reachable via `pcall` — `call` raises instead) |
| Integer | Lua integer |
| Bulk string | Lua string |
| Null (any form) | `false` |
| Boolean (RESP3) | Lua boolean |
| Double (RESP3) | Lua number |
| Array / Set / Push | 1-indexed Lua table |
| Map (RESP3) | 1-indexed Lua table of alternating key, value, key, value, ... (**not** a nested structure — see note) |

**Map flattening is a deliberate simplification.** Lua only has one table
type, so there's no clean way to carry "this is specifically a map, not an
array" through to your script the way Python's engine can with a real
`dict` (see [`python-api.md`](python-api.md#converting-a-reply-to-a-python-value)).
A RESP map `{a: 1, b: 2}` arrives as `{"a", 1, "b", 2}` — read it in pairs if
you need key/value semantics back.

## Converting a Lua value to a reply

What a function callback's `return` value (or an `EVAL` script's final
expression) becomes as the `FCALL`/`EVAL` reply:

| Lua value | RESP reply |
|---|---|
| `nil` | Null |
| `false` | Null |
| `true` | Integer `1` |
| Integer | Integer |
| Float | Integer (**truncated toward zero**, not rounded — `3.9` becomes `3`) |
| String | Bulk string |
| Table with a string `err` field | Error, that string verbatim (no `ERR` prefix added) |
| Table with a string `ok` field | Simple string, that string verbatim |
| Any other table | Array — built by reading `t[1]`, `t[2]`, ... and **stopping at the first `nil` hole** (Lua's own array-length convention; a table with gaps silently truncates there) |
| Anything else (function, userdata, ...) | Null |

## Errors

An error can reach the client two different ways, with very different
formatting. Verified by calling each of these through a real `redisx-server`
+ `redisx-cli`, not just read off the source:

1. **An explicit `{err = "..."}` return** (whether you built the table
   yourself or are passing through what `redis.pcall` gave you) — passed
   through byte-for-byte, no prefix added:

   ```lua
   return {err = 'my custom message'}
   ```
   ```
   (error) my custom message
   ```

   If you want an `ERR`-style code, put it in the string yourself —
   `{err = 'ERR my message'}` comes back as exactly `(error) ERR my
   message`, nothing extra added.

2. **An uncaught Lua error** — a `redis.call` failure you didn't wrap in
   `pcall`, an ordinary Lua runtime error (bad arithmetic, calling a nil
   value, ...), or a compile-time syntax error — is *not* just the plain
   message. `mlua` formats an uncaught error as `runtime error: <message>
   stack traceback: ...` (a real Lua stack traceback, normally multi-line),
   and this port's own wrapping then adds one more unconditional `ERR `
   prefix on top of that, regardless of what the message already started
   with:

   ```lua
   redis.call('LPUSH', 'strkey', 'x')  -- strkey holds a String
   ```
   ```
   (error) ERR runtime error: WRONGTYPE Operation against a key holding the
     wrong kind of value stack traceback: [C]: in field 'call' [string
     "..."]:1: in main chunk
   ```

   Because the `ERR ` prefix is added unconditionally rather than checked
   for first, a `redis.call` error whose own message already started with
   `ERR` — such as the read-only-mode rejection under `FCALL_RO` (see
   [`functions-intro.md`](functions-intro.md#calling-a-function-fcall)) —
   comes back double-prefixed: `ERR runtime error: ERR Write commands are
   not allowed ...`. Harmless, but worth recognizing if you see it.

Either way, the message is run through one sanitizing pass first: any
`\r`/`\n` in it is replaced with a space — it is *not* stripped or
shortened, so case 2's traceback is still fully present, just flattened onto
one line instead of several. RESP's simple-error line can't contain a
literal CR or LF without desyncing the protocol, and this was an actual bug
found by testing against real `redis-cli` during development
(`redis_document/25_redisX.md`, Phase 6) — without the sanitizing step,
`redis-cli` would report `Error: Bad simple string value` instead of your
script's actual error.

In short: if you want a clean, predictable error message rather than a
traceback-flavored one, always route script-detected errors through an
explicit `return {err = "..."}` rather than letting a Lua error escape
uncaught.

## Security: no sandbox by default

**This is the most important practical difference from real Redis.** Real
Redis's Lua interpreter is deliberately stripped down: no `os` or `io`
library, no filesystem or process access, a locked-down global table so a
script can't leak state between runs. redisX constructs its Lua VM with
`mlua::Lua::new()` — the full standard library, unmodified. A loaded
function can call `os.execute(...)`, read/write arbitrary files via `io`,
and so on, with the same privileges as the `redisx-server` process itself.

In practice: treat `FUNCTION LOAD`/`EVAL` as equivalent to giving someone
shell access to the server process, not as a safely sandboxed scripting
surface. Fine for your own trusted scripts; not something to expose to
untrusted callers.

## Not implemented

Present in real Redis's Lua API but absent here — calling any of these
raises a plain Lua "attempt to call a nil value" error, since the names
simply don't exist on the `redis` table (or as globals) in this port:

| Missing | What it would have done | Workaround |
|---|---|---|
| `redis.error_reply(msg)` / `redis.status_reply(msg)` | Convenience wrappers for `{err=msg}`/`{ok=msg}` | Build the table yourself: `{err = msg}` / `{ok = msg}` |
| `redis.sha1hex(s)` | SHA1 of a string | None built in — compute it client-side and pass it as an `ARGV`, or call `SCRIPT LOAD`/`EVAL` itself (which hashes internally) if that's your actual goal |
| `cjson.encode`/`cjson.decode` | JSON (de)serialization | None — no JSON library is linked in at all |
| `redis.log(level, message)` | Write to the server log | None — nothing a script does is logged anywhere |
| `redis.setresp(n)` | Switch the RESP protocol version `redis.call` replies use | None — replies always use the conversion table above regardless of the calling client's negotiated protocol |
| `redis.replicate_commands()` | Opt in/out of effects replication | None needed — effects replication (see [`programmability.md`](programmability.md#how-writes-replicate)) is always on, unconditionally |
| `redis.setrepl(flag)` | Fine-tune whether a specific write replicates | None |
| `redis.breakpoint()` / `redis.debug()` | Hooks for the `--ldb` debugger | None — there is no debugger; see [`debugging.md`](debugging.md) |
| `struct`, `bit`, `cmsgpack` libraries | Binary packing, bitwise ops, MessagePack | None linked in |

## See also

- [`functions-intro.md`](functions-intro.md) — `FUNCTION LOAD`/`FCALL`
- [`eval-intro.md`](eval-intro.md) — `EVAL`/`EVALSHA`
- [`python-api.md`](python-api.md) — the same API surface, Python side
- [`../README.md`](../README.md) — worked examples using this API end to end
