# Programmability

redisX lets you run custom logic on the server itself, in the same spot in
the request path a normal command handler runs, through two separate
mechanisms:

| | Legacy scripting | Functions |
|---|---|---|
| Commands | `EVAL`/`EVALSHA`, `SCRIPT` | `FUNCTION`, `FCALL`/`FCALL_RO` |
| Languages | Lua only | Lua or Python |
| How code reaches the server | Sent inline with every call (or by SHA1 after `SCRIPT LOAD`) | Loaded once (`FUNCTION LOAD`), called by name afterwards |
| Survives a server restart | No — the script cache is in memory only | Yes — libraries are persisted to the RDB file and reloaded automatically |
| `KEYS`/`ARGV` | Lua globals, set fresh before each run | Parameters to the registered callback (`keys`, `args`) |
| Reference | [`eval-intro.md`](eval-intro.md) | [`functions-intro.md`](functions-intro.md) |

Functions are the one to reach for by default — they're named, persisted, and
work from either language. `EVAL` exists alongside them (same as real Redis
still ships both) mainly for one-off scripts a client wants to send inline
without a separate load step.

Both paths end up calling the exact same engine code
(`crates/redisx-server/src/script.rs`, `script_lua.rs`, `script_python.rs`),
so everything below about atomicity, error handling, and the `redis.call`/
`redis.pcall` API applies identically to a Lua `EVAL` body, a Lua `FUNCTION`,
and a Python `FUNCTION`.

## Engines

- **Lua** — always available, no build flag needed. Backed by `mlua`
  (vendored Lua 5.4; nothing needs to be installed on the host).
- **Python** — only in a binary built with `cargo build --features python`
  (`install.sh` already does this). Backed by `pyo3`, auto-detecting
  whichever Python 3 is already on the host. `FUNCTION LOAD`ing a `#!python`
  library on a binary built without the feature fails with: `ERR Python
  engine not compiled into this build (rebuild with --features python)`.

Both engines coexist in the same running server — you can load a Lua library
and a Python library side by side and `FCALL` either one, as long as their
registered function names don't collide (see
[`functions-intro.md`](functions-intro.md#known-gap-no-cross-library-name-collision-check)
for a real gap in that collision check).

## Atomicity

redisX is a single-threaded event-reactor server (see
`redis_document/25_redisX.md`): one command runs to completion before the
next one starts, and `FCALL`/`EVAL` are no exception — a whole function or
script body runs without any other client's command interleaving partway
through. This is what makes a "check, then write" pattern inside a single
function (e.g. [`scripts/*/40_distributed_lock`](../README.md),
[`90_atomic_transfer`](../README.md)) genuinely atomic, where doing the same
check and write as two separate round trips from a client would not be.

## How writes replicate

A write a script performs via `redis.call(...)` goes through the exact same
`Server::dispatch` path a normal client command does, so it is propagated to
replicas/AOF as that individual command — not as the whole script body. This
is "effects replication": if your Lua function does
`redis.call('INCR', KEYS[1])`, what gets replicated is an `INCR`, not the
Lua source. There is no verbatim-script replication mode to opt in or out of
(real Redis's `redis.replicate_commands()` toggles between the two; it isn't
implemented here because effects replication is always on).

One consequence worth knowing: `FCALL`/`EVAL` themselves are **not** marked
as write commands in the command table. Each `redis.call` write inside
propagates on its own the moment it runs; marking the outer `FCALL` as a
write too would double-propagate every write a function makes.

## What's intentionally not here

Ported with an explicit scope cut (`redis_document/25_redisX.md`, Phase 6 and
Phase 9) — these are not bugs, just not built:

- **Function flags** (`no-writes`, `allow-oom`, `allow-stale`, ...) — not
  parsed or enforced. The one exception is `FCALL_RO`, which *is* enforced,
  but generically (any write command attempted via `redis.call` while running
  under `FCALL_RO` is rejected), not via a per-function `no-writes` flag.
- **`FUNCTION DUMP`/`FUNCTION RESTORE`**, **`FUNCTION STATS`**,
  **`FUNCTION KILL`**.
- **`SCRIPT KILL`**, and more generally any busy-script protection — a script
  that loops forever blocks the single reactor thread indefinitely. There is
  no `lua-time-limit`-style timeout and nothing to kill a running script from
  another connection with.
- **No sandboxing** — see the security note in
  [`lua-api.md`](lua-api.md#security-no-sandbox-by-default) and
  [`python-api.md`](python-api.md#security-no-sandbox-by-default). This is
  the single biggest behavioral difference from real Redis worth knowing
  about before trusting an untrusted script author.

## See also

- [`functions-intro.md`](functions-intro.md) — `FUNCTION LOAD`/`FCALL`, library format
- [`eval-intro.md`](eval-intro.md) — `EVAL`/`EVALSHA`/`SCRIPT`
- [`lua-api.md`](lua-api.md) — the `redis.*` Lua API, full reference
- [`python-api.md`](python-api.md) — the `redis.*` Python API, full reference
- [`debugging.md`](debugging.md) — there is no built-in debugger; what to use instead
- [`../README.md`](../README.md) — the 10-level worked example curriculum (Lua + Python side by side)
