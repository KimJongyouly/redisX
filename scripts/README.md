# FUNCTION LOAD examples (Lua + Python)

[README](../README.md) · [Architecture](../architecture/README.md) · **Scripts**

Ready-to-load `FUNCTION`/`FCALL` library examples for redisX's two scripting
engines (`lua/`, `mlua`-backed; `python/`, `pyo3`-backed — see
`redis_document/25_redisX.md`'s Phase 6/9/B/C for how each engine was
ported). Each pair implements the exact same behavior in both languages, so
they double as a side-by-side reference for the two languages' conventions.

Filenames are numbered `00`–`95`, basic to expert, each a step up in what it
demonstrates — see the table below. Numbers are spaced out on purpose (not
`00`, `01`, `02`, ...) so a new example can be inserted later (e.g. `05`,
`45`) without renaming anything around it.

For the language/engine reference itself (not worked examples — the `redis.*`
API, `FUNCTION`/`EVAL` semantics, error formatting, what's not implemented),
see [`docs/`](docs/):

- [`docs/programmability.md`](docs/programmability.md) — `FUNCTION` vs `EVAL`, atomicity, replication, engines
- [`docs/functions-intro.md`](docs/functions-intro.md) — `FUNCTION LOAD`/`DELETE`/`LIST`, `FCALL`/`FCALL_RO`
- [`docs/eval-intro.md`](docs/eval-intro.md) — `EVAL`/`EVALSHA`, `SCRIPT LOAD`/`EXISTS`/`FLUSH`
- [`docs/lua-api.md`](docs/lua-api.md) — the full `redis.*` Lua API
- [`docs/python-api.md`](docs/python-api.md) — the full `redis.*` Python API
- [`docs/debugging.md`](docs/debugging.md) — there is no built-in debugger; what to use instead

## Prerequisites

- `redisx-server` running (plain build works for `lua/`; the `python/`
  examples need a binary built with `--features python` — `install.sh`
  already does this, see the repo root README).
- `redisx-cli` on your `PATH` (or run it via `cargo run -p redisx-cli --bin
  redisx-cli --`).
- Level 95 only, either language: a binary built with `--features polars`
  (`install.sh` also already does this — a plain `cargo build --workspace`
  does not; `polars` is a separate flag from `--features python`, combine
  both with `--features python,polars`).

## Loading and calling an example

`FUNCTION LOAD` takes the whole library file (shebang line included) as a
single argument, so load straight from a file with `-x` (reads the last
argument from stdin) instead of pasting the body inline:

```bash
redisx-cli -x FUNCTION LOAD < scripts/lua/00_hello.lua
redisx-cli FCALL echo 0 world
# -> "world"
```

Swap `lua` for `python` to load the same level on the other engine — the
`FCALL` call looks identical either way; only the library source differs.
Each pair intentionally shares its library/function name across languages
(easier to compare side by side), so `FUNCTION LOAD` of one rejects with
"Library '...' already exists" while the other is still loaded — run
`redisx-cli FUNCTION DELETE <name>` first if you want to switch languages for
the same level without restarting the server.

## Checking and deleting what's loaded

`FUNCTION LIST` shows every library currently loaded in this server process
(libraries don't survive a restart — there's no persistence for them, so
after restarting you're back to an empty list until you `LOAD` again):

```bash
redisx-cli FUNCTION LIST
# (nothing printed — an empty array — if nothing is loaded yet)

redisx-cli -x FUNCTION LOAD < scripts/lua/00_hello.lua
redisx-cli FUNCTION LIST
# library_name
# hello
# engine
# LUA
# functions
# name
# echo
# description
#
# flags
```

Each library shows its `library_name`, `engine` (`LUA` or `PYTHON`), and a
`functions` array (one entry per function the library registered, each with
`name`/`description`/`flags` — `description`/`flags` are always empty in
this fork, since `redis.register_function` here only ever takes a plain
`name, callback` pair, never the extended form with a description or flags
real Redis also allows).

`FUNCTION DELETE <library_name>` removes a whole library and every function
it registered (not individual functions — matching real Redis, deletion is
always library-granularity):

```bash
redisx-cli FUNCTION DELETE hello
# OK
redisx-cli FUNCTION LIST
# (nothing printed)

redisx-cli FUNCTION DELETE hello
# ERR Library not found
```

This fork doesn't implement `FUNCTION FLUSH` (delete everything at once) —
loop over `FUNCTION LIST`'s `library_name` fields and `FUNCTION DELETE` each
one if you need to clear all of them.

## The levels

| # | File | Registers | Demonstrates |
|---|---|---|---|
| 00 | `hello` | `echo` | The absolute basics: `FUNCTION LOAD`/`FCALL` round-tripping one argument. |
| 10 | `incrby_limit` | `incrby_limit` | A single `redis.call`, a conditional, and an error reply — an atomic bounded counter. |
| 20 | `cache_aside` | `get_or_compute` | Combining `GET`+`SET`+`EXPIRE` and returning a compound value (Lua: positional array; Python: real dict → RESP map — see the file for why they differ). |
| 30 | `rate_limiter` | `allow` | `INCR` + `EXPIRE ... NX` — a fixed-window rate limiter. |
| 40 | `distributed_lock` | `lock`, `unlock` | `SET ... NX PX` + a token-checked compare-and-delete — the textbook single-instance Redis lock. |
| 50 | `leaderboard` | `submit_score`, `increment_score` | `ZADD`/`ZINCRBY` + `ZREVRANK` in one round trip — atomic "read your own write" on a sorted set. |
| 60 | `sliding_window_rate_limiter` | `allow` | A real data structure (a per-bucket ZSET of call timestamps) instead of a plain counter — a precise sliding window, not just a fixed one. |
| 70 | `task_queue` | `ensure_group`, `enqueue`, `claim_and_complete` | Streams + consumer groups: a minimal reliable work queue, claim-and-ack in one round trip. |
| 80 | `bloom_filter` | `bf_add`, `bf_check` | Building a data structure Redis core doesn't ship (no RedisBloom here) directly on `SETBIT`/`GETBIT`, with hashing implemented in the script itself. |
| 90 | `atomic_transfer` | `transfer` | The capstone: a multi-key invariant (a balance can't go negative) enforced atomically across two keys, no `WATCH`/`MULTI`/retry-loop needed. |
| 95 | `dataframe_query` | Lua: `load_csv`, `load_parquet`, `query`, `pretty`, `to_csv`, `to_parquet`. Python: all but `load_parquet`/`to_parquet` — see below. | A value type beyond the five core ones: `redis.call` driving the native `DataFrame` type's load/query/dump surface (`DF.FROMCSV`/`DF.FROMPARQUET`/`DF.SHAPE`/`DF.QUERY`/`DF.PRETTY`/`DF.TOCSV`/`DF.TOPARQUET`) exactly like any other command — ingest and read back the shape in one round trip, query two ways (RESP array vs. a pretty-printed table), dump back out. Requires `--features polars` (see Prerequisites). **The one pair in this directory that isn't function-for-function identical**: Parquet is binary, and the Python engine's `redis.call` lossily decodes every RESP bulk string crossing it as UTF-8 (see `docs/python-api.md`) — verified to actually corrupt a real `.parquet` file in transit, so the Python file omits `load_parquet`/`to_parquet` rather than ship something that looks like it works and doesn't. CSV is plain text, so it's unaffected and both languages have the same four other functions. |

```bash
# 10: a counter that refuses to cross 100
redisx-cli -x FUNCTION LOAD < scripts/lua/10_incrby_limit.lua
redisx-cli FCALL incrby_limit 1 mycounter 30 100   # -> 30
redisx-cli FCALL incrby_limit 1 mycounter 90 100   # -> (error) ERR would exceed limit of 100 (currently 30)

# 30: at most 3 calls per 10-second window
redisx-cli -x FUNCTION LOAD < scripts/python/30_rate_limiter.py
redisx-cli FCALL allow 1 api:demo 3 10   # -> 2
redisx-cli FCALL allow 1 api:demo 3 10   # -> 1
redisx-cli FCALL allow 1 api:demo 3 10   # -> 0
redisx-cli FCALL allow 1 api:demo 3 10   # -> (error) ERR rate limit exceeded, retry in ...ms

# 90: a transfer that can't overdraw
redisx-cli -x FUNCTION LOAD < scripts/lua/90_atomic_transfer.lua
redisx-cli SET acct:alice 100
redisx-cli SET acct:bob 0
redisx-cli FCALL transfer 2 acct:alice acct:bob 30     # -> {70, 30}
redisx-cli FCALL transfer 2 acct:alice acct:bob 1000   # -> (error) ERR insufficient funds: have 70, need 1000

# 95: ingest CSV, read the shape back in the same round trip, query two
# ways, dump back out (server must be built with --features polars — see
# Prerequisites)
redisx-cli -x FUNCTION LOAD < scripts/python/95_dataframe_query.py
redisx-cli FCALL load_csv 1 staff "$(printf 'name,age,dept\nalice,30,eng\nbob,25,sales\ncarol,35,eng\n')"
# -> {3, 3}   (3 rows, 3 columns)
redisx-cli FCALL query 1 staff "SELECT name FROM self WHERE dept = 'eng' ORDER BY name"
# -> {{"alice"}, {"carol"}}
redisx-cli --raw FCALL pretty 1 staff   # --raw: see the file's own note on why
# -> shape: (3, 3)
#    ┌───────┬─────┬───────┐
#    │ name  ┆ age ┆ dept  │
#    │ ---   ┆ --- ┆ ---   │
#    │ str   ┆ i64 ┆ str   │
#    ╞═══════╪═════╪═══════╡
#    │ alice ┆ 30  ┆ eng   │
#    │ bob   ┆ 25  ┆ sales │
#    │ carol ┆ 35  ┆ eng   │
#    └───────┴─────┴───────┘
redisx-cli FCALL to_csv 1 staff
# -> name,age,dept
#    alice,30,eng
#    bob,25,sales
#    carol,35,eng
```

Levels 10, 30, 40, 60, and 90 are all check-then-write in a single round trip
on purpose: redisX runs a library function to completion without
interleaving any other command (single-threaded event loop — see
`redis_document/25_redisX.md`), so this is genuinely atomic in a way the
same check done as separate client-side calls would not be.

## Writing your own

- **Lua**: `KEYS`/`ARGV` are the 1-indexed tables `keys`/`args` passed into
  your registered function (`keys[1]`, `args[1]`, ...). Return `{err =
  "..."}` for an error reply, `{ok = "..."}` for a status reply (like
  `+OK`), anything else is converted directly (string/number/table →
  bulk/integer/array — a table is *only* ever a flat positional array, no
  named-field map support).
- **Python**: `keys`/`args` are ordinary 0-indexed lists (`keys[0]`,
  `args[0]`, ...). Return `{'err': '...'}`/`{'ok': '...'}` the same way;
  `None`/`str`/`int`/`list`/`dict` otherwise map directly to RESP (a `dict`
  really does become a RESP map, unlike Lua's flattened table).
- Both: `redis.call(...)` raises on a Redis-level error (so an unhandled one
  propagates out of your function as the FCALL's own error reply);
  `redis.pcall(...)` returns the error as a value instead, for when you want
  to inspect or recover from it yourself. A library's top level must call
  `redis.register_function(name, callback)` at least once, or `FUNCTION
  LOAD` rejects it with "No functions registered".
