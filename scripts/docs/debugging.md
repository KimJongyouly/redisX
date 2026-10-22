# Debugging scripts and functions

**There is no built-in debugger in redisX, for either engine.** Real Redis's
Lua debugger (`redis-cli --ldb`, `SCRIPT DEBUG`, `redis.breakpoint()`/
`redis.debug()`) was explicitly left out of scope when `redisx-cli` was
ported (`redis_document/25_redisX.md`, Phase 8: "`redis-cli.c`'s ... Lua
debugger (`--ldb`) ... explicitly out of scope"), and nothing resembling it
exists for the Python engine either (there never was one in real Redis to
port from, since real Redis has no Python engine). `SCRIPT DEBUG` is not a
recognized `SCRIPT` subcommand at all — see [`eval-intro.md`](eval-intro.md).

This page is about what to actually do instead, not a feature reference.

## 1. Compile-time errors are usually enough to find a typo

`FUNCTION LOAD`/`EVAL` surface a Lua/Python syntax error immediately, with
the interpreter's own message, sanitized to a single line (see
[`lua-api.md`](lua-api.md#errors)):

```
redisx-cli -x FUNCTION LOAD < broken.lua
# -> (error) ERR Error compiling function: syntax error: [string "broken"]:1: ')' expected near <eof>
```

For Lua specifically, `SCRIPT LOAD` doubles as a free syntax check without
actually registering anything callable — load it, see if it's accepted,
`SCRIPT FLUSH` it away if you don't want to keep it cached.

## 2. Runtime errors tell you what ran, not what didn't

A function/script that fails partway through tells you via its error
message exactly where it stopped (the Lua/Python interpreter's own error
text — a bad index, a `WRONGTYPE` from `redis.call`, etc. — see
[`lua-api.md`](lua-api.md#errors)/[`python-api.md`](python-api.md#errors)).
Since there's no `redis.log`, there's no way to have a script "print" its own
intermediate state to any server-visible log — the only visibility into what
a script did is its return value and whatever keyspace side effects it left
behind.

A practical pattern: have the function under development temporarily
`return` the intermediate value you want to inspect, call it with `FCALL`
from `redisx-cli`, read the value, then put the real `return` back.

## 3. Build up a script interactively before wrapping it as a function

Since `EVAL` compiles and runs its body fresh every call with no load step
(see [`eval-intro.md`](eval-intro.md)), it's the fastest loop for iterating
on Lua logic — paste a candidate body straight into `redisx-cli EVAL "..." 0`,
see what comes back, adjust, repeat. Once it does what you want, move the
same logic into a `FUNCTION` library for persistence
(see [`functions-intro.md`](functions-intro.md)). There's no Python
equivalent of this loop (`EVAL` is Lua-only) — for Python, reload the whole
library with `FUNCTION LOAD REPLACE` on each iteration instead.

## 4. Test the exact scenario against a real binary

This project's own convention (`redis_document/25_redisX.md`'s testing
notes, and every phase's own verification section) is: unit tests prove the
engine machinery works in isolation, but the only thing that proves a given
script/function behaves correctly end to end is running it against a real
`redisx-server` binary with real `redisx-cli`, including the error paths.
The worked examples in [`../README.md`](../README.md) were all verified this
way — load the library, call it with both valid and invalid input, and read
the exact reply `redisx-cli` prints (including the formatted/raw-output
distinction `redisx-cli --no-raw` makes visible).

## 5. `redis.pcall`/`pcall()` to narrow down which call is failing

Since an uncaught error from `redis.call` propagates immediately and stops
the function (see [`lua-api.md`](lua-api.md) and
[`python-api.md`](python-api.md) for the `redis.call`/`redis.pcall`
reference), wrapping individual calls in `redis.pcall` (or, in Python, an
ordinary `try`/`except` around `redis.call`) while developing lets you
isolate exactly which command inside a multi-step function is the one
raising, instead of only knowing that *something* in the function failed.

## See also

- [`lua-api.md`](lua-api.md#errors) / [`python-api.md`](python-api.md#errors) — exactly how errors are formatted
- [`eval-intro.md`](eval-intro.md) — the fast iterate-and-run loop (`EVAL`)
- [`functions-intro.md`](functions-intro.md) — persisting a finished script as a `FUNCTION`
