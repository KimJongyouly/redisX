# Eval scripts

`EVAL`/`EVALSHA` is the legacy, Lua-only scripting path — kept alongside
`FUNCTION` the same way real Redis still supports both (`eval.c` was never
extended to other engines; only `FUNCTION`'s engine table was). Prefer
`FUNCTION` for anything you'll call more than once or want to survive a
restart; see [`programmability.md`](programmability.md) for the comparison.

## `EVAL script numkeys [key [key ...]] [arg [arg ...]]`

```
redisx-cli EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey myvalue
# -> OK
redisx-cli GET mykey
# -> "myvalue"
```

Unlike a `FUNCTION`'s callback, there's no `(keys, args)` parameter list —
the script body is compiled fresh on every single `EVAL` call, and reads
`KEYS`/`ARGV` as ordinary Lua globals (1-indexed), exactly matching real
`eval.c`'s convention:

```lua
return {KEYS[1], ARGV[1]}
```

Every `EVAL` call also caches the script body under `SHA1(body)` as a side
effect — even if you never call `SCRIPT LOAD` yourself, a later `EVALSHA` of
that same body will find it.

## `EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]`

Runs a previously cached script by its SHA1 digest instead of resending the
whole body:

```
redisx-cli SCRIPT LOAD "return ARGV[1]"
# -> "098e0f0d1448c0a81dafe820f66d460eb09263da"
redisx-cli EVALSHA 098e0f0d1448c0a81dafe820f66d460eb09263da 0 hello
# -> "hello"
```

Calling `EVALSHA` with a digest the cache doesn't know returns:

```
(error) NOSCRIPT No matching script. Please use EVAL.
```

The usual client-side pattern (also how real Redis clients behave) is:
try `EVALSHA` first, and on `NOSCRIPT` fall back to a plain `EVAL` of the
full body.

## `SCRIPT` subcommands

| Subcommand | Behavior |
|---|---|
| `SCRIPT LOAD <script>` | Compiles nothing by itself — just computes and caches `SHA1(script)`, returning the hex digest, so a later `EVALSHA` can find it without ever having run it. |
| `SCRIPT EXISTS sha [sha ...]` | Returns an array of `0`/`1`, one per argument, for whether each digest is currently cached. |
| `SCRIPT FLUSH` | Clears the entire script cache unconditionally (no `ASYNC`/`SYNC` distinction — both would behave identically here anyway, since there's no background thread to defer the free to). |

`SCRIPT KILL` and `SCRIPT DEBUG` do not exist — there is no way to interrupt
a running script from another connection, and no built-in debugger (see
[`debugging.md`](debugging.md)).

## The cache does not survive a restart

Unlike `FUNCTION`-loaded libraries (persisted to the RDB file, see
[`functions-intro.md`](functions-intro.md#persistence)), the `EVAL`/`SCRIPT
LOAD` cache lives only in server memory. After a restart, any `EVALSHA` a
client still remembers will come back `NOSCRIPT` until that client resends
the body via `EVAL` (or `SCRIPT LOAD`) at least once — the same behavior
real clients are already written to expect, since even real Redis doesn't
guarantee the script cache across a restart.

## Errors

Same convention as `FUNCTION`'s Lua engine (see
[`lua-api.md`](lua-api.md#errors)): an uncaught Lua error comes back as
`ERR <message>`, while a script that explicitly `return`s `{err = "..."}`
gets that string back verbatim, with no `ERR` prefix added.

## See also

- [`functions-intro.md`](functions-intro.md) — the persisted, multi-engine alternative
- [`lua-api.md`](lua-api.md) — the full `redis.*` Lua API, shared with `FUNCTION`'s Lua engine
