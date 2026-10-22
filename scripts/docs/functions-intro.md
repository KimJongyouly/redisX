# Functions

`FUNCTION`/`FCALL` is the persisted, named way to run custom logic in
redisX, in either Lua or Python. See [`programmability.md`](programmability.md)
for how this compares to legacy `EVAL`.

## Libraries

A *library* is the unit `FUNCTION LOAD` operates on: one source blob that
can register one or more named functions. Every library starts with a
mandatory shebang line naming its engine and its own name:

```
#!lua name=mylib
```
```
#!python name=mylib
```

Anything after that first line is the library body, run once at load time.
A library body is expected to call `redis.register_function` at least once;
`FUNCTION LOAD` of a body that registers nothing fails with `ERR No
functions registered`, and a body missing the shebang line entirely fails
with `ERR Missing library metadata`.

### Registering a function

**Lua** accepts two call shapes:

```lua
redis.register_function('myfunc', function(keys, args) ... end)

-- or, the table form:
redis.register_function{
  function_name = 'myfunc',
  callback = function(keys, args) ... end,
}
```

Only `function_name` and `callback` are read out of the table form — real
Redis's table form also accepts a `flags` list (e.g. `{'no-writes'}`); passing
one here is not an error, but it is silently ignored (see the "known gaps"
section below).

**Python** accepts only the positional form:

```python
import redis

def myfunc(keys, args):
    ...

redis.register_function('myfunc', myfunc)
```

### Calling a function: `FCALL`

```
FCALL funcname numkeys [key [key ...]] [arg [arg ...]]
FCALL_RO funcname numkeys [key [key ...]] [arg [arg ...]]
```

The registered callback receives exactly two arguments:

| | Lua | Python |
|---|---|---|
| Keys | 1-indexed table: `keys[1]`, `keys[2]`, ... | 0-indexed list: `keys[0]`, `keys[1]`, ... |
| Args | 1-indexed table: `args[1]`, `args[2]`, ... | 0-indexed list: `args[0]`, `args[1]`, ... |

Each indexes its own language's native convention rather than forcing one
language's idiom onto the other.

`FCALL_RO` runs the same function but rejects it the moment it tries to
issue a write command through `redis.call`/`redis.pcall`, with a message
containing:

```
Write commands are not allowed when calling a function in read-only mode (FCALL_RO)
```

(If the write was attempted via `redis.call` and not caught with
`redis.pcall`, what actually reaches the client is that text wrapped in the
engine's own uncaught-error formatting — see
[`lua-api.md`](lua-api.md#errors)/[`python-api.md`](python-api.md#errors) for
exactly what that looks like.)

This check is generic (based on whether the *attempted command* is a write),
not based on a declared `no-writes` flag on the function itself — so the same
function can be called through plain `FCALL` (writes allowed) or `FCALL_RO`
(writes rejected) depending on which one the caller uses; there's no way to
mark a function as "always read-only regardless of how it's called."

## `FUNCTION LOAD [REPLACE] <code>`

Loads a library. Returns the library's own name on success (not the function
names). Loading a library whose name is already in use fails unless
`REPLACE` is given:

```
redisx-cli -x FUNCTION LOAD < scripts/lua/10_incrby_limit.lua
# -> "incrby_limit"
redisx-cli -x FUNCTION LOAD < scripts/lua/10_incrby_limit.lua
# -> (error) ERR Library 'incrby_limit' already exists
```

`REPLACE` is the first argument after `LOAD` (`FUNCTION LOAD REPLACE
<code>`), matching real Redis's syntax. On success it atomically swaps out
every function the old version of the library owned for the new set (a
function present in the old version but dropped from the new one is removed,
not left stale).

A compile-time error (bad shebang, Lua/Python syntax error, no functions
registered) is returned as `ERR ...` and the previous library (if any) is
left untouched.

## `FUNCTION DELETE <libname>`

Removes a library and every function it registered. Deleting an unknown
library name returns `ERR Library not found`.

## `FUNCTION LIST`

Returns one map per loaded library:

```
1) 1) "library_name"
   2) "incrby_limit"
   3) "engine"
   4) "LUA"
   5) "functions"
   6) 1) 1) "name"
         2) "incrby_limit"
         3) "description"
         4) (nil)
         5) "flags"
         6) (empty array)
```

`description` is always `nil` and `flags` is always an empty array — neither
is tracked anywhere, since nothing in this port parses function flags or a
description field out of `register_function`'s arguments.

`FUNCTION LIST` takes no filters here: real Redis's `LIBRARYNAME <name>` and
`WITHCODE` options are accepted syntactically (extra arguments are simply
ignored) but have no effect — `WITHCODE` never adds a `library_code` field,
and the full library list is always returned regardless of `LIBRARYNAME`.

## Persistence

Loaded libraries are written to the RDB file (a `FUNCTION2`-opcode record
holding each library's original, unmodified source — shebang line included)
and reloaded with a plain `function_load` call when the server restarts, so
`FUNCTION LOAD`ed libraries survive a restart the same way keys do. This is
the main practical reason to prefer `FUNCTION` over `EVAL`/`SCRIPT LOAD` for
anything long-lived — the `EVAL` script cache (see
[`eval-intro.md`](eval-intro.md)) is not persisted.

## Known gap: no cross-library name-collision check

Real Redis rejects `FUNCTION LOAD` outright if a function name it would
register is already owned by a *different* library. redisX does not check
this: loading library B that happens to register a function name already
owned by library A silently steals that name out from under A (A's own
`FUNCTION LIST` entry still lists the name, but calling it now runs B's
callback). If A is later deleted, the name disappears from the function
table entirely even though A's `FUNCTION LIST` entry claimed to still own it
up to that point. In practice this only bites if you reuse a function name
across two unrelated libraries — the worked examples in
[`../README.md`](../README.md) are careful to keep every level's function
names distinct for this reason.

## See also

- [`lua-api.md`](lua-api.md) / [`python-api.md`](python-api.md) — what `redis.call`/`redis.pcall` can do inside a registered function
- [`../README.md`](../README.md) — ten worked examples, 00 through 90
