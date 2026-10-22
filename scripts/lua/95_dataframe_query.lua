#!lua name=dataframe_query
-- Level 95 (native DataFrame type, `--features polars` only): the full
-- round trip — load from CSV or Parquet, query two ways, dump back out to
-- CSV or Parquet — all via plain `redis.call`. The point isn't that
-- wrapping these in a function is more useful than calling `DF.*` directly
-- (it mostly isn't, here); it's proving the `DF.*` commands this fork's
-- native DataFrame value type added need no special handling from a
-- script at all: `FUNCTION`/`FCALL` just sees them as ordinary commands,
-- same as `SET`/`ZADD`/anything else.
--
-- Requires a server built with `--features polars` (`install.sh` already
-- does this — see the repo root README's build section — but a plain
-- `cargo build --workspace` without the feature flag does not).
-- `FUNCTION LOAD` of this library itself always succeeds regardless (the
-- shebang/body has nothing polars-specific in it); it's `FCALL`ing any of
-- these functions that fails, the moment their `redis.call('DF....', ...)`
-- hits a server without the feature: `ERR runtime error: ERR unknown
-- command 'DF.FROMCSV' stack traceback: ...` (see `docs/lua-api.md`'s
-- "Errors" section for why an uncaught `redis.call` failure looks like
-- that instead of a plain one-line message).
--
-- Load:  redisx-cli -x FUNCTION LOAD < scripts/lua/95_dataframe_query.lua
--
-- `load_csv`/`load_parquet` (KEYS[1]=the DataFrame key, ARGV[1]=the file's
-- content — CSV text with a header row, or raw Parquet bytes) both combine
-- the import with reading back the freshly-ingested shape in the same
-- round trip — nothing else can touch the key in between, same atomicity
-- property every other level in this directory relies on:
--   redisx-cli FCALL load_csv 1 staff "$(printf 'name,age,dept\nalice,30,eng\nbob,25,sales\ncarol,35,eng\n')"
--   redisx-cli -x FCALL load_parquet 1 staff2 < staff.parquet
--
-- `query`/`pretty` both run read-only SQL against an already-loaded
-- DataFrame (KEYS[1]=the key, ARGV[1]=an optional SQL string — the table
-- name inside it is always `self`) — `query` returns the same RESP-array
-- shape `DF.QUERY` would, `pretty` the same box-drawing-table bulk string
-- `DF.PRETTY` would:
--   redisx-cli FCALL query 1 staff "SELECT name FROM self WHERE dept = 'eng' ORDER BY name"
--   redisx-cli --no-raw FCALL pretty 1 staff
--
-- One real wrinkle worth knowing about `pretty` specifically: `redisx-cli`
-- special-cases the literal command name `DF.PRETTY` to always render its
-- reply raw/unescaped (see `docs/` and the repo root README) — that
-- special case keys off the command name on the wire, so it does NOT
-- apply here, since what's actually sent is `FCALL`, not `DF.PRETTY`. Add
-- `--raw` yourself if you want the unescaped table out of `FCALL pretty`
-- the same way plain `DF.PRETTY` gives you by default.
--
-- `to_csv`/`to_parquet` (KEYS[1]=the key, no ARGV) dump the whole frame
-- back out, exactly like `DF.TOCSV`/`DF.TOPARQUET`:
--   redisx-cli FCALL to_csv 1 staff
--   redisx-cli FCALL to_parquet 1 staff > staff_export.parquet   -- see the
--   -- root README's shell-redirect caveat: this one corrupts the binary
--   -- output the same way `DF.TOPARQUET` directly would, nothing
--   -- FCALL-specific about it.
--
-- `load_parquet`/`to_parquet` only exist in this Lua version, not the
-- Python sibling file — a genuine engine difference, not an oversight.
-- Lua strings are raw byte buffers, so Parquet's binary content survives
-- `redis.call`'s keys/args and reply conversion exactly (verified: an
-- `FCALL`-wrapped `DF.TOPARQUET` round trip here is byte-for-byte identical
-- to calling `DF.TOPARQUET` directly). The Python engine instead decodes
-- every RESP bulk string crossing that same boundary via
-- `String::from_utf8_lossy` — harmless for the UTF-8 text every other
-- example deals with, but it silently mangles genuinely binary data like
-- Parquet bytes. See `scripts/python/95_dataframe_query.py`'s header
-- comment for the verified failure and `docs/python-api.md`'s reply
-- conversion table for the general rule this falls out of.
redis.register_function('load_csv', function(keys, args)
  redis.call('DF.FROMCSV', keys[1], args[1])
  return redis.call('DF.SHAPE', keys[1])
end)

redis.register_function('load_parquet', function(keys, args)
  redis.call('DF.FROMPARQUET', keys[1], args[1])
  return redis.call('DF.SHAPE', keys[1])
end)

redis.register_function('query', function(keys, args)
  return redis.call('DF.QUERY', keys[1], args[1])
end)

redis.register_function('pretty', function(keys, args)
  if args[1] == nil then
    return redis.call('DF.PRETTY', keys[1])
  end
  return redis.call('DF.PRETTY', keys[1], args[1])
end)

redis.register_function('to_csv', function(keys, args)
  return redis.call('DF.TOCSV', keys[1])
end)

redis.register_function('to_parquet', function(keys, args)
  return redis.call('DF.TOPARQUET', keys[1])
end)
