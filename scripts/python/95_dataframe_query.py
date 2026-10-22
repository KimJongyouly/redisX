#!python name=dataframe_query
# Level 95 (native DataFrame type, `--features polars` only): load from CSV,
# query two ways, dump back out to CSV — all via plain `redis.call`. The
# point isn't that wrapping these in a function is more useful than calling
# `DF.*` directly (it mostly isn't, here); it's proving the `DF.*` commands
# this fork's native DataFrame value type added need no special handling
# from a script at all: `FUNCTION`/`FCALL` just sees them as ordinary
# commands, same as `SET`/`ZADD`/anything else.
#
# Requires a server built with `--features polars` (`install.sh` already
# does this — see the repo root README's build section — but a plain
# `cargo build --workspace` without the feature flag does not).
# `FUNCTION LOAD` of this library itself always succeeds regardless (the
# shebang/body has nothing polars-specific in it); it's `FCALL`ing any of
# these functions that fails, the moment their `redis.call('DF....', ...)`
# hits a server without the feature: `ERR RuntimeError: ERR unknown command
# 'DF.FROMCSV'` (see `docs/python-api.md`'s "Errors" section for why an
# uncaught `redis.call` failure is prefixed with the exception class name
# like this instead of just the plain message).
#
# ## No `load_parquet`/`to_parquet` here — found a real engine limitation
#
# The Lua sibling of this file (`scripts/lua/95_dataframe_query.lua`) has
# `load_parquet`/`to_parquet` wrapping `DF.FROMPARQUET`/`DF.TOPARQUET`; this
# file deliberately does not, because — verified by actually trying it, not
# a theoretical concern — **the Python engine corrupts binary data passed
# through `redis.call`'s keys/args or returned as its reply.** `script_python.rs`
# decodes every RESP bulk string crossing that boundary via
# `String::from_utf8_lossy` (four call sites: `FCALL`'s keys/args into your
# function, and any `redis.call`/`redis.pcall` reply bulk string) — fine for
# the UTF-8 text every other example here deals with, but Parquet bytes
# are binary and essentially never valid UTF-8, so they get silently
# mangled by lossy replacement-character substitution at that boundary,
# with no error raised anywhere. Confirmed by round-tripping a real
# `.parquet` file through an `FCALL`-wrapped `DF.TOPARQUET`: the byte count
# changed (1264 bytes out vs. 1164 from calling `DF.TOPARQUET` directly on
# the same key) and the result failed to parse as Parquet at all
# (`pyarrow`: "Couldn't deserialize thrift: TProtocolException: Invalid
# data"). This is a general Python-engine limitation, not specific to
# Parquet or to this command — anything binary passed through a Python
# `FUNCTION` is at risk, which is also why `docs/python-api.md`'s reply
# conversion table already calls out "RESP bulk strings are exposed as
# Python `str` ... arbitrary binary payloads get lossily decoded" as a
# documented simplification. If you need Parquet (or any other binary
# format) in or out of a DataFrame from Python code, call
# `DF.FROMPARQUET`/`DF.TOPARQUET` directly from your client instead of
# routing it through a Python `FCALL` — or use the Lua engine, which has no
# such limitation (Lua strings are raw byte buffers, not forced UTF-8).
#
# Load:  redisx-cli -x FUNCTION LOAD < scripts/python/95_dataframe_query.py
#
# `load_csv` (keys[0]=the DataFrame key, args[0]=CSV text with a header row)
# combines the import with reading back the freshly-ingested shape in the
# same round trip — nothing else can touch the key in between, same
# atomicity property every other level in this directory relies on:
#   redisx-cli FCALL load_csv 1 staff "$(printf 'name,age,dept\nalice,30,eng\nbob,25,sales\ncarol,35,eng\n')"
#
# `query`/`pretty` both run read-only SQL against an already-loaded
# DataFrame (keys[0]=the key, args[0]=an optional SQL string — the table
# name inside it is always `self`) — `query` returns the same RESP-array
# shape `DF.QUERY` would, `pretty` the same box-drawing-table bulk string
# `DF.PRETTY` would:
#   redisx-cli FCALL query 1 staff "SELECT name FROM self WHERE dept = 'eng' ORDER BY name"
#   redisx-cli --raw FCALL pretty 1 staff
#
# One real wrinkle worth knowing about `pretty` specifically: `redisx-cli`
# special-cases the literal command name `DF.PRETTY` to always render its
# reply raw/unescaped (see `docs/` and the repo root README) — that
# special case keys off the command name on the wire, so it does NOT apply
# here, since what's actually sent is `FCALL`, not `DF.PRETTY`. Add `--raw`
# yourself if you want the unescaped table out of `FCALL pretty` the same
# way plain `DF.PRETTY` gives you by default.
#
# `to_csv` (keys[0]=the key, no args) dumps the whole frame back out as
# text, exactly like `DF.TOCSV` — safe here (unlike `to_parquet` above)
# because CSV is already text, so the lossy UTF-8 round trip this engine
# does is a no-op for it:
#   redisx-cli FCALL to_csv 1 staff
import redis


def load_csv(keys, args):
    redis.call('DF.FROMCSV', keys[0], args[0])
    return redis.call('DF.SHAPE', keys[0])


def query(keys, args):
    return redis.call('DF.QUERY', keys[0], args[0])


def pretty(keys, args):
    if len(args) == 0:
        return redis.call('DF.PRETTY', keys[0])
    return redis.call('DF.PRETTY', keys[0], args[0])


def to_csv(keys, args):
    return redis.call('DF.TOCSV', keys[0])


redis.register_function('load_csv', load_csv)
redis.register_function('query', query)
redis.register_function('pretty', pretty)
redis.register_function('to_csv', to_csv)
