#!lua name=cache_aside
-- Level 20 (compound replies): "cache-aside" with a server-computed value.
-- `get_or_compute` returns the cached value if the key exists, otherwise
-- computes one (here: a trivial transform of the input, standing in for
-- "some expensive computation you'd rather not redo on every miss"), stores
-- it with a TTL, and tells the caller whether it was a hit — useful for
-- deciding whether to log/measure cache effectiveness. Doing the GET +
-- compute + SET as one FCALL means there's no window between "found
-- nothing" and "wrote the computed value" where a second caller could
-- duplicate the (possibly expensive) computation.
--
-- This engine's Lua->RESP conversion only turns a table into a flat array
-- or an {err=...}/{ok=...} reply (no generic map support — see
-- script_lua.rs's module doc), so the compound result here is a positional
-- 2-element array `{hit_flag, value}` rather than named fields. Compare
-- with python/20_cache_aside.py, which returns a real dict.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/20_cache_aside.lua
-- Call: redisx-cli FCALL get_or_compute 1 greeting:world world 60
--       (KEYS[1]=cache key, ARGV[1]=input to "compute" from, ARGV[2]=TTL seconds)
--       -> 1) (integer) 0   2) "HELLO, WORLD!"      (first call: miss)
--       -> 1) (integer) 1   2) "HELLO, WORLD!"      (second call: hit)
redis.register_function('get_or_compute', function(keys, args)
  local cached = redis.call('GET', keys[1])
  if cached ~= false then
    return { 1, cached }
  end
  local computed = 'HELLO, ' .. string.upper(args[1]) .. '!'
  redis.call('SET', keys[1], computed, 'EX', args[2])
  return { 0, computed }
end)
