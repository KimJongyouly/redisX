#!lua name=sliding_window_rate_limiter
-- Level 60 (a real data structure, not just counters): a precise
-- sliding-window limiter, unlike 30_rate_limiter's cheaper fixed window.
-- Keeps a sorted set of this bucket's recent call timestamps, evicts
-- everything older than `window_ms` (via ZRANGEBYSCORE + ZREM — this fork
-- has no ZREMRANGEBYSCORE), and only admits the call if what's left is
-- still under `limit`. Costs more than the fixed-window version (O(log N +
-- evicted) instead of O(1)), but never has the fixed window's edge case of
-- allowing up to 2x `limit` calls across a window boundary.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/60_sliding_window_rate_limiter.lua
-- Call: redisx-cli FCALL allow 1 api:user:42 5 1000
--       (KEYS[1]=bucket key, ARGV[1]=limit, ARGV[2]=window milliseconds)
redis.register_function('allow', function(keys, args)
  local limit = tonumber(args[1])
  local window_ms = tonumber(args[2])
  local t = redis.call('TIME')
  local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  local seq_key = keys[1] .. ':seq'

  local stale = redis.call('ZRANGEBYSCORE', keys[1], '-inf', now_ms - window_ms)
  if #stale > 0 then
    redis.call('ZREM', keys[1], table.unpack(stale))
  end

  local count = redis.call('ZCARD', keys[1])
  if count >= limit then
    return { err = 'ERR rate limit exceeded, window is ' .. window_ms .. 'ms' }
  end

  local seq = redis.call('INCR', seq_key)
  redis.call('ZADD', keys[1], now_ms, now_ms .. '-' .. seq)
  local ttl_secs = math.ceil(window_ms / 1000)
  redis.call('EXPIRE', keys[1], ttl_secs)
  redis.call('EXPIRE', seq_key, ttl_secs)
  return limit - count - 1
end)
