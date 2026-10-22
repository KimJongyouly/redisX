#!lua name=rate_limiter
-- Level 30 (combining several calls): fixed-window rate limiter — allows at
-- most `limit` calls against `keys[1]` per `window` seconds, resetting the
-- window on the first call after it expires. INCR + a one-shot EXPIRE
-- (guarded by NX so a retry never resets an in-progress window) done as a
-- single FCALL closes the same race a naive client-side
-- GET-then-INCR-then-EXPIRE would have. See 60_sliding_window_rate_limiter
-- for the more precise (and more expensive) alternative that doesn't allow
-- bursts across a window boundary.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/30_rate_limiter.lua
-- Call: redisx-cli FCALL allow 1 api:user:42 10 60
--       (KEYS[1]=bucket key, ARGV[1]=limit, ARGV[2]=window seconds)
--       -> remaining calls allowed this window, or an error once exhausted
redis.register_function('allow', function(keys, args)
  local limit = tonumber(args[1])
  local window = tonumber(args[2])
  local count = redis.call('INCR', keys[1])
  if count == 1 then
    redis.call('EXPIRE', keys[1], window, 'NX')
  end
  if count > limit then
    local ttl_ms = redis.call('PTTL', keys[1])
    return { err = 'ERR rate limit exceeded, retry in ' .. ttl_ms .. 'ms' }
  end
  return limit - count
end)
