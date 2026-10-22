#!lua name=incrby_limit
-- Level 10 (basic atomicity): atomically increments a numeric key by
-- `delta`, but refuses the write entirely (no partial state) if the result
-- would exceed `max`. This is the canonical reason to do arithmetic
-- server-side instead of client-side: a client doing GET, checking the
-- limit itself, then SET has a race window between the two round trips
-- where another client can slip in between — redisx-server runs an FCALL to
-- completion without interleaving any other command (see
-- redis_document/25_redisX.md on the single-threaded event loop), so this
-- check-then-write is genuinely atomic.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/10_incrby_limit.lua
-- Call: redisx-cli FCALL incrby_limit 1 mycounter 5 100
--       (KEYS[1]=mycounter, ARGV[1]=delta, ARGV[2]=max)
--       -> new value, or an error if it would have exceeded 100
redis.register_function('incrby_limit', function(keys, args)
  local delta = tonumber(args[1])
  local max = tonumber(args[2])
  local current = tonumber(redis.call('GET', keys[1])) or 0
  local updated = current + delta
  if updated > max then
    return { err = 'ERR would exceed limit of ' .. max .. ' (currently ' .. current .. ')' }
  end
  redis.call('SET', keys[1], updated)
  return updated
end)
