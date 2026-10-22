#!lua name=distributed_lock
-- Level 40 (ownership-checked writes): the textbook single-instance Redis
-- lock. `lock` acquires it with `SET key token NX PX ttl_ms` (first writer
-- wins, auto-expires if the holder crashes); `unlock` only deletes it if the
-- caller's token still matches — without that check, a slow holder could
-- delete a lock that already timed out and was since acquired by someone
-- else. The compare-then-delete has to happen in one atomic step for the
-- same reason 10_incrby_limit's check-then-write does: a plain client-side
-- GET then DEL has a race window a concurrent acquirer could land in.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/40_distributed_lock.lua
-- Call: redisx-cli FCALL lock 1 resource:42 token-abc 5000
--       redisx-cli FCALL unlock 1 resource:42 token-abc
--       (KEYS[1]=lock key, ARGV[1]=caller-chosen unique token, ARGV[2]=TTL ms)
redis.register_function('lock', function(keys, args)
  local acquired = redis.call('SET', keys[1], args[1], 'NX', 'PX', args[2])
  if acquired == false then
    return { err = 'ERR lock already held' }
  end
  return acquired
end)

redis.register_function('unlock', function(keys, args)
  local token = args[1]
  local current = redis.call('GET', keys[1])
  if current == token then
    redis.call('DEL', keys[1])
    return 1
  end
  return 0
end)
