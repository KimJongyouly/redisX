#!lua name=leaderboard
-- Level 50 (read-your-write atomically): submits (or increments) a score
-- and atomically returns the member's new rank (0 = top) in the same round
-- trip — avoiding a second ZREVRANK call (and the race of the board
-- changing between the two) just to show a player "you're now #N".
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/50_leaderboard.lua
-- Call: redisx-cli FCALL submit_score 1 weekly:top player:7 1500
--       redisx-cli FCALL increment_score 1 weekly:top player:7 50
--       (KEYS[1]=leaderboard key, ARGV[1]=member, ARGV[2]=absolute score / delta)
redis.register_function('submit_score', function(keys, args)
  redis.call('ZADD', keys[1], args[2], args[1])
  return redis.call('ZREVRANK', keys[1], args[1])
end)

redis.register_function('increment_score', function(keys, args)
  redis.call('ZINCRBY', keys[1], args[2], args[1])
  return redis.call('ZREVRANK', keys[1], args[1])
end)
