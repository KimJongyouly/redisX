#!python name=sliding_window_rate_limiter
# Level 60 (a real data structure, not just counters): a precise
# sliding-window limiter, unlike 30_rate_limiter's cheaper fixed window.
# Keeps a sorted set of this bucket's recent call timestamps, evicts
# everything older than `window_ms` (via ZRANGEBYSCORE + ZREM — this fork
# has no ZREMRANGEBYSCORE), and only admits the call if what's left is still
# under `limit`. Costs more than the fixed-window version (O(log N +
# evicted) instead of O(1)), but never has the fixed window's edge case of
# allowing up to 2x `limit` calls across a window boundary.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/60_sliding_window_rate_limiter.py
# Call: redisx-cli FCALL allow 1 api:user:42 5 1000
#       (keys[0]=bucket key, args[0]=limit, args[1]=window milliseconds)
import math
import redis


def allow(keys, args):
    limit = int(args[0])
    window_ms = int(args[1])
    t = redis.call('TIME')
    now_ms = int(t[0]) * 1000 + int(t[1]) // 1000
    seq_key = keys[0] + ':seq'

    stale = redis.call('ZRANGEBYSCORE', keys[0], '-inf', now_ms - window_ms)
    if stale:
        redis.call('ZREM', keys[0], *stale)

    count = redis.call('ZCARD', keys[0])
    if count >= limit:
        return {'err': 'ERR rate limit exceeded, window is %dms' % window_ms}

    seq = redis.call('INCR', seq_key)
    redis.call('ZADD', keys[0], now_ms, '%d-%d' % (now_ms, seq))
    ttl_secs = math.ceil(window_ms / 1000)
    redis.call('EXPIRE', keys[0], ttl_secs)
    redis.call('EXPIRE', seq_key, ttl_secs)
    return limit - count - 1


redis.register_function('allow', allow)
