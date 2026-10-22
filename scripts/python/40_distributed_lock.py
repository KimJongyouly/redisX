#!python name=distributed_lock
# Level 40 (ownership-checked writes): the textbook single-instance Redis
# lock. `lock` acquires it with `SET key token NX PX ttl_ms` (first writer
# wins, auto-expires if the holder crashes); `unlock` only deletes it if the
# caller's token still matches — without that check, a slow holder could
# delete a lock that already timed out and was since acquired by someone
# else. The compare-then-delete has to happen in one atomic step for the
# same reason 10_incrby_limit's check-then-write does: a plain client-side
# GET then DEL has a race window a concurrent acquirer could land in.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/40_distributed_lock.py
# Call: redisx-cli FCALL lock 1 resource:42 token-abc 5000
#       redisx-cli FCALL unlock 1 resource:42 token-abc
#       (keys[0]=lock key, args[0]=caller-chosen unique token, args[1]=TTL ms)
import redis


def lock(keys, args):
    token = args[0]
    ttl_ms = args[1]
    acquired = redis.call('SET', keys[0], token, 'NX', 'PX', ttl_ms)
    if acquired is None:
        return {'err': 'ERR lock already held'}
    return acquired


def unlock(keys, args):
    token = args[0]
    current = redis.call('GET', keys[0])
    if current == token:
        redis.call('DEL', keys[0])
        return 1
    return 0


redis.register_function('lock', lock)
redis.register_function('unlock', unlock)
