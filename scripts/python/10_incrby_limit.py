#!python name=incrby_limit
# Level 10 (basic atomicity): atomically increments a numeric key by
# `delta`, but refuses the write entirely (no partial state) if the result
# would exceed `max`. This is the canonical reason to do arithmetic
# server-side instead of client-side: a client doing GET, checking the limit
# itself, then SET has a race window between the two round trips where
# another client can slip in between — redisx-server runs an FCALL to
# completion without interleaving any other command (see
# redis_document/25_redisX.md on the single-threaded event loop), so this
# check-then-write is genuinely atomic.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/10_incrby_limit.py
# Call: redisx-cli FCALL incrby_limit 1 mycounter 5 100
#       (keys[0]=mycounter, args[0]=delta, args[1]=max)
#       -> new value, or an error if it would have exceeded 100
import redis


def incrby_limit(keys, args):
    delta = int(args[0])
    maximum = int(args[1])
    current_raw = redis.call('GET', keys[0])
    current = int(current_raw) if current_raw is not None else 0
    updated = current + delta
    if updated > maximum:
        return {'err': 'ERR would exceed limit of %d (currently %d)' % (maximum, current)}
    redis.call('SET', keys[0], str(updated))
    return updated


redis.register_function('incrby_limit', incrby_limit)
