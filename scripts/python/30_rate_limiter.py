#!python name=rate_limiter
# Level 30 (combining several calls): fixed-window rate limiter — allows at
# most `limit` calls against `keys[0]` per `window` seconds, resetting the
# window on the first call after it expires. INCR + a one-shot EXPIRE
# (guarded by NX so a retry never resets an in-progress window) done as a
# single FCALL closes the same race a naive client-side
# GET-then-INCR-then-EXPIRE would have. See 60_sliding_window_rate_limiter
# for the more precise (and more expensive) alternative that doesn't allow
# bursts across a window boundary.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/30_rate_limiter.py
# Call: redisx-cli FCALL allow 1 api:user:42 10 60
#       (keys[0]=bucket key, args[0]=limit, args[1]=window seconds)
#       -> remaining calls allowed this window, or an error once exhausted
import redis


def allow(keys, args):
    limit = int(args[0])
    window = int(args[1])
    count = redis.call('INCR', keys[0])
    if count == 1:
        redis.call('EXPIRE', keys[0], window, 'NX')
    if count > limit:
        ttl_ms = redis.call('PTTL', keys[0])
        return {'err': 'ERR rate limit exceeded, retry in %sms' % ttl_ms}
    return limit - count


redis.register_function('allow', allow)
