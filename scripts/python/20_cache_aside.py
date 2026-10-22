#!python name=cache_aside
# Level 20 (compound replies): "cache-aside" with a server-computed value.
# `get_or_compute` returns the cached value if the key exists, otherwise
# computes one (here: a trivial transform of the input, standing in for
# "some expensive computation you'd rather not redo on every miss"), stores
# it with a TTL, and tells the caller whether it was a hit — useful for
# deciding whether to log/measure cache effectiveness. Doing the GET +
# compute + SET as one FCALL means there's no window between "found nothing"
# and "wrote the computed value" where a second caller could duplicate the
# (possibly expensive) computation.
#
# Unlike Lua (see lua/20_cache_aside.lua's module doc), this engine's
# Python->RESP conversion turns a real dict into a genuine RESP map, so the
# compound result here is `{'hit': ..., 'value': ...}` instead of a
# positional array.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/20_cache_aside.py
# Call: redisx-cli FCALL get_or_compute 1 greeting:world world 60
#       (keys[0]=cache key, args[0]=input to "compute" from, args[1]=TTL seconds)
import redis


def get_or_compute(keys, args):
    cached = redis.call('GET', keys[0])
    if cached is not None:
        return {'hit': 1, 'value': cached}
    computed = 'HELLO, %s!' % args[0].upper()
    redis.call('SET', keys[0], computed, 'EX', args[1])
    return {'hit': 0, 'value': computed}


redis.register_function('get_or_compute', get_or_compute)
