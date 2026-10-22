#!python name=bloom_filter
# Level 80 (building a data structure Redis core doesn't have): a
# probabilistic set-membership filter built directly on SETBIT/GETBIT — no
# native Bloom filter type exists here (real Redis only gets one via the
# separate RedisBloom module), so this hashes each item itself, using the
# standard "double hashing" trick (Kirsch-Mitzenmacher) to derive K bit
# positions from just two real hash computations: position_i = (h1 + i*h2)
# mod M. `bf_add` sets those K bits; `bf_check` returns 1 only if every one
# of them is already set (meaning: "possibly present" — false positives are
# expected and part of the design, false negatives are not).
#
# M (bitmap size) and K (hash count) are fixed constants for this example; a
# real deployment sizes both from the expected item count and acceptable
# false-positive rate.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/80_bloom_filter.py
# Call: redisx-cli FCALL bf_add 1 seen:urls https://example.com
#       redisx-cli FCALL bf_check 1 seen:urls https://example.com   -> 1
#       redisx-cli FCALL bf_check 1 seen:urls https://never-added   -> 0 (usually)
import redis

M = 8192  # bitmap size in bits
K = 3     # number of bit positions set per item
MASK32 = 0xFFFFFFFF


def fnv1a(s, seed):
    h = (2166136261 ^ seed) & MASK32
    for byte in s.encode('utf-8'):
        h ^= byte
        h = (h * 16777619) & MASK32
    return h


def positions(item):
    h1 = fnv1a(item, 0)
    h2 = fnv1a(item, 0x9e3779b9)
    return [(h1 + i * h2) % M for i in range(K)]


def bf_add(keys, args):
    for p in positions(args[0]):
        redis.call('SETBIT', keys[0], p, 1)
    return {'ok': 'OK'}


def bf_check(keys, args):
    for p in positions(args[0]):
        if redis.call('GETBIT', keys[0], p) == 0:
            return 0
    return 1


redis.register_function('bf_add', bf_add)
redis.register_function('bf_check', bf_check)
