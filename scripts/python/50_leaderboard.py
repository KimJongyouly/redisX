#!python name=leaderboard
# Level 50 (read-your-write atomically): submits (or increments) a score and
# atomically returns the member's new rank (0 = top) in the same round trip
# — avoiding a second ZREVRANK call (and the race of the board changing
# between the two) just to show a player "you're now #N".
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/50_leaderboard.py
# Call: redisx-cli FCALL submit_score 1 weekly:top player:7 1500
#       redisx-cli FCALL increment_score 1 weekly:top player:7 50
#       (keys[0]=leaderboard key, args[0]=member, args[1]=absolute score / delta)
import redis


def submit_score(keys, args):
    redis.call('ZADD', keys[0], args[1], args[0])
    return redis.call('ZREVRANK', keys[0], args[0])


def increment_score(keys, args):
    redis.call('ZINCRBY', keys[0], args[1], args[0])
    return redis.call('ZREVRANK', keys[0], args[0])


redis.register_function('submit_score', submit_score)
redis.register_function('increment_score', increment_score)
