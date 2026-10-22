#!python name=task_queue
# Level 70 (Streams + consumer groups): a minimal reliable work queue.
# `enqueue` appends a job (auto-creating the stream); `claim_and_complete`
# claims exactly one pending job for a consumer and acknowledges it in the
# same round trip. In a real worker you'd do the actual work between the
# XREADGROUP and the XACK — this example acks immediately to keep the
# "claim and act atomically" shape visible without external side effects. A
# crash between the two round trips of the client-side equivalent leaves the
# job stuck pending (usually desirable for reliability); folding that gap
# into one FCALL only makes sense when your job itself is fast/idempotent
# and you'd rather not pay the extra round trip.
#
# Load:  redisx-cli -x FUNCTION LOAD < scripts/python/70_task_queue.py
# Setup (once per stream+group): redisx-cli FCALL ensure_group 1 jobs workers
# Call:  redisx-cli FCALL enqueue 1 jobs payload do-the-thing
#        redisx-cli FCALL claim_and_complete 1 jobs workers worker-1
#        (keys[0]=stream key; enqueue's args are field/value pairs;
#         claim_and_complete's args are group, consumer)
import redis


def ensure_group(keys, args):
    try:
        redis.call('XGROUP', 'CREATE', keys[0], args[0], '$', 'MKSTREAM')
    except Exception:
        pass  # BUSYGROUP: a previous call already created it
    return {'ok': 'OK'}


def enqueue(keys, args):
    return redis.call('XADD', keys[0], '*', *args)


def claim_and_complete(keys, args):
    group = args[0]
    consumer = args[1]
    reply = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', '1', 'STREAMS', keys[0], '>')
    if reply is None:
        return None
    entries = reply[0][1]
    entry_id = entries[0][0]
    redis.call('XACK', keys[0], group, entry_id)
    return entry_id


redis.register_function('ensure_group', ensure_group)
redis.register_function('enqueue', enqueue)
redis.register_function('claim_and_complete', claim_and_complete)
