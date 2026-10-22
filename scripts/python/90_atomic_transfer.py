#!python name=atomic_transfer
# Level 90 (capstone: multi-key invariant): moves `amount` from one balance
# key to another, refusing the whole operation (no partial state — never
# debits without also crediting) if the source doesn't have enough. This is
# the textbook case for scripting across *multiple* keys: real Redis's own
# docs reach for WATCH/MULTI/EXEC to do this safely from a client, which
# needs a retry loop under contention; a single FCALL needs neither —
# nothing else can run between the balance check and the two writes.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/90_atomic_transfer.py
# Call: redisx-cli SET acct:alice 100
#       redisx-cli SET acct:bob 0
#       redisx-cli FCALL transfer 2 acct:alice acct:bob 30
#       (keys[0]=from, keys[1]=to, args[0]=amount)
#       -> {'from': 70, 'to': 30}, or an error (and zero writes) if alice can't cover it
import redis


def transfer(keys, args):
    amount = int(args[0])
    if amount <= 0:
        return {'err': 'ERR amount must be a positive number'}
    from_raw = redis.call('GET', keys[0])
    from_balance = int(from_raw) if from_raw is not None else 0
    if from_balance < amount:
        return {'err': 'ERR insufficient funds: have %d, need %d' % (from_balance, amount)}
    new_from = redis.call('DECRBY', keys[0], amount)
    new_to = redis.call('INCRBY', keys[1], amount)
    return {'from': new_from, 'to': new_to}


redis.register_function('transfer', transfer)
