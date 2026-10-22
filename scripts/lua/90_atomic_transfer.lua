#!lua name=atomic_transfer
-- Level 90 (capstone: multi-key invariant): moves `amount` from one balance
-- key to another, refusing the whole operation (no partial state — never
-- debits without also crediting) if the source doesn't have enough. This is
-- the textbook case for scripting across *multiple* keys: real Redis's own
-- docs reach for WATCH/MULTI/EXEC to do this safely from a client, which
-- needs a retry loop under contention; a single FCALL needs neither —
-- nothing else can run between the balance check and the two writes.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/90_atomic_transfer.lua
-- Call: redisx-cli SET acct:alice 100
--       redisx-cli SET acct:bob 0
--       redisx-cli FCALL transfer 2 acct:alice acct:bob 30
--       (KEYS[1]=from, KEYS[2]=to, ARGV[1]=amount)
--       -> {90, 30}, or an error (and zero writes) if alice can't cover it
redis.register_function('transfer', function(keys, args)
  local amount = tonumber(args[1])
  if amount == nil or amount <= 0 then
    return { err = 'ERR amount must be a positive number' }
  end
  local from_balance = tonumber(redis.call('GET', keys[1])) or 0
  if from_balance < amount then
    return { err = 'ERR insufficient funds: have ' .. from_balance .. ', need ' .. amount }
  end
  local new_from = redis.call('DECRBY', keys[1], amount)
  local new_to = redis.call('INCRBY', keys[2], amount)
  return { new_from, new_to }
end)
