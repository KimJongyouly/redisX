#!lua name=task_queue
-- Level 70 (Streams + consumer groups): a minimal reliable work queue.
-- `enqueue` appends a job (auto-creating the stream); `claim_and_complete`
-- claims exactly one pending job for a consumer and acknowledges it in the
-- same round trip. In a real worker you'd do the actual work between the
-- XREADGROUP and the XACK — this example acks immediately to keep the
-- "claim and act atomically" shape visible without external side effects. A
-- crash between the two round trips of the client-side equivalent leaves
-- the job stuck pending (usually desirable for reliability); folding that
-- gap into one FCALL only makes sense when your job itself is fast/
-- idempotent and you'd rather not pay the extra round trip.
--
-- Load:  redisx-cli -x FUNCTION LOAD < scripts/lua/70_task_queue.lua
-- Setup (once per stream+group): redisx-cli FCALL ensure_group 1 jobs workers
-- Call:  redisx-cli FCALL enqueue 1 jobs payload do-the-thing
--        redisx-cli FCALL claim_and_complete 1 jobs workers worker-1
--        (KEYS[1]=stream key; enqueue's ARGV are field/value pairs;
--         claim_and_complete's ARGV are group, consumer)
redis.register_function('ensure_group', function(keys, args)
  -- BUSYGROUP just means a previous call already created it — fine either
  -- way, pcall only exists to swallow that one expected error.
  pcall(function()
    redis.call('XGROUP', 'CREATE', keys[1], args[1], '$', 'MKSTREAM')
  end)
  return { ok = 'OK' }
end)

redis.register_function('enqueue', function(keys, args)
  return redis.call('XADD', keys[1], '*', table.unpack(args))
end)

redis.register_function('claim_and_complete', function(keys, args)
  local group = args[1]
  local consumer = args[2]
  local reply = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', '1', 'STREAMS', keys[1], '>')
  if reply == false then
    return nil
  end
  local entries = reply[1][2]
  local id = entries[1][1]
  redis.call('XACK', keys[1], group, id)
  return id
end)
