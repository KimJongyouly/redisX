#!lua name=bloom_filter
-- Level 80 (building a data structure Redis core doesn't have): a
-- probabilistic set-membership filter built directly on SETBIT/GETBIT — no
-- native Bloom filter type exists here (real Redis only gets one via the
-- separate RedisBloom module), so this hashes each item itself, using the
-- standard "double hashing" trick (Kirsch-Mitzenmacher) to derive K bit
-- positions from just two real hash computations: position_i = (h1 + i*h2)
-- mod M. `bf_add` sets those K bits; `bf_check` returns 1 only if every one
-- of them is already set (meaning: "possibly present" — false positives are
-- expected and part of the design, false negatives are not).
--
-- M (bitmap size) and K (hash count) are fixed constants for this example;
-- a real deployment sizes both from the expected item count and acceptable
-- false-positive rate.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/80_bloom_filter.lua
-- Call: redisx-cli FCALL bf_add 1 seen:urls https://example.com
--       redisx-cli FCALL bf_check 1 seen:urls https://example.com   -> 1
--       redisx-cli FCALL bf_check 1 seen:urls https://never-added   -> 0 (usually)
local M = 8192  -- bitmap size in bits
local K = 3     -- number of bit positions set per item

local function fnv1a(str, seed)
  local hash = (2166136261 ~ seed) & 0xFFFFFFFF
  for i = 1, #str do
    hash = hash ~ string.byte(str, i)
    hash = (hash * 16777619) & 0xFFFFFFFF
  end
  return hash
end

local function positions(item)
  local h1 = fnv1a(item, 0)
  local h2 = fnv1a(item, 0x9e3779b9)
  local pos = {}
  for i = 0, K - 1 do
    pos[i + 1] = (h1 + i * h2) % M
  end
  return pos
end

redis.register_function('bf_add', function(keys, args)
  for _, p in ipairs(positions(args[1])) do
    redis.call('SETBIT', keys[1], p, 1)
  end
  return { ok = 'OK' }
end)

redis.register_function('bf_check', function(keys, args)
  for _, p in ipairs(positions(args[1])) do
    if redis.call('GETBIT', keys[1], p) == 0 then
      return 0
    end
  end
  return 1
end)
