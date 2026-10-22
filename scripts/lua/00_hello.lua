#!lua name=hello
-- Level 00 (basics): registers one function, `echo`, that just returns its
-- first argument. Load this first to confirm FUNCTION LOAD/FCALL work end to
-- end before trying the more advanced levels in this folder.
--
-- Load: redisx-cli -x FUNCTION LOAD < scripts/lua/00_hello.lua
-- Call: redisx-cli FCALL echo 0 world
--       -> "world"
redis.register_function('echo', function(keys, args)
  return args[1]
end)
