#!python name=hello
# Level 00 (basics): registers one function, `echo`, that just returns its
# first argument. Load this first to confirm FUNCTION LOAD/FCALL work end to
# end before trying the more advanced levels in this folder.
#
# Load: redisx-cli -x FUNCTION LOAD < scripts/python/00_hello.py
# Call: redisx-cli FCALL echo 0 world
#       -> "world"
import redis


def echo(keys, args):
    return args[0]


redis.register_function('echo', echo)
