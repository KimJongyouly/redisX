-- String(키/값) 커맨드 둘러보기.
--
-- 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
-- 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
-- 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py와 같은
-- 시나리오를 Lua(redis-lua)로 옮긴 것이다.
--
-- 실행: lua string.lua
local demo = require "redisx_demo"
local client = demo.connect()

local demo_keys = { "greeting", "page:views", "user:1:session", "k1", "k2", "k3" }
client:del(table.unpack(demo_keys))

demo.section("SET / GET")
client:set("greeting", "hello world")
print("GET greeting ->", client:get("greeting"))

demo.section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)")
client:append("greeting", "!")
print("APPEND 이후:", client:get("greeting"))
print("GETRANGE greeting 0 4 ->", client:getrange("greeting", 0, 4))
client:setrange("greeting", 6, "redisX") -- "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
print("SETRANGE 이후:", client:get("greeting"))

demo.section("MSET / MGET (여러 키 한 번에)")
client:mset("k1", "v1", "k2", "v2", "k3", "v3")
print("MGET k1 k2 k3 ->", table.concat(client:mget("k1", "k2", "k3"), ", "))

demo.section("INCR / INCRBY (카운터)")
client:del("page:views")
for _ = 1, 3 do
	client:incr("page:views")
end
client:incrby("page:views", 10)
print("page:views ->", client:get("page:views"))

demo.section("EXPIRE / TTL / PERSIST")
client:set("user:1:session", "token-abc")
client:expire("user:1:session", 2)
print("설정 직후 TTL ->", client:ttl("user:1:session"))
os.execute("sleep 2.2")
print("2.2초 후 EXISTS ->", client:exists("user:1:session") == 1, "(TTL이 지나 자동 삭제됨)")

client:set("user:1:session", "token-def")
client:expire("user:1:session", 60)
client:persist("user:1:session")
print("PERSIST 이후 TTL ->", client:ttl("user:1:session"), "(-1 = 만료 없음)")

print("\n정리:", client:del(table.unpack(demo_keys)), "개 키 삭제")
