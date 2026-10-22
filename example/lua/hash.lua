-- Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
--
-- 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
-- Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
-- 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
-- example/python/hash.py와 같은 시나리오.
--
-- redis-lua(2.0.4)에 미리 등록된 `hset`은 그 시절 그대로 한 번에 필드
-- 하나만 넣는 옛날 HSET이라, redisx_demo.lua에서 여러 필드를 한 번에
-- 받는 현대 HSET으로 덮어써서 다시 등록해 둔 것을 그대로 쓴다 — redisX는
-- 레거시 HMSET 자체를 구현하지 않았다(HSET이 이미 다중 필드를 받으므로).
--
-- 실행: lua hash.lua
local demo = require "redisx_demo"
local client = demo.connect()

local user_key = "user:42"
client:del(user_key)

demo.section("HSET으로 여러 필드를 한 번에 채우기")
client:hset(user_key, "name", "jiyeon", "level", "5", "coins", "100", "logins", "0")
local function print_hash(key)
	local fields = {}
	for k, v in pairs(client:hgetall(key)) do
		table.insert(fields, k .. "=" .. v)
	end
	table.sort(fields)
	print(table.concat(fields, ", "))
end
io.write("HGETALL user:42 -> ")
print_hash(user_key)

demo.section("HGET / HMGET으로 필드 일부만 읽기")
print("HGET user:42 name ->", client:hget(user_key, "name"))
print("HMGET user:42 name level ->", table.concat(client:hmget(user_key, "name", "level"), ", "))

demo.section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가")
client:hincrby(user_key, "coins", 50)
client:hincrby(user_key, "logins", 1)
io.write("HGETALL user:42 -> ")
print_hash(user_key)

demo.section("HEXISTS / HDEL")
print("HEXISTS user:42 level ->", client:hexists(user_key, "level"))
client:hdel(user_key, "logins")
io.write("HDEL 이후 HGETALL -> ")
print_hash(user_key)

demo.section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기")
local raffle = "raffle:entries"
client:del(raffle)
client:hset(raffle, "alice", "1", "bob", "1", "carol", "1", "dave", "1")
print("당첨자 한 명 ->", client:hrandfield(raffle))
print("HRANDFIELD -3 (중복 허용 3명 뽑기) ->", table.concat(client:hrandfield(raffle, -3), ", "))

print("\n정리:", client:del(user_key, raffle), "개 키 삭제")
