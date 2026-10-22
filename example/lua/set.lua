-- Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
--
-- 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
-- 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
-- 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
-- example/python/set.py와 같은 시나리오.
--
-- 실행: lua set.lua
local demo = require "redisx_demo"
local client = demo.connect()

local function joined(t)
	return table.concat(t, ", ")
end

local post_a, post_b, archive = "post:1:tags", "post:2:tags", "tags:archive"
client:del(post_a, post_b, archive)

demo.section("SADD로 글마다 태그 달기")
client:sadd(post_a, "rust", "redis", "database")
client:sadd(post_b, "rust", "web", "async")
print("post:1:tags ->", joined(client:smembers(post_a)))
print("post:2:tags ->", joined(client:smembers(post_b)))

demo.section("SINTER / SUNION / SDIFF로 집합 연산")
print("둘 다 쓴 태그 (SINTER) ->", joined(client:sinter(post_a, post_b)))
print("둘 중 하나라도 쓴 태그 (SUNION) ->", joined(client:sunion(post_a, post_b)))
print("post:1에만 있는 태그 (SDIFF) ->", joined(client:sdiff(post_a, post_b)))

demo.section("SINTERSTORE로 교집합을 새 키에 저장")
local common_key = "tags:common"
client:del(common_key)
client:sinterstore(common_key, post_a, post_b)
print("tags:common ->", joined(client:smembers(common_key)))

demo.section("SISMEMBER / SCARD")
print("post:1에 'redis' 태그가 있나? ->", client:sismember(post_a, "redis"))
print("post:1 태그 개수 (SCARD) ->", client:scard(post_a))

demo.section("SMOVE로 태그 하나를 보관함으로 옮기기")
client:smove(post_a, archive, "database")
print("post:1:tags ->", joined(client:smembers(post_a)))
print("tags:archive ->", joined(client:smembers(archive)))

demo.section("SPOP으로 무작위로 하나 뽑아서 제거")
local popped = client:spop(post_b)
print("뽑힌 태그 ->", popped, "/ 남은 post:2:tags ->", joined(client:smembers(post_b)))

print("\n정리:", client:del(post_a, post_b, archive, common_key), "개 키 삭제")
