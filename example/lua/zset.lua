-- ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
--
-- ZADD로 점수를 매기고, ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를 올리고,
-- ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로 합산하는
-- 것까지 — example/python/zset.py와 같은 시나리오.
--
-- redis-lua의 zadd는 기본 멀티벌크 그대로라 실제 프로토콜과 동일하게
-- `score member score member ...` 순서로 넘긴다. ZPOPMAX/ZPOPMIN은
-- redis-lua 2.0.4 시절엔 없던 커맨드라 redisx_demo에서 define_command로
-- 추가 등록해 둔 것을 그대로 쓴다.
--
-- 실행: lua zset.lua
local demo = require "redisx_demo"
local client = demo.connect()

local function print_ranks(rows)
	local parts = {}
	for _, pair in ipairs(rows) do
		table.insert(parts, pair[1] .. "=" .. pair[2])
	end
	print(table.concat(parts, ", "))
end

local board, weekly, combined = "leaderboard", "leaderboard:weekly", "leaderboard:combined"
client:del(board, weekly, combined)

demo.section("ZADD로 점수 매기기")
client:zadd(board, 120, "alice", 98, "bob", 150, "carol", 110, "dave")
io.write("전체 랭킹 (ZREVRANGE, 점수 포함) -> ")
print_ranks(client:zrevrange(board, 0, -1, { withscores = true }))

demo.section("ZSCORE / ZREVRANK로 한 명만 조회")
print("carol 점수 (ZSCORE) ->", client:zscore(board, "carol"))
print("carol 등수, 0부터 (ZREVRANK) ->", client:zrevrank(board, "carol"))

demo.section("ZINCRBY로 점수 올리기 (보너스 획득)")
client:zincrby(board, 30, "bob")
io.write("bob 보너스 이후 전체 랭킹 -> ")
print_ranks(client:zrevrange(board, 0, -1, { withscores = true }))

demo.section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기")
io.write("100점 이상 120점 이하 -> ")
print_ranks(client:zrangebyscore(board, 100, 120, { withscores = true }))

demo.section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거")
print("ZPOPMAX ->", table.concat(client:zpopmax(board), " "))
print("ZPOPMIN ->", table.concat(client:zpopmin(board), " "))
io.write("남은 랭킹 -> ")
print_ranks(client:zrevrange(board, 0, -1, { withscores = true }))

demo.section("ZUNIONSTORE로 이번 주 보너스 점수와 합산")
client:zadd(board, 120, "alice", 128, "bob", 110, "dave") -- 방금 뽑아서 지워진 것 복원
client:zadd(weekly, 20, "alice", 5, "bob", 40, "eve")
client:zunionstore(combined, 2, board, weekly)
io.write("합산 랭킹 (ZUNIONSTORE) -> ")
print_ranks(client:zrevrange(combined, 0, -1, { withscores = true }))

print("\n정리:", client:del(board, weekly, combined), "개 키 삭제")
