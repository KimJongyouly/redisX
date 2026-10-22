-- HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
--
-- 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
-- HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
-- 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
-- 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
-- example/python/hyperloglog.py와 같은 시나리오.
--
-- 실행: lua hyperloglog.lua
local demo = require "redisx_demo"
local client = demo.connect()

math.randomseed(0)

local hll_mon, hll_tue, hll_week = "visitors:mon:hll", "visitors:tue:hll", "visitors:week:hll"
local exact_set = "visitors:mon:exact"
client:del(hll_mon, hll_tue, hll_week, exact_set)

demo.section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기")
-- 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
-- 3,000명 정도의 유니크 방문자만 나오게 만든다.
for _ = 1, 10000 do
	local visitor_id = "user-" .. math.random(1, 3000)
	client:pfadd(hll_mon, visitor_id)
	client:sadd(exact_set, visitor_id)
end

local exact_count = client:scard(exact_set)
local approx_count = client:pfcount(hll_mon)
local error_pct = math.abs(approx_count - exact_count) / exact_count * 100
print(string.format("정확한 유니크 방문자 수 (SCARD)   -> %d", exact_count))
print(string.format("HyperLogLog 근사치 (PFCOUNT)      -> %d", approx_count))
print(string.format("오차율                             -> %.2f%% (표준 오차는 약 0.81%%)", error_pct))

demo.section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)")
print("Set 메모리 사용량 근사치 (MEMORY USAGE) ->", client:memory("USAGE", exact_set), "bytes")
print("HLL 메모리 사용량 근사치 (MEMORY USAGE) ->", client:memory("USAGE", hll_mon), "bytes")

demo.section("PFMERGE로 이틀치 방문자 로그 합치기")
for _ = 1, 5000 do
	client:pfadd(hll_tue, "user-" .. math.random(2000, 6000)) -- 월요일과 일부 겹침
end
client:pfmerge(hll_week, hll_mon, hll_tue)
print("월요일 근사 방문자 수 ->", client:pfcount(hll_mon))
print("화요일 근사 방문자 수 ->", client:pfcount(hll_tue))
print("합산(PFMERGE) 근사 방문자 수 ->", client:pfcount(hll_week), "(단순 합보다 적은 건 겹치는 방문자가 있어서)")

print("\n정리:", client:del(hll_mon, hll_tue, hll_week, exact_set), "개 키 삭제")
