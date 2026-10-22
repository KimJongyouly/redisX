"""HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.

유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.

실행:
    python 06_hyperloglog.py
"""

import random

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


hll_mon, hll_tue, hll_week = "visitors:mon:hll", "visitors:tue:hll", "visitors:week:hll"
exact_set = "visitors:mon:exact"
r.delete(hll_mon, hll_tue, hll_week, exact_set)

section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기")
random.seed(0)
# 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
# 3,000명 정도의 유니크 방문자만 나오게 만든다.
for _ in range(10_000):
    visitor_id = f"user-{random.randint(1, 3000)}"
    r.pfadd(hll_mon, visitor_id)
    r.sadd(exact_set, visitor_id)

exact_count = r.scard(exact_set)
approx_count = r.pfcount(hll_mon)
error_pct = abs(approx_count - exact_count) / exact_count * 100
print(f"정확한 유니크 방문자 수 (SCARD)   -> {exact_count}")
print(f"HyperLogLog 근사치 (PFCOUNT)      -> {approx_count}")
print(f"오차율                             -> {error_pct:.2f}% (표준 오차는 약 0.81%)")

section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)")
print("Set 메모리 사용량 근사치 (MEMORY USAGE) ->", r.memory_usage(exact_set), "bytes")
print("HLL 메모리 사용량 근사치 (MEMORY USAGE) ->", r.memory_usage(hll_mon), "bytes")

section("PFMERGE로 이틀치 방문자 로그 합치기")
for _ in range(5_000):
    r.pfadd(hll_tue, f"user-{random.randint(2000, 6000)}")  # 월요일과 일부 겹침
r.pfmerge(hll_week, hll_mon, hll_tue)
print("월요일 근사 방문자 수 ->", r.pfcount(hll_mon))
print("화요일 근사 방문자 수 ->", r.pfcount(hll_tue))
print("합산(PFMERGE) 근사 방문자 수 ->", r.pfcount(hll_week), "(단순 합보다 적은 건 겹치는 방문자가 있어서)")

print("\n정리:", r.delete(hll_mon, hll_tue, hll_week, exact_set), "개 키 삭제")
