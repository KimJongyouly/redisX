"""String(키/값) 커맨드 둘러보기.

기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
만료(TTL) 관련 커맨드까지 — Redis를 처음 접할 때 가장 먼저 보는 타입이다.

실행:
    python 01_string.py
"""

import time

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


# 데모가 매번 같은 상태에서 시작하도록 사용하는 키를 미리 지운다.
demo_keys = ["greeting", "page:views", "user:1:session", "k1", "k2", "k3"]
r.delete(*demo_keys)

section("SET / GET")
r.set("greeting", "hello world")
print("GET greeting ->", r.get("greeting"))

section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)")
r.append("greeting", "!")
print("APPEND 이후:", r.get("greeting"))
print("GETRANGE greeting 0 4 ->", r.getrange("greeting", 0, 4))
r.setrange("greeting", 6, "redisX")  # "world"(6글자) 자리를 "redisX"로 덮어씀
print("SETRANGE 이후:", r.get("greeting"))

section("MSET / MGET (여러 키 한 번에)")
r.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
print("MGET k1 k2 k3 ->", r.mget("k1", "k2", "k3"))

section("INCR / INCRBY (카운터)")
r.delete("page:views")
for _ in range(3):
    r.incr("page:views")
r.incrby("page:views", 10)
print("page:views ->", r.get("page:views"))

section("EXPIRE / TTL / PERSIST")
r.set("user:1:session", "token-abc")
r.expire("user:1:session", 2)
print("설정 직후 TTL ->", r.ttl("user:1:session"), "초")
time.sleep(2.2)
print("2.2초 후 EXISTS ->", bool(r.exists("user:1:session")), "(TTL이 지나 자동 삭제됨)")

r.set("user:1:session", "token-def", ex=60)
r.persist("user:1:session")
print("PERSIST 이후 TTL ->", r.ttl("user:1:session"), "(-1 = 만료 없음)")

print("\n정리:", r.delete(*demo_keys), "개 키 삭제")
