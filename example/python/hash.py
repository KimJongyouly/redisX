"""Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.

사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다.

실행:
    python 03_hash.py
"""

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


user_key = "user:42"
r.delete(user_key)

section("HSET으로 여러 필드를 한 번에 채우기")
r.hset(user_key, mapping={"name": "jiyeon", "level": "5", "coins": "100", "logins": "0"})
print("HGETALL user:42 ->", r.hgetall(user_key))

section("HGET / HMGET으로 필드 일부만 읽기")
print("HGET user:42 name ->", r.hget(user_key, "name"))
print("HMGET user:42 name level ->", r.hmget(user_key, "name", "level"))

section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가")
r.hincrby(user_key, "coins", 50)
r.hincrby(user_key, "logins", 1)
print("HGETALL user:42 ->", r.hgetall(user_key))

section("HEXISTS / HDEL")
print("HEXISTS user:42 level ->", bool(r.hexists(user_key, "level")))
r.hdel(user_key, "logins")
print("HDEL 이후 HGETALL ->", r.hgetall(user_key))

section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기")
r.delete("raffle:entries")
r.hset("raffle:entries", mapping={"alice": "1", "bob": "1", "carol": "1", "dave": "1"})
winner = r.hrandfield("raffle:entries")
print("당첨자 한 명 ->", winner)
print("HRANDFIELD -3 (중복 허용 3명 뽑기) ->", r.hrandfield("raffle:entries", -3))

print("\n정리:", r.delete(user_key, "raffle:entries"), "개 키 삭제")
