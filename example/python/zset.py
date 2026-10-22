"""ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.

ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
합산하는 것까지 — 게임 리더보드에서 흔히 하는 작업들이다.

실행:
    python 05_zset.py
"""

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


board, weekly, combined = "leaderboard", "leaderboard:weekly", "leaderboard:combined"
r.delete(board, weekly, combined)

section("ZADD로 점수 매기기")
r.zadd(board, {"alice": 120, "bob": 98, "carol": 150, "dave": 110})
print("전체 랭킹 (ZREVRANGE, 점수 포함) ->", r.zrevrange(board, 0, -1, withscores=True))

section("ZSCORE / ZRANK / ZREVRANK로 한 명만 조회")
print("carol 점수 (ZSCORE) ->", r.zscore(board, "carol"))
print("carol 등수, 0부터 (ZREVRANK) ->", r.zrevrank(board, "carol"))

section("ZINCRBY로 점수 올리기 (보너스 획득)")
r.zincrby(board, 30, "bob")
print("bob 보너스 이후 전체 랭킹 ->", r.zrevrange(board, 0, -1, withscores=True))

section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기")
print("100점 이상 120점 이하 ->", r.zrangebyscore(board, 100, 120, withscores=True))

section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거")
print("ZPOPMAX ->", r.zpopmax(board))
print("ZPOPMIN ->", r.zpopmin(board))
print("남은 랭킹 ->", r.zrevrange(board, 0, -1, withscores=True))

section("ZUNIONSTORE로 이번 주 보너스 점수와 합산")
r.zadd(board, {"alice": 120, "bob": 128, "dave": 110})  # 방금 뽑아서 지워진 것 복원
r.zadd(weekly, {"alice": 20, "bob": 5, "eve": 40})
r.zunionstore(combined, [board, weekly])
print("합산 랭킹 (ZUNIONSTORE) ->", r.zrevrange(combined, 0, -1, withscores=True))

print("\n정리:", r.delete(board, weekly, combined), "개 키 삭제")
