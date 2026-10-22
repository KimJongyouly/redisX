// ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
//
// ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
// 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
// 합산하는 것까지 — example/python/zset.py와 같은 시나리오.
//
// 실행:
//
//	go run ./zset
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func section(title string) {
	fmt.Printf("\n--- %s ---\n", title)
}

func addr() string {
	if a := os.Getenv("REDISX_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:2417"
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()

	board, weekly, combined := "leaderboard", "leaderboard:weekly", "leaderboard:combined"
	rdb.Del(ctx, board, weekly, combined)

	section("ZADD로 점수 매기기")
	rdb.ZAdd(ctx, board,
		redis.Z{Score: 120, Member: "alice"},
		redis.Z{Score: 98, Member: "bob"},
		redis.Z{Score: 150, Member: "carol"},
		redis.Z{Score: 110, Member: "dave"},
	)
	fmt.Println("전체 랭킹 (ZREVRANGE, 점수 포함) ->", rdb.ZRevRangeWithScores(ctx, board, 0, -1).Val())

	section("ZSCORE / ZREVRANK로 한 명만 조회")
	fmt.Println("carol 점수 (ZSCORE) ->", rdb.ZScore(ctx, board, "carol").Val())
	fmt.Println("carol 등수, 0부터 (ZREVRANK) ->", rdb.ZRevRank(ctx, board, "carol").Val())

	section("ZINCRBY로 점수 올리기 (보너스 획득)")
	rdb.ZIncrBy(ctx, board, 30, "bob")
	fmt.Println("bob 보너스 이후 전체 랭킹 ->", rdb.ZRevRangeWithScores(ctx, board, 0, -1).Val())

	section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기")
	inRange := rdb.ZRangeByScoreWithScores(ctx, board, &redis.ZRangeBy{Min: "100", Max: "120"})
	fmt.Println("100점 이상 120점 이하 ->", inRange.Val())

	section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거")
	fmt.Println("ZPOPMAX ->", rdb.ZPopMax(ctx, board).Val())
	fmt.Println("ZPOPMIN ->", rdb.ZPopMin(ctx, board).Val())
	fmt.Println("남은 랭킹 ->", rdb.ZRevRangeWithScores(ctx, board, 0, -1).Val())

	section("ZUNIONSTORE로 이번 주 보너스 점수와 합산")
	rdb.ZAdd(ctx, board, // 방금 뽑아서 지워진 것 복원
		redis.Z{Score: 120, Member: "alice"},
		redis.Z{Score: 128, Member: "bob"},
		redis.Z{Score: 110, Member: "dave"},
	)
	rdb.ZAdd(ctx, weekly,
		redis.Z{Score: 20, Member: "alice"},
		redis.Z{Score: 5, Member: "bob"},
		redis.Z{Score: 40, Member: "eve"},
	)
	rdb.ZUnionStore(ctx, combined, &redis.ZStore{Keys: []string{board, weekly}})
	fmt.Println("합산 랭킹 (ZUNIONSTORE) ->", rdb.ZRevRangeWithScores(ctx, combined, 0, -1).Val())

	deleted, _ := rdb.Del(ctx, board, weekly, combined).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}
