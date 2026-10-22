// Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
//
// 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
// Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
// 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
// example/python/hash.py와 같은 시나리오.
//
// 실행:
//
//	go run ./hash
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

	userKey := "user:42"
	rdb.Del(ctx, userKey)

	section("HSET으로 여러 필드를 한 번에 채우기")
	rdb.HSet(ctx, userKey, "name", "jiyeon", "level", "5", "coins", "100", "logins", "0")
	fmt.Println("HGETALL user:42 ->", rdb.HGetAll(ctx, userKey).Val())

	section("HGET / HMGET으로 필드 일부만 읽기")
	fmt.Println("HGET user:42 name ->", rdb.HGet(ctx, userKey, "name").Val())
	fmt.Println("HMGET user:42 name level ->", rdb.HMGet(ctx, userKey, "name", "level").Val())

	section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가")
	rdb.HIncrBy(ctx, userKey, "coins", 50)
	rdb.HIncrBy(ctx, userKey, "logins", 1)
	fmt.Println("HGETALL user:42 ->", rdb.HGetAll(ctx, userKey).Val())

	section("HEXISTS / HDEL")
	fmt.Println("HEXISTS user:42 level ->", rdb.HExists(ctx, userKey, "level").Val())
	rdb.HDel(ctx, userKey, "logins")
	fmt.Println("HDEL 이후 HGETALL ->", rdb.HGetAll(ctx, userKey).Val())

	section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기")
	raffle := "raffle:entries"
	rdb.Del(ctx, raffle)
	rdb.HSet(ctx, raffle, "alice", "1", "bob", "1", "carol", "1", "dave", "1")
	winner := rdb.HRandField(ctx, raffle, 1).Val()
	fmt.Println("당첨자 한 명 ->", winner)
	fmt.Println("HRANDFIELD -3 (중복 허용 3명 뽑기) ->", rdb.HRandField(ctx, raffle, -3).Val())

	deleted, _ := rdb.Del(ctx, userKey, raffle).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}
