// String(키/값) 커맨드 둘러보기.
//
// 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
// 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
// 만료(TTL) 관련 커맨드까지 — example/python의 같은 이름 예제와 동일한
// 시나리오를 Go(go-redis)로 옮긴 것이다.
//
// 실행:
//
//	go run ./string
package main

import (
	"context"
	"fmt"
	"os"
	"time"

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

	demoKeys := []string{"greeting", "page:views", "user:1:session", "k1", "k2", "k3"}
	rdb.Del(ctx, demoKeys...)

	section("SET / GET")
	rdb.Set(ctx, "greeting", "hello world", 0)
	fmt.Println("GET greeting ->", mustStr(rdb.Get(ctx, "greeting")))

	section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)")
	rdb.Append(ctx, "greeting", "!")
	fmt.Println("APPEND 이후:", mustStr(rdb.Get(ctx, "greeting")))
	fmt.Println("GETRANGE greeting 0 4 ->", mustStr(rdb.GetRange(ctx, "greeting", 0, 4)))
	rdb.SetRange(ctx, "greeting", 6, "redisX") // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
	fmt.Println("SETRANGE 이후:", mustStr(rdb.Get(ctx, "greeting")))

	section("MSET / MGET (여러 키 한 번에)")
	rdb.MSet(ctx, "k1", "v1", "k2", "v2", "k3", "v3")
	fmt.Println("MGET k1 k2 k3 ->", mustSlice(rdb.MGet(ctx, "k1", "k2", "k3")))

	section("INCR / INCRBY (카운터)")
	rdb.Del(ctx, "page:views")
	for i := 0; i < 3; i++ {
		rdb.Incr(ctx, "page:views")
	}
	rdb.IncrBy(ctx, "page:views", 10)
	fmt.Println("page:views ->", mustStr(rdb.Get(ctx, "page:views")))

	section("EXPIRE / TTL / PERSIST")
	rdb.Set(ctx, "user:1:session", "token-abc", 0)
	rdb.Expire(ctx, "user:1:session", 2*time.Second)
	fmt.Println("설정 직후 TTL ->", formatTTL(mustDur(rdb.TTL(ctx, "user:1:session"))))
	time.Sleep(2200 * time.Millisecond)
	n, _ := rdb.Exists(ctx, "user:1:session").Result()
	fmt.Println("2.2초 후 EXISTS ->", n == 1, "(TTL이 지나 자동 삭제됨)")

	rdb.Set(ctx, "user:1:session", "token-def", 60*time.Second)
	rdb.Persist(ctx, "user:1:session")
	fmt.Println("PERSIST 이후 TTL ->", formatTTL(mustDur(rdb.TTL(ctx, "user:1:session"))))

	deleted, _ := rdb.Del(ctx, demoKeys...).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}

func mustStr(cmd *redis.StringCmd) string {
	v, err := cmd.Result()
	if err != nil && err != redis.Nil {
		panic(err)
	}
	return v
}

func mustSlice(cmd *redis.SliceCmd) []interface{} {
	v, err := cmd.Result()
	if err != nil {
		panic(err)
	}
	return v
}

func mustDur(cmd *redis.DurationCmd) time.Duration {
	v, err := cmd.Result()
	if err != nil {
		panic(err)
	}
	return v
}

// go-redis의 TTL()은 -1("만료 없음")/-2("키 없음") 같은 특수값을 초 단위로
// 환산하지 않고 그대로 Duration(나노초)으로 돌려준다 — 그대로 출력하면
// "-1ns"처럼 보여서, 사람이 읽기 좋은 형태로 바꿔준다.
func formatTTL(d time.Duration) string {
	switch d {
	case -1:
		return "-1 (만료 없음)"
	case -2:
		return "-2 (키 없음)"
	default:
		return d.String()
	}
}
