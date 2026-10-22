// Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
//
// 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
// 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
// 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
// example/python/set.py와 같은 시나리오.
//
// 실행:
//
//	go run ./set
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

	postA, postB, archive := "post:1:tags", "post:2:tags", "tags:archive"
	rdb.Del(ctx, postA, postB, archive)

	section("SADD로 글마다 태그 달기")
	rdb.SAdd(ctx, postA, "rust", "redis", "database")
	rdb.SAdd(ctx, postB, "rust", "web", "async")
	fmt.Println("post:1:tags ->", rdb.SMembers(ctx, postA).Val())
	fmt.Println("post:2:tags ->", rdb.SMembers(ctx, postB).Val())

	section("SINTER / SUNION / SDIFF로 집합 연산")
	fmt.Println("둘 다 쓴 태그 (SINTER) ->", rdb.SInter(ctx, postA, postB).Val())
	fmt.Println("둘 중 하나라도 쓴 태그 (SUNION) ->", rdb.SUnion(ctx, postA, postB).Val())
	fmt.Println("post:1에만 있는 태그 (SDIFF) ->", rdb.SDiff(ctx, postA, postB).Val())

	section("SINTERSTORE로 교집합을 새 키에 저장")
	commonKey := "tags:common"
	rdb.Del(ctx, commonKey)
	rdb.SInterStore(ctx, commonKey, postA, postB)
	fmt.Println("tags:common ->", rdb.SMembers(ctx, commonKey).Val())

	section("SISMEMBER / SCARD")
	fmt.Println("post:1에 'redis' 태그가 있나? ->", rdb.SIsMember(ctx, postA, "redis").Val())
	fmt.Println("post:1 태그 개수 (SCARD) ->", rdb.SCard(ctx, postA).Val())

	section("SMOVE로 태그 하나를 보관함으로 옮기기")
	rdb.SMove(ctx, postA, archive, "database")
	fmt.Println("post:1:tags ->", rdb.SMembers(ctx, postA).Val())
	fmt.Println("tags:archive ->", rdb.SMembers(ctx, archive).Val())

	section("SPOP으로 무작위로 하나 뽑아서 제거")
	popped := rdb.SPop(ctx, postB).Val()
	fmt.Println("뽑힌 태그 ->", popped, "/ 남은 post:2:tags ->", rdb.SMembers(ctx, postB).Val())

	deleted, _ := rdb.Del(ctx, postA, postB, archive, commonKey).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}
