// HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
//
// 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
// HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
// 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
// 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
// example/python/hyperloglog.py와 같은 시나리오.
//
// 실행:
//
//	go run ./hyperloglog
package main

import (
	"context"
	"fmt"
	"math/rand"
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

	hllMon, hllTue, hllWeek := "visitors:mon:hll", "visitors:tue:hll", "visitors:week:hll"
	exactSet := "visitors:mon:exact"
	rdb.Del(ctx, hllMon, hllTue, hllWeek, exactSet)

	section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기")
	r := rand.New(rand.NewSource(0))
	// 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
	// 3,000명 정도의 유니크 방문자만 나오게 만든다.
	for i := 0; i < 10_000; i++ {
		visitorID := fmt.Sprintf("user-%d", r.Intn(3000)+1)
		rdb.PFAdd(ctx, hllMon, visitorID)
		rdb.SAdd(ctx, exactSet, visitorID)
	}

	exactCount := rdb.SCard(ctx, exactSet).Val()
	approxCount := rdb.PFCount(ctx, hllMon).Val()
	errorPct := float64(abs(approxCount-exactCount)) / float64(exactCount) * 100
	fmt.Printf("정확한 유니크 방문자 수 (SCARD)   -> %d\n", exactCount)
	fmt.Printf("HyperLogLog 근사치 (PFCOUNT)      -> %d\n", approxCount)
	fmt.Printf("오차율                             -> %.2f%% (표준 오차는 약 0.81%%)\n", errorPct)

	section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)")
	fmt.Println("Set 메모리 사용량 근사치 (MEMORY USAGE) ->", rdb.MemoryUsage(ctx, exactSet).Val(), "bytes")
	fmt.Println("HLL 메모리 사용량 근사치 (MEMORY USAGE) ->", rdb.MemoryUsage(ctx, hllMon).Val(), "bytes")

	section("PFMERGE로 이틀치 방문자 로그 합치기")
	for i := 0; i < 5_000; i++ {
		rdb.PFAdd(ctx, hllTue, fmt.Sprintf("user-%d", r.Intn(4000)+2000)) // 월요일과 일부 겹침
	}
	rdb.PFMerge(ctx, hllWeek, hllMon, hllTue)
	fmt.Println("월요일 근사 방문자 수 ->", rdb.PFCount(ctx, hllMon).Val())
	fmt.Println("화요일 근사 방문자 수 ->", rdb.PFCount(ctx, hllTue).Val())
	fmt.Println("합산(PFMERGE) 근사 방문자 수 ->", rdb.PFCount(ctx, hllWeek).Val(), "(단순 합보다 적은 건 겹치는 방문자가 있어서)")

	deleted, _ := rdb.Del(ctx, hllMon, hllTue, hllWeek, exactSet).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
