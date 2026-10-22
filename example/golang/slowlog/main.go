// SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.
//
// 실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
// 포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
// 남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 CHANGE.md 1절 참고).
// 그래서 redisX에서 SLOWLOG를 쓰려면 slowlog-file-path를 실제 경로로
// 설정해야 하고(기본값은 ""로 비활성화), SLOWLOG GET/LEN은 매번 그 파일을
// 읽어서 답한다 — 메모리에 아무것도 들고 있지 않다. go-redis의
// SlowLogGet/Len/Reset은 real Redis의 6필드 응답 포맷(id, 시각, 소요시간,
// 커맨드, 클라이언트 주소, 클라이언트 이름)을 그대로 파싱하므로 redisX의
// 응답과도 그대로 맞는다.
//
// 실행:
//
//	go run ./slowlog
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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

	section("현재 설정 확인 (기본값은 비활성화 상태)")
	pathCfg, _ := rdb.ConfigGet(ctx, "slowlog-file-path").Result()
	fmt.Println("slowlog-file-path ->", pathCfg)
	thresholdCfg, _ := rdb.ConfigGet(ctx, "slowlog-log-slower-than").Result()
	fmt.Println("slowlog-log-slower-than (마이크로초) ->", thresholdCfg)

	section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기")
	logPath := filepath.Join(os.TempDir(), "redisx-slowlog-demo-go.log")
	os.Remove(logPath)
	rdb.ConfigSet(ctx, "slowlog-file-path", logPath)
	rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0") // 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

	section("몇 가지 커맨드를 실행해서 로그에 쌓기")
	rdb.Set(ctx, "k", "v", 0)
	rdb.Get(ctx, "k")
	rdb.LPush(ctx, "mylist", "a", "b", "c")
	rdb.Del(ctx, "mylist", "k")

	section("SLOWLOG LEN / GET으로 읽기")
	fmt.Println("SLOWLOG LEN ->", rdb.SlowLogLen(ctx).Val())
	entries, _ := rdb.SlowLogGet(ctx, 5).Result()
	for _, e := range entries {
		fmt.Printf("  #%d  %v  %s  %v\n", e.ID, e.Duration, e.ClientAddr, e.Args)
	}

	section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)")
	content, _ := os.ReadFile(logPath)
	fmt.Print(string(content))

	section("SLOWLOG RESET으로 파일 비우기")
	rdb.SlowLogReset(ctx)
	// 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
	// 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
	fmt.Println("RESET 이후 SLOWLOG LEN ->", rdb.SlowLogLen(ctx).Val())

	section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기")
	rdb.ConfigSet(ctx, "slowlog-log-slower-than", "10000") // redisX 기본값
	rdb.ConfigSet(ctx, "slowlog-file-path", "")            // 기본값(비활성화)으로 복원
	os.Remove(logPath)
	fmt.Println("완료")
}
