// List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
//
// LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
// 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
// 옮기는 흐름 — example/python/list.py와 같은 시나리오를 Go로
// 옮긴 것이다.
//
// 실행:
//
//	go run ./list
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

	queue, done := "tasks:todo", "tasks:done"
	rdb.Del(ctx, queue, done)

	section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)")
	rdb.RPush(ctx, queue, "reboot-db", "send-email", "resize-image")
	fmt.Println("LRANGE tasks:todo 0 -1 ->", rdb.LRange(ctx, queue, 0, -1).Val())

	section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기")
	rdb.LPush(ctx, queue, "URGENT:rotate-logs")
	fmt.Println("LRANGE tasks:todo 0 -1 ->", rdb.LRange(ctx, queue, 0, -1).Val())

	section("LINSERT로 특정 작업 바로 뒤에 끼워넣기")
	rdb.LInsert(ctx, queue, "AFTER", "send-email", "send-email-retry")
	fmt.Println("LRANGE tasks:todo 0 -1 ->", rdb.LRange(ctx, queue, 0, -1).Val())

	section("LPOS로 작업 위치 찾기, LREM으로 제거")
	pos, _ := rdb.LPos(ctx, queue, "send-email-retry", redis.LPosArgs{}).Result()
	fmt.Println("send-email-retry의 위치 ->", pos)
	rdb.LRem(ctx, queue, 1, "send-email-retry") // 결국 재시도는 취소하기로 함
	fmt.Println("LREM 이후 LRANGE ->", rdb.LRange(ctx, queue, 0, -1).Val())

	section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기")
	task := rdb.LPop(ctx, queue).Val()
	fmt.Println("처리할 작업 ->", task)
	rdb.RPush(ctx, done, task)
	rdb.LMove(ctx, queue, done, "LEFT", "RIGHT") // 다음 작업도 바로 완료 큐로 옮겨본다
	fmt.Println("tasks:todo ->", rdb.LRange(ctx, queue, 0, -1).Val())
	fmt.Println("tasks:done ->", rdb.LRange(ctx, done, 0, -1).Val())

	section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)")
	rdb.RPush(ctx, done, "old-task-1", "old-task-2", "old-task-3")
	rdb.LTrim(ctx, done, -2, -1)
	fmt.Println("LTRIM 이후 tasks:done ->", rdb.LRange(ctx, done, 0, -1).Val())

	deleted, _ := rdb.Del(ctx, queue, done).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}
