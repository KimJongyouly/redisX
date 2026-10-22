// Pub/Sub(발행/구독) 둘러보기.
//
// SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
// 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
// 지우고 시작할 "키"가 없다. go-redis는 구독을 위해 *redis.PubSub(내부적으로
// 전용 연결을 하나 빌려 씀)을 돌려주므로, 발행은 원래 쓰던 클라이언트로 그냥
// 하면 된다.
//
// 실행:
//
//	go run ./pubsub
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

const (
	channel        = "notifications"
	pattern        = "notif.*"
	patternChannel = "notif.sports"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()

	section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)")
	sub := rdb.Subscribe(ctx, channel)
	defer sub.Close()
	if err := sub.PSubscribe(ctx, pattern); err != nil {
		panic(err)
	}
	// 구독 확인 메시지(*redis.Subscription) 두 개를 먼저 받아 넘긴다.
	confirm1, _ := sub.Receive(ctx)
	fmt.Println("구독 확인 ->", confirm1)
	confirm2, _ := sub.Receive(ctx)
	fmt.Println("패턴 구독 확인 ->", confirm2)

	section("PUBLISH (발행은 원래 클라이언트로)")
	n, _ := rdb.Publish(ctx, channel, "서버 점검이 10분 후 시작됩니다").Result()
	fmt.Printf("PUBLISH %s -> 받은 구독자 수: %d\n", channel, n)
	msg, _ := sub.ReceiveMessage(ctx)
	fmt.Println("구독자가 받은 메시지 ->", msg.Channel, ":", msg.Payload)

	section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)")
	n, _ = rdb.Publish(ctx, patternChannel, "손흥민 결승골!").Result()
	fmt.Printf("PUBLISH %s -> 받은 구독자 수: %d\n", patternChannel, n)
	msg, _ = sub.ReceiveMessage(ctx)
	fmt.Println("패턴으로 받은 메시지 ->", "pattern="+msg.Pattern, "channel="+msg.Channel, ":", msg.Payload)

	section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)")
	channels, _ := rdb.PubSubChannels(ctx, "*").Result()
	fmt.Println("PUBSUB CHANNELS ->", channels)
	numSub, _ := rdb.PubSubNumSub(ctx, channel).Result()
	fmt.Println("PUBSUB NUMSUB ->", numSub)
	numPat, _ := rdb.PubSubNumPat(ctx).Result()
	fmt.Println("PUBSUB NUMPAT ->", numPat)

	section("UNSUBSCRIBE / PUNSUBSCRIBE")
	sub.Unsubscribe(ctx, channel)
	sub.PUnsubscribe(ctx, pattern)
	time.Sleep(200 * time.Millisecond) // 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
	n, _ = rdb.Publish(ctx, channel, "이제 아무도 안 듣는다").Result()
	fmt.Printf("구독 해제 후 PUBLISH -> 받은 구독자 수: %d (0이어야 정상)\n", n)

	fmt.Println("\n정리 완료")
}
