// Pub/Sub(발행/구독) 둘러보기.
//
// SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
// 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
// 지우고 시작할 "키"가 없다. hiredis는 구독 전용 API가 따로 없다 — 평범한
// `redisCommand`로 SUBSCRIBE를 보내고 나면 그 연결은 구독 모드가 되고,
// 그 뒤로는 `redisCommand`(새 커맨드 전송) 대신 `next_push`(common.h,
// `redisGetReply`를 감싼 것)로 서버가 밀어주는 프레임을 하나씩 읽는다.
// 그래서 발행은 별도 연결을 하나 더 연다.
//
// 실행:
//   make && ./bin/pubsub
#include "common.h"
#include <unistd.h>

static const char *CHANNEL = "notifications";
static const char *PATTERN = "notif.*";
static const char *PATTERN_CHANNEL = "notif.sports";

int main(void) {
    redisContext *sub = redisx_connect();
    redisContext *pub = redisx_connect();

    section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)");
    redisReply *r = cmd(sub, "SUBSCRIBE %s", CHANNEL);
    printf("구독 확인 -> %s %s\n", r->element[0]->str, r->element[1]->str);
    freeReplyObject(r);
    r = cmd(sub, "PSUBSCRIBE %s", PATTERN);
    printf("패턴 구독 확인 -> %s %s\n", r->element[0]->str, r->element[1]->str);
    freeReplyObject(r);

    section("PUBLISH (발행자는 별도 연결)");
    r = cmd(pub, "PUBLISH %s %s", CHANNEL, "서버 점검이 10분 후 시작됩니다");
    printf("PUBLISH %s -> 받은 구독자 수: %lld\n", CHANNEL, r->integer);
    freeReplyObject(r);
    r = next_push(sub); // ["message", channel, payload]
    printf("구독자가 받은 메시지 -> %s : %s\n", r->element[1]->str, r->element[2]->str);
    freeReplyObject(r);

    section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)");
    r = cmd(pub, "PUBLISH %s %s", PATTERN_CHANNEL, "손흥민 결승골!");
    printf("PUBLISH %s -> 받은 구독자 수: %lld\n", PATTERN_CHANNEL, r->integer);
    freeReplyObject(r);
    r = next_push(sub); // ["pmessage", pattern, channel, payload]
    printf("패턴으로 받은 메시지 -> pattern=%s channel=%s : %s\n", r->element[1]->str, r->element[2]->str, r->element[3]->str);
    freeReplyObject(r);

    section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)");
    r = cmd(pub, "PUBSUB CHANNELS *");
    printf("PUBSUB CHANNELS -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(pub, "PUBSUB NUMSUB %s", CHANNEL);
    printf("PUBSUB NUMSUB -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(pub, "PUBSUB NUMPAT");
    printf("PUBSUB NUMPAT -> %lld\n", r->integer);
    freeReplyObject(r);

    section("UNSUBSCRIBE / PUNSUBSCRIBE");
    r = cmd(sub, "UNSUBSCRIBE %s", CHANNEL);
    freeReplyObject(r);
    r = cmd(sub, "PUNSUBSCRIBE %s", PATTERN);
    freeReplyObject(r);
    usleep(200 * 1000); // 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
    r = cmd(pub, "PUBLISH %s %s", CHANNEL, "이제 아무도 안 듣는다");
    printf("구독 해제 후 PUBLISH -> 받은 구독자 수: %lld (0이어야 정상)\n", r->integer);
    freeReplyObject(r);

    redisFree(sub);
    redisFree(pub);
    printf("\n정리 완료\n");
    return 0;
}
