// Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
//
// 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
// 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
// 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
// example/python/set.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/set
#include "common.h"

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL post:1:tags post:2:tags tags:archive");

    section("SADD로 글마다 태그 달기");
    cmdf(c, "SADD post:1:tags rust redis database");
    cmdf(c, "SADD post:2:tags rust web async");
    redisReply *r = cmd(c, "SMEMBERS post:1:tags");
    printf("post:1:tags -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "SMEMBERS post:2:tags");
    printf("post:2:tags -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("SINTER / SUNION / SDIFF로 집합 연산");
    r = cmd(c, "SINTER post:1:tags post:2:tags");
    printf("둘 다 쓴 태그 (SINTER) -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "SUNION post:1:tags post:2:tags");
    printf("둘 중 하나라도 쓴 태그 (SUNION) -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "SDIFF post:1:tags post:2:tags");
    printf("post:1에만 있는 태그 (SDIFF) -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("SINTERSTORE로 교집합을 새 키에 저장");
    cmdf(c, "DEL tags:common");
    cmdf(c, "SINTERSTORE tags:common post:1:tags post:2:tags");
    r = cmd(c, "SMEMBERS tags:common");
    printf("tags:common -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("SISMEMBER / SCARD");
    r = cmd(c, "SISMEMBER post:1:tags redis");
    printf("post:1에 'redis' 태그가 있나? -> %s\n", r->integer ? "true" : "false");
    freeReplyObject(r);
    r = cmd(c, "SCARD post:1:tags");
    printf("post:1 태그 개수 (SCARD) -> %lld\n", r->integer);
    freeReplyObject(r);

    section("SMOVE로 태그 하나를 보관함으로 옮기기");
    cmdf(c, "SMOVE post:1:tags tags:archive database");
    r = cmd(c, "SMEMBERS post:1:tags");
    printf("post:1:tags -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "SMEMBERS tags:archive");
    printf("tags:archive -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("SPOP으로 무작위로 하나 뽑아서 제거");
    r = cmd(c, "SPOP post:2:tags");
    printf("뽑힌 태그 -> %s / ", r->str);
    freeReplyObject(r);
    r = cmd(c, "SMEMBERS post:2:tags");
    printf("남은 post:2:tags -> ");
    print_reply_line(r);
    freeReplyObject(r);

    r = cmd(c, "DEL post:1:tags post:2:tags tags:archive tags:common");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
