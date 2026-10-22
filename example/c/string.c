// String(키/값) 커맨드 둘러보기.
//
// 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
// 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
// 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py와 같은
// 시나리오를 hiredis로 옮긴 것이다.
//
// 실행:
//   make && ./bin/string
#include "common.h"
#include <unistd.h>

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL greeting page:views user:1:session k1 k2 k3");

    section("SET / GET");
    cmdf(c, "SET greeting %s", "hello world");
    redisReply *r = cmd(c, "GET greeting");
    printf("GET greeting -> %s\n", r->str);
    freeReplyObject(r);

    section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)");
    cmdf(c, "APPEND greeting !");
    r = cmd(c, "GET greeting");
    printf("APPEND 이후: %s\n", r->str);
    freeReplyObject(r);

    r = cmd(c, "GETRANGE greeting 0 4");
    printf("GETRANGE greeting 0 4 -> %s\n", r->str);
    freeReplyObject(r);

    cmdf(c, "SETRANGE greeting 6 redisX"); // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
    r = cmd(c, "GET greeting");
    printf("SETRANGE 이후: %s\n", r->str);
    freeReplyObject(r);

    section("MSET / MGET (여러 키 한 번에)");
    cmdf(c, "MSET k1 v1 k2 v2 k3 v3");
    r = cmd(c, "MGET k1 k2 k3");
    printf("MGET k1 k2 k3 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("INCR / INCRBY (카운터)");
    cmdf(c, "DEL page:views");
    for (int i = 0; i < 3; i++) cmdf(c, "INCR page:views");
    cmdf(c, "INCRBY page:views 10");
    r = cmd(c, "GET page:views");
    printf("page:views -> %s\n", r->str);
    freeReplyObject(r);

    section("EXPIRE / TTL / PERSIST");
    cmdf(c, "SET user:1:session token-abc");
    cmdf(c, "EXPIRE user:1:session 2");
    r = cmd(c, "TTL user:1:session");
    printf("설정 직후 TTL -> %lld 초\n", r->integer);
    freeReplyObject(r);

    usleep(2200 * 1000); // 2.2초
    r = cmd(c, "EXISTS user:1:session");
    printf("2.2초 후 EXISTS -> %s (TTL이 지나 자동 삭제됨)\n", r->integer ? "true" : "false");
    freeReplyObject(r);

    cmdf(c, "SET user:1:session token-def EX 60");
    cmdf(c, "PERSIST user:1:session");
    r = cmd(c, "TTL user:1:session");
    printf("PERSIST 이후 TTL -> %lld (-1 = 만료 없음)\n", r->integer);
    freeReplyObject(r);

    r = cmd(c, "DEL greeting page:views user:1:session k1 k2 k3");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
