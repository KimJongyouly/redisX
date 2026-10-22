// Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
//
// 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
// Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
// 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
// example/python/hash.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/hash
#include "common.h"

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL user:42");

    section("HSET으로 여러 필드를 한 번에 채우기");
    cmdf(c, "HSET user:42 name jiyeon level 5 coins 100 logins 0");
    redisReply *r = cmd(c, "HGETALL user:42");
    printf("HGETALL user:42 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("HGET / HMGET으로 필드 일부만 읽기");
    r = cmd(c, "HGET user:42 name");
    printf("HGET user:42 name -> %s\n", r->str);
    freeReplyObject(r);
    r = cmd(c, "HMGET user:42 name level");
    printf("HMGET user:42 name level -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가");
    cmdf(c, "HINCRBY user:42 coins 50");
    cmdf(c, "HINCRBY user:42 logins 1");
    r = cmd(c, "HGETALL user:42");
    printf("HGETALL user:42 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("HEXISTS / HDEL");
    r = cmd(c, "HEXISTS user:42 level");
    printf("HEXISTS user:42 level -> %s\n", r->integer ? "true" : "false");
    freeReplyObject(r);
    cmdf(c, "HDEL user:42 logins");
    r = cmd(c, "HGETALL user:42");
    printf("HDEL 이후 HGETALL -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기");
    cmdf(c, "DEL raffle:entries");
    cmdf(c, "HSET raffle:entries alice 1 bob 1 carol 1 dave 1");
    r = cmd(c, "HRANDFIELD raffle:entries");
    printf("당첨자 한 명 -> %s\n", r->str);
    freeReplyObject(r);
    r = cmd(c, "HRANDFIELD raffle:entries -3");
    printf("HRANDFIELD -3 (중복 허용 3명 뽑기) -> ");
    print_reply_line(r);
    freeReplyObject(r);

    r = cmd(c, "DEL user:42 raffle:entries");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
