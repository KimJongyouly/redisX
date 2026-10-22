// ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
//
// ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
// 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
// 합산하는 것까지 — example/python/zset.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/zset
#include "common.h"

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL leaderboard leaderboard:weekly leaderboard:combined");

    section("ZADD로 점수 매기기");
    cmdf(c, "ZADD leaderboard 120 alice 98 bob 150 carol 110 dave");
    redisReply *r = cmd(c, "ZREVRANGE leaderboard 0 -1 WITHSCORES");
    printf("전체 랭킹 (ZREVRANGE, 점수 포함) -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("ZSCORE / ZREVRANK로 한 명만 조회");
    r = cmd(c, "ZSCORE leaderboard carol");
    printf("carol 점수 (ZSCORE) -> %s\n", r->str);
    freeReplyObject(r);
    r = cmd(c, "ZREVRANK leaderboard carol");
    printf("carol 등수, 0부터 (ZREVRANK) -> %lld\n", r->integer);
    freeReplyObject(r);

    section("ZINCRBY로 점수 올리기 (보너스 획득)");
    cmdf(c, "ZINCRBY leaderboard 30 bob");
    r = cmd(c, "ZREVRANGE leaderboard 0 -1 WITHSCORES");
    printf("bob 보너스 이후 전체 랭킹 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기");
    r = cmd(c, "ZRANGEBYSCORE leaderboard 100 120 WITHSCORES");
    printf("100점 이상 120점 이하 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거");
    r = cmd(c, "ZPOPMAX leaderboard");
    printf("ZPOPMAX -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "ZPOPMIN leaderboard");
    printf("ZPOPMIN -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "ZREVRANGE leaderboard 0 -1 WITHSCORES");
    printf("남은 랭킹 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("ZUNIONSTORE로 이번 주 보너스 점수와 합산");
    cmdf(c, "ZADD leaderboard 120 alice 128 bob 110 dave"); // 방금 뽑아서 지워진 것 복원
    cmdf(c, "ZADD leaderboard:weekly 20 alice 5 bob 40 eve");
    cmdf(c, "ZUNIONSTORE leaderboard:combined 2 leaderboard leaderboard:weekly");
    r = cmd(c, "ZREVRANGE leaderboard:combined 0 -1 WITHSCORES");
    printf("합산 랭킹 (ZUNIONSTORE) -> ");
    print_reply_line(r);
    freeReplyObject(r);

    r = cmd(c, "DEL leaderboard leaderboard:weekly leaderboard:combined");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
