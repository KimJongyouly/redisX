// HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
//
// 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
// HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
// 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
// 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
// example/python/hyperloglog.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/hyperloglog
#include "common.h"
#include <stdlib.h>
#include <math.h>

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL visitors:mon:hll visitors:tue:hll visitors:week:hll visitors:mon:exact");

    section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기");
    srand(0);
    // 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
    // 3,000명 정도의 유니크 방문자만 나오게 만든다.
    for (int i = 0; i < 10000; i++) {
        char visitor_id[32];
        snprintf(visitor_id, sizeof(visitor_id), "user-%d", (rand() % 3000) + 1);
        cmdf(c, "PFADD visitors:mon:hll %s", visitor_id);
        cmdf(c, "SADD visitors:mon:exact %s", visitor_id);
    }

    redisReply *r = cmd(c, "SCARD visitors:mon:exact");
    long long exact_count = r->integer;
    freeReplyObject(r);
    r = cmd(c, "PFCOUNT visitors:mon:hll");
    long long approx_count = r->integer;
    freeReplyObject(r);
    double error_pct = fabs((double)(approx_count - exact_count)) / (double)exact_count * 100.0;
    printf("정확한 유니크 방문자 수 (SCARD)   -> %lld\n", exact_count);
    printf("HyperLogLog 근사치 (PFCOUNT)      -> %lld\n", approx_count);
    printf("오차율                             -> %.2f%% (표준 오차는 약 0.81%%)\n", error_pct);

    section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)");
    r = cmd(c, "MEMORY USAGE visitors:mon:exact");
    printf("Set 메모리 사용량 근사치 (MEMORY USAGE) -> %lld bytes\n", r->integer);
    freeReplyObject(r);
    r = cmd(c, "MEMORY USAGE visitors:mon:hll");
    printf("HLL 메모리 사용량 근사치 (MEMORY USAGE) -> %lld bytes\n", r->integer);
    freeReplyObject(r);

    section("PFMERGE로 이틀치 방문자 로그 합치기");
    for (int i = 0; i < 5000; i++) {
        char visitor_id[32];
        snprintf(visitor_id, sizeof(visitor_id), "user-%d", (rand() % 4000) + 2000); // 월요일과 일부 겹침
        cmdf(c, "PFADD visitors:tue:hll %s", visitor_id);
    }
    cmdf(c, "PFMERGE visitors:week:hll visitors:mon:hll visitors:tue:hll");
    r = cmd(c, "PFCOUNT visitors:mon:hll");
    printf("월요일 근사 방문자 수 -> %lld\n", r->integer);
    freeReplyObject(r);
    r = cmd(c, "PFCOUNT visitors:tue:hll");
    printf("화요일 근사 방문자 수 -> %lld\n", r->integer);
    freeReplyObject(r);
    r = cmd(c, "PFCOUNT visitors:week:hll");
    printf("합산(PFMERGE) 근사 방문자 수 -> %lld (단순 합보다 적은 건 겹치는 방문자가 있어서)\n", r->integer);
    freeReplyObject(r);

    r = cmd(c, "DEL visitors:mon:hll visitors:tue:hll visitors:week:hll visitors:mon:exact");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
