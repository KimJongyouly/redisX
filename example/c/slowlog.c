// SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.
//
// 실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
// 포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
// 남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 CHANGE.md 1절 참고).
// 그래서 redisX에서 SLOWLOG를 쓰려면 slowlog-file-path를 실제 경로로
// 설정해야 하고(기본값은 ""로 비활성화), SLOWLOG GET/LEN은 매번 그 파일을
// 읽어서 답한다 — 메모리에 아무것도 들고 있지 않다. hiredis에는 SLOWLOG용
// 타입 헬퍼가 없으니 그냥 일반 커맨드로 보내고 배열 구조를 직접 훑는다.
//
// 실행:
//   make && ./bin/slowlog
#include "common.h"
#include <stdio.h>

static void dump_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        printf("(파일을 열 수 없음: %s)\n", path);
        return;
    }
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        fwrite(buf, 1, n, stdout);
    }
    fclose(f);
}

int main(void) {
    redisContext *c = redisx_connect();

    section("현재 설정 확인 (기본값은 비활성화 상태)");
    redisReply *r = cmd(c, "CONFIG GET slowlog-file-path");
    printf("slowlog-file-path -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "CONFIG GET slowlog-log-slower-than");
    printf("slowlog-log-slower-than (마이크로초) -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기");
    const char *log_path = "/tmp/redisx-slowlog-demo-c.log";
    remove(log_path);
    cmdf(c, "CONFIG SET slowlog-file-path %s", log_path);
    cmdf(c, "CONFIG SET slowlog-log-slower-than 0"); // 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

    section("몇 가지 커맨드를 실행해서 로그에 쌓기");
    cmdf(c, "SET k v");
    cmdf(c, "GET k");
    cmdf(c, "LPUSH mylist a b c");
    cmdf(c, "DEL mylist k");

    section("SLOWLOG LEN / GET으로 읽기");
    r = cmd(c, "SLOWLOG LEN");
    printf("SLOWLOG LEN -> %lld\n", r->integer);
    freeReplyObject(r);

    r = cmd(c, "SLOWLOG GET 5");
    for (size_t i = 0; i < r->elements; i++) {
        redisReply *entry = r->element[i];
        long long id = entry->element[0]->integer;
        long long duration_us = entry->element[2]->integer;
        redisReply *argv = entry->element[3];
        const char *peer = entry->element[4]->str;
        printf("  #%lld  %lldus  %s  ", id, duration_us, peer);
        for (size_t j = 0; j < argv->elements; j++) {
            if (j) printf(" ");
            printf("%s", argv->element[j]->str);
        }
        printf("\n");
    }
    freeReplyObject(r);

    section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)");
    dump_file(log_path);

    section("SLOWLOG RESET으로 파일 비우기");
    cmdf(c, "SLOWLOG RESET");
    // 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
    // 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
    r = cmd(c, "SLOWLOG LEN");
    printf("RESET 이후 SLOWLOG LEN -> %lld\n", r->integer);
    freeReplyObject(r);

    section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기");
    cmdf(c, "CONFIG SET slowlog-log-slower-than 10000"); // redisX 기본값
    cmdf(c, "CONFIG SET slowlog-file-path %s", "");       // 기본값(비활성화)으로 복원
    remove(log_path);
    printf("완료\n");

    redisFree(c);
    return 0;
}
