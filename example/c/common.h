// 8개 예제 파일이 공유하는 아주 작은 헬퍼 — 연결, 섹션 헤더 출력, 커맨드
// 실행 보일러플레이트(체크 후 반환 / fire-and-forget)만 담당한다.
#ifndef REDISX_EXAMPLE_COMMON_H
#define REDISX_EXAMPLE_COMMON_H

#include <hiredis/hiredis.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static inline const char *redisx_host(void) {
    const char *h = getenv("REDISX_HOST");
    return h ? h : "127.0.0.1";
}

static inline int redisx_port(void) {
    const char *p = getenv("REDISX_PORT");
    return p ? atoi(p) : 2417;
}

static inline redisContext *redisx_connect(void) {
    redisContext *c = redisConnect(redisx_host(), redisx_port());
    if (c == NULL || c->err) {
        fprintf(stderr, "connection error: %s\n", c ? c->errstr : "can't allocate redis context");
        exit(1);
    }
    return c;
}

static inline void section(const char *title) {
    printf("\n--- %s ---\n", title);
}

// 체크된 redisCommand — 커맨드 자체가 실패하거나 Redis 에러를 응답하면
// 바로 종료한다. 반환된 reply는 호출한 쪽에서 freeReplyObject()할 것.
static inline redisReply *cmd(redisContext *c, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    redisReply *r = redisvCommand(c, format, ap);
    va_end(ap);
    if (r == NULL) {
        fprintf(stderr, "command error: %s\n", c->errstr);
        exit(1);
    }
    if (r->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "redis error: %s\n", r->str);
        exit(1);
    }
    return r;
}

// 결과가 필요 없는 커맨드(SET/DEL/EXPIRE 등)용 — 실행하고 바로 해제한다.
static inline void cmdf(redisContext *c, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    redisReply *r = redisvCommand(c, format, ap);
    va_end(ap);
    if (r == NULL) {
        fprintf(stderr, "command error: %s\n", c->errstr);
        exit(1);
    }
    if (r->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "redis error: %s\n", r->str);
        exit(1);
    }
    freeReplyObject(r);
}

// SUBSCRIBE/PSUBSCRIBE 이후 서버가 그 연결로 비동기로 밀어주는 다음 프레임
// (구독 확인 응답 자체든, 실제 message/pmessage든) 하나를 읽는다 — `cmd`와
// 달리 **새 커맨드를 보내지 않는다**(hiredis의 블로킹 `redisGetReply`를 그냥
// 감싼 것, pub/sub 전용 예제에서만 쓴다).
static inline redisReply *next_push(redisContext *c) {
    void *reply = NULL;
    if (redisGetReply(c, &reply) != REDIS_OK || reply == NULL) {
        fprintf(stderr, "pubsub read error: %s\n", c->errstr);
        exit(1);
    }
    return (redisReply *)reply;
}

// 응답을 사람이 읽기 좋게 재귀적으로 출력 (배열은 [a, b, c] 형태).
static inline void print_reply(redisReply *r) {
    if (!r) {
        printf("(null)");
        return;
    }
    switch (r->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS:
            printf("%s", r->str);
            break;
        case REDIS_REPLY_INTEGER:
            printf("%lld", r->integer);
            break;
        case REDIS_REPLY_NIL:
            printf("(nil)");
            break;
        case REDIS_REPLY_DOUBLE:
            printf("%g", r->dval);
            break;
        case REDIS_REPLY_ARRAY:
            printf("[");
            for (size_t i = 0; i < r->elements; i++) {
                if (i) printf(", ");
                print_reply(r->element[i]);
            }
            printf("]");
            break;
        default:
            printf("(type=%d)", r->type);
    }
}

static inline void print_reply_line(redisReply *r) {
    print_reply(r);
    printf("\n");
}

#endif
