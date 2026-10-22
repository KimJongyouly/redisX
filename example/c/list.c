// List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
//
// LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
// 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
// 옮기는 흐름 — example/python/list.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/list
#include "common.h"

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL tasks:todo tasks:done");

    section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)");
    cmdf(c, "RPUSH tasks:todo reboot-db send-email resize-image");
    redisReply *r = cmd(c, "LRANGE tasks:todo 0 -1");
    printf("LRANGE tasks:todo 0 -1 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기");
    cmdf(c, "LPUSH tasks:todo URGENT:rotate-logs");
    r = cmd(c, "LRANGE tasks:todo 0 -1");
    printf("LRANGE tasks:todo 0 -1 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("LINSERT로 특정 작업 바로 뒤에 끼워넣기");
    cmdf(c, "LINSERT tasks:todo AFTER send-email send-email-retry");
    r = cmd(c, "LRANGE tasks:todo 0 -1");
    printf("LRANGE tasks:todo 0 -1 -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("LPOS로 작업 위치 찾기, LREM으로 제거");
    r = cmd(c, "LPOS tasks:todo send-email-retry");
    printf("send-email-retry의 위치 -> %lld\n", r->integer);
    freeReplyObject(r);
    cmdf(c, "LREM tasks:todo 1 send-email-retry"); // 결국 재시도는 취소하기로 함
    r = cmd(c, "LRANGE tasks:todo 0 -1");
    printf("LREM 이후 LRANGE -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기");
    r = cmd(c, "LPOP tasks:todo");
    printf("처리할 작업 -> %s\n", r->str);
    cmdf(c, "RPUSH tasks:done %s", r->str);
    freeReplyObject(r);
    cmdf(c, "LMOVE tasks:todo tasks:done LEFT RIGHT"); // 다음 작업도 바로 완료 큐로 옮겨본다
    r = cmd(c, "LRANGE tasks:todo 0 -1");
    printf("tasks:todo -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "LRANGE tasks:done 0 -1");
    printf("tasks:done -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)");
    cmdf(c, "RPUSH tasks:done old-task-1 old-task-2 old-task-3");
    cmdf(c, "LTRIM tasks:done -2 -1");
    r = cmd(c, "LRANGE tasks:done 0 -1");
    printf("LTRIM 이후 tasks:done -> ");
    print_reply_line(r);
    freeReplyObject(r);

    r = cmd(c, "DEL tasks:todo tasks:done");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
