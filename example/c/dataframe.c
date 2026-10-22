// DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
//
// 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
// 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
// 있다. C에는 polars 같은 네이티브 DataFrame 라이브러리가 없으므로, 여기서는
// DF.QUERY의 RESP 행 배열을 직접 훑고 DF.PRETTY로 보기 좋은 표를 받는다 —
// example/python/dataframe_csv.py, example/golang/dataframe/main.go와
// 동일한 시나리오(작은 재고 CSV).
//
// 실행:
//   make && ./bin/dataframe
#include "common.h"
#include <string.h>

static const char *CSV_TEXT =
    "name,category,price,qty\n"
    "widget,tools,9.99,100\n"
    "gadget,electronics,29.99,50\n"
    "gizmo,electronics,19.99,75\n"
    "bolt,tools,0.99,500\n"
    "screw,tools,0.49,1000\n";

int main(void) {
    redisContext *c = redisx_connect();

    cmdf(c, "DEL inventory");

    section("DF.FROMCSV로 인라인 CSV 적재");
    redisReply *r = redisCommand(c, "DF.FROMCSV inventory %s", CSV_TEXT);
    if (r == NULL) {
        fprintf(stderr, "command error: %s\n", c->errstr);
        redisFree(c);
        return 1;
    }
    if (r->type == REDIS_REPLY_ERROR) {
        if (strcasestr(r->str, "unknown command") != NULL) {
            fprintf(stderr,
                    "DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.\n"
                    "cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.\n");
            freeReplyObject(r);
            redisFree(c);
            return 1;
        }
        fprintf(stderr, "redis error: %s\n", r->str);
        freeReplyObject(r);
        redisFree(c);
        return 1;
    }
    freeReplyObject(r);

    section("DF.SHAPE / DF.COLUMNS로 구조 확인");
    r = cmd(c, "DF.SHAPE inventory");
    printf("DF.SHAPE inventory -> ");
    print_reply_line(r);
    freeReplyObject(r);
    r = cmd(c, "DF.COLUMNS inventory");
    printf("DF.COLUMNS inventory -> ");
    print_reply_line(r);
    freeReplyObject(r);

    section("DF.QUERY로 카테고리별 재고 가치 집계");
    r = cmd(c, "DF.QUERY inventory %s",
            "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category");
    for (size_t i = 0; i < r->elements; i++) {
        redisReply *row = r->element[i];
        // RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string으로
        // 온다(이 포크의 Reply::Double 인코딩 규칙) — REDIS_REPLY_STRING으로
        // 취급한다.
        printf("  %-12s total_value=%s\n", row->element[0]->str, row->element[1]->str);
    }
    freeReplyObject(r);

    section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)");
    r = cmd(c, "DF.PRETTY inventory");
    printf("%s\n", r->str);
    freeReplyObject(r);

    section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교");
    r = cmd(c, "DF.TOCSV inventory");
    printf("%s", r->str);
    printf("원본과 동일? -> %s\n", strcmp(r->str, CSV_TEXT) == 0 ? "true" : "false");
    freeReplyObject(r);

    r = cmd(c, "DEL inventory");
    printf("\n정리: %lld 개 키 삭제\n", r->integer);
    freeReplyObject(r);

    redisFree(c);
    return 0;
}
