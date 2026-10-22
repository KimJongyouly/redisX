// DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
//
// 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
// 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
// 있다. C++에도 polars 같은 네이티브 DataFrame 라이브러리가 없으므로,
// 여기서는 DF.QUERY의 RESP 행 배열을 직접 훑고 DF.PRETTY로 보기 좋은 표를
// 받는다 — example/python/dataframe_csv.py,
// example/golang/dataframe/main.go와 동일한 시나리오(작은 재고 CSV).
//
// 실행:
//   make && ./bin/dataframe
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <iostream>
#include <string>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

static const char* CSV_TEXT =
    "name,category,price,qty\n"
    "widget,tools,9.99,100\n"
    "gadget,electronics,29.99,50\n"
    "gizmo,electronics,19.99,75\n"
    "bolt,tools,0.99,500\n"
    "screw,tools,0.49,1000\n";

static bool containsCaseInsensitive(const std::string& haystack, const std::string& needle) {
    auto it = std::search(haystack.begin(), haystack.end(), needle.begin(), needle.end(), [](char a, char b) {
        return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
    });
    return it != haystack.end();
}

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL inventory");

    section("DF.FROMCSV로 인라인 CSV 적재");
    try {
        r.command("DF.FROMCSV inventory %s", CSV_TEXT);
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        if (containsCaseInsensitive(msg, "unknown command")) {
            std::cerr << "DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.\n"
                      << "cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.\n";
            return 1;
        }
        throw;
    }

    section("DF.SHAPE / DF.COLUMNS로 구조 확인");
    std::cout << "DF.SHAPE inventory -> " << r.command("DF.SHAPE inventory").toString() << "\n";
    std::cout << "DF.COLUMNS inventory -> " << r.command("DF.COLUMNS inventory").toString() << "\n";

    section("DF.QUERY로 카테고리별 재고 가치 집계");
    auto rows = r.command(
        "DF.QUERY inventory %s", "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category");
    redisReply* rowsArr = rows.get();
    for (size_t i = 0; i < rowsArr->elements; i++) {
        redisReply* row = rowsArr->element[i];
        // RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string으로
        // 온다(이 포크의 Reply::Double 인코딩 규칙) — 문자열 그대로 쓴다.
        std::printf("  %-12s total_value=%s\n", row->element[0]->str, row->element[1]->str);
    }

    section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)");
    std::cout << r.command("DF.PRETTY inventory").toString() << "\n";

    section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교");
    std::string exported = r.command("DF.TOCSV inventory").toString();
    std::cout << exported;
    std::cout << "원본과 동일? -> " << (exported == CSV_TEXT ? "true" : "false") << "\n";

    std::cout << "\n정리: " << r.command("DEL inventory").toString() << " 개 키 삭제\n";
    return 0;
}
