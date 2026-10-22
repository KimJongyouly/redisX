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
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    section("현재 설정 확인 (기본값은 비활성화 상태)");
    std::cout << "slowlog-file-path -> " << r.command("CONFIG GET slowlog-file-path").toString() << "\n";
    std::cout << "slowlog-log-slower-than (마이크로초) -> " << r.command("CONFIG GET slowlog-log-slower-than").toString() << "\n";

    section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기");
    const std::string logPath = "/tmp/redisx-slowlog-demo-cpp.log";
    std::remove(logPath.c_str());
    r.command("CONFIG SET slowlog-file-path %s", logPath.c_str());
    r.command("CONFIG SET slowlog-log-slower-than 0");  // 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

    section("몇 가지 커맨드를 실행해서 로그에 쌓기");
    r.command("SET k v");
    r.command("GET k");
    r.command("LPUSH mylist a b c");
    r.command("DEL mylist k");

    section("SLOWLOG LEN / GET으로 읽기");
    std::cout << "SLOWLOG LEN -> " << r.command("SLOWLOG LEN").toString() << "\n";

    auto entries = r.command("SLOWLOG GET 5");
    redisReply* arr = entries.get();
    for (size_t i = 0; i < arr->elements; i++) {
        redisReply* entry = arr->element[i];
        long long id = entry->element[0]->integer;
        long long durationUs = entry->element[2]->integer;
        redisReply* argv = entry->element[3];
        const char* peer = entry->element[4]->str;
        std::cout << "  #" << id << "  " << durationUs << "us  " << peer << "  ";
        for (size_t j = 0; j < argv->elements; j++) {
            if (j) std::cout << " ";
            std::cout << argv->element[j]->str;
        }
        std::cout << "\n";
    }

    section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)");
    std::ifstream file(logPath);
    if (file) {
        std::cout << file.rdbuf();
    } else {
        std::cout << "(파일을 열 수 없음: " << logPath << ")\n";
    }

    section("SLOWLOG RESET으로 파일 비우기");
    r.command("SLOWLOG RESET");
    // 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
    // 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
    std::cout << "RESET 이후 SLOWLOG LEN -> " << r.command("SLOWLOG LEN").toString() << "\n";

    section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기");
    r.command("CONFIG SET slowlog-log-slower-than 10000");  // redisX 기본값
    r.command("CONFIG SET slowlog-file-path %s", "");        // 기본값(비활성화)으로 복원
    std::remove(logPath.c_str());
    std::cout << "완료\n";

    return 0;
}
