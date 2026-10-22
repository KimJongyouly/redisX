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
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL visitors:mon:hll visitors:tue:hll visitors:week:hll visitors:mon:exact");

    section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기");
    std::srand(0);
    // 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
    // 3,000명 정도의 유니크 방문자만 나오게 만든다.
    for (int i = 0; i < 10000; i++) {
        char visitorId[32];
        std::snprintf(visitorId, sizeof(visitorId), "user-%d", (std::rand() % 3000) + 1);
        r.command("PFADD visitors:mon:hll %s", visitorId);
        r.command("SADD visitors:mon:exact %s", visitorId);
    }

    long long exactCount = r.command("SCARD visitors:mon:exact")->integer;
    long long approxCount = r.command("PFCOUNT visitors:mon:hll")->integer;
    double errorPct = std::fabs(static_cast<double>(approxCount - exactCount)) / static_cast<double>(exactCount) * 100.0;
    std::cout << "정확한 유니크 방문자 수 (SCARD)   -> " << exactCount << "\n";
    std::cout << "HyperLogLog 근사치 (PFCOUNT)      -> " << approxCount << "\n";
    std::printf("오차율                             -> %.2f%% (표준 오차는 약 0.81%%)\n", errorPct);

    section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)");
    std::cout << "Set 메모리 사용량 근사치 (MEMORY USAGE) -> " << r.command("MEMORY USAGE visitors:mon:exact").toString() << " bytes\n";
    std::cout << "HLL 메모리 사용량 근사치 (MEMORY USAGE) -> " << r.command("MEMORY USAGE visitors:mon:hll").toString() << " bytes\n";

    section("PFMERGE로 이틀치 방문자 로그 합치기");
    for (int i = 0; i < 5000; i++) {
        char visitorId[32];
        std::snprintf(visitorId, sizeof(visitorId), "user-%d", (std::rand() % 4000) + 2000);  // 월요일과 일부 겹침
        r.command("PFADD visitors:tue:hll %s", visitorId);
    }
    r.command("PFMERGE visitors:week:hll visitors:mon:hll visitors:tue:hll");
    std::cout << "월요일 근사 방문자 수 -> " << r.command("PFCOUNT visitors:mon:hll").toString() << "\n";
    std::cout << "화요일 근사 방문자 수 -> " << r.command("PFCOUNT visitors:tue:hll").toString() << "\n";
    std::cout << "합산(PFMERGE) 근사 방문자 수 -> " << r.command("PFCOUNT visitors:week:hll").toString()
              << " (단순 합보다 적은 건 겹치는 방문자가 있어서)\n";

    std::cout << "\n정리: " << r.command("DEL visitors:mon:hll visitors:tue:hll visitors:week:hll visitors:mon:exact").toString()
              << " 개 키 삭제\n";
    return 0;
}
