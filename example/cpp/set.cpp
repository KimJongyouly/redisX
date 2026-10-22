// Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
//
// 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
// 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
// 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
// example/python/set.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/set
#include <iostream>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL post:1:tags post:2:tags tags:archive");

    section("SADD로 글마다 태그 달기");
    r.command("SADD post:1:tags rust redis database");
    r.command("SADD post:2:tags rust web async");
    std::cout << "post:1:tags -> " << r.command("SMEMBERS post:1:tags").toString() << "\n";
    std::cout << "post:2:tags -> " << r.command("SMEMBERS post:2:tags").toString() << "\n";

    section("SINTER / SUNION / SDIFF로 집합 연산");
    std::cout << "둘 다 쓴 태그 (SINTER) -> " << r.command("SINTER post:1:tags post:2:tags").toString() << "\n";
    std::cout << "둘 중 하나라도 쓴 태그 (SUNION) -> " << r.command("SUNION post:1:tags post:2:tags").toString() << "\n";
    std::cout << "post:1에만 있는 태그 (SDIFF) -> " << r.command("SDIFF post:1:tags post:2:tags").toString() << "\n";

    section("SINTERSTORE로 교집합을 새 키에 저장");
    r.command("DEL tags:common");
    r.command("SINTERSTORE tags:common post:1:tags post:2:tags");
    std::cout << "tags:common -> " << r.command("SMEMBERS tags:common").toString() << "\n";

    section("SISMEMBER / SCARD");
    bool isMember = r.command("SISMEMBER post:1:tags redis")->integer != 0;
    std::cout << "post:1에 'redis' 태그가 있나? -> " << (isMember ? "true" : "false") << "\n";
    std::cout << "post:1 태그 개수 (SCARD) -> " << r.command("SCARD post:1:tags").toString() << "\n";

    section("SMOVE로 태그 하나를 보관함으로 옮기기");
    r.command("SMOVE post:1:tags tags:archive database");
    std::cout << "post:1:tags -> " << r.command("SMEMBERS post:1:tags").toString() << "\n";
    std::cout << "tags:archive -> " << r.command("SMEMBERS tags:archive").toString() << "\n";

    section("SPOP으로 무작위로 하나 뽑아서 제거");
    std::string popped = r.command("SPOP post:2:tags").toString();
    std::cout << "뽑힌 태그 -> " << popped << " / 남은 post:2:tags -> " << r.command("SMEMBERS post:2:tags").toString() << "\n";

    std::cout << "\n정리: " << r.command("DEL post:1:tags post:2:tags tags:archive tags:common").toString() << " 개 키 삭제\n";
    return 0;
}
