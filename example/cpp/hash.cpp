// Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
//
// 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
// Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
// 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
// example/python/hash.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/hash
#include <iostream>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL user:42");

    section("HSET으로 여러 필드를 한 번에 채우기");
    r.command("HSET user:42 name jiyeon level 5 coins 100 logins 0");
    std::cout << "HGETALL user:42 -> " << r.command("HGETALL user:42").toString() << "\n";

    section("HGET / HMGET으로 필드 일부만 읽기");
    std::cout << "HGET user:42 name -> " << r.command("HGET user:42 name").toString() << "\n";
    std::cout << "HMGET user:42 name level -> " << r.command("HMGET user:42 name level").toString() << "\n";

    section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가");
    r.command("HINCRBY user:42 coins 50");
    r.command("HINCRBY user:42 logins 1");
    std::cout << "HGETALL user:42 -> " << r.command("HGETALL user:42").toString() << "\n";

    section("HEXISTS / HDEL");
    bool exists = r.command("HEXISTS user:42 level")->integer != 0;
    std::cout << "HEXISTS user:42 level -> " << (exists ? "true" : "false") << "\n";
    r.command("HDEL user:42 logins");
    std::cout << "HDEL 이후 HGETALL -> " << r.command("HGETALL user:42").toString() << "\n";

    section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기");
    r.command("DEL raffle:entries");
    r.command("HSET raffle:entries alice 1 bob 1 carol 1 dave 1");
    std::cout << "당첨자 한 명 -> " << r.command("HRANDFIELD raffle:entries").toString() << "\n";
    std::cout << "HRANDFIELD -3 (중복 허용 3명 뽑기) -> " << r.command("HRANDFIELD raffle:entries -3").toString() << "\n";

    std::cout << "\n정리: " << r.command("DEL user:42 raffle:entries").toString() << " 개 키 삭제\n";
    return 0;
}
