// String(키/값) 커맨드 둘러보기.
//
// 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
// 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
// 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py와 같은
// 시나리오를 hiredis 기반 C++ 래퍼로 옮긴 것이다.
//
// 실행:
//   make && ./bin/string
#include <chrono>
#include <iostream>
#include <thread>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL greeting page:views user:1:session k1 k2 k3");

    section("SET / GET");
    r.command("SET greeting %s", "hello world");
    std::cout << "GET greeting -> " << r.command("GET greeting").toString() << "\n";

    section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)");
    r.command("APPEND greeting !");
    std::cout << "APPEND 이후: " << r.command("GET greeting").toString() << "\n";
    std::cout << "GETRANGE greeting 0 4 -> " << r.command("GETRANGE greeting 0 4").toString() << "\n";
    r.command("SETRANGE greeting 6 redisX");  // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
    std::cout << "SETRANGE 이후: " << r.command("GET greeting").toString() << "\n";

    section("MSET / MGET (여러 키 한 번에)");
    r.command("MSET k1 v1 k2 v2 k3 v3");
    std::cout << "MGET k1 k2 k3 -> " << r.command("MGET k1 k2 k3").toString() << "\n";

    section("INCR / INCRBY (카운터)");
    r.command("DEL page:views");
    for (int i = 0; i < 3; i++) r.command("INCR page:views");
    r.command("INCRBY page:views 10");
    std::cout << "page:views -> " << r.command("GET page:views").toString() << "\n";

    section("EXPIRE / TTL / PERSIST");
    r.command("SET user:1:session token-abc");
    r.command("EXPIRE user:1:session 2");
    std::cout << "설정 직후 TTL -> " << r.command("TTL user:1:session").toString() << " 초\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(2200));
    bool exists = r.command("EXISTS user:1:session")->integer != 0;
    std::cout << "2.2초 후 EXISTS -> " << (exists ? "true" : "false") << " (TTL이 지나 자동 삭제됨)\n";

    r.command("SET user:1:session token-def EX 60");
    r.command("PERSIST user:1:session");
    std::cout << "PERSIST 이후 TTL -> " << r.command("TTL user:1:session").toString() << " (-1 = 만료 없음)\n";

    std::cout << "\n정리: " << r.command("DEL greeting page:views user:1:session k1 k2 k3").toString() << " 개 키 삭제\n";
    return 0;
}
