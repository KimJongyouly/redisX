// ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
//
// ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
// 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
// 합산하는 것까지 — example/python/zset.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/zset
#include <iostream>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL leaderboard leaderboard:weekly leaderboard:combined");

    section("ZADD로 점수 매기기");
    r.command("ZADD leaderboard 120 alice 98 bob 150 carol 110 dave");
    std::cout << "전체 랭킹 (ZREVRANGE, 점수 포함) -> " << r.command("ZREVRANGE leaderboard 0 -1 WITHSCORES").toString() << "\n";

    section("ZSCORE / ZREVRANK로 한 명만 조회");
    std::cout << "carol 점수 (ZSCORE) -> " << r.command("ZSCORE leaderboard carol").toString() << "\n";
    std::cout << "carol 등수, 0부터 (ZREVRANK) -> " << r.command("ZREVRANK leaderboard carol").toString() << "\n";

    section("ZINCRBY로 점수 올리기 (보너스 획득)");
    r.command("ZINCRBY leaderboard 30 bob");
    std::cout << "bob 보너스 이후 전체 랭킹 -> " << r.command("ZREVRANGE leaderboard 0 -1 WITHSCORES").toString() << "\n";

    section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기");
    std::cout << "100점 이상 120점 이하 -> " << r.command("ZRANGEBYSCORE leaderboard 100 120 WITHSCORES").toString() << "\n";

    section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거");
    std::cout << "ZPOPMAX -> " << r.command("ZPOPMAX leaderboard").toString() << "\n";
    std::cout << "ZPOPMIN -> " << r.command("ZPOPMIN leaderboard").toString() << "\n";
    std::cout << "남은 랭킹 -> " << r.command("ZREVRANGE leaderboard 0 -1 WITHSCORES").toString() << "\n";

    section("ZUNIONSTORE로 이번 주 보너스 점수와 합산");
    r.command("ZADD leaderboard 120 alice 128 bob 110 dave");  // 방금 뽑아서 지워진 것 복원
    r.command("ZADD leaderboard:weekly 20 alice 5 bob 40 eve");
    r.command("ZUNIONSTORE leaderboard:combined 2 leaderboard leaderboard:weekly");
    std::cout << "합산 랭킹 (ZUNIONSTORE) -> " << r.command("ZREVRANGE leaderboard:combined 0 -1 WITHSCORES").toString() << "\n";

    std::cout << "\n정리: " << r.command("DEL leaderboard leaderboard:weekly leaderboard:combined").toString() << " 개 키 삭제\n";
    return 0;
}
