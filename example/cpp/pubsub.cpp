// Pub/Sub(발행/구독) 둘러보기.
//
// SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
// 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
// 지우고 시작할 "키"가 없다. hiredis(와 이 얇은 래퍼)는 구독 전용 API가
// 따로 없다 — 평범한 command()로 SUBSCRIBE를 보내고 나면 그 연결은 구독
// 모드가 되고, 그 뒤로는 command() 대신 nextPush()(redisx_client.hpp,
// `redisGetReply`를 감싼 것)로 서버가 밀어주는 프레임을 하나씩 읽는다.
// 그래서 발행은 별도 연결을 하나 더 연다.
//
// 실행:
//   make && ./bin/pubsub
#include <chrono>
#include <iostream>
#include <thread>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

static const char* CHANNEL = "notifications";
static const char* PATTERN = "notif.*";
static const char* PATTERN_CHANNEL = "notif.sports";

int main() {
    Client sub(Client::hostFromEnv(), Client::portFromEnv());
    Client pub(Client::hostFromEnv(), Client::portFromEnv());

    section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)");
    {
        auto r = sub.command("SUBSCRIBE %s", CHANNEL);
        std::cout << "구독 확인 -> " << r->element[0]->str << " " << r->element[1]->str << "\n";
    }
    {
        auto r = sub.command("PSUBSCRIBE %s", PATTERN);
        std::cout << "패턴 구독 확인 -> " << r->element[0]->str << " " << r->element[1]->str << "\n";
    }

    section("PUBLISH (발행자는 별도 연결)");
    {
        auto r = pub.command("PUBLISH %s %s", CHANNEL, "서버 점검이 10분 후 시작됩니다");
        std::cout << "PUBLISH " << CHANNEL << " -> 받은 구독자 수: " << r->integer << "\n";
    }
    {
        auto r = sub.nextPush(); // ["message", channel, payload]
        std::cout << "구독자가 받은 메시지 -> " << r->element[1]->str << " : " << r->element[2]->str << "\n";
    }

    section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)");
    {
        auto r = pub.command("PUBLISH %s %s", PATTERN_CHANNEL, "손흥민 결승골!");
        std::cout << "PUBLISH " << PATTERN_CHANNEL << " -> 받은 구독자 수: " << r->integer << "\n";
    }
    {
        auto r = sub.nextPush(); // ["pmessage", pattern, channel, payload]
        std::cout << "패턴으로 받은 메시지 -> pattern=" << r->element[1]->str << " channel=" << r->element[2]->str << " : " << r->element[3]->str << "\n";
    }

    section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)");
    std::cout << "PUBSUB CHANNELS -> " << pub.command("PUBSUB CHANNELS *").toString() << "\n";
    std::cout << "PUBSUB NUMSUB -> " << pub.command("PUBSUB NUMSUB %s", CHANNEL).toString() << "\n";
    std::cout << "PUBSUB NUMPAT -> " << pub.command("PUBSUB NUMPAT").toString() << "\n";

    section("UNSUBSCRIBE / PUNSUBSCRIBE");
    sub.command("UNSUBSCRIBE %s", CHANNEL);
    sub.command("PUNSUBSCRIBE %s", PATTERN);
    std::this_thread::sleep_for(std::chrono::milliseconds(200)); // 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
    {
        auto r = pub.command("PUBLISH %s %s", CHANNEL, "이제 아무도 안 듣는다");
        std::cout << "구독 해제 후 PUBLISH -> 받은 구독자 수: " << r->integer << " (0이어야 정상)\n";
    }

    std::cout << "\n정리 완료\n";
    return 0;
}
