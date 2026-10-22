// List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
//
// LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
// 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
// 옮기는 흐름 — example/python/list.py와 같은 시나리오.
//
// 실행:
//   make && ./bin/list
#include <iostream>

#include "redisx_client.hpp"

using redisx::Client;
using redisx::section;

int main() {
    Client r(Client::hostFromEnv(), Client::portFromEnv());

    r.command("DEL tasks:todo tasks:done");

    section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)");
    r.command("RPUSH tasks:todo reboot-db send-email resize-image");
    std::cout << "LRANGE tasks:todo 0 -1 -> " << r.command("LRANGE tasks:todo 0 -1").toString() << "\n";

    section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기");
    r.command("LPUSH tasks:todo URGENT:rotate-logs");
    std::cout << "LRANGE tasks:todo 0 -1 -> " << r.command("LRANGE tasks:todo 0 -1").toString() << "\n";

    section("LINSERT로 특정 작업 바로 뒤에 끼워넣기");
    r.command("LINSERT tasks:todo AFTER send-email send-email-retry");
    std::cout << "LRANGE tasks:todo 0 -1 -> " << r.command("LRANGE tasks:todo 0 -1").toString() << "\n";

    section("LPOS로 작업 위치 찾기, LREM으로 제거");
    std::cout << "send-email-retry의 위치 -> " << r.command("LPOS tasks:todo send-email-retry").toString() << "\n";
    r.command("LREM tasks:todo 1 send-email-retry");  // 결국 재시도는 취소하기로 함
    std::cout << "LREM 이후 LRANGE -> " << r.command("LRANGE tasks:todo 0 -1").toString() << "\n";

    section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기");
    std::string task = r.command("LPOP tasks:todo").toString();
    std::cout << "처리할 작업 -> " << task << "\n";
    r.command("RPUSH tasks:done %s", task.c_str());
    r.command("LMOVE tasks:todo tasks:done LEFT RIGHT");  // 다음 작업도 바로 완료 큐로 옮겨본다
    std::cout << "tasks:todo -> " << r.command("LRANGE tasks:todo 0 -1").toString() << "\n";
    std::cout << "tasks:done -> " << r.command("LRANGE tasks:done 0 -1").toString() << "\n";

    section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)");
    r.command("RPUSH tasks:done old-task-1 old-task-2 old-task-3");
    r.command("LTRIM tasks:done -2 -1");
    std::cout << "LTRIM 이후 tasks:done -> " << r.command("LRANGE tasks:done 0 -1").toString() << "\n";

    std::cout << "\n정리: " << r.command("DEL tasks:todo tasks:done").toString() << " 개 키 삭제\n";
    return 0;
}
