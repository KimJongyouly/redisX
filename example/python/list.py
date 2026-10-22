"""List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.

LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
옮기는 흐름 — 작업 큐에서 흔히 쓰는 패턴을 그대로 밟아본다.

실행:
    python 02_list.py
"""

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


queue, done = "tasks:todo", "tasks:done"
r.delete(queue, done)

section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)")
r.rpush(queue, "reboot-db", "send-email", "resize-image")
print("LRANGE tasks:todo 0 -1 ->", r.lrange(queue, 0, -1))

section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기")
r.lpush(queue, "URGENT:rotate-logs")
print("LRANGE tasks:todo 0 -1 ->", r.lrange(queue, 0, -1))

section("LINSERT로 특정 작업 바로 뒤에 끼워넣기")
r.linsert(queue, "AFTER", "send-email", "send-email-retry")
print("LRANGE tasks:todo 0 -1 ->", r.lrange(queue, 0, -1))

section("LPOS로 작업 위치 찾기, LREM으로 제거")
pos = r.lpos(queue, "send-email-retry")
print("send-email-retry의 위치 ->", pos)
r.lrem(queue, 1, "send-email-retry")  # 결국 재시도는 취소하기로 함
print("LREM 이후 LRANGE ->", r.lrange(queue, 0, -1))

section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기")
task = r.lpop(queue)
print("처리할 작업 ->", task)
r.rpush(done, task)
r.lmove(queue, done, "LEFT", "RIGHT")  # 다음 작업도 바로 완료 큐로 옮겨본다
print("tasks:todo ->", r.lrange(queue, 0, -1))
print("tasks:done ->", r.lrange(done, 0, -1))

section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)")
r.rpush(done, "old-task-1", "old-task-2", "old-task-3")
r.ltrim(done, -2, -1)
print("LTRIM 이후 tasks:done ->", r.lrange(done, 0, -1))

print("\n정리:", r.delete(queue, done), "개 키 삭제")
