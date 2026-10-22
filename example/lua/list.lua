-- List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
--
-- LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
-- 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
-- 옮기는 흐름 — example/python/list.py와 같은 시나리오.
--
-- 실행: lua list.lua
local demo = require "redisx_demo"
local client = demo.connect()

local queue, done = "tasks:todo", "tasks:done"
client:del(queue, done)

demo.section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)")
client:rpush(queue, "reboot-db", "send-email", "resize-image")
print("LRANGE tasks:todo 0 -1 ->", table.concat(client:lrange(queue, 0, -1), ", "))

demo.section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기")
client:lpush(queue, "URGENT:rotate-logs")
print("LRANGE tasks:todo 0 -1 ->", table.concat(client:lrange(queue, 0, -1), ", "))

demo.section("LINSERT로 특정 작업 바로 뒤에 끼워넣기")
client:linsert(queue, "AFTER", "send-email", "send-email-retry")
print("LRANGE tasks:todo 0 -1 ->", table.concat(client:lrange(queue, 0, -1), ", "))

demo.section("LPOS로 작업 위치 찾기, LREM으로 제거")
print("send-email-retry의 위치 ->", client:lpos(queue, "send-email-retry"))
client:lrem(queue, 1, "send-email-retry") -- 결국 재시도는 취소하기로 함
print("LREM 이후 LRANGE ->", table.concat(client:lrange(queue, 0, -1), ", "))

demo.section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기")
local task = client:lpop(queue)
print("처리할 작업 ->", task)
client:rpush(done, task)
client:lmove(queue, done, "LEFT", "RIGHT") -- 다음 작업도 바로 완료 큐로 옮겨본다
print("tasks:todo ->", table.concat(client:lrange(queue, 0, -1), ", "))
print("tasks:done ->", table.concat(client:lrange(done, 0, -1), ", "))

demo.section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)")
client:rpush(done, "old-task-1", "old-task-2", "old-task-3")
client:ltrim(done, -2, -1)
print("LTRIM 이후 tasks:done ->", table.concat(client:lrange(done, 0, -1), ", "))

print("\n정리:", client:del(queue, done), "개 키 삭제")
