-- SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.
--
-- 실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
-- 포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
-- 남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 CHANGE.md 1절 참고).
-- 그래서 redisX에서 SLOWLOG를 쓰려면 slowlog-file-path를 실제 경로로
-- 설정해야 하고(기본값은 ""로 비활성화), SLOWLOG GET/LEN은 매번 그 파일을
-- 읽어서 답한다. redis-lua(2.0.4)의 SLOWLOG 응답 파서는 id/timestamp/
-- duration/command 네 필드만 구조화해주고(그 시절 real Redis 응답 포맷이
-- 그랬다) redisX가 추가로 내려주는 클라이언트 주소/이름 필드는 조용히
-- 버린다 — 이 데모에서는 어차피 그 네 필드만 보여준다.
--
-- 실행: lua slowlog.lua
local demo = require "redisx_demo"
local client = demo.connect()

demo.section("현재 설정 확인 (기본값은 비활성화 상태)")
print("slowlog-file-path ->", client:config("GET", "slowlog-file-path")["slowlog-file-path"])
print("slowlog-log-slower-than (마이크로초) ->", client:config("GET", "slowlog-log-slower-than")["slowlog-log-slower-than"])

demo.section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기")
local log_path = os.getenv("TMPDIR") or "/tmp/"
if log_path:sub(-1) ~= "/" then
	log_path = log_path .. "/"
end
log_path = log_path .. "redisx-slowlog-demo-lua.log"
os.remove(log_path)
client:config("SET", "slowlog-file-path", log_path)
client:config("SET", "slowlog-log-slower-than", "0") -- 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

demo.section("몇 가지 커맨드를 실행해서 로그에 쌓기")
client:set("k", "v")
client:get("k")
client:lpush("mylist", "a", "b", "c")
client:del("mylist", "k")

demo.section("SLOWLOG LEN / GET으로 읽기")
print("SLOWLOG LEN ->", client:slowlog("LEN"))
for _, entry in ipairs(client:slowlog("GET", 5)) do
	print(string.format("  #%d  %dus  %s", entry.id, entry.duration, table.concat(entry.command, " ")))
end

demo.section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)")
local f = io.open(log_path, "r")
io.write(f:read("a"))
f:close()

demo.section("SLOWLOG RESET으로 파일 비우기")
client:slowlog("RESET")
-- 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
-- 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
print("RESET 이후 SLOWLOG LEN ->", client:slowlog("LEN"))

demo.section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기")
client:config("SET", "slowlog-log-slower-than", "10000") -- redisX 기본값
client:config("SET", "slowlog-file-path", "") -- 기본값(비활성화)으로 복원
os.remove(log_path)
print("완료")
