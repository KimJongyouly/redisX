-- 8개 예제 스크립트가 공유하는 아주 작은 헬퍼.
--
-- redisX로 붙는 Redis 클라이언트 자체는 `redis-lua`(nrk/redis-lua, LuaSocket
-- 기반) 그대로 쓴다 — Lua 표준 라이브러리에는 소켓 기능이 아예 없어서 TCP
-- 통신 자체가 외부 라이브러리 없이는 불가능하고, redis-lua가 가장 널리
-- 알려진 선택지라 그대로 썼다. 다만 redis-lua 2.0.4는 꽤 오래된 버전이라
-- HRANDFIELD/LMOVE/LPOS/PFADD/PFCOUNT/PFMERGE/MEMORY/ZPOPMAX/ZPOPMIN처럼
-- 나중에 추가된 커맨드는 미리 등록되어 있지 않다 -- `client:define_command
-- (name)`으로 그 자리에서 커맨드를 추가로 등록해주면 (RESP 멀티벌크
-- 요청/응답 파싱은 라이브러리가 그대로 처리해주므로) 다른 커맨드와
-- 똑같이 쓸 수 있다. `hset`은 이미 등록돼 있지만 2.0 시절 "필드 하나만"
-- 받는 옛날 HSET이라, 여러 필드를 한 번에 받는(그리고 redisX가 실제로
-- 구현한) 현대 HSET으로 덮어써서 다시 등록한다 -- redisX는 레거시 HMSET
-- 자체를 아예 구현하지 않았다(HSET이 이미 다중 필드를 받으므로).
--
-- 실행 인터프리터: Homebrew의 `lua` 포뮬러가 설치한 `lua`(5.5) 바이너리.
local redis = require "redis"

local M = {}

function M.connect()
	local addr = os.getenv("REDISX_ADDR") or "127.0.0.1:2417"
	local host, port = addr:match("^([^:]+):(%d+)$")
	local client = redis.connect(host, tonumber(port))
	local extra = {
		"hrandfield", "lmove", "lpos", "pfadd", "pfcount", "pfmerge",
		"memory", "zpopmax", "zpopmin", "hset",
	}
	for _, name in ipairs(extra) do
		client:define_command(name)
	end
	return client
end

function M.section(title)
	print("\n--- " .. title .. " ---")
end

return M
