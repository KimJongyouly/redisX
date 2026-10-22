-- Pub/Sub(발행/구독) 둘러보기.
--
-- SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
-- 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
-- 지우고 시작할 "키"가 없다. redis-lua는 subscribe/psubscribe/publish가
-- 이미 등록돼 있고(다른 예제들이 `define_command`로 보강해야 했던 최신
-- 커맨드들과 달리 pub/sub은 redis-lua 2.0 시절부터 있었다), `client:pubsub
-- {subscribe = {...}}`가 코루틴 기반 반복자를 돌려준다 -- 호출할 때마다
-- 다음 메시지 하나를 블로킹으로 기다린다. 구독 연결은 그 반복자 전용이 되니
-- 발행은 별도 연결을 하나 더 연다.
--
-- 실행: lua pubsub.lua
local demo = require "redisx_demo"
local sub = demo.connect()
local pub = demo.connect()

local CHANNEL = "notifications"
local PATTERN = "notif.*"
local PATTERN_CHANNEL = "notif.sports"

demo.section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)")
local consumer = sub:pubsub({ subscribe = CHANNEL, psubscribe = PATTERN })
local msg, abort = consumer()
print("구독 확인 ->", msg.kind, msg.channel)
msg, abort = consumer()
print("패턴 구독 확인 ->", msg.kind, msg.channel)

demo.section("PUBLISH (발행자는 별도 연결)")
local n = pub:publish(CHANNEL, "서버 점검이 10분 후 시작됩니다")
print("PUBLISH " .. CHANNEL .. " -> 받은 구독자 수:", n)
msg, abort = consumer()
print("구독자가 받은 메시지 ->", msg.channel, ":", msg.payload)

demo.section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)")
n = pub:publish(PATTERN_CHANNEL, "손흥민 결승골!")
print("PUBLISH " .. PATTERN_CHANNEL .. " -> 받은 구독자 수:", n)
msg, abort = consumer()
print("패턴으로 받은 메시지 -> pattern=" .. msg.pattern, "channel=" .. msg.channel, ":", msg.payload)

demo.section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)")
-- redis-lua는 PUBSUB CHANNELS/NUMSUB/NUMPAT에 타입드 메서드가 없다
-- (다른 예제들의 DF.*/SLOWLOG와 같은 사정) -- define_command로 그 자리에서
-- 등록해서 쓴다.
pub:define_command("pubsub")
print("PUBSUB CHANNELS ->", table.concat(pub:pubsub("channels", "*"), ", "))
local numsub = pub:pubsub("numsub", CHANNEL)
print("PUBSUB NUMSUB ->", numsub[1], numsub[2])
print("PUBSUB NUMPAT ->", pub:pubsub("numpat"))

demo.section("UNSUBSCRIBE / PUNSUBSCRIBE")
-- consumer()가 호출할 때마다 돌려주는 두 번째 값(abort, 위에서 이미 받아둠)을
-- 부르면 UNSUBSCRIBE/PUNSUBSCRIBE를 보내고, 그 확인 메시지들까지 다 받고
-- 나면(구독 수가 0이 되면) 코루틴이 끝나 다음 호출부터 nil을 돌려준다.
abort()
repeat
	msg = consumer()
until msg == nil
os.execute("sleep 0.2") -- 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
n = pub:publish(CHANNEL, "이제 아무도 안 듣는다")
print("구독 해제 후 PUBLISH -> 받은 구독자 수:", n, "(0이어야 정상)")

print("\n정리 완료")
