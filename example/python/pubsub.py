"""Pub/Sub(발행/구독) 둘러보기.

`SUBSCRIBE`/`PSUBSCRIBE`로 채널을 구독하고, 별도 연결에서 `PUBLISH`로
메시지를 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과
달리 지우고 시작할 "키"가 없다. 구독 중인 연결은 일반 커맨드를 보낼 수 없는
전용 모드로 들어가므로(redis-py의 `PubSub` 객체가 이를 숨겨준다), 발행자는
**별도의 연결**을 하나 더 만든다.

실행:
    python pubsub.py
"""

import time

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


CHANNEL = "notifications"
PATTERN = "notif.*"
PATTERN_CHANNEL = "notif.sports"

section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)")
sub = r.pubsub(ignore_subscribe_messages=False)
sub.subscribe(CHANNEL)
sub.psubscribe(PATTERN)
# 구독 확인 메시지(kind: "subscribe"/"psubscribe") 두 개를 먼저 받아 넘긴다.
print("구독 확인 ->", sub.get_message(timeout=2))
print("패턴 구독 확인 ->", sub.get_message(timeout=2))

section("PUBLISH (발행자는 별도 연결)")
pub = redis.Redis(host="127.0.0.1", port=2417)
n = pub.publish(CHANNEL, "서버 점검이 10분 후 시작됩니다")
print(f"PUBLISH {CHANNEL} -> 받은 구독자 수: {n}")
msg = sub.get_message(timeout=2)
print("구독자가 받은 메시지 ->", msg["channel"], ":", msg["data"])

section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)")
n = pub.publish(PATTERN_CHANNEL, "손흥민 결승골!")
print(f"PUBLISH {PATTERN_CHANNEL} -> 받은 구독자 수: {n}")
msg = sub.get_message(timeout=2)
print("패턴으로 받은 메시지 ->", f"pattern={msg['pattern']}", f"channel={msg['channel']}", ":", msg["data"])

section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)")
print("PUBSUB CHANNELS ->", pub.pubsub_channels())
print("PUBSUB NUMSUB ->", pub.pubsub_numsub(CHANNEL))
print("PUBSUB NUMPAT ->", pub.pubsub_numpat())

section("UNSUBSCRIBE / PUNSUBSCRIBE")
sub.unsubscribe(CHANNEL)
sub.punsubscribe(PATTERN)
print("구독 해제 확인 ->", sub.get_message(timeout=2))
print("패턴 구독 해제 확인 ->", sub.get_message(timeout=2))
time.sleep(0.2)  # 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
n = pub.publish(CHANNEL, "이제 아무도 안 듣는다")
print(f"구독 해제 후 PUBLISH -> 받은 구독자 수: {n} (0이어야 정상)")

sub.close()
print("\n정리 완료")
