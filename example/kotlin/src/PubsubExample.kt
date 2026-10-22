import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub

/**
 * Pub/Sub(발행/구독) 둘러보기.
 *
 * SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
 * 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
 * 지우고 시작할 "키"가 없다. example/java/src/PubsubExample.java와 같은
 * 시나리오를 Kotlin(Jedis)으로 옮긴 것이다 — Jedis의 subscribe()가 스레드를
 * 영원히 블로킹하는 성질, JedisPubSub 안에서 psubscribe()를 추가로 걸 수
 * 있다는 점까지 동일하다.
 *
 * 실행: run.sh에 있는 정확한 커맨드 참고 (PubsubExampleKt가 Kotlin이
 * top-level fun main()에 자동으로 붙이는 클래스 이름이다).
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

const val CHANNEL = "notifications"
const val PATTERN = "notif.*"
const val PATTERN_CHANNEL = "notif.sports"

fun main() {
    val parts = addr().split(":")
    val host = parts[0]
    val port = parts[1].toInt()

    val subscribed = CountDownLatch(2) // onSubscribe + onPSubscribe
    val gotMessage = CountDownLatch(1)
    val gotPatternMessage = CountDownLatch(1)
    val unsubscribed = CountDownLatch(2) // onUnsubscribe + onPUnsubscribe

    section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 스레드/연결 하나)")
    val subJedis = Jedis(host, port)
    val listener = object : JedisPubSub() {
        override fun onSubscribe(channel: String, subscribedChannels: Int) {
            println("구독 확인 -> $channel ($subscribedChannels)")
            subscribed.countDown()
            this.psubscribe(PATTERN) // 같은 연결에 패턴 구독을 추가로 건다
        }

        override fun onPSubscribe(pattern: String, subscribedChannels: Int) {
            println("패턴 구독 확인 -> $pattern ($subscribedChannels)")
            subscribed.countDown()
        }

        override fun onMessage(channel: String, message: String) {
            println("구독자가 받은 메시지 -> $channel : $message")
            gotMessage.countDown()
        }

        override fun onPMessage(pattern: String, channel: String, message: String) {
            println("패턴으로 받은 메시지 -> pattern=$pattern channel=$channel : $message")
            gotPatternMessage.countDown()
        }

        override fun onUnsubscribe(channel: String, subscribedChannels: Int) = unsubscribed.countDown()
        override fun onPUnsubscribe(pattern: String, subscribedChannels: Int) = unsubscribed.countDown()
    }
    val subscriberThread = Thread { subJedis.subscribe(listener, CHANNEL) }
    subscriberThread.start()
    subscribed.await(2, TimeUnit.SECONDS)

    Jedis(host, port).use { pub ->
        section("PUBLISH (발행자는 별도 연결)")
        var n = pub.publish(CHANNEL, "서버 점검이 10분 후 시작됩니다")
        println("PUBLISH $CHANNEL -> 받은 구독자 수: $n")
        gotMessage.await(2, TimeUnit.SECONDS)

        section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)")
        n = pub.publish(PATTERN_CHANNEL, "손흥민 결승골!")
        println("PUBLISH $PATTERN_CHANNEL -> 받은 구독자 수: $n")
        gotPatternMessage.await(2, TimeUnit.SECONDS)

        section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)")
        println("PUBSUB CHANNELS -> ${pub.pubsubChannels()}")
        println("PUBSUB NUMSUB -> ${pub.pubsubNumSub(CHANNEL)}")
        println("PUBSUB NUMPAT -> ${pub.pubsubNumPat()}")

        section("UNSUBSCRIBE / PUNSUBSCRIBE")
        listener.unsubscribe(CHANNEL)
        listener.punsubscribe(PATTERN)
        unsubscribed.await(2, TimeUnit.SECONDS)
        subscriberThread.join(2000)
        n = pub.publish(CHANNEL, "이제 아무도 안 듣는다")
        println("구독 해제 후 PUBLISH -> 받은 구독자 수: $n (0이어야 정상)")
    }
    subJedis.close()

    println("\n정리 완료")
}
