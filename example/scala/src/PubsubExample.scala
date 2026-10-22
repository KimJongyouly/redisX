import java.util.concurrent.{CountDownLatch, TimeUnit}
import redis.clients.jedis.{Jedis, JedisPubSub}

// Pub/Sub(발행/구독) 둘러보기.
//
// SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
// 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
// 지우고 시작할 "키"가 없다. example/java/src/PubsubExample.java와 같은
// 시나리오를 Scala(Jedis)로 옮긴 것이다 — Jedis의 subscribe()가 스레드를
// 영원히 블로킹하는 성질, JedisPubSub 안에서 psubscribe()를 추가로 걸 수
// 있다는 점까지 동일하다.

def addr(): String = sys.env.getOrElse("REDISX_ADDR", "127.0.0.1:2417")

def section(title: String): Unit = println(s"\n--- $title ---")

val Channel = "notifications"
val Pattern = "notif.*"
val PatternChannel = "notif.sports"

@main def PubsubExample(): Unit =
  val Array(host, port) = addr().split(":")

  val subscribed = new CountDownLatch(2) // onSubscribe + onPSubscribe
  val gotMessage = new CountDownLatch(1)
  val gotPatternMessage = new CountDownLatch(1)
  val unsubscribed = new CountDownLatch(2) // onUnsubscribe + onPUnsubscribe

  section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 스레드/연결 하나)")
  val subJedis = new Jedis(host, port.toInt)
  val listener = new JedisPubSub:
    override def onSubscribe(channel: String, subscribedChannels: Int): Unit =
      println(s"구독 확인 -> $channel ($subscribedChannels)")
      subscribed.countDown()
      this.psubscribe(Pattern) // 같은 연결에 패턴 구독을 추가로 건다

    override def onPSubscribe(pattern: String, subscribedChannels: Int): Unit =
      println(s"패턴 구독 확인 -> $pattern ($subscribedChannels)")
      subscribed.countDown()

    override def onMessage(channel: String, message: String): Unit =
      println(s"구독자가 받은 메시지 -> $channel : $message")
      gotMessage.countDown()

    override def onPMessage(pattern: String, channel: String, message: String): Unit =
      println(s"패턴으로 받은 메시지 -> pattern=$pattern channel=$channel : $message")
      gotPatternMessage.countDown()

    override def onUnsubscribe(channel: String, subscribedChannels: Int): Unit = unsubscribed.countDown()
    override def onPUnsubscribe(pattern: String, subscribedChannels: Int): Unit = unsubscribed.countDown()

  val subscriberThread = new Thread(() => subJedis.subscribe(listener, Channel))
  subscriberThread.start()
  subscribed.await(2, TimeUnit.SECONDS)

  val pub = new Jedis(host, port.toInt)
  try
    section("PUBLISH (발행자는 별도 연결)")
    var n = pub.publish(Channel, "서버 점검이 10분 후 시작됩니다")
    println(s"PUBLISH $Channel -> 받은 구독자 수: $n")
    gotMessage.await(2, TimeUnit.SECONDS)

    section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)")
    n = pub.publish(PatternChannel, "손흥민 결승골!")
    println(s"PUBLISH $PatternChannel -> 받은 구독자 수: $n")
    gotPatternMessage.await(2, TimeUnit.SECONDS)

    section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)")
    println(s"PUBSUB CHANNELS -> ${pub.pubsubChannels()}")
    println(s"PUBSUB NUMSUB -> ${pub.pubsubNumSub(Channel)}")
    println(s"PUBSUB NUMPAT -> ${pub.pubsubNumPat()}")

    section("UNSUBSCRIBE / PUNSUBSCRIBE")
    listener.unsubscribe(Channel)
    listener.punsubscribe(Pattern)
    unsubscribed.await(2, TimeUnit.SECONDS)
    subscriberThread.join(2000)
    n = pub.publish(Channel, "이제 아무도 안 듣는다")
    println(s"구독 해제 후 PUBLISH -> 받은 구독자 수: $n (0이어야 정상)")
  finally
    pub.close()
    subJedis.close()

  println("\n정리 완료")
