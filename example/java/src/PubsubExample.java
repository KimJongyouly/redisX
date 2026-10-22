import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Pub/Sub(발행/구독) 둘러보기.
 *
 * SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
 * 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
 * 지우고 시작할 "키"가 없다.
 *
 * Jedis의 {@code subscribe(JedisPubSub, channel...)}는 호출한 스레드를
 * 그 자리에서 영원히(구독이 전부 풀릴 때까지) 블로킹하며 콜백을 호출하므로
 * 별도 스레드에서 돌린다. {@link JedisPubSub}는 이미 구독 중인 연결에서
 * {@code this.psubscribe(...)}를 더 부를 수 있게 해줘서, 패턴 구독도 같은
 * 연결/같은 콜백 객체에서 {@code onSubscribe} 안에 추가로 걸면 된다 — PHP처럼
 * 프로세스를 두 개로 나눌 필요가 없다. 발행과 PUBSUB 조회는 별도 연결로.
 *
 * 실행: java -cp "lib/*:out" PubsubExample
 */
public class PubsubExample {
    static String addr() {
        String a = System.getenv("REDISX_ADDR");
        return a != null ? a : "127.0.0.1:2417";
    }

    static void section(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    static final String CHANNEL = "notifications";
    static final String PATTERN = "notif.*";
    static final String PATTERN_CHANNEL = "notif.sports";

    public static void main(String[] args) throws Exception {
        String[] parts = addr().split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        CountDownLatch subscribed = new CountDownLatch(2); // onSubscribe + onPSubscribe
        CountDownLatch gotMessage = new CountDownLatch(1);
        CountDownLatch gotPatternMessage = new CountDownLatch(1);
        CountDownLatch unsubscribed = new CountDownLatch(2); // onUnsubscribe + onPUnsubscribe

        section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 스레드/연결 하나)");
        Jedis subJedis = new Jedis(host, port);
        JedisPubSub listener = new JedisPubSub() {
            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                System.out.println("구독 확인 -> " + channel + " (" + subscribedChannels + ")");
                subscribed.countDown();
                this.psubscribe(PATTERN); // 같은 연결에 패턴 구독을 추가로 건다
            }

            @Override
            public void onPSubscribe(String pattern, int subscribedChannels) {
                System.out.println("패턴 구독 확인 -> " + pattern + " (" + subscribedChannels + ")");
                subscribed.countDown();
            }

            @Override
            public void onMessage(String channel, String message) {
                System.out.println("구독자가 받은 메시지 -> " + channel + " : " + message);
                gotMessage.countDown();
            }

            @Override
            public void onPMessage(String pattern, String channel, String message) {
                System.out.println("패턴으로 받은 메시지 -> pattern=" + pattern + " channel=" + channel + " : " + message);
                gotPatternMessage.countDown();
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                unsubscribed.countDown();
            }

            @Override
            public void onPUnsubscribe(String pattern, int subscribedChannels) {
                unsubscribed.countDown();
            }
        };
        Thread subscriberThread = new Thread(() -> subJedis.subscribe(listener, CHANNEL));
        subscriberThread.start();
        subscribed.await(2, TimeUnit.SECONDS);

        try (Jedis pub = new Jedis(host, port)) {
            section("PUBLISH (발행자는 별도 연결)");
            long n = pub.publish(CHANNEL, "서버 점검이 10분 후 시작됩니다");
            System.out.println("PUBLISH " + CHANNEL + " -> 받은 구독자 수: " + n);
            gotMessage.await(2, TimeUnit.SECONDS);

            section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)");
            n = pub.publish(PATTERN_CHANNEL, "손흥민 결승골!");
            System.out.println("PUBLISH " + PATTERN_CHANNEL + " -> 받은 구독자 수: " + n);
            gotPatternMessage.await(2, TimeUnit.SECONDS);

            section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)");
            System.out.println("PUBSUB CHANNELS -> " + pub.pubsubChannels());
            System.out.println("PUBSUB NUMSUB -> " + pub.pubsubNumSub(CHANNEL));
            System.out.println("PUBSUB NUMPAT -> " + pub.pubsubNumPat());

            section("UNSUBSCRIBE / PUNSUBSCRIBE");
            listener.unsubscribe(CHANNEL);
            listener.punsubscribe(PATTERN);
            unsubscribed.await(2, TimeUnit.SECONDS);
            subscriberThread.join(2000);
            n = pub.publish(CHANNEL, "이제 아무도 안 듣는다");
            System.out.println("구독 해제 후 PUBLISH -> 받은 구독자 수: " + n + " (0이어야 정상)");
        } finally {
            subJedis.close();
        }

        System.out.println("\n정리 완료");
    }
}
