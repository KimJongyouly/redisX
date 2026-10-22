import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

/**
 * String(키/값) 커맨드 둘러보기.
 *
 * 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
 * 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
 * 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py와 같은 시나리오를
 * Java(Jedis)로 옮긴 것이다.
 *
 * 실행: java -cp "lib/*:out" StringExample
 */
public class StringExample {
    static String addr() {
        String a = System.getenv("REDISX_ADDR");
        return a != null ? a : "127.0.0.1:2417";
    }

    static void section(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    public static void main(String[] args) throws InterruptedException {
        String[] parts = addr().split(":");
        try (Jedis jedis = new Jedis(parts[0], Integer.parseInt(parts[1]))) {
            String[] demoKeys = {"greeting", "page:views", "user:1:session", "k1", "k2", "k3"};
            jedis.del(demoKeys);

            section("SET / GET");
            jedis.set("greeting", "hello world");
            System.out.println("GET greeting -> " + jedis.get("greeting"));

            section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)");
            jedis.append("greeting", "!");
            System.out.println("APPEND 이후: " + jedis.get("greeting"));
            System.out.println("GETRANGE greeting 0 4 -> " + jedis.getrange("greeting", 0, 4));
            jedis.setrange("greeting", 6, "redisX"); // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
            System.out.println("SETRANGE 이후: " + jedis.get("greeting"));

            section("MSET / MGET (여러 키 한 번에)");
            jedis.mset("k1", "v1", "k2", "v2", "k3", "v3");
            System.out.println("MGET k1 k2 k3 -> " + jedis.mget("k1", "k2", "k3"));

            section("INCR / INCRBY (카운터)");
            jedis.del("page:views");
            for (int i = 0; i < 3; i++) {
                jedis.incr("page:views");
            }
            jedis.incrBy("page:views", 10);
            System.out.println("page:views -> " + jedis.get("page:views"));

            section("EXPIRE / TTL / PERSIST");
            jedis.set("user:1:session", "token-abc");
            jedis.expire("user:1:session", 2);
            System.out.println("설정 직후 TTL -> " + formatTTL(jedis.ttl("user:1:session")));
            Thread.sleep(2200);
            System.out.println("2.2초 후 EXISTS -> " + jedis.exists("user:1:session") + " (TTL이 지나 자동 삭제됨)");

            // redisX implements plain `SET ... EX ...`, not the legacy
            // `SETEX` command — Jedis's own setex() sends SETEX literally,
            // which redisX doesn't register at all.
            jedis.set("user:1:session", "token-def", SetParams.setParams().ex(60));
            jedis.persist("user:1:session");
            System.out.println("PERSIST 이후 TTL -> " + formatTTL(jedis.ttl("user:1:session")));

            long deleted = jedis.del(demoKeys);
            System.out.println("\n정리: " + deleted + " 개 키 삭제");
        }
    }

    // Jedis의 ttl()은 -1("만료 없음")/-2("키 없음")도 그냥 숫자로 돌려주므로,
    // 사람이 읽기 좋은 형태로 바꿔준다.
    static String formatTTL(long ttl) {
        if (ttl == -1) return "-1 (만료 없음)";
        if (ttl == -2) return "-2 (키 없음)";
        return ttl + "s";
    }
}
