import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;

/**
 * Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
 *
 * 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
 * Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
 * 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
 * example/python/hash.py와 같은 시나리오.
 *
 * 실행: java -cp "lib/*:out" HashExample
 */
public class HashExample {
    static String addr() {
        String a = System.getenv("REDISX_ADDR");
        return a != null ? a : "127.0.0.1:2417";
    }

    static void section(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    public static void main(String[] args) {
        String[] parts = addr().split(":");
        try (Jedis jedis = new Jedis(parts[0], Integer.parseInt(parts[1]))) {
            String userKey = "user:42";
            jedis.del(userKey);

            section("HSET으로 여러 필드를 한 번에 채우기");
            Map<String, String> profile = new LinkedHashMap<>();
            profile.put("name", "jiyeon");
            profile.put("level", "5");
            profile.put("coins", "100");
            profile.put("logins", "0");
            jedis.hset(userKey, profile);
            System.out.println("HGETALL user:42 -> " + jedis.hgetAll(userKey));

            section("HGET / HMGET으로 필드 일부만 읽기");
            System.out.println("HGET user:42 name -> " + jedis.hget(userKey, "name"));
            System.out.println("HMGET user:42 name level -> " + jedis.hmget(userKey, "name", "level"));

            section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가");
            jedis.hincrBy(userKey, "coins", 50);
            jedis.hincrBy(userKey, "logins", 1);
            System.out.println("HGETALL user:42 -> " + jedis.hgetAll(userKey));

            section("HEXISTS / HDEL");
            System.out.println("HEXISTS user:42 level -> " + jedis.hexists(userKey, "level"));
            jedis.hdel(userKey, "logins");
            System.out.println("HDEL 이후 HGETALL -> " + jedis.hgetAll(userKey));

            section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기");
            String raffle = "raffle:entries";
            jedis.del(raffle);
            Map<String, String> entries = new LinkedHashMap<>();
            entries.put("alice", "1");
            entries.put("bob", "1");
            entries.put("carol", "1");
            entries.put("dave", "1");
            jedis.hset(raffle, entries);
            String winner = jedis.hrandfield(raffle);
            System.out.println("당첨자 한 명 -> " + winner);
            List<String> threeWithDupes = jedis.hrandfield(raffle, -3);
            System.out.println("HRANDFIELD -3 (중복 허용 3명 뽑기) -> " + threeWithDupes);

            long deleted = jedis.del(userKey, raffle);
            System.out.println("\n정리: " + deleted + " 개 키 삭제");
        }
    }
}
