import redis.clients.jedis.Jedis;

/**
 * Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
 *
 * 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
 * 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
 * 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
 * example/python/set.py와 같은 시나리오.
 *
 * 실행: java -cp "lib/*:out" SetExample
 */
public class SetExample {
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
            String postA = "post:1:tags", postB = "post:2:tags", archive = "tags:archive";
            jedis.del(postA, postB, archive);

            section("SADD로 글마다 태그 달기");
            jedis.sadd(postA, "rust", "redis", "database");
            jedis.sadd(postB, "rust", "web", "async");
            System.out.println("post:1:tags -> " + jedis.smembers(postA));
            System.out.println("post:2:tags -> " + jedis.smembers(postB));

            section("SINTER / SUNION / SDIFF로 집합 연산");
            System.out.println("둘 다 쓴 태그 (SINTER) -> " + jedis.sinter(postA, postB));
            System.out.println("둘 중 하나라도 쓴 태그 (SUNION) -> " + jedis.sunion(postA, postB));
            System.out.println("post:1에만 있는 태그 (SDIFF) -> " + jedis.sdiff(postA, postB));

            section("SINTERSTORE로 교집합을 새 키에 저장");
            String commonKey = "tags:common";
            jedis.del(commonKey);
            jedis.sinterstore(commonKey, postA, postB);
            System.out.println("tags:common -> " + jedis.smembers(commonKey));

            section("SISMEMBER / SCARD");
            System.out.println("post:1에 'redis' 태그가 있나? -> " + jedis.sismember(postA, "redis"));
            System.out.println("post:1 태그 개수 (SCARD) -> " + jedis.scard(postA));

            section("SMOVE로 태그 하나를 보관함으로 옮기기");
            jedis.smove(postA, archive, "database");
            System.out.println("post:1:tags -> " + jedis.smembers(postA));
            System.out.println("tags:archive -> " + jedis.smembers(archive));

            section("SPOP으로 무작위로 하나 뽑아서 제거");
            String popped = jedis.spop(postB);
            System.out.println("뽑힌 태그 -> " + popped + " / 남은 post:2:tags -> " + jedis.smembers(postB));

            long deleted = jedis.del(postA, postB, archive, commonKey);
            System.out.println("\n정리: " + deleted + " 개 키 삭제");
        }
    }
}
