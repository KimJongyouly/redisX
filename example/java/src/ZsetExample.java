import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

/**
 * ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
 *
 * ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
 * 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
 * 합산하는 것까지 — example/python/zset.py와 같은 시나리오.
 *
 * 실행: java -cp "lib/*:out" ZsetExample
 */
public class ZsetExample {
    static String addr() {
        String a = System.getenv("REDISX_ADDR");
        return a != null ? a : "127.0.0.1:2417";
    }

    static void section(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    static String fmt(List<Tuple> tuples) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < tuples.size(); i++) {
            Tuple t = tuples.get(i);
            if (i > 0) sb.append(", ");
            sb.append(t.getElement()).append("=").append(t.getScore());
        }
        return sb.append("]").toString();
    }

    public static void main(String[] args) {
        String[] parts = addr().split(":");
        try (Jedis jedis = new Jedis(parts[0], Integer.parseInt(parts[1]))) {
            String board = "leaderboard", weekly = "leaderboard:weekly", combined = "leaderboard:combined";
            jedis.del(board, weekly, combined);

            section("ZADD로 점수 매기기");
            Map<String, Double> initial = new LinkedHashMap<>();
            initial.put("alice", 120.0);
            initial.put("bob", 98.0);
            initial.put("carol", 150.0);
            initial.put("dave", 110.0);
            jedis.zadd(board, initial);
            System.out.println("전체 랭킹 (ZREVRANGE, 점수 포함) -> " + fmt(jedis.zrevrangeWithScores(board, 0, -1)));

            section("ZSCORE / ZREVRANK로 한 명만 조회");
            System.out.println("carol 점수 (ZSCORE) -> " + jedis.zscore(board, "carol"));
            System.out.println("carol 등수, 0부터 (ZREVRANK) -> " + jedis.zrevrank(board, "carol"));

            section("ZINCRBY로 점수 올리기 (보너스 획득)");
            jedis.zincrby(board, 30, "bob");
            System.out.println("bob 보너스 이후 전체 랭킹 -> " + fmt(jedis.zrevrangeWithScores(board, 0, -1)));

            section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기");
            System.out.println("100점 이상 120점 이하 -> " + fmt(jedis.zrangeByScoreWithScores(board, 100, 120)));

            section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거");
            Tuple top = jedis.zpopmax(board);
            System.out.println("ZPOPMAX -> " + top.getElement() + "=" + top.getScore());
            Tuple bottom = jedis.zpopmin(board);
            System.out.println("ZPOPMIN -> " + bottom.getElement() + "=" + bottom.getScore());
            System.out.println("남은 랭킹 -> " + fmt(jedis.zrevrangeWithScores(board, 0, -1)));

            section("ZUNIONSTORE로 이번 주 보너스 점수와 합산");
            Map<String, Double> restored = new LinkedHashMap<>(); // 방금 뽑아서 지워진 것 복원
            restored.put("alice", 120.0);
            restored.put("bob", 128.0);
            restored.put("dave", 110.0);
            jedis.zadd(board, restored);
            Map<String, Double> weeklyBonus = new LinkedHashMap<>();
            weeklyBonus.put("alice", 20.0);
            weeklyBonus.put("bob", 5.0);
            weeklyBonus.put("eve", 40.0);
            jedis.zadd(weekly, weeklyBonus);
            jedis.zunionstore(combined, board, weekly);
            System.out.println("합산 랭킹 (ZUNIONSTORE) -> " + fmt(jedis.zrevrangeWithScores(combined, 0, -1)));

            long deleted = jedis.del(board, weekly, combined);
            System.out.println("\n정리: " + deleted + " 개 키 삭제");
        }
    }
}
