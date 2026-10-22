import redis.clients.jedis.Jedis
import redis.clients.jedis.resps.Tuple

/**
 * ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
 *
 * ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
 * 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
 * 합산하는 것까지 — example/python/zset.py와 같은 시나리오.
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

fun fmt(tuples: List<Tuple>): String =
    tuples.joinToString(prefix = "[", postfix = "]") { "${it.element}=${it.score}" }

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        val board = "leaderboard"
        val weekly = "leaderboard:weekly"
        val combined = "leaderboard:combined"
        jedis.del(board, weekly, combined)

        section("ZADD로 점수 매기기")
        jedis.zadd(board, linkedMapOf("alice" to 120.0, "bob" to 98.0, "carol" to 150.0, "dave" to 110.0))
        println("전체 랭킹 (ZREVRANGE, 점수 포함) -> ${fmt(jedis.zrevrangeWithScores(board, 0, -1))}")

        section("ZSCORE / ZREVRANK로 한 명만 조회")
        println("carol 점수 (ZSCORE) -> ${jedis.zscore(board, "carol")}")
        println("carol 등수, 0부터 (ZREVRANK) -> ${jedis.zrevrank(board, "carol")}")

        section("ZINCRBY로 점수 올리기 (보너스 획득)")
        jedis.zincrby(board, 30.0, "bob")
        println("bob 보너스 이후 전체 랭킹 -> ${fmt(jedis.zrevrangeWithScores(board, 0, -1))}")

        section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기")
        println("100점 이상 120점 이하 -> ${fmt(jedis.zrangeByScoreWithScores(board, 100.0, 120.0))}")

        section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거")
        val top = jedis.zpopmax(board)
        println("ZPOPMAX -> ${top.element}=${top.score}")
        val bottom = jedis.zpopmin(board)
        println("ZPOPMIN -> ${bottom.element}=${bottom.score}")
        println("남은 랭킹 -> ${fmt(jedis.zrevrangeWithScores(board, 0, -1))}")

        section("ZUNIONSTORE로 이번 주 보너스 점수와 합산")
        jedis.zadd(board, linkedMapOf("alice" to 120.0, "bob" to 128.0, "dave" to 110.0)) // 방금 뽑아서 지워진 것 복원
        jedis.zadd(weekly, linkedMapOf("alice" to 20.0, "bob" to 5.0, "eve" to 40.0))
        jedis.zunionstore(combined, board, weekly)
        println("합산 랭킹 (ZUNIONSTORE) -> ${fmt(jedis.zrevrangeWithScores(combined, 0, -1))}")

        val deleted = jedis.del(board, weekly, combined)
        println("\n정리: $deleted 개 키 삭제")
    }
}
