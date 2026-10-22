import redis.clients.jedis.Jedis
import redis.clients.jedis.resps.Tuple
import scala.jdk.CollectionConverters.*

// ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
//
// ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
// 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
// 합산하는 것까지 — example/python/zset.py와 같은 시나리오.

def addr(): String = sys.env.getOrElse("REDISX_ADDR", "127.0.0.1:2417")

def section(title: String): Unit = println(s"\n--- $title ---")

def fmt(tuples: java.util.List[Tuple]): String =
  tuples.asScala.map(t => s"${t.getElement}=${t.getScore}").mkString("[", ", ", "]")

@main def ZsetExample(): Unit =
  val Array(host, port) = addr().split(":")
  val jedis = new Jedis(host, port.toInt)
  try
    val board = "leaderboard"
    val weekly = "leaderboard:weekly"
    val combined = "leaderboard:combined"
    jedis.del(board, weekly, combined)

    section("ZADD로 점수 매기기")
    jedis.zadd(board, Map("alice" -> 120.0, "bob" -> 98.0, "carol" -> 150.0, "dave" -> 110.0)
      .map((k, v) => (k, java.lang.Double.valueOf(v))).asJava)
    println(s"전체 랭킹 (ZREVRANGE, 점수 포함) -> ${fmt(jedis.zrevrangeWithScores(board, 0, -1))}")

    section("ZSCORE / ZREVRANK로 한 명만 조회")
    println(s"carol 점수 (ZSCORE) -> ${jedis.zscore(board, "carol")}")
    println(s"carol 등수, 0부터 (ZREVRANK) -> ${jedis.zrevrank(board, "carol")}")

    section("ZINCRBY로 점수 올리기 (보너스 획득)")
    jedis.zincrby(board, 30.0, "bob")
    println(s"bob 보너스 이후 전체 랭킹 -> ${fmt(jedis.zrevrangeWithScores(board, 0, -1))}")

    section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기")
    println(s"100점 이상 120점 이하 -> ${fmt(jedis.zrangeByScoreWithScores(board, 100.0, 120.0))}")

    section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거")
    val top = jedis.zpopmax(board)
    println(s"ZPOPMAX -> ${top.getElement}=${top.getScore}")
    val bottom = jedis.zpopmin(board)
    println(s"ZPOPMIN -> ${bottom.getElement}=${bottom.getScore}")
    println(s"남은 랭킹 -> ${fmt(jedis.zrevrangeWithScores(board, 0, -1))}")

    section("ZUNIONSTORE로 이번 주 보너스 점수와 합산")
    // 방금 뽑아서 지워진 것 복원
    jedis.zadd(board, Map("alice" -> 120.0, "bob" -> 128.0, "dave" -> 110.0)
      .map((k, v) => (k, java.lang.Double.valueOf(v))).asJava)
    jedis.zadd(weekly, Map("alice" -> 20.0, "bob" -> 5.0, "eve" -> 40.0)
      .map((k, v) => (k, java.lang.Double.valueOf(v))).asJava)
    jedis.zunionstore(combined, board, weekly)
    println(s"합산 랭킹 (ZUNIONSTORE) -> ${fmt(jedis.zrevrangeWithScores(combined, 0, -1))}")

    val deleted = jedis.del(board, weekly, combined)
    println(s"\n정리: $deleted 개 키 삭제")
  finally jedis.close()
