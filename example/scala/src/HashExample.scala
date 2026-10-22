import redis.clients.jedis.Jedis
import scala.jdk.CollectionConverters.*

// Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
//
// 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
// Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
// 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
// example/python/hash.py와 같은 시나리오.

def addr(): String = sys.env.getOrElse("REDISX_ADDR", "127.0.0.1:2417")

def section(title: String): Unit = println(s"\n--- $title ---")

@main def HashExample(): Unit =
  val Array(host, port) = addr().split(":")
  val jedis = new Jedis(host, port.toInt)
  try
    val userKey = "user:42"
    jedis.del(userKey)

    section("HSET으로 여러 필드를 한 번에 채우기")
    jedis.hset(userKey, Map("name" -> "jiyeon", "level" -> "5", "coins" -> "100", "logins" -> "0").asJava)
    println(s"HGETALL user:42 -> ${jedis.hgetAll(userKey)}")

    section("HGET / HMGET으로 필드 일부만 읽기")
    println(s"HGET user:42 name -> ${jedis.hget(userKey, "name")}")
    println(s"HMGET user:42 name level -> ${jedis.hmget(userKey, "name", "level")}")

    section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가")
    jedis.hincrBy(userKey, "coins", 50)
    jedis.hincrBy(userKey, "logins", 1)
    println(s"HGETALL user:42 -> ${jedis.hgetAll(userKey)}")

    section("HEXISTS / HDEL")
    println(s"HEXISTS user:42 level -> ${jedis.hexists(userKey, "level")}")
    jedis.hdel(userKey, "logins")
    println(s"HDEL 이후 HGETALL -> ${jedis.hgetAll(userKey)}")

    section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기")
    val raffle = "raffle:entries"
    jedis.del(raffle)
    jedis.hset(raffle, Map("alice" -> "1", "bob" -> "1", "carol" -> "1", "dave" -> "1").asJava)
    val winner = jedis.hrandfield(raffle)
    println(s"당첨자 한 명 -> $winner")
    println(s"HRANDFIELD -3 (중복 허용 3명 뽑기) -> ${jedis.hrandfield(raffle, -3)}")

    val deleted = jedis.del(userKey, raffle)
    println(s"\n정리: $deleted 개 키 삭제")
  finally jedis.close()
