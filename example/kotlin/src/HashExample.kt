import redis.clients.jedis.Jedis

/**
 * Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
 *
 * 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
 * Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
 * 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
 * example/python/hash.py와 같은 시나리오.
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        val userKey = "user:42"
        jedis.del(userKey)

        section("HSET으로 여러 필드를 한 번에 채우기")
        jedis.hset(userKey, linkedMapOf("name" to "jiyeon", "level" to "5", "coins" to "100", "logins" to "0"))
        println("HGETALL user:42 -> ${jedis.hgetAll(userKey)}")

        section("HGET / HMGET으로 필드 일부만 읽기")
        println("HGET user:42 name -> ${jedis.hget(userKey, "name")}")
        println("HMGET user:42 name level -> ${jedis.hmget(userKey, "name", "level")}")

        section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가")
        jedis.hincrBy(userKey, "coins", 50)
        jedis.hincrBy(userKey, "logins", 1)
        println("HGETALL user:42 -> ${jedis.hgetAll(userKey)}")

        section("HEXISTS / HDEL")
        println("HEXISTS user:42 level -> ${jedis.hexists(userKey, "level")}")
        jedis.hdel(userKey, "logins")
        println("HDEL 이후 HGETALL -> ${jedis.hgetAll(userKey)}")

        section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기")
        val raffle = "raffle:entries"
        jedis.del(raffle)
        jedis.hset(raffle, linkedMapOf("alice" to "1", "bob" to "1", "carol" to "1", "dave" to "1"))
        val winner = jedis.hrandfield(raffle)
        println("당첨자 한 명 -> $winner")
        println("HRANDFIELD -3 (중복 허용 3명 뽑기) -> ${jedis.hrandfield(raffle, -3)}")

        val deleted = jedis.del(userKey, raffle)
        println("\n정리: $deleted 개 키 삭제")
    }
}
