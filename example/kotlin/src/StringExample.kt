import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

/**
 * String(키/값) 커맨드 둘러보기.
 *
 * 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
 * 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
 * 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py와 같은 시나리오를
 * Kotlin(Jedis)으로 옮긴 것이다.
 *
 * 실행: run.sh에 있는 정확한 커맨드 참고 (StringExampleKt가 Kotlin이 top-level
 * fun main()에 자동으로 붙이는 클래스 이름이다).
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

// Jedis의 ttl()은 -1("만료 없음")/-2("키 없음")도 그냥 숫자로 돌려주므로,
// 사람이 읽기 좋은 형태로 바꿔준다.
fun formatTTL(ttl: Long): String = when (ttl) {
    -1L -> "-1 (만료 없음)"
    -2L -> "-2 (키 없음)"
    else -> "${ttl}s"
}

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        val demoKeys = arrayOf("greeting", "page:views", "user:1:session", "k1", "k2", "k3")
        jedis.del(*demoKeys)

        section("SET / GET")
        jedis.set("greeting", "hello world")
        println("GET greeting -> ${jedis.get("greeting")}")

        section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)")
        jedis.append("greeting", "!")
        println("APPEND 이후: ${jedis.get("greeting")}")
        println("GETRANGE greeting 0 4 -> ${jedis.getrange("greeting", 0, 4)}")
        jedis.setrange("greeting", 6, "redisX") // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
        println("SETRANGE 이후: ${jedis.get("greeting")}")

        section("MSET / MGET (여러 키 한 번에)")
        jedis.mset("k1", "v1", "k2", "v2", "k3", "v3")
        println("MGET k1 k2 k3 -> ${jedis.mget("k1", "k2", "k3")}")

        section("INCR / INCRBY (카운터)")
        jedis.del("page:views")
        repeat(3) { jedis.incr("page:views") }
        jedis.incrBy("page:views", 10)
        println("page:views -> ${jedis.get("page:views")}")

        section("EXPIRE / TTL / PERSIST")
        jedis.set("user:1:session", "token-abc")
        jedis.expire("user:1:session", 2)
        println("설정 직후 TTL -> ${formatTTL(jedis.ttl("user:1:session"))}")
        Thread.sleep(2200)
        println("2.2초 후 EXISTS -> ${jedis.exists("user:1:session")} (TTL이 지나 자동 삭제됨)")

        // redisX는 평범한 `SET ... EX ...`만 구현하고 레거시 `SETEX`는 등록돼
        // 있지 않다 — Jedis의 setex()는 SETEX를 그대로 보내므로 대신 SetParams를 쓴다.
        jedis.set("user:1:session", "token-def", SetParams.setParams().ex(60))
        jedis.persist("user:1:session")
        println("PERSIST 이후 TTL -> ${formatTTL(jedis.ttl("user:1:session"))}")

        val deleted = jedis.del(*demoKeys)
        println("\n정리: $deleted 개 키 삭제")
    }
}
