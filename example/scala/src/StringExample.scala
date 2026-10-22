import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

// String(키/값) 커맨드 둘러보기.
//
// 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
// 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
// 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py와 같은 시나리오를
// Scala(Jedis)로 옮긴 것이다.

def addr(): String = sys.env.getOrElse("REDISX_ADDR", "127.0.0.1:2417")

def section(title: String): Unit = println(s"\n--- $title ---")

// Jedis의 ttl()은 -1("만료 없음")/-2("키 없음")도 그냥 숫자로 돌려주므로,
// 사람이 읽기 좋은 형태로 바꿔준다.
def formatTTL(ttl: Long): String = ttl match
  case -1 => "-1 (만료 없음)"
  case -2 => "-2 (키 없음)"
  case n  => s"${n}s"

@main def StringExample(): Unit =
  val Array(host, port) = addr().split(":")
  val jedis = new Jedis(host, port.toInt)
  try
    val demoKeys = Array("greeting", "page:views", "user:1:session", "k1", "k2", "k3")
    jedis.del(demoKeys*)

    section("SET / GET")
    jedis.set("greeting", "hello world")
    println(s"GET greeting -> ${jedis.get("greeting")}")

    section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)")
    jedis.append("greeting", "!")
    println(s"APPEND 이후: ${jedis.get("greeting")}")
    println(s"GETRANGE greeting 0 4 -> ${jedis.getrange("greeting", 0, 4)}")
    jedis.setrange("greeting", 6, "redisX") // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
    println(s"SETRANGE 이후: ${jedis.get("greeting")}")

    section("MSET / MGET (여러 키 한 번에)")
    jedis.mset("k1", "v1", "k2", "v2", "k3", "v3")
    println(s"MGET k1 k2 k3 -> ${jedis.mget("k1", "k2", "k3")}")

    section("INCR / INCRBY (카운터)")
    jedis.del("page:views")
    for _ <- 0 until 3 do jedis.incr("page:views")
    jedis.incrBy("page:views", 10)
    println(s"page:views -> ${jedis.get("page:views")}")

    section("EXPIRE / TTL / PERSIST")
    jedis.set("user:1:session", "token-abc")
    jedis.expire("user:1:session", 2)
    println(s"설정 직후 TTL -> ${formatTTL(jedis.ttl("user:1:session"))}")
    Thread.sleep(2200)
    println(s"2.2초 후 EXISTS -> ${jedis.exists("user:1:session")} (TTL이 지나 자동 삭제됨)")

    // redisX는 평범한 `SET ... EX ...`만 구현하고 레거시 `SETEX`는 등록돼
    // 있지 않다 — Jedis의 setex()는 SETEX를 그대로 보내므로 대신 SetParams를 쓴다.
    jedis.set("user:1:session", "token-def", SetParams.setParams().ex(60))
    jedis.persist("user:1:session")
    println(s"PERSIST 이후 TTL -> ${formatTTL(jedis.ttl("user:1:session"))}")

    val deleted = jedis.del(demoKeys*)
    println(s"\n정리: $deleted 개 키 삭제")
  finally jedis.close()
