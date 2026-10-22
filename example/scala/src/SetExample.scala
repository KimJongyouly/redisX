import redis.clients.jedis.Jedis

// Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
//
// 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
// 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
// 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
// example/python/set.py와 같은 시나리오.

def addr(): String = sys.env.getOrElse("REDISX_ADDR", "127.0.0.1:2417")

def section(title: String): Unit = println(s"\n--- $title ---")

@main def SetExample(): Unit =
  val Array(host, port) = addr().split(":")
  val jedis = new Jedis(host, port.toInt)
  try
    val postA = "post:1:tags"
    val postB = "post:2:tags"
    val archive = "tags:archive"
    jedis.del(postA, postB, archive)

    section("SADD로 글마다 태그 달기")
    jedis.sadd(postA, "rust", "redis", "database")
    jedis.sadd(postB, "rust", "web", "async")
    println(s"post:1:tags -> ${jedis.smembers(postA)}")
    println(s"post:2:tags -> ${jedis.smembers(postB)}")

    section("SINTER / SUNION / SDIFF로 집합 연산")
    println(s"둘 다 쓴 태그 (SINTER) -> ${jedis.sinter(postA, postB)}")
    println(s"둘 중 하나라도 쓴 태그 (SUNION) -> ${jedis.sunion(postA, postB)}")
    println(s"post:1에만 있는 태그 (SDIFF) -> ${jedis.sdiff(postA, postB)}")

    section("SINTERSTORE로 교집합을 새 키에 저장")
    val commonKey = "tags:common"
    jedis.del(commonKey)
    jedis.sinterstore(commonKey, postA, postB)
    println(s"tags:common -> ${jedis.smembers(commonKey)}")

    section("SISMEMBER / SCARD")
    println(s"post:1에 'redis' 태그가 있나? -> ${jedis.sismember(postA, "redis")}")
    println(s"post:1 태그 개수 (SCARD) -> ${jedis.scard(postA)}")

    section("SMOVE로 태그 하나를 보관함으로 옮기기")
    jedis.smove(postA, archive, "database")
    println(s"post:1:tags -> ${jedis.smembers(postA)}")
    println(s"tags:archive -> ${jedis.smembers(archive)}")

    section("SPOP으로 무작위로 하나 뽑아서 제거")
    val popped = jedis.spop(postB)
    println(s"뽑힌 태그 -> $popped / 남은 post:2:tags -> ${jedis.smembers(postB)}")

    val deleted = jedis.del(postA, postB, archive, commonKey)
    println(s"\n정리: $deleted 개 키 삭제")
  finally jedis.close()
