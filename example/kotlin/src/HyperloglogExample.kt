import redis.clients.jedis.Jedis
import kotlin.random.Random

/**
 * HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
 *
 * 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
 * HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
 * 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
 * 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
 * example/python/hyperloglog.py와 같은 시나리오.
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        val hllMon = "visitors:mon:hll"
        val hllTue = "visitors:tue:hll"
        val hllWeek = "visitors:week:hll"
        val exactSet = "visitors:mon:exact"
        jedis.del(hllMon, hllTue, hllWeek, exactSet)

        section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기")
        val r = Random(0)
        // 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
        // 3,000명 정도의 유니크 방문자만 나오게 만든다.
        repeat(10_000) {
            val visitorId = "user-${r.nextInt(3000) + 1}"
            jedis.pfadd(hllMon, visitorId)
            jedis.sadd(exactSet, visitorId)
        }

        val exactCount = jedis.scard(exactSet)
        val approxCount = jedis.pfcount(hllMon)
        val errorPct = kotlin.math.abs(approxCount - exactCount).toDouble() / exactCount * 100
        println("정확한 유니크 방문자 수 (SCARD)   -> $exactCount")
        println("HyperLogLog 근사치 (PFCOUNT)      -> $approxCount")
        println("오차율                             -> ${"%.2f".format(errorPct)}% (표준 오차는 약 0.81%)")

        section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)")
        println("Set 메모리 사용량 근사치 (MEMORY USAGE) -> ${jedis.memoryUsage(exactSet)} bytes")
        println("HLL 메모리 사용량 근사치 (MEMORY USAGE) -> ${jedis.memoryUsage(hllMon)} bytes")

        section("PFMERGE로 이틀치 방문자 로그 합치기")
        repeat(5_000) {
            jedis.pfadd(hllTue, "user-${r.nextInt(4000) + 2000}") // 월요일과 일부 겹침
        }
        jedis.pfmerge(hllWeek, hllMon, hllTue)
        println("월요일 근사 방문자 수 -> ${jedis.pfcount(hllMon)}")
        println("화요일 근사 방문자 수 -> ${jedis.pfcount(hllTue)}")
        println("합산(PFMERGE) 근사 방문자 수 -> ${jedis.pfcount(hllWeek)} (단순 합보다 적은 건 겹치는 방문자가 있어서)")

        val deleted = jedis.del(hllMon, hllTue, hllWeek, exactSet)
        println("\n정리: $deleted 개 키 삭제")
    }
}
