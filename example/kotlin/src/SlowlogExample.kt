import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/**
 * SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.
 *
 * 실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
 * 포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
 * 남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 CHANGE.md 1절 참고).
 * 그래서 redisX에서 SLOWLOG를 쓰려면 slowlog-file-path를 실제 경로로
 * 설정해야 하고(기본값은 ""로 비활성화), SLOWLOG GET/LEN은 매번 그 파일을
 * 읽어서 답한다 — 메모리에 아무것도 들고 있지 않다.
 *
 * Jedis의 타입드 slowlogGet()은 real Redis처럼 클라이언트 주소 필드가 항상
 * "ip:port" 형태라고 가정하고 HostAndPort로 파싱하는데, redisX는 (원본 C
 * 포크의 설계를 그대로 이어받아) 포트 없이 IP만 돌려줘서 그 파싱이 깨진다 —
 * example/java의 같은 예제에서 실제로 돌려보고서야 드러난 차이. 그래서
 * 여기서도 SLOWLOG GET만 raw sendCommand로 직접 받아 파싱한다(LEN/RESET은
 * 단순 정수/상태 응답이라 타입드 메서드 그대로 써도 문제없다).
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        section("현재 설정 확인 (기본값은 비활성화 상태)")
        println("slowlog-file-path -> ${jedis.configGet("slowlog-file-path")}")
        println("slowlog-log-slower-than (마이크로초) -> ${jedis.configGet("slowlog-log-slower-than")}")

        section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기")
        val logPath = Path.of(System.getProperty("java.io.tmpdir"), "redisx-slowlog-demo-kotlin.log")
        Files.deleteIfExists(logPath)
        jedis.configSet("slowlog-file-path", logPath.toString())
        jedis.configSet("slowlog-log-slower-than", "0") // 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

        section("몇 가지 커맨드를 실행해서 로그에 쌓기")
        jedis.set("k", "v")
        jedis.get("k")
        jedis.lpush("mylist", "a", "b", "c")
        jedis.del("mylist", "k")

        section("SLOWLOG LEN / GET으로 읽기")
        println("SLOWLOG LEN -> ${jedis.slowlogLen()}")
        val slowlogCmd = ProtocolCommand { "SLOWLOG".toByteArray(StandardCharsets.UTF_8) }
        @Suppress("UNCHECKED_CAST")
        val entries = jedis.sendCommand(slowlogCmd, "GET", "5") as List<List<Any>>
        for (entry in entries) { // id, timestamp, duration_us, argv, peer, client-name
            val id = entry[0] as Long
            val durationUs = entry[2] as Long
            @Suppress("UNCHECKED_CAST")
            val argv = entry[3] as List<ByteArray>
            val cmd = argv.joinToString(" ") { String(it, StandardCharsets.UTF_8) }
            val peer = String(entry[4] as ByteArray, StandardCharsets.UTF_8)
            println("  #$id  ${durationUs}us  $peer  $cmd")
        }

        section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)")
        print(Files.readString(logPath))

        section("SLOWLOG RESET으로 파일 비우기")
        jedis.slowlogReset()
        // 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
        // 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
        println("RESET 이후 SLOWLOG LEN -> ${jedis.slowlogLen()}")

        section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기")
        jedis.configSet("slowlog-log-slower-than", "10000") // redisX 기본값
        jedis.configSet("slowlog-file-path", "") // 기본값(비활성화)으로 복원
        Files.deleteIfExists(logPath)
        println("완료")
    }
}
