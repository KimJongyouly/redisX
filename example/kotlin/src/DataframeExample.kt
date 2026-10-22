import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.exceptions.JedisDataException
import java.nio.charset.StandardCharsets

/**
 * DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
 *
 * 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
 * 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
 * 있다. Jedis에는 DF.* 전용 타입드 메서드가 없으므로 sendCommand로 raw RESP
 * 커맨드를 직접 보낸다 — 다른 언어의 같은 이름(dataframe) 예제들과 동일한
 * 시나리오(example/python/dataframe_csv.py, example/golang/dataframe).
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

// Jedis에는 DF.* 같은 임의의 커맨드 이름을 위한 타입드 메서드가 없어서,
// ProtocolCommand(SAM 인터페이스, getRaw()만 있으면 됨)를 즉석에서 만들어
// sendCommand에 넘긴다.
fun cmd(name: String): ProtocolCommand = ProtocolCommand { name.toByteArray(StandardCharsets.UTF_8) }

fun str(o: Any?): String? = if (o == null) null else String(o as ByteArray, StandardCharsets.UTF_8)

val CSV_TEXT = "name,category,price,qty\n" +
    "widget,tools,9.99,100\n" +
    "gadget,electronics,29.99,50\n" +
    "gizmo,electronics,19.99,75\n" +
    "bolt,tools,0.99,500\n" +
    "screw,tools,0.49,1000\n"

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        val key = "inventory"
        jedis.del(key)

        section("DF.FROMCSV로 인라인 CSV 적재")
        try {
            jedis.sendCommand(cmd("DF.FROMCSV"), key, CSV_TEXT)
        } catch (e: JedisDataException) {
            if (e.message?.lowercase()?.contains("unknown command") == true) {
                println("DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.")
                println("cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.")
                return
            }
            throw e
        }

        section("DF.SHAPE / DF.COLUMNS로 구조 확인")
        println("DF.SHAPE inventory -> ${jedis.sendCommand(cmd("DF.SHAPE"), key)}")
        @Suppress("UNCHECKED_CAST")
        val cols = (jedis.sendCommand(cmd("DF.COLUMNS"), key) as List<Any>).map { str(it) }
        println("DF.COLUMNS inventory -> $cols")

        section("DF.QUERY로 카테고리별 재고 가치 집계")
        @Suppress("UNCHECKED_CAST")
        val rows = jedis.sendCommand(
            cmd("DF.QUERY"), key,
            "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category",
        ) as List<List<Any>>
        for (row in rows) {
            // RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string(바이트
            // 배열)으로 온다 — sendCommand로 받은 그대로는 그냥 ByteArray일 뿐이다.
            println("  %-12s total_value=%s".format(str(row[0]), str(row[1])))
        }

        section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)")
        println(str(jedis.sendCommand(cmd("DF.PRETTY"), key)))

        section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교")
        val exported = str(jedis.sendCommand(cmd("DF.TOCSV"), key))
        print(exported)
        println("원본과 동일? -> ${exported == CSV_TEXT}")

        val deleted = jedis.del(key)
        println("\n정리: $deleted 개 키 삭제")
    }
}
