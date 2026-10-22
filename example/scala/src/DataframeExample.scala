import redis.clients.jedis.Jedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.exceptions.JedisDataException
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*

// DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
//
// 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
// 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
// 있다. Jedis에는 DF.* 전용 타입드 메서드가 없으므로 sendCommand로 raw RESP
// 커맨드를 직접 보낸다 — 다른 언어의 같은 이름(dataframe) 예제들과 동일한
// 시나리오(example/python/dataframe_csv.py, example/golang/dataframe).

def addr(): String = sys.env.getOrElse("REDISX_ADDR", "127.0.0.1:2417")

def section(title: String): Unit = println(s"\n--- $title ---")

// Jedis에는 DF.* 같은 임의의 커맨드 이름을 위한 타입드 메서드가 없어서,
// ProtocolCommand(SAM 인터페이스, getRaw()만 있으면 됨)를 즉석에서 만들어
// sendCommand에 넘긴다.
def cmd(name: String): ProtocolCommand = () => name.getBytes(StandardCharsets.UTF_8)

def str(o: Object): String = if o == null then null else new String(o.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8)

val CsvText = "name,category,price,qty\n" +
  "widget,tools,9.99,100\n" +
  "gadget,electronics,29.99,50\n" +
  "gizmo,electronics,19.99,75\n" +
  "bolt,tools,0.99,500\n" +
  "screw,tools,0.49,1000\n"

@main def DataframeExample(): Unit =
  val Array(host, port) = addr().split(":")
  val jedis = new Jedis(host, port.toInt)
  try
    val key = "inventory"
    jedis.del(key)

    section("DF.FROMCSV로 인라인 CSV 적재")
    try jedis.sendCommand(cmd("DF.FROMCSV"), key, CsvText)
    catch
      case e: JedisDataException =>
        if e.getMessage != null && e.getMessage.toLowerCase.contains("unknown command") then
          println("DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.")
          println("cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.")
          return
        else throw e

    section("DF.SHAPE / DF.COLUMNS로 구조 확인")
    println(s"DF.SHAPE inventory -> ${jedis.sendCommand(cmd("DF.SHAPE"), key)}")
    val cols = jedis.sendCommand(cmd("DF.COLUMNS"), key)
      .asInstanceOf[java.util.List[Object]].asScala.map(str)
    println(s"DF.COLUMNS inventory -> $cols")

    section("DF.QUERY로 카테고리별 재고 가치 집계")
    val rows = jedis.sendCommand(
      cmd("DF.QUERY"), key,
      "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category",
    ).asInstanceOf[java.util.List[java.util.List[Object]]]
    for row <- rows.asScala do
      // RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string(바이트
      // 배열)으로 온다 — sendCommand로 받은 그대로는 그냥 Array[Byte]일 뿐이다.
      println(f"  ${str(row.get(0))}%-12s total_value=${str(row.get(1))}")

    section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)")
    println(str(jedis.sendCommand(cmd("DF.PRETTY"), key)))

    section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교")
    val exported = str(jedis.sendCommand(cmd("DF.TOCSV"), key))
    print(exported)
    println(s"원본과 동일? -> ${exported == CsvText}")

    val deleted = jedis.del(key)
    println(s"\n정리: $deleted 개 키 삭제")
  finally jedis.close()
