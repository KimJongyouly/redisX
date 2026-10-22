import java.nio.charset.StandardCharsets;
import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
 *
 * 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
 * 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
 * 있다. Jedis에는 DF.* 전용 타입드 메서드가 없으므로 sendCommand로 raw RESP
 * 커맨드를 직접 보낸다 — 다른 언어의 같은 이름(dataframe) 예제들과 동일한
 * 시나리오(example/python/dataframe_csv.py, example/golang/dataframe).
 *
 * 실행: java -cp "lib/*:out" DataframeExample
 */
public class DataframeExample {
    static String addr() {
        String a = System.getenv("REDISX_ADDR");
        return a != null ? a : "127.0.0.1:2417";
    }

    static void section(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    // Jedis에는 DF.* 같은 임의의 커맨드 이름을 위한 타입드 메서드가 없어서,
    // ProtocolCommand(함수형 인터페이스, getRaw()만 있으면 됨)를 즉석에서
    // 만들어 sendCommand에 넘긴다.
    static ProtocolCommand cmd(String name) {
        return () -> name.getBytes(StandardCharsets.UTF_8);
    }

    static String str(Object o) {
        return o == null ? null : new String((byte[]) o, StandardCharsets.UTF_8);
    }

    static final String CSV_TEXT = "name,category,price,qty\n"
            + "widget,tools,9.99,100\n"
            + "gadget,electronics,29.99,50\n"
            + "gizmo,electronics,19.99,75\n"
            + "bolt,tools,0.99,500\n"
            + "screw,tools,0.49,1000\n";

    public static void main(String[] args) {
        String[] parts = addr().split(":");
        try (Jedis jedis = new Jedis(parts[0], Integer.parseInt(parts[1]))) {
            String key = "inventory";
            jedis.del(key);

            section("DF.FROMCSV로 인라인 CSV 적재");
            try {
                jedis.sendCommand(cmd("DF.FROMCSV"), key, CSV_TEXT);
            } catch (JedisDataException e) {
                if (e.getMessage() != null && e.getMessage().toLowerCase().contains("unknown command")) {
                    System.out.println("DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.");
                    System.out.println("cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.");
                    return;
                }
                throw e;
            }

            section("DF.SHAPE / DF.COLUMNS로 구조 확인");
            Object shape = jedis.sendCommand(cmd("DF.SHAPE"), key);
            System.out.println("DF.SHAPE inventory -> " + shape);
            @SuppressWarnings("unchecked")
            List<Object> cols = (List<Object>) jedis.sendCommand(cmd("DF.COLUMNS"), key);
            cols.replaceAll(DataframeExample::str);
            System.out.println("DF.COLUMNS inventory -> " + cols);

            section("DF.QUERY로 카테고리별 재고 가치 집계");
            @SuppressWarnings("unchecked")
            List<Object> rows = (List<Object>) jedis.sendCommand(cmd("DF.QUERY"), key,
                    "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category");
            for (Object rowObj : rows) {
                @SuppressWarnings("unchecked")
                List<Object> row = (List<Object>) rowObj;
                // RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string(바이트
                // 배열)으로 온다 — sendCommand로 받은 그대로는 그냥 byte[]일 뿐이다.
                System.out.printf("  %-12s total_value=%s%n", str(row.get(0)), str(row.get(1)));
            }

            section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)");
            Object pretty = jedis.sendCommand(cmd("DF.PRETTY"), key);
            System.out.println(str(pretty));

            section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교");
            String exported = str(jedis.sendCommand(cmd("DF.TOCSV"), key));
            System.out.print(exported);
            System.out.println("원본과 동일? -> " + exported.equals(CSV_TEXT));

            long deleted = jedis.del(key);
            System.out.println("\n정리: " + deleted + " 개 키 삭제");
        }
    }
}
