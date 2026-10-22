import java.util.Random;
import redis.clients.jedis.Jedis;

/**
 * HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
 *
 * 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
 * HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
 * 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
 * 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
 * example/python/hyperloglog.py와 같은 시나리오.
 *
 * 실행: java -cp "lib/*:out" HyperloglogExample
 */
public class HyperloglogExample {
    static String addr() {
        String a = System.getenv("REDISX_ADDR");
        return a != null ? a : "127.0.0.1:2417";
    }

    static void section(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    public static void main(String[] args) {
        String[] parts = addr().split(":");
        try (Jedis jedis = new Jedis(parts[0], Integer.parseInt(parts[1]))) {
            String hllMon = "visitors:mon:hll", hllTue = "visitors:tue:hll", hllWeek = "visitors:week:hll";
            String exactSet = "visitors:mon:exact";
            jedis.del(hllMon, hllTue, hllWeek, exactSet);

            section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기");
            Random r = new Random(0);
            // 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
            // 3,000명 정도의 유니크 방문자만 나오게 만든다.
            for (int i = 0; i < 10_000; i++) {
                String visitorId = "user-" + (r.nextInt(3000) + 1);
                jedis.pfadd(hllMon, visitorId);
                jedis.sadd(exactSet, visitorId);
            }

            long exactCount = jedis.scard(exactSet);
            long approxCount = jedis.pfcount(hllMon);
            double errorPct = Math.abs(approxCount - exactCount) / (double) exactCount * 100;
            System.out.printf("정확한 유니크 방문자 수 (SCARD)   -> %d%n", exactCount);
            System.out.printf("HyperLogLog 근사치 (PFCOUNT)      -> %d%n", approxCount);
            System.out.printf("오차율                             -> %.2f%% (표준 오차는 약 0.81%%)%n", errorPct);

            section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)");
            System.out.println("Set 메모리 사용량 근사치 (MEMORY USAGE) -> " + jedis.memoryUsage(exactSet) + " bytes");
            System.out.println("HLL 메모리 사용량 근사치 (MEMORY USAGE) -> " + jedis.memoryUsage(hllMon) + " bytes");

            section("PFMERGE로 이틀치 방문자 로그 합치기");
            for (int i = 0; i < 5_000; i++) {
                jedis.pfadd(hllTue, "user-" + (r.nextInt(4000) + 2000)); // 월요일과 일부 겹침
            }
            jedis.pfmerge(hllWeek, hllMon, hllTue);
            System.out.println("월요일 근사 방문자 수 -> " + jedis.pfcount(hllMon));
            System.out.println("화요일 근사 방문자 수 -> " + jedis.pfcount(hllTue));
            System.out.println("합산(PFMERGE) 근사 방문자 수 -> " + jedis.pfcount(hllWeek)
                    + " (단순 합보다 적은 건 겹치는 방문자가 있어서)");

            long deleted = jedis.del(hllMon, hllTue, hllWeek, exactSet);
            System.out.println("\n정리: " + deleted + " 개 키 삭제");
        }
    }
}
