# redisX Kotlin 예제

Jedis(표준 동기 Java Redis 클라이언트)를 그대로 쓴다 — Kotlin 전용 Redis
클라이언트는 필요 없다. 이 저장소엔 Maven/Gradle이 없으므로 jar를 직접
받아서 `kotlinc`/`java`만으로 빌드·실행한다.

```bash
./fetch-deps.sh   # lib/에 jedis + 의존성 jar 받기 (한 번만)
./build.sh        # src/*.kt 각각을 out/<이름>.jar로 컴파일 (-include-runtime)
./run.sh StringExample          # key/value 둘러보기
./run.sh ListExample            # List — 작업 큐
./run.sh HashExample            # Hash — 사용자 프로필
./run.sh SetExample             # Set — 태그 집합 연산
./run.sh ZsetExample            # ZSet — 리더보드
./run.sh HyperloglogExample     # HyperLogLog — 근사 카디널리티
./run.sh SlowlogExample         # 파일 기반 SLOWLOG
./run.sh DataframeExample       # DF.* (서버가 --features polars로 빌드됐을 때만)
./run.sh PubsubExample          # 실시간 알림 채널 — SUBSCRIBE/PSUBSCRIBE/PUBLISH
```

기본으로 `127.0.0.1:2417`에 접속한다 — `REDISX_ADDR=host:port` 환경변수로
바꿀 수 있다.

Kotlin의 top-level `fun main()`은 컴파일되면 파일명 + `Kt`가 클래스 이름이
된다(`StringExample.kt` → `StringExampleKt`) — `run.sh`가 이미 그렇게
처리한다.
