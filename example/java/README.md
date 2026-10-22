# redisX Java 예제

[Jedis](https://github.com/redis/jedis)(표준 동기 Java Redis 클라이언트)로 쓴
예제들이다. 이 저장소엔 Maven/Gradle이 없으므로(바닐라 JDK만 가정), 빌드
도구 없이 `javac`/`java`만으로 돌아가게 되어 있다.

```bash
./fetch-deps.sh          # lib/에 jedis + 런타임 의존성 jar 다운로드 (최초 1회)
./build.sh                # src/*.java -> out/
./run.sh StringExample    # 기본 접속 대상: 127.0.0.1:2417
```

`lib/`, `out/`은 둘 다 재생성 가능한 산출물이라 `.gitignore` 대상이다 — 이미
받아놨다면 `fetch-deps.sh`는 다시 받지 않고 건너뛴다.

접속 대상은 `REDISX_ADDR` 환경변수(`host:port`)로 바꿀 수 있다:

```bash
REDISX_ADDR=127.0.0.1:17601 ./run.sh SlowlogExample
```

## 예제 목록

| 클래스 | 다루는 내용 |
|---|---|
| `StringExample` | SET/GET/APPEND/GETRANGE/SETRANGE/MSET/INCR/EXPIRE/TTL |
| `ListExample` | LPUSH/RPUSH/LINSERT/LREM/LTRIM/LPOS/LMOVE — 작업 큐 예제 |
| `HashExample` | HSET/HGETALL/HINCRBY/HRANDFIELD — 사용자 프로필 예제 |
| `SetExample` | SADD/SINTER/SUNION/SDIFF/SMOVE/SPOP — 태그 집합 연산 예제 |
| `ZsetExample` | ZADD/ZRANGEBYSCORE/ZINCRBY/ZUNIONSTORE — 리더보드 예제 |
| `HyperloglogExample` | PFADD/PFCOUNT/PFMERGE — 근사 카디널리티를 실제 Set과 비교 |
| `SlowlogExample` | redisX의 파일 기반 SLOWLOG를 켜고 직접 읽어보기 |
| `DataframeExample` | 인라인 CSV를 DF.FROMCSV로 적재하고 DF.QUERY/DF.PRETTY/DF.TOCSV까지(`--features polars` 빌드 필요) |
| `PubsubExample` | SUBSCRIBE/PSUBSCRIBE/PUBLISH/PUBSUB CHANNELS·NUMSUB·NUMPAT/UNSUBSCRIBE — 실시간 알림 채널 예제 |

`example/python`, `example/golang`과 같은 시나리오(같은 데모 키, 같은
서술)를 Java로 옮긴 것이다.

## Jedis API 관련 메모 (redisX와 다르게 동작하는 부분)

- **`SETEX`는 redisX에 없는 커맨드다** — redisX는 레거시 `SETEX`/`PSETEX`를
  따로 구현하지 않고 `SET key val EX seconds` 형태만 지원한다. `Jedis.setex()`는
  리터럴 `SETEX` 커맨드를 보내므로 `unknown command 'SETEX'` 에러가 난다 —
  `jedis.set(key, val, SetParams.setParams().ex(seconds))`를 대신 쓸 것
  (`StringExample` 참고).
- **`SLOWLOG GET`의 클라이언트 주소 필드는 포트가 없다** — 원본 C 포크의
  설계를 그대로 이어받아 redisX는 `"127.0.0.1"`만 돌려주는데(저장소 루트
  `CHANGE.md` 1절 참고), Jedis의 타입드 `slowlogGet()`은 이 필드가 항상
  `"ip:port"` 형태라고 가정하고 `HostAndPort`로 파싱하려다
  `StringIndexOutOfBoundsException`으로 깨진다. `SlowlogExample`은 그래서
  `SLOWLOG GET`만 `sendCommand`로 raw 커맨드를 보내 직접 파싱한다
  (`LEN`/`RESET`은 단순 응답이라 타입드 메서드 그대로 써도 문제없다).
- **`DF.*`는 Jedis에 타입드 메서드가 없다** — `jedis.sendCommand(cmd, ...)`로
  임의의 커맨드 이름을 보내면 된다(`DataframeExample` 참고). RESP2에는 전용
  `Double` 타입이 없어서, `DF.QUERY`의 숫자 응답도 다른 bulk string 셀과
  마찬가지로 `byte[]`로 온다.
