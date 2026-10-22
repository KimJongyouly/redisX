# redisX 클라이언트 예제 모음

redisX를 **클라이언트로서** 사용해보는 예제들이다 — `redisX/scripts/`(서버
안에서 `FUNCTION LOAD`로 돌아가는 Lua/Python 스크립트)와는 완전히 다른
것이니 혼동하지 말 것. 여기 있는 건 전부 평범한 TCP 클라이언트로 붙는,
독립적으로 실행 가능한 프로그램이다.

같은 9개 시나리오를 11개 언어로 똑같이 옮겨뒀다 — 각 언어가 **그 생태계에서
가장 널리 쓰이는 평범한 Redis 클라이언트**를 쓴다(redisX 전용 SDK는 없다,
`DF.*`를 포함해 전부 그냥 RESP 커맨드이기 때문). 같은 데모 키·같은 흐름·같은
한글 설명을 쓰므로 언어 사이를 나란히 비교해 보기에도 좋다.

## 공통 시나리오 (모든 언어 동일)

| 파일명(언어마다 확장자/대소문자 관례만 다름) | 다루는 것 | 비유 |
|---|---|---|
| `string`(Python만 `redis_string.py`†) | `SET`/`GET`/`APPEND`/`GETRANGE`/`SETRANGE`/`MSET`/`INCR`/`EXPIRE`/`TTL`/`PERSIST` | — |
| `list` | `LPUSH`/`RPUSH`/`LINSERT`/`LREM`/`LTRIM`/`LPOS`/`LMOVE` | 작업 큐 |
| `hash` | `HSET`/`HGETALL`/`HINCRBY`/`HRANDFIELD` | 사용자 프로필 |
| `set` | `SADD`/`SINTER`/`SUNION`/`SDIFF`/`SINTERSTORE`/`SMOVE`/`SPOP` | 글 태그 집합 연산 |
| `zset` | `ZADD`/`ZRANGEBYSCORE`/`ZINCRBY`/`ZPOPMIN`/`ZPOPMAX`/`ZUNIONSTORE` | 리더보드 |
| `hyperloglog` | `PFADD`/`PFCOUNT`/`PFMERGE`를 실제 `SADD`/`SCARD`와 나란히 비교 | 방문자 수 근사 카운팅 |
| `slowlog` | redisX의 파일 기반 `SLOWLOG`를 `CONFIG SET`으로 켜고 직접 읽기 | — |
| `dataframe` | 인라인 CSV를 `DF.FROMCSV`로 적재 → `DF.SHAPE`/`DF.COLUMNS`/`DF.QUERY`/`DF.PRETTY`/`DF.TOCSV` | 재고 테이블 SQL 집계 |
| `pubsub` | `SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH`/`PUBSUB CHANNELS`·`NUMSUB`·`NUMPAT`/`UNSUBSCRIBE`/`PUNSUBSCRIBE` | 실시간 알림 채널 |

`dataframe`만 서버가 `--features polars`로 빌드됐을 때만 동작한다(`cargo
build --release -p redisx-server --features polars` — `install.sh`로 설치한
사본은 이미 이 feature가 켜져 있다). 나머지 8개는 기본 빌드로 충분하다.

† Python만 `string.py`가 아니라 `redis_string.py`다 — Python은 스크립트를
실행할 때 그 스크립트가 있는 폴더를 `sys.path`에 먼저 넣는데, 그 폴더에
`string.py`가 있으면 표준 라이브러리 `string` 모듈(`logging`을 비롯해 많은
표준 라이브러리 코드가 내부적으로 import한다) 대신 이 예제 파일이 import돼
버려서 **그 폴더의 모든 스크립트가** `ImportError`로 깨진다(직접 재현해서
확인한 동작 — Python 자신이 바로 이 사실을 에러 메시지에서 알려준다).
`list`/`set` 등 다른 이름은 실제 import 가능한 모듈명이 아니라서 문제없다.

`pubsub`는 키스페이스와 무관한 메시징 기능이라, 다른 8개와 달리 "지우고
시작할 키"가 없다 — 그래서 자기 완결성의 기준도 다르다: 매 실행마다 새
연결로 구독/발행을 새로 맺고 끝에는 전부 구독 해제하므로, 반복 실행해도
이전 실행의 구독이 남아 있지 않다(redisX 서버 자체도 재시작할 필요 없음).

모든 예제는 자기 완결적이다 — 실행 전에 자신이 쓸 데모 키를 지우고
시작하고, 끝나면 다시 지운다. 몇 번을 반복 실행해도 같은 결과가 나오고,
서로 독립적이라 어떤 순서로 돌려도 상관없다.

## 접속 대상 바꾸기

기본값은 전부 `127.0.0.1:2417`(redisX 기본 포트)이다. 다른 서버/포트로
돌리려면 언어별로 다음 환경변수를 쓴다(C/C++만 호스트/포트가 분리돼 있다):

| 언어 | 환경변수 |
|---|---|
| Python, Go, Rust, Dart, Lua, PHP, Java, Kotlin, Scala | `REDISX_ADDR` (`host:port`) |
| C, C++ | `REDISX_HOST`(기본 `127.0.0.1`), `REDISX_PORT`(기본 `2417`) |

```bash
REDISX_ADDR=127.0.0.1:7799 python redis_string.py   # 예: Python
REDISX_PORT=7799 ./bin/string                          # 예: C
```

## 언어별 요약

| 언어 | 위치 | 클라이언트 | 설치 | 실행(첫 예제) |
|---|---|---|---|---|
| Python | `python/` | [`redis-py`](https://github.com/redis/redis-py) (+ DataFrame 예제는 `polars`) | `pip install redis polars` | `python redis_string.py` |
| Go | `golang/` | [`go-redis`](https://github.com/redis/go-redis) `v9` | 없음(첫 빌드 때 `go`가 알아서 받음) | `cd golang && go run ./string` |
| Rust | `rust/` | [`redis`](https://crates.io/crates/redis) crate `1.x`(메인 워크스페이스와 무관한 독립 Cargo 프로젝트) | 없음(`cargo`가 알아서 받음) | `cd rust && cargo run --bin string` |
| C | `c/` | [hiredis](https://github.com/redis/hiredis) | `brew install hiredis` (Linux: `apt install libhiredis-dev` 등) | `cd c && make && ./bin/string` |
| C++ | `cpp/` | hiredis + 얇은 RAII 래퍼(`redisx_client.hpp`) | `brew install hiredis` | `cd cpp && make && ./bin/string` |
| Lua | `lua/` | [`redis-lua`](https://luarocks.org/modules/nrk/redis-lua)(LuaSocket 기반) | `brew install lua luarocks && luarocks install luasocket redis-lua` | `cd lua && lua string.lua` |
| Dart | `dart/` | [`redis`](https://pub.dev/packages/redis) 패키지 | `brew install dart-sdk` → `cd dart && dart pub get` | `cd dart && dart run bin/string.dart` |
| PHP | `php/` | [phpredis](https://github.com/phpredis/phpredis) 확장(C 확장, Composer 불필요) | `brew install php`(`redis` 확장 기본 포함) | `cd php && php string.php` |
| Java | `java/` | [Jedis](https://github.com/redis/jedis)(Maven/Gradle 없이 `javac`/`java`만) | JDK만 있으면 됨 | `cd java && ./fetch-deps.sh && ./build.sh && ./run.sh StringExample` |
| Kotlin | `kotlin/` | Jedis(Kotlin 전용 클라이언트 불필요) | `brew install kotlin` | `cd kotlin && ./fetch-deps.sh && ./build.sh && ./run.sh StringExample` |
| Scala | `scala/` | Jedis(Scala 전용 클라이언트 불필요) | `brew install scala`(Scala 3 통합 CLI, 내부적으로 scala-cli) | `cd scala && ./fetch-deps.sh && ./run.sh StringExample` |

Java/Kotlin/Scala는 이 저장소에 Maven/Gradle/sbt가 전혀 없다는 전제로,
`fetch-deps.sh`가 Maven Central에서 Jedis + 최소 런타임 의존성(`commons-pool2`,
`slf4j-api`/`slf4j-nop`) jar를 `lib/`에 직접 받아온다 — `lib/`과 빌드 산출물
(`out/`, Scala는 `.scala-build/`/`.bsp/`)은 전부 재생성 가능해서
`.gitignore` 대상이다.

## 소스 구조

```
example/
├── python/     예제당 파일 하나(*.py) + DataFrame 전용 헬퍼 클래스(redis_df.py)
├── golang/     go.mod 하나 + 예제당 하위 디렉터리(string/, list/, ...) — 각각 main.go
├── rust/       독립 Cargo.toml(own [workspace]) + src/bin/*.rs — 파일명이 곧 바이너리명
├── c/          Makefile + 예제당 .c 파일 + 공용 common.h, 바이너리는 bin/
├── cpp/        Makefile + 예제당 .cpp 파일 + 공용 redisx_client.hpp, 바이너리는 bin/
├── lua/        예제당 .lua 파일 + 공용 redisx_demo.lua(연결/레거시 커맨드 보강)
├── dart/       pubspec.yaml + bin/*.dart
├── php/        예제당 .php 파일 (의존성 없음, phpredis 확장만 있으면 됨)
├── java/       src/*.java + fetch-deps.sh/build.sh/run.sh + 자체 README.md
├── kotlin/     src/*.kt + fetch-deps.sh/build.sh/run.sh + 자체 README.md
└── scala/      src/*.scala + fetch-deps.sh/run.sh(빌드 별도 없음) + 자체 README.md
```

Python에는 예제가 두 개 더 있다: `redis_df.py`(`DF.QUERY`의 SQL 텍스트를
파싱해 컬럼명을 동적으로 매핑해주는 `redis_df` 클라이언트 클래스)와
`dataframe.py`(이미 `DF.FROMPARQUET`/`DF.FROMCSV`로 적재해둔 실제
데이터 키를 대상으로 하는 예제 — 다른 언어의 `dataframe` 예제처럼 자기
완결적이지 않고 사전 데이터 적재가 필요하다. 자기 완결적인 쪽을 원하면
`dataframe_csv.py`를 볼 것).

## 각 언어가 실제로 부딪힌 redisX 고유 동작들

전부 "일단 짜봤다"가 아니라 실제 서버로 검증하다가 직접 걸려서 알게 된
것들이다 — 각 언어의 `slowlog`/`dataframe` 예제 주석에 더 자세히 적혀있다:

- **`SLOWLOG`는 메모리가 아니라 파일 기반**이다(redisX가 포팅한 원본 C
  포크의 설계 — 저장소 루트 `CHANGE.md` 1절 참고). `slowlog-file-path`가
  빈 문자열인 기본 상태에서는 `SLOWLOG GET`/`LEN`이 항상 빈 결과를 낸다 —
  `CONFIG SET`으로 경로를 지정해야 실제로 기록되기 시작한다.
- **`SLOWLOG GET`의 클라이언트 주소 필드엔 포트가 없다**(`"127.0.0.1"`만,
  `"127.0.0.1:12345"`가 아님) — 이것도 원본 C 포크를 그대로 이어받은
  동작이다. Jedis(Java/Kotlin/Scala)의 타입드 `slowlogGet()`은 이 필드가
  항상 `host:port` 형태라고 가정하고 파싱하다가
  `StringIndexOutOfBoundsException`으로 깨진다 — 그래서 세 언어의
  `SlowlogExample`은 `SLOWLOG GET`만 raw 커맨드로 직접 보내고 파싱한다
  (`LEN`/`RESET`은 문제없이 타입드 메서드를 쓴다). go-redis(Go)와
  `redis` crate(Rust)는 이 필드를 그냥 문자열로만 다뤄서 문제가 없었다.
- **RESP2엔 전용 `Double` 타입이 없다** — `DF.QUERY`가 돌려주는 숫자
  집계 결과(예: `SUM(price * qty)`)도 다른 텍스트 셀과 똑같이 bulk
  string(바이트열/문자열)으로 온다. 각 언어의 `dataframe` 예제가 이를
  숫자가 아니라 문자열로 받아 그대로 출력하는 이유다.
- **`DF.*`/`SLOWLOG`는 대부분의 클라이언트 라이브러리에 타입드 메서드가
  없다** — RESP 커맨드 자체는 평범하므로, 다들 "임의의 커맨드를 보내는"
  탈출구(`sendCommand`/`rawCommand`/`execute_command`/`Do`/
  `cmd(...).arg(...)` 등 라이브러리마다 이름은 다르지만 전부 존재)를 쓴다.
- **레거시 `SETEX`/`HMSET` 같은 커맨드는 redisX에 아예 없다** — `SET key
  val EX seconds`/다중 필드 `HSET`으로 통합됐다(이 포크의 의도적 축소).
  일부 클라이언트 라이브러리의 "편의 메서드"가 내부적으로 레거시 커맨드
  이름을 그대로 보내는 경우(예: Jedis의 `setex()`) `unknown command`
  에러가 나므로, `string`/`hash` 예제들은 현대 형태(`SET ... EX ...`,
  다중 필드 `HSET`)를 쓰는 메서드로 바꿔 썼다.
- **Lua(`redis-lua` 2.0.4)는 꽤 오래된 라이브러리라 `HRANDFIELD`/`LMOVE`/
  `LPOS`/`PFADD`/`PFCOUNT`/`PFMERGE`/`MEMORY`/`ZPOPMIN`/`ZPOPMAX`처럼
  나중에 추가된 커맨드가 미리 등록되어 있지 않다** — `redisx_demo.lua`가
  `client:define_command(name)`으로 그 자리에서 등록해준다(RESP 파싱
  자체는 라이브러리가 그대로 처리). 이 라이브러리의 구버전 `HSET`(필드
  하나만 받음)도 redisX가 실제로 구현한 다중 필드 `HSET`으로 덮어써서
  다시 등록한다.
- **구독 중인 연결은 일반 커맨드를 보낼 수 없다** — 모든 언어가 `pubsub`
  예제에서 구독자용 연결과 발행자용 연결을 별도로 연다. 그 안에서도 라이브러리
  마다 "구독 모드"를 다루는 방식이 완전히 다르다는 게 실제로 돌려보고서야
  드러났다:
  - **phpredis(PHP)의 `subscribe()`/`psubscribe()`는 콜백을 등록하고 그
    자리에서 영원히(콜백 안에서 `unsubscribe()`를 부를 때까지) 블로킹한다**
    — 같은 프로세스 안에서 그다음 줄로 넘어가 `PUBLISH`를 보낼 방법이 없다.
    `pubsub.php`는 `pcntl_fork()`로 구독자를 별도 프로세스로 떼어내고(연결도
    fork 이후에 각자 새로 맺는다), exact 채널/패턴 채널도 각각 별도 fork로
    나눴다 — 한 프로세스에서 SUBSCRIBE와 PSUBSCRIBE를 동시에 유지할 방법이
    이 블로킹 콜백 API로는 마땅치 않아서다.
  - **Jedis(Java/Kotlin/Scala)는 반대로, 이미 구독 중인 연결의 콜백 객체
    (`JedisPubSub`) 안에서 `this.psubscribe(...)`를 추가로 불러 같은 연결에
    패턴 구독을 더 걸 수 있다** — PHP처럼 프로세스/연결을 나눌 필요 없이,
    `subscribe()`를 돌리는 스레드 하나로 exact+패턴 구독을 동시에 받는다.
    다만 `subscribe()` 자체가 호출한 스레드를 블로킹하는 건 동일해서 별도
    스레드에서 돌리고, 발행/`PUBSUB` 조회는 메인 스레드의 별도 연결로 한다.
  - **`redis` crate(Rust) 1.x는 `subscribe()`/`psubscribe()`가 서버의 구독
    확인 응답을 내부적으로 알아서 소비한다** — redis-py/go-redis처럼 그
    확인 메시지를 `get_message()`로 직접 받아서 출력할 필요가 없다(`cache_
    messages_until_received_response`가 "message"/"pmessage"가 아닌 응답을
    받을 때까지 읽다가 반환하기 때문). 같은 크레이트에 `PUBSUB CHANNELS`/
    `NUMSUB`/`NUMPAT`의 타입드 메서드도 없어서(`DF.*`/`SLOWLOG`와 같은
    사정) raw `cmd()`로 보낸다.
  - **Dart `redis` 패키지의 `PubSub`는 `Stream`을 돌려준다** — 외부
    패키지(`package:async`) 없이도 Dart SDK 내장 `StreamIterator`로 한
    번에 하나씩 꺼내 쓸 수 있다.
