# CHANGE.md

`redisX`는 **바닐라 Redis가 아니라 `redis_src`(그 자체로 커스텀 변경이 들어간 C 포크)를, 서브시스템 단위로 Rust에 전면 재구현한 프로젝트**다. 이 문서가 다루는 "변경사항"은 C↔Rust 라인 단위 diff가 아니라, **동작·스코프·설계 관점에서 `redis_src`와 달라진 지점**이다 — 포팅 과정의 모든 결정과 그 근거(무엇을 바이트 단위로 재현했고, 무엇을 의도적으로 단순화했고, 왜인지)는 [`./redis_document/25_redisX.md`](./redis_document/25_redisX.md)에 Phase별로 전부 기록되어 있다. 


---

## 0. 가장 큰 변화: C → Rust 전면 재구현

| 항목 | `redis_src` (C) | `redisX` (Rust) |
|---|---|---|
| 언어/메모리 관리 | 수동 메모리 관리(`zmalloc` 등), C 포인터 | 소유권 기반 자동 메모리 관리, `unsafe`는 스크립트 재진입·바이트 파싱 등 명확히 경계 지은 곳에만 |
| 이벤트 루프 | `ae.c`(epoll/kqueue/evport/select 자체 구현) | `mio` 위에 `ae.c`와 동일한 "콜백을 순서대로 호출하는 리액터" 모양을 그대로 재현(`redisx-net::ae`) — `tokio`의 `async`/`.await`는 "명령 실행은 절대 끼어들지 않는다"는 Redis 핵심 불변식을 전역적으로 재증명해야 하는 부담 때문에 기각 |
| 메모리 할당자 | glibc/jemalloc 가능 | **Rust 기본 allocator만 사용, jemalloc 도입 안 함** — `maxmemory` 축출(8개 정책)/`UNLINK`+`lazyfree-lazy-*`(5개 설정)/`MEMORY USAGE`+`STATS`+`DOCTOR`는 jemalloc 없이도 구현 가능한 부분까지 실제로 동작(Phase 26); `MALLOC-STATS`/`DEBUG`/진짜 active defrag만 "jemalloc 아레나 압축 API가 필수"라 명시적 범위 밖으로 남음 |
| 코드 구조 | 단일 `src/` 디렉터리, 203개 파일 | Cargo 워크스페이스 4크레이트: `redisx-core`(sds/crc/intset/dict/listpack/ziplist/zipmap/kvstore/lzf/sha1/sha256), `redisx-net`(RESP2/3 파서+인코더+디코더, `mio` 이벤트 루프), `redisx-server`(디스패치/전 데이터 타입/영속성/복제/스크립팅/운영 커맨드), `redisx-cli`(REPL + `redisx-benchmark`) |
| 온디스크/와이어 포맷 | 기준(origin) | **1:1 바이트 호환이 최우선 원칙** — listpack/intset/RDB 프레이밍/CRC64는 실제 Redis 바이너리와 상호 로드 가능하도록 교차검증됨(Stream/DataFrame처럼 real Redis에 아예 없는 타입만 포크 전용 포맷) |
| 기본 포트 | `6379` | **`2417`**(특별한 의미보다는 나의 전화번호 4자리로 ㅋ) |
| 테스트 전략 | `tests/*.tcl`(219개 파일, 8.5만 라인) | 기존 Tcl 스위트를 채택하지 않음 — SLOWLOG/AOF 포맷처럼 의도된 차이를 실패로 오판하고, 클러스터/블로킹/컨슈머그룹처럼 범위 밖이던 영역을 검증하는 비중이 커서 그대로 돌리면 노이즈가 됨. 대신 **유닛테스트 + 실제 `redis-server`/`redis-cli`/raw 소켓 교차검증**을 전 Phase 일관 적용(§4-6 확정 결정) |

---

## 1. `redis_src`의 커스텀 변경 — redisX가 그대로 이어받은 것

`redis_src/CHANGE.md`가 "바닐라 Redis와 다른 점"으로 정리해둔 4가지는, redisX 입장에서는 **포팅 대상 자체의 사양**이다. 아래는 그 4가지가 redisX에서 각각 어떻게 됐는지.

| `redis_src`의 커스텀 변경 | redisX에서의 상태 |
|---|---|
| **SLOWLOG** — 메모리 저장 제거, 파일 기반 로깅/tail 읽기 | **바이트 포맷까지 그대로 포팅**. 라인 포맷·설정 키(`slowlog-file-path`)·`GET`/`LEN`/`RESET`이 파일을 직접 읽고 쓰는 동작까지 동일. **유일한 의도적 이탈**: C는 `localtime_r`로 시스템 로컬 타임존을 쓰지만, redisX는 TZ 데이터베이스 의존성을 새로 끌어오지 않기 위해 **항상 UTC**로 쓰고 읽는다(저장된 유닉스 타임스탬프 자체는 왕복 정확, 사람이 파일을 직접 읽을 때 보이는 날짜·시각 칸만 로컬이 아닌 UTC) |
| **AOF** — MONITOR 스타일 텍스트 포맷, 복구 포기 | C가 이미 내린 "복구 포기, 쓰기 전용 로그" 결정을 **그대로 승계**(Phase 4). 한 줄 포맷(`<ts>.<usec> [<dbid> <peer>] "ARG1" ...`)이 C 포크 문서의 예시와 바이트 단위로 일치함을 실제 생성 파일로 확인. AOF 로딩/`BGREWRITEAOF`/BASE-INCR manifest는 복구 자체가 범위 밖이라 아예 구현하지 않음 |
| **Python 스크립팅 엔진** — Lua와 공존하는 `FUNCTION`/`FCALL` 엔진 | **엔진 추상화 설계 철학은 유지, 구현 수단은 완전히 다름**(Phase 6). C의 `function_python.c`(CPython C API 직접 호출)는 참고하지 않고 `pyo3`(`auto-initialize`)로 대체, Lua는 `mlua`(vendored Lua 5.4)로 신규 구현 — 두 엔진이 한 프로세스에서 동시에 로드·실행되는 것은 C 포크와 동일한 목표. **redisX 쪽이 더 나아간 점**: `redis.call()` 재진입을 스레드로컬로 해결해 스크립트 내부 쓰기가 복제/AOF/WATCH/CLIENT TRACKING을 전부 통과하도록 설계했고, Lua/Python 에러 메시지의 CR/LF가 RESP 프로토콜을 깨뜨리는 버그를 실제 `redis-cli`로 발견해 수정(C 포크엔 없던 검증) |
| **Make 빌드 — `BUILD_PYTHON` 자동 감지/설치** | **설계 목표는 동일, 구현은 Cargo 생태계 관례로 치환**(Phase B). `python3-config` 탐색 스크립트 대신 `pyo3-build-config`가 빌드 시점에 자동 탐지 — C가 직접 짠 탐색 로직을 재구현할 필요 자체가 없었음. `make python-setup`(OS 패키지 설치 자동화)에 대응하는 기능은 **의도적으로 제공 안 함**(Cargo 생태계에서 빌드 도구가 시스템 패키지를 설치하는 것은 기대되지 않는 동작). C가 수동으로 넣어야 했던 rpath 임베딩(`-Wl,-rpath,...`)은 `build.rs`가 `pyo3_build_config`에서 읽어 자동 생성 — **"Python 미사용 시 기존과 완전히 동일"이라는 C 포크의 핵심 불변식은 redisX에서도 그대로 유지**(기본 빌드엔 libpython 의존성 0) |

---

## 2. `redis_src`에는 없던, redisX가 새로 추가한 기능

redisX는 `redis_src`(그리고 바닐라 Redis 대부분의 기능)에 없던 기능까지 구현했다.

| 영역 | 내용 | Phase |
|---|---|---|
| **전체 ACL 사용자 테이블** | `ACL SETUSER/GETUSER/DELUSER/LIST/USERS/CAT/WHOAMI/DRYRUN`, 커맨드(`+cmd`/`-cmd`/`+@category`)·키 패턴(`~pattern`)·채널 패턴(`&pattern`) 권한. `redis_src`는 전역 `requirepass` 하나뿐이었음(redisX도 처음엔 ACL-lite로 시작했다가 이후 전체 테이블로 확장, `default` 사용자 인증은 여전히 `requirepass`와 병행) | 22 |
| **Redis Cluster** | 해시 슬롯·`MOVED`/`CROSSSLOT`/`CLUSTERDOWN`·`CLUSTER` 서브커맨드 전체·가십 수렴·`READONLY` 레플리카 stale-read. 가십은 `cluster_legacy.c`와 **바이트 호환 안 하는 신규 설계**(길이-프리픽스 텍스트 프레임, redisX끼리만 통신) — 리샤딩 댄스(`ASK`/`MIGRATE`)·자동 장애조치·`nodes.conf` 영속화는 의도적으로 제외 | 23 |
| **TLS** | `rustls` 기반 평문/TLS 듀얼 리스너(서버 사이드) + `tls-replication`(아웃바운드 `REPLICAOF`도 TLS화). `redis_src`/바닐라 모두 OpenSSL 기반 TLS가 있었지만, redisX는 순수 Rust 구현으로 전면 재설계 | 18, 21 |
| **블로킹 커맨드** | `BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BZPOPMIN`/`BZPOPMAX` + `XREAD BLOCK`/`XREADGROUP ... BLOCK`. 단일 스레드 리액터에서 FIFO 공정성·파이프라이닝·타임아웃까지 구현 | 14, 17 |
| **Stream 컨슈머 그룹** | `XGROUP`/`XREADGROUP`/`XACK`/`XCLAIM`/`XAUTOCLAIM`/`XPENDING`(요약+확장) — real Redis의 `rax` 기반 PEL 대신 `BTreeMap` 기반으로 구현, RDB 영속화(포크 전용 옵코드)까지 포함 | 16 |
| **네이티브 `polars` DataFrame 타입** | `redis_src`/바닐라 Redis 어디에도 없는 **완전히 새로운 값 타입**(`--features polars` 옵트인). `DF.FROMCSV`/`DF.FROMPARQUET`/`DF.SET`/`DF.GET`/`DF.TOCSV`/`DF.TOPARQUET`/`DF.SHAPE`/`DF.COLUMNS`/`DF.QUERY`(`polars::sql::SQLContext` 기반 실제 SQL)/`DF.PRETTY`. Arrow IPC 바이트로 RDB 영속화, pandas와 Parquet 상호운용성까지 검증 | 24 |
| **`maxmemory` 축출 + lazyfree + 메모리 진단** | `redis_src`도 바닐라 Redis도 원래 갖고 있던 기능이지만, redisX는 한동안 "설정 키만 있고 동작은 없음"으로 범위 밖에 둬 뒀던 것을 이번에 전부 실제 구현했다: 8개 `maxmemory-policy` 전부(샘플링 대신 전체 스캔으로 real Redis보다 더 정확한 후보 선택), `UNLINK`+`lazyfree-lazy-*` 5개 설정 전부(전용 백그라운드 스레드로 실제 비동기 해제), `MEMORY STATS`(뒷받침 가능한 필드만)/`DOCTOR`(OS에서 읽은 실제 RSS 기반 진단)/`OBJECT IDLETIME`+`FREQ`(실제 LRU/LFU side table). 진짜 active defrag만 jemalloc 없이는 여전히 불가능 — 대신 `mem_fragmentation_ratio`로 단편화를 진단하고 "재시작이 안전한 완화책"임을 `MEMORY DOCTOR`가 명시 | 26 |
| **설정 파일 + 독립 설치** | `redisx.conf`(자동 생성/로드), `install.sh`(cargo 없이 실행 가능한 사본을 `/usr/local/bin/redisX`에 설치, 쉘 PATH 안내) | 15 |
| **레거시 RDB 옵코드 전수 지원** | ziplist/zipmap(7.0 이전), 구버전 plain list/zset, `FUNCTION2`(라이브러리 진짜 왕복 지원), `SLOT_INFO`/`MODULE_AUX`/`KEY_META`(의미는 몰라도 안전하게 건너뜀) — 이 덕분에 real Redis가 클러스터/모듈 상태로 만든 덤프도 나머지 키스페이스는 정상 로드 | 12, 20 |
| **부분 재동기화(`PSYNC +CONTINUE`)** | 복제 백로그 기반 부분 재동기화 — `redis_src`/이번 포팅 초기 범위엔 전체 재동기화만 있었음 | 9 |
| **`ZRANGEBYLEX`/`ZREVRANGEBYLEX`/`ZLEXCOUNT`, `ZUNION`/`ZINTER`/`ZDIFF`/`ZINTERCARD` 등** | 처음엔 "활용도 낮음"으로 범위 밖이었다가 이후 전부 보강 | 13, 19 |
| **예제 스크립트 라이브러리 (`scripts/`)** | Lua/Python 양쪽으로 00~90 난이도의 `FUNCTION LOAD` 예제 10쌍(캐시, 분산 락, 리더보드, 슬라이딩 윈도우 레이트리미터, 작업 큐, 블룸 필터, 원자적 송금, DataFrame 쿼리 등) — `redis_src`엔 전혀 없던 신규 문서/자산 | 9 이후 추가 |

---

## 3. 의도적인 단순화 / 스코프 축소 (알아둬야 할 것)

포팅 원칙("바이트 포맷은 1:1, 성능 최적화는 정확성 뒤로")에 따라 아래는 **버그가 아니라 문서화된 설계 결정**이다.

| 영역 | 단순화 내용 |
|---|---|
| 자료구조 | `rax`(Stream 전용) → `BTreeMap`으로 대체, `adlist`/`fwtree`/Fenwick 트리 생략(O(n) 선형 탐색으로 대체), ziplist/zipmap은 **디코드 전용**(쓰기 경로 없음), `List`는 항상 단일 `ListPack`(quicklist 미포팅), `Set`은 intset↔hashtable 2단계(listpack 중간 단계 생략), `ZSet`은 skiplist+dict 대신 정렬 `Vec` 이진탐색 |
| 해시 필드 TTL | `HEXPIRE`/`HPEXPIRE`/`HPERSIST`/`HTTL` 계열(및 그 기반인 `ebuckets`/`estore`/`mstr`) — **로드맵 전체에서 유일하게 끝까지 미구현으로 남은 항목** |
| `HSCAN`/`SSCAN`/`ZSCAN` | 커서가 항상 `0`, 한 번에 전부 반환(프로토콜은 지키되 비증분) — `SCAN`만 진짜 증분 스캔 |
| `WAIT`/`FAILOVER` | `WAIT`는 ack 오프셋 대기가 아니라 "연결된 레플리카 수" 즉시 반환, `FAILOVER`는 "원격 승격 제어 채널 없음"을 명시한 에러로 응답 — `REPLICAOF NO ONE` 수동 승격이 유일한 경로 |
| 운영 진단 | `DEBUG`/`LATENCY`/`bio`/`hotkeys`/`childinfo`/`syscheck` — fork 기반 프로세스 모델·jemalloc 아레나에 결합된 기능이라 범위 밖. `evict`(= `maxmemory` 축출)/`lazyfree`는 더 이상 이 목록에 없음 — Phase 26에서 실제 구현됨(§0 표, "메모리 관리" README 절 참고). 진짜 active defrag(jemalloc 아레나 압축 API 필수)만 여전히 범위 밖 |
| ACL | selector·서브커맨드 단위 규칙(`+config|get`) 없음, 카테고리 10개 큐레이션 집합(real Redis의 세분화 태그 미지원), 키/채널 검사는 `argv[1]` 휴리스틱(다중 키 커맨드의 2번째 키부터는 미검사) |
| Cluster | 리샤딩 댄스(`ASK`/`MIGRATE`/`IMPORTING`/`MIGRATING`) 없음, 자동/투표 기반 장애조치 없음, `nodes.conf` 영속화 없음(재시작하면 토폴로지 소실), 장애 감지는 로컬 측정만(쿼럼 없음) |
| TLS | 서버 사이드 클라이언트 인증서 검증/SNI/암호화 스위트 튜닝 없음, 인증서 런타임 리로드 없음, `tls-replication`은 CA 신뢰 저장소 없이 서명 검증만 수행(마스터 인증서 체인 미검증), TLS 전용 모드(`port 0`) 미지원 |
| Stream/DataFrame RDB 포맷 | real Redis와 호환되지 않는 **포크 전용 옵코드**(각각 200, 201) — 애초에 real Redis엔 없는 타입이므로 하위 호환 신경 쓸 이유가 없음 |
| `MEMORY` 커맨드 | `USAGE`는 jemalloc 아레나 통계 없이 값 내용 기반 근사치(Phase 2). `STATS`/`DOCTOR`는 Phase 26에서 뒷받침 가능한 필드(`used_memory`/`keys.count`/OS 기반 `used_memory_rss`/`mem_fragmentation_ratio`)만 실제 구현 — `allocator.*` 등 jemalloc 전용 필드, `MALLOC-STATS`/`PURGE`는 여전히 범위 밖 |

---

## 4. 포팅 과정에서 발견/수정한 실제 버그

유닛테스트와 "실제 바이너리 + `redis-cli`/raw 소켓" 교차검증을 병행한 덕분에, 포팅 자체의 실수뿐 아니라 설계상 간과됐던 지점들을 실제로 잡아냈다.

- **`requirepass` 평문 비교** — Phase 9 ACL-lite 구현 당시 `sha256`이 없어 비밀번호를 평문 비교하고 있었음. real Redis는 SHA-256 해시 비교 + `time_independent_strcmp`를 쓴다는 점을 확인하고 Phase 11에서 교정. "비밀번호의 SHA-256 다이제스트 자체로는 인증 안 됨"을 실제 바이너리로 증명.
- **`FCALL_RO`가 쓰기를 막지 못함** — 이름만 `_RO`고 실제로는 스크립트 내부 쓰기가 전부 통과되던 버그, Phase 10에서 발견/수정.
- **`RESET`이 핸들러 없이 선언만 되어 있었음** — Phase 10에서 실제 구현 추가.
- **블로킹 커맨드 이중 깨우기** — `BRPOPLPUSH` 재시도가 재귀적으로 같은 클라이언트를 또 깨우던 버그, 등록 해제 순서를 뒤집어 Phase 14에서 수정.
- **블로킹 커맨드가 WRONGTYPE과 "데이터 없음"을 구분 못 함** — 타입이 다른데도 계속 대기하던 버그, Phase 14에서 수정.
- **`master_repl_offset` 과소 계산** — `propagate()`가 `SELECT` 프리픽스 바이트를 오프셋 계산에서 빼먹어, 부분 재동기화 도입(Phase 9) 중 발견/수정.
- **클러스터 가십 라이브락** — 왕복 요청/응답 방식으로 설계했다가 두 노드의 동시 기동 시 cron 틱이 영원히 맞물리는 라이브락을 실제 바이너리로 재현, 일방향 푸시 방식으로 재설계(Phase 23).
- **ACL `ADMIN_COMMANDS`에 `CLUSTER` 누락** — `+@read -@admin` 사용자가 `CLUSTER ADDSLOTS`/`RESET`/`FORGET` 같은 admin급 서브커맨드를 실행할 수 있던 실제 보안 구멍, Phase 23 후속 감사에서 발견/수정.
- **Lua/Python 에러 메시지의 CR/LF가 RESP 프로토콜을 깨뜨림** — 멀티라인 에러가 심플에러 라인에 섞여 `redis-cli`가 깨지던 버그, `sanitize_error()`로 Phase 6에서 수정, 이후 DataFrame 에러(Phase 24 후속)에도 동일 패턴(`df_error()`) 적용.
- **`redisx-cli -x`가 바이너리 안전하지 않음** — stdin을 `read_to_string`(UTF-8 전용)으로 읽어 바이너리(Arrow IPC 등)를 통째로 누락시키던 버그, Phase 24 후속 패스에서 `read_to_end`로 교체해 수정.
- **`install.sh`의 `/usr/bin` 기본 설치 경로** — 최신 macOS SIP가 차단하는 경로였음을 실사용 중 발견, `/usr/local/bin`으로 변경 + `sudo mkdir` 후 소유권 미이전 2차 버그까지 수정.
- **`redis_src` 자체의 무관한 기존 버그 발견**(redisX 포팅 범위 밖, 기록만 남김): 실제 Redis를 redisX의 레플리카로 붙이는 반대 방향 복제 테스트 중, `redis_src`의 Python 엔진 통합 코드가 `emptyData()` 시 `pythonEngineInitEngine()`을 재호출해 세그폴트하는 버그를 발견 — 이 redisX Rust 포팅과는 무관하며 `redis_src` 자체의 수정이 필요한 이슈로 남겨둠.

---

## 5. 빌드 · 실행 (redisX 실제 기준)

`redis_src`는 `make`/`make BUILD_PYTHON=yes`였지만, redisX는 Cargo 워크스페이스다.

```bash
# 기본 빌드 (Lua 스크립팅만, Python/polars 의존성 전혀 없음)
cargo build --workspace

# Python FUNCTION/FCALL 엔진 포함
cargo build -p redisx-server --features python

# 네이티브 polars DataFrame 타입(DF.*) 포함
cargo build -p redisx-server --features polars

# 둘 다
cargo build -p redisx-server --features python,polars

# 실행 (기본 포트 2417)
cargo run -p redisx-server --bin redisx-server
cargo run -p redisx-cli --bin redisx-cli -- -p 2417

# cargo 없이 독립 실행 가능한 사본 설치 (기본: /usr/local/bin/redisX, Python+polars 포함 빌드)
./install.sh

# 테스트
cargo test --workspace
cargo clippy --workspace --all-targets   # 전 Phase에 걸쳐 경고 0개 유지가 기준
```


