# Redis 설정 시스템 (redis.conf) 소스 분석

## 1. 설정 시스템 전체 구조

Redis의 설정 시스템은 두 축으로 구성된다.

| 구성 요소 | 파일 | 역할 |
|---|---|---|
| 설정 파일 | `redis.conf` (2,494줄) | 사람이 읽는 설정 문서 |
| 파싱·적용 엔진 | `src/config.c` (3,776줄) | 파싱, CONFIG GET/SET/REWRITE 구현 |
| 플랫폼 감지 헤더 | `src/config.h` (387줄) | OS/아키텍처별 기능 감지 매크로 |

---

## 2. 설정 타입 시스템 (config.c)

### 2-1. 핵심 자료구조

```c
struct standardConfig {
    const char *name;        // 설정 이름 (e.g., "maxmemory")
    const char *alias;       // 별칭 (하위 호환)
    unsigned int flags;      // IMMUTABLE / MODIFIABLE / PROTECTED 등
    typeInterface interface;  // init / set / get / rewrite 함수 포인터
    typeData data;           // 타입별 데이터 (기본값, 유효성 검증 fn)
    configType type;         // 타입 구분
    void *privdata;          // 모듈 설정용 확장 포인터
};

dict *configs = NULL;        // 런타임 설정 딕셔너리 (이름 → standardConfig*)
```

### 2-2. 설정 값 타입 (5종)

| 타입 상수 | C 타입 | 예시 설정 |
|---|---|---|
| `BOOL_CONFIG` | `int` (0/1) | `daemonize`, `appendonly` |
| `STRING_CONFIG` | `char *` | `dbfilename`, `aclfile` |
| `SDS_CONFIG` | `sds` | `masterauth`, `requirepass` |
| `ENUM_CONFIG` | `int` (enum 값) | `loglevel`, `maxmemory-policy` |
| `NUMERIC_CONFIG` | int/long/size_t 등 10종 | `port`, `maxmemory`, `hz` |

`NUMERIC_CONFIG`는 union으로 int, uint, long, ulong, long long, ulong long, size_t, ssize_t, off_t, time_t를 모두 지원한다.

### 2-3. 설정 플래그

| 플래그 | 의미 |
|---|---|
| `IMMUTABLE_CONFIG` | 서버 시작 이후 변경 불가 (CONFIG SET 거부) |
| `MODIFIABLE_CONFIG` | 런타임 CONFIG SET 허용 |
| `PROTECTED_CONFIG` | `enable-protected-configs` 설정에 따라 접근 제어 |
| `MULTI_ARG_CONFIG` | 공백 구분 다중 인자 허용 (e.g., `save`, `client-output-buffer-limit`) |
| `HIDDEN_CONFIG` | CONFIG GET 응답에서 숨김 |

```c
// 예시: dbfilename은 런타임 변경 가능하지만 보호된 설정
createStringConfig("dbfilename", NULL,
    MODIFIABLE_CONFIG | PROTECTED_CONFIG, ...);

// 예시: daemonize는 시작 후 변경 불가
createBoolConfig("daemonize", NULL, IMMUTABLE_CONFIG, ...);
```

### 2-4. typeInterface — 4개 함수 포인터

```c
typedef struct typeInterface {
    void (*init)(standardConfig *config);    // 서버 시작 시 기본값 초기화
    int  (*set)(config, argv, argc, err);    // 파일 파싱 / CONFIG SET 시 호출
    apply_fn apply;                          // set() 후 실제 적용 (CONFIG SET 전용)
    sds  (*get)(standardConfig *config);     // CONFIG GET 응답 생성
    void (*rewrite)(config, name, state);    // CONFIG REWRITE 시 파일에 기록
} typeInterface;
```

`set()`은 반환값이 3가지다:
- `1` — 성공
- `2` — 변경 없음 (이미 같은 값)
- `0` — 실패 (err 문자열 설정)

### 2-5. Enum 매핑 테이블

설정 문자열 ↔ 정수값 변환에 `configEnum[]` 배열을 사용한다.

```c
configEnum maxmemory_policy_enum[] = {
    {"volatile-lru",    MAXMEMORY_VOLATILE_LRU},
    {"volatile-lfu",    MAXMEMORY_VOLATILE_LFU},
    {"volatile-random", MAXMEMORY_VOLATILE_RANDOM},
    {"volatile-ttl",    MAXMEMORY_VOLATILE_TTL},
    {"volatile-lrm",    MAXMEMORY_VOLATILE_LRM},
    {"allkeys-lru",     MAXMEMORY_ALLKEYS_LRU},
    {"allkeys-lfu",     MAXMEMORY_ALLKEYS_LFU},
    {"allkeys-random",  MAXMEMORY_ALLKEYS_RANDOM},
    {"allkeys-lrm",     MAXMEMORY_ALLKEYS_LRM},
    {"noeviction",      MAXMEMORY_NO_EVICTION},
    {NULL, 0}
};

configEnum loglevel_enum[] = {
    {"debug", LL_DEBUG}, {"verbose", LL_VERBOSE},
    {"notice", LL_NOTICE}, {"warning", LL_WARNING},
    {"nothing", LL_NOTHING}, {NULL, 0}
};

configEnum aof_fsync_enum[] = {
    {"everysec", AOF_FSYNC_EVERYSEC},
    {"always",   AOF_FSYNC_ALWAYS},
    {"no",       AOF_FSYNC_NO},
    {NULL, 0}
};
```

---

## 3. 설정 파일 파싱 흐름 (`loadServerConfigFromString`)

```
redis.conf 파일 읽기
    │
    ├─ 줄 단위 분리 (sdssplitlen)
    ├─ 주석(#), 빈 줄 스킵
    ├─ 공백/따옴표 인식 토큰 분리 (sdssplitargs)
    │
    ├─ [표준 설정] lookupConfig(argv[0]) → standardConfig
    │       ├─ MULTI_ARG_CONFIG: 공백으로 추가 분리
    │       └─ config->interface.set() 호출
    │
    ├─ [deprecated 설정] 무시 처리
    │       (list-max-ziplist-entries, lua-replicate-commands 등)
    │
    └─ [특수 지시자 직접 처리]
            ├─ include → loadServerConfig() 재귀 호출 (와일드카드 지원)
            ├─ rename-command → 커맨드 테이블 수정
            ├─ bind → 네트워크 바인드 목록 구성
            ├─ user → ACL 규칙 파싱
            ├─ loadmodule → 모듈 로드 큐 추가
            └─ save → RDB 저장 포인트 추가
```

**`include`의 특성:**
- 와일드카드(`*`) 지원, 알파벳 순으로 처리
- `include`는 `CONFIG REWRITE`로 재기록되지 않음
- 파일 선두에 놓으면 후속 설정이 우선, 말미에 놓으면 include가 우선

---

## 4. CONFIG 명령어 구현

### 4-1. CONFIG SET

```
CONFIG SET param1 val1 [param2 val2 ...]
```

처리 흐름:
1. 파라미터를 딕셔너리에서 조회
2. `IMMUTABLE_CONFIG` 확인 → 거부
3. `PROTECTED_CONFIG` 확인 → `enable-protected-configs` 값에 따라 허용/거부
4. `config->interface.set()` 호출
5. 성공 시 `config->interface.apply()` 호출 (실제 적용)

여러 파라미터를 한 번에 지정하면 **트랜잭션**처럼 동작한다: 하나라도 실패하면 이전에 성공한 것도 롤백된다.

### 4-2. CONFIG GET

```
CONFIG GET pattern [pattern ...]
```

- glob 패턴 매칭으로 복수 설정 반환
- `HIDDEN_CONFIG` 플래그 설정은 응답에서 제외
- 반환: `[name, value, name, value, ...]` 형식

### 4-3. CONFIG REWRITE

```
CONFIG REWRITE
```

- 현재 서버 설정 값을 기존 `redis.conf`에 다시 기록
- 이미 존재하는 항목은 해당 줄을 업데이트, 없으면 파일 끝에 추가
- `include` 지시자는 재기록되지 않음

---

## 5. redis.conf 섹션별 설정 분석

### 5-1. INCLUDES

```
include /path/to/local.conf
include /path/to/fragments/*.conf
```

- 와일드카드 지원, 알파벳 순 처리
- 중첩 include 가능
- CONFIG REWRITE 시 보존되지 않음

---

### 5-2. MODULES

```
loadmodule /path/to/module.so [arg ...]
```

- 시작 시 `.so` 모듈 동적 로드
- 로드 실패 시 서버 시작 중단
- 여러 `loadmodule` 지시자 허용

---

### 5-3. NETWORK (네트워크)

| 설정 | 기본값 | 설명 |
|---|---|---|
| `bind` | `127.0.0.1 -::1` | 바인드 주소 (`-` 접두사 = 실패 시 무시) |
| `bind-source-addr` | — | 아웃바운드 연결용 소스 IP |
| `protected-mode` | `yes` | 비밀번호 없을 때 로컬 연결만 허용 |
| `port` | `6379` | TCP 포트 (0이면 비활성화) |
| `tcp-backlog` | `511` | listen() 백로그 크기 |
| `unixsocket` | — | Unix 도메인 소켓 경로 |
| `unixsocketperm` | `700` | Unix 소켓 파일 권한 |
| `timeout` | `0` | 클라이언트 유휴 타임아웃 (초, 0=비활성화) |
| `tcp-keepalive` | `300` | TCP SO_KEEPALIVE 주기 (초) |
| `socket-mark-id` | `0` | OS별 소켓 마킹 ID (Linux: SO_MARK) |

**`enable-protected-configs` / `enable-debug-command` / `enable-module-command`:**
- `no` — 차단 (기본)
- `yes` — 모든 연결 허용
- `local` — 로컬 연결(127.0.0.1, ::1, Unix 소켓)만 허용

---

### 5-4. TLS/SSL

| 설정 | 설명 |
|---|---|
| `tls-port` | TLS 리스닝 포트 |
| `tls-cert-file` | 서버 인증서 (PEM) |
| `tls-key-file` | 서버 개인 키 |
| `tls-key-file-pass` | 키 파일 암호 |
| `tls-client-cert-file` | 클라이언트 역할 시 인증서 (별도 사용 시) |
| `tls-ca-cert-file` / `tls-ca-cert-dir` | CA 인증서 |
| `tls-auth-clients` | `yes`(기본)/`no`/`optional` — 클라이언트 인증서 요구 수준 |
| `tls-auth-clients-user` | `CN`/`off` — TLS 인증서 CN 필드로 자동 사용자 인증 |
| `tls-replication` | 복제 링크 TLS 활성화 |
| `tls-cluster` | 클러스터 버스 TLS 활성화 |
| `tls-protocols` | 허용 TLS 버전 ("TLSv1.2 TLSv1.3") |
| `tls-ciphers` | ≤TLSv1.2 암호 스위트 |
| `tls-ciphersuites` | TLSv1.3 암호 스위트 |
| `tls-session-caching` | 세션 캐시 활성화 (기본 yes) |
| `tls-session-cache-size` | 세션 캐시 크기 (기본 20480) |

---

### 5-5. GENERAL (일반)

| 설정 | 기본값 | 설명 |
|---|---|---|
| `daemonize` | `no` | 데몬 실행 여부 (**IMMUTABLE**) |
| `supervised` | `no` | upstart/systemd/auto 연동 |
| `pidfile` | `/var/run/redis_6379.pid` | PID 파일 경로 (**IMMUTABLE**) |
| `loglevel` | `notice` | debug/verbose/notice/warning/nothing |
| `logfile` | `""` | 로그 파일 (빈 문자열=stdout) |
| `syslog-enabled` | `no` | syslog 출력 (**IMMUTABLE**) |
| `syslog-ident` | `redis` | syslog 식별자 |
| `syslog-facility` | `local0` | syslog 시설 |
| `databases` | `16` | DB 수 (SELECT 0~N-1) |
| `always-show-logo` | `no` | ASCII 아트 로고 항상 표시 (**IMMUTABLE**) |
| `set-proc-title` | `yes` | 프로세스 이름 설정 여부 (**IMMUTABLE**) |
| `proc-title-template` | `{title} {listen-addr} {server-mode}` | 프로세스 이름 템플릿 |
| `locale-collate` | `""` | 문자열 비교 로케일 |
| `hide-user-data-from-log` | `no` | 로그에서 PII 제거 |

---

### 5-6. SNAPSHOTTING (RDB 스냅샷)

| 설정 | 기본값 | 설명 |
|---|---|---|
| `save` | (없음) | RDB 저장 조건: `save <초> <변경수>` (복수 설정 가능) |
| `stop-writes-on-bgsave-error` | `yes` | 백그라운드 저장 실패 시 쓰기 중단 |
| `rdbcompression` | `yes` | LZF 압축 |
| `rdbchecksum` | `yes` | CRC64 체크섬 (**IMMUTABLE**) |
| `sanitize-dump-payload` | `no` | RDB/RESTORE 로드 시 무결성 검사 |
| `dbfilename` | `dump.rdb` | RDB 파일명 (**PROTECTED**) |
| `rdb-del-sync-files` | `no` | 영속성 비활성 인스턴스에서 복제용 RDB 즉시 삭제 |
| `dir` | `./` | 작업 디렉토리 (RDB, AOF 저장 위치) |

**`save` 설정 예시:**
```
save 3600 1       # 1시간에 1건 변경
save 300 100      # 5분에 100건 변경
save 60 10000     # 1분에 10000건 변경
save ""           # RDB 비활성화
```

---

### 5-7. REPLICATION (복제)

| 설정 | 기본값 | 설명 |
|---|---|---|
| `replicaof` | — | 마스터 지정: `replicaof <ip> <port>` |
| `masterauth` | — | 마스터 인증 비밀번호 |
| `masteruser` | — | 마스터 ACL 사용자명 (Redis 6+) |
| `replica-serve-stale-data` | `yes` | 마스터 연결 끊김 시 오래된 데이터 제공 여부 |
| `replica-read-only` | `yes` | 레플리카 읽기 전용 |
| `repl-diskless-sync` | `yes` | 디스크 없는 복제 동기화 |
| `repl-diskless-sync-delay` | `5` | diskless 전송 대기 시간(초) |
| `repl-diskless-sync-max-replicas` | `0` | 최대 기다릴 레플리카 수 (0=무제한) |
| `repl-diskless-load` | `disabled` | disabled/swapdb/flushdb/on-empty-db |
| `repl-ping-replica-period` | `10` | 마스터→레플리카 PING 주기(초) |
| `repl-timeout` | `60` | 복제 타임아웃(초) |
| `repl-disable-tcp-nodelay` | `no` | TCP_NODELAY 비활성화 (대역폭 절약 vs 지연) |
| `repl-backlog-size` | `1mb` | 부분 재동기화 백로그 버퍼 크기 |
| `repl-backlog-ttl` | `3600` | 레플리카 없을 때 백로그 유지 시간(초) |
| `replica-priority` | `100` | Sentinel 승격 우선순위 (0=승격 불가) |
| `min-replicas-to-write` | `0` | 쓰기 허용 최소 레플리카 수 |
| `min-replicas-max-lag` | `10` | 허용 최대 레플리카 지연(초) |
| `replica-announce-ip/port` | — | NAT/Docker 환경에서 레플리카 공개 주소 |
| `propagation-error-behavior` | `ignore` | 복제 오류 시 ignore/panic/panic-on-replicas |

---

### 5-8. KEYS TRACKING

| 설정 | 기본값 | 설명 |
|---|---|---|
| `tracking-table-max-keys` | `1000000` | 클라이언트 사이드 캐싱 무효화 테이블 최대 크기 |

---

### 5-9. SECURITY (보안)

#### ACL 규칙 문법

```
user <이름> [on|off] [+@<카테고리>] [-<명령>] [~<키패턴>] [&<채널패턴>] [><비밀번호>]
```

| 규칙 | 의미 |
|---|---|
| `on` / `off` | 사용자 활성화/비활성화 |
| `+<cmd>` / `-<cmd>` | 명령 허용/거부 |
| `+@<category>` | 카테고리 전체 허용 (@all, @read, @write, @admin 등) |
| `~<pattern>` | 키 접근 패턴 |
| `%R~<pattern>` / `%W~<pattern>` | 읽기/쓰기 전용 키 패턴 |
| `&<pattern>` | Pub/Sub 채널 패턴 |
| `><password>` | 비밀번호 추가 |
| `nopass` | 비밀번호 없이 인증 |
| `reset` | 사용자 초기화 |
| `(<options>)` | 선택자(selector) 생성 |

**ACL 카테고리:** `@keyspace`, `@read`, `@write`, `@admin`, `@dangerous`, `@connection`, `@blocking`, `@fast`, `@slow`, `@pubsub`, `@transaction`, `@scripting`, `@set`, `@sortedset`, `@list`, `@hash`, `@string`, `@bitmap`, `@hyperloglog`, `@geo`, `@stream`

| 설정 | 기본값 | 설명 |
|---|---|---|
| `acllog-max-len` | `128` | ACL 로그 최대 항목 수 |
| `aclfile` | — | 외부 ACL 파일 경로 (**IMMUTABLE**, conf 내 user와 혼용 불가) |
| `requirepass` | — | 기본 사용자 비밀번호 (ACL 호환 레이어) |
| `acl-pubsub-default` | `resetchannels` | 신규 사용자 Pub/Sub 기본 권한 |
| `rename-command` | — | 명령어 이름 변경 (deprecated, ACL 사용 권장) |

---

### 5-10. CLIENTS

| 설정 | 기본값 | 설명 |
|---|---|---|
| `maxclients` | `10000` | 최대 동시 연결 수 |

---

### 5-11. MEMORY MANAGEMENT

| 설정 | 기본값 | 설명 |
|---|---|---|
| `maxmemory` | (없음) | 최대 메모리 사용량 (bytes, mb, gb 단위 지원) |
| `maxmemory-policy` | `noeviction` | 메모리 초과 시 축출 정책 |
| `maxmemory-samples` | `5` | LRU/LFU 근사 알고리즘 샘플 수 (최대 64) |
| `maxmemory-eviction-tenacity` | `10` | 축출 처리 적극성 (0=최소 지연, 100=지연 무시) |
| `replica-ignore-maxmemory` | `yes` | 레플리카에서 maxmemory 무시 |
| `active-expire-effort` | `1` | 능동적 키 만료 처리 강도 (1~10) |
| `maxmemory-clients` | `0` | 클라이언트 전체 메모리 상한 (0=비활성) |

**축출 정책:**

| 정책 | 대상 | 알고리즘 |
|---|---|---|
| `volatile-lru` | TTL 있는 키 | LRU (최근 사용 시간 기준) |
| `volatile-lfu` | TTL 있는 키 | LFU (사용 빈도 기준) |
| `volatile-lrm` | TTL 있는 키 | LRM (최근 쓰기 시간 기준) |
| `volatile-random` | TTL 있는 키 | 무작위 |
| `volatile-ttl` | TTL 있는 키 | TTL 짧은 순 |
| `allkeys-lru` | 모든 키 | LRU |
| `allkeys-lfu` | 모든 키 | LFU |
| `allkeys-lrm` | 모든 키 | LRM |
| `allkeys-random` | 모든 키 | 무작위 |
| `noeviction` | — | 축출 안 함, 쓰기 오류 반환 |

---

### 5-12. LAZY FREEING

```
lazyfree-lazy-eviction   no   # 축출 시 비동기 삭제
lazyfree-lazy-expire     no   # TTL 만료 시 비동기 삭제
lazyfree-lazy-server-del no   # 서버 내부 삭제(RENAME 등) 비동기화
replica-lazy-flush       no   # 레플리카 전체 플러시 비동기화
lazyfree-lazy-user-del   no   # DEL을 UNLINK처럼 동작
lazyfree-lazy-user-flush no   # FLUSHDB/FLUSHALL 기본 비동기화
```

---

### 5-13. THREADED I/O

| 설정 | 기본값 | 설명 |
|---|---|---|
| `io-threads` | `1` | I/O 스레드 수 (1=메인 스레드만) |

- 4코어 이상에서만 권장 (코어 수 - 1 정도)
- 읽기/파싱과 쓰기 모두 스레드화

---

### 5-14. KERNEL 제어

| 설정 | 기본값 | 설명 |
|---|---|---|
| `oom-score-adj` | `no` | Linux OOM killer 점수 제어 (no/yes/relative/absolute) |
| `oom-score-adj-values` | `0 200 800` | master, replica, 자식 프로세스별 점수 |
| `disable-thp` | `yes` | Linux THP(Transparent Huge Pages) 비활성화 |

---

### 5-15. APPEND ONLY MODE (AOF)

| 설정 | 기본값 | 설명 |
|---|---|---|
| `appendonly` | `no` | AOF 활성화 |
| `appendfilename` | `appendonly.aof` | AOF 기본 파일명 (**IMMUTABLE**) |
| `appenddirname` | `appendonlydir` | AOF 전용 디렉토리 (**IMMUTABLE**) |
| `appendfsync` | `everysec` | fsync 정책: always/everysec/no |
| `no-appendfsync-on-rewrite` | `no` | 백그라운드 재작성 중 fsync 생략 |
| `auto-aof-rewrite-percentage` | `100` | 자동 재작성 트리거 증가율 (%) |
| `auto-aof-rewrite-min-size` | `64mb` | 자동 재작성 최소 크기 |
| `aof-load-truncated` | `yes` | 잘린 AOF 로드 허용 |
| `aof-load-corrupt-tail-max-size` | `0` | 손상된 끝부분 자동 복구 최대 크기 (0=비활성) |
| `aof-use-rdb-preamble` | `yes` | AOF 기본 파일에 RDB 형식 사용 |
| `aof-timestamp-enabled` | `no` | AOF 타임스탬프 기록 (point-in-time 복구용) |

**Redis 7+ AOF 파일 구조:**
```
appendonly.aof.1.base.rdb       ← RDB 형식 베이스 파일
appendonly.aof.1.incr.aof       ← 증분 명령 파일
appendonly.aof.2.incr.aof       ← 추가 증분 파일
appendonly.aof.manifest         ← 매니페스트 (순서 관리)
```

---

### 5-16. SHUTDOWN

| 설정 | 기본값 | 설명 |
|---|---|---|
| `shutdown-timeout` | `10` | 레플리카 동기화 대기 시간(초) |
| `shutdown-on-sigint` | `default` | SIGINT 수신 시 종료 옵션 |
| `shutdown-on-sigterm` | `default` | SIGTERM 수신 시 종료 옵션 |

종료 옵션 플래그: `default`, `save`, `nosave`, `now`, `force` (조합 가능, `save`+`nosave` 동시 불가)

---

### 5-17. SCRIPT / FUNCTION 실행 제한

| 설정 | 기본값 | 설명 |
|---|---|---|
| `busy-reply-threshold` (= `lua-time-limit`) | `5000` | Lua/Function 최대 실행 시간(ms) |

초과 시 대부분 명령에 `BUSY` 오류 반환, `SCRIPT KILL`/`FUNCTION KILL`만 허용.

---

### 5-18. REDIS CLUSTER

| 설정 | 기본값 | 설명 |
|---|---|---|
| `cluster-enabled` | `no` | 클러스터 모드 활성화 (**IMMUTABLE**) |
| `cluster-config-file` | `nodes.conf` | 클러스터 설정 파일 (**IMMUTABLE**) |
| `cluster-node-timeout` | `15000` | 노드 장애 판정 타임아웃(ms) |
| `cluster-port` | `0` | 클러스터 버스 포트 (0=명령 포트+10000) |
| `cluster-replica-validity-factor` | `10` | 레플리카 페일오버 허용 최대 유휴 배수 |
| `cluster-migration-barrier` | `1` | 레플리카 마이그레이션 최소 여분 수 |
| `cluster-require-full-coverage` | `yes` | 슬롯 미커버 시 쿼리 중단 여부 |
| `cluster-replica-no-failover` | `no` | 자동 페일오버 비활성화 |
| `cluster-allow-reads-when-down` | `no` | 다운 상태에서 읽기 허용 |
| `cluster-announce-ip/port/tls-port/bus-port` | — | NAT/Docker 환경 공개 주소 설정 |
| `cluster-preferred-endpoint-type` | `ip` | ip/hostname/unknown-endpoint |
| `cluster-slot-stats-enabled` | `no` | 슬롯별 통계 수집 (cpu/net/mem/yes/no) |
| `cluster-link-sendbuf-limit` | `0` | 클러스터 링크 전송 버퍼 상한 |
| `cluster-compatibility-sample-ratio` | `0` | 클러스터 명령 호환성 샘플링 비율 (0~100%) |
| `cluster-slot-migration-write-pause-timeout` | `10000` | 슬롯 이전 쓰기 일시 중지 최대 시간(ms) |

---

### 5-19. SLOW LOG

| 설정 | 기본값 | 설명 |
|---|---|---|
| `slowlog-log-slower-than` | `10000` | 기록 기준 실행 시간(μs), 음수=비활성, 0=전체 기록 |
| `slowlog-max-len` | `128` | 슬로우 로그 최대 항목 수 |

---

### 5-20. LATENCY 모니터링

| 설정 | 기본값 | 설명 |
|---|---|---|
| `latency-monitor-threshold` | `0` | 레이턴시 기록 기준(ms), 0=비활성 |
| `latency-tracking` | `yes` | 명령별 레이턴시 분포 추적 |
| `latency-tracking-info-percentiles` | `50 99 99.9` | INFO latencystats 제공 백분위 |

---

### 5-21. EVENT NOTIFICATION (키스페이스 알림)

```
notify-keyspace-events ""     # 기본: 비활성
notify-keyspace-events KEA    # 모든 이벤트 활성화
notify-keyspace-events Ex     # 만료 키 이벤트만
```

| 문자 | 이벤트 종류 |
|---|---|
| `K` | 키스페이스 이벤트 (`__keyspace@N__:key`) |
| `E` | 키이벤트 (`__keyevent@N__:event`) |
| `g` | DEL, EXPIRE, RENAME 등 generic 명령 |
| `$` | String 명령 |
| `l` / `s` / `h` / `z` | List/Set/Hash/ZSet 명령 |
| `x` | 만료(Expired) 이벤트 |
| `e` | 축출(Evicted) 이벤트 |
| `n` | 신규 키 생성 이벤트 |
| `t` | Stream 명령 |
| `A` | g$lshzxetd 전체 (n, m, o, c 제외) |

---

### 5-22. ADVANCED CONFIG (인코딩 임계값)

각 자료구조는 크기가 작을 때 압축 인코딩을 사용하며, 아래 임계값 초과 시 일반 인코딩으로 전환된다.

| 설정 | 기본값 | 설명 |
|---|---|---|
| `hash-max-listpack-entries` | `512` | Hash listpack 최대 항목 수 |
| `hash-max-listpack-value` | `64` | Hash listpack 최대 필드 크기(bytes) |
| `list-max-listpack-size` | `-2` | List 노드 크기 (-2=8KB, 양수=항목 수) |
| `list-compress-depth` | `0` | List 양쪽 비압축 노드 수 |
| `set-max-intset-entries` | `512` | 정수 Set intset 최대 항목 수 |
| `set-max-listpack-entries` | `128` | 비정수 Set listpack 최대 항목 수 |
| `set-max-listpack-value` | `64` | Set listpack 최대 항목 크기 |
| `zset-max-listpack-entries` | `128` | Sorted Set listpack 최대 항목 수 |
| `zset-max-listpack-value` | `64` | Sorted Set listpack 최대 항목 크기 |
| `hll-sparse-max-bytes` | `3000` | HyperLogLog sparse → dense 전환 임계값 |
| `stream-node-max-bytes` | `4096` | Stream 기수 트리 노드 최대 크기 |
| `stream-node-max-entries` | `100` | Stream 노드 최대 항목 수 |
| `stream-idmp-duration` | `100` | IDMP 중복 감지 윈도우(초) |
| `stream-idmp-maxsize` | `100` | IDMP 추적 최대 항목 수 |

---

### 5-23. 기타 운영 설정

| 설정 | 기본값 | 설명 |
|---|---|---|
| `hz` | `10` | 백그라운드 작업 주기 (1~500) |
| `dynamic-hz` | `yes` | 클라이언트 수에 따른 적응형 HZ |
| `aof-rewrite-incremental-fsync` | `yes` | AOF 재작성 시 4MB마다 fsync |
| `rdb-save-incremental-fsync` | `yes` | RDB 저장 시 4MB마다 fsync |
| `activerehashing` | `yes` | 능동적 해시 테이블 재해싱 |
| `lfu-log-factor` | `10` | LFU 카운터 증가 로그 계수 |
| `lfu-decay-time` | `1` | LFU 카운터 감소 시간(분) |
| `jemalloc-bg-thread` | `yes` | jemalloc 백그라운드 정리 스레드 |
| `activedefrag` | `no` | 온라인 메모리 조각화 정리 (jemalloc 전용) |
| `client-output-buffer-limit` | (타입별) | 클라이언트 출력 버퍼 상한 |
| `client-query-buffer-limit` | `1gb` | 클라이언트 입력 쿼리 버퍼 상한 |
| `proto-max-bulk-len` | `512mb` | 단일 벌크 요청 최대 크기 |
| `slowlog-log-slower-than` | `10000` | 슬로우 로그 기준(μs) |
| `server-cpulist` / `bio-cpulist` | — | CPU 코어 고정 (Linux/FreeBSD) |
| `ignore-warnings` | — | 시작 경고 억제 목록 |
| `key-memory-histograms` | `no` | 타입별 메모리 히스토그램 수집 (시작 시만 적용) |

**`client-output-buffer-limit` 기본값:**
```
client-output-buffer-limit normal  0      0     0    # 무제한
client-output-buffer-limit replica 256mb  64mb  60   # hard 256MB, soft 64MB/60초
client-output-buffer-limit pubsub  32mb   8mb   60   # hard 32MB, soft 8MB/60초
```

---

## 6. 단위 표기법

redis.conf에서 메모리 크기는 대소문자 구분 없이 다음 단위를 사용한다:

| 표기 | 크기 |
|---|---|
| `1k` | 1,000 bytes |
| `1kb` | 1,024 bytes |
| `1m` | 1,000,000 bytes |
| `1mb` | 1,048,576 bytes |
| `1g` | 1,000,000,000 bytes |
| `1gb` | 1,073,741,824 bytes |

---

## 7. 설정 적용 흐름 다이어그램

```
서버 시작
    │
    ├─ [1] initServerConfig()
    │       모든 standardConfig에 대해 interface.init() 호출 → 기본값 설정
    │
    ├─ [2] loadServerConfig(redis.conf)
    │       └─ loadServerConfigFromString()
    │               ├─ include 재귀 처리
    │               ├─ lookupConfig → interface.set() → server.xxx 필드 갱신
    │               └─ 특수 지시자 (save/bind/user/loadmodule 등) 직접 처리
    │
    ├─ [3] 런타임 변경: CONFIG SET
    │       ├─ IMMUTABLE 체크
    │       ├─ PROTECTED 체크
    │       ├─ interface.set()
    │       └─ interface.apply() → 실제 서버 동작 변경
    │
    ├─ [4] CONFIG GET → interface.get() → 응답 생성
    │
    └─ [5] CONFIG REWRITE → 현재 설정을 redis.conf에 재기록
```

---

## 8. 설계 특징 요약

| 특징 | 내용 |
|---|---|
| **통일된 타입 시스템** | 5가지 타입 + 함수 포인터 인터페이스로 모든 설정을 일관되게 처리 |
| **불변/가변 구분** | `IMMUTABLE_CONFIG`으로 재시작 필요 설정을 컴파일 타임에 명확히 구분 |
| **보호 설정** | `PROTECTED_CONFIG` + `enable-protected-configs`로 파일 경로 등 민감 설정 보호 |
| **런타임 재설정** | 대부분의 설정이 `CONFIG SET`으로 무중단 변경 가능 |
| **설정 파일 재작성** | `CONFIG REWRITE`로 메모리 상태를 파일로 역동기화 |
| **include 합성** | 파일 분할과 와일드카드로 환경별 설정 조합 가능 |
| **Enum 테이블** | 문자열 ↔ 정수 변환을 테이블로 관리하여 오타 방지 및 INFO 출력 일관성 유지 |
| **deprecated 처리** | 구버전 설정 키를 오류 없이 무시하는 명시적 목록 관리 |
