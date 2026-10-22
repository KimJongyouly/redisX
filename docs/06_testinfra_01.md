# Redis 테스트 인프라 소스 분석

## 1. 전체 구조

Redis 테스트는 **Tcl(Tool Command Language) 8.5+** 기반의 자체 제작 테스트 프레임워크를 사용한다. 별도의 외부 테스트 라이브러리 없이 순수 Tcl 스크립트로 서버 생명주기, 병렬 실행, 검증을 모두 처리한다.

```
redis/
├── runtest                    ← 단위·통합 테스트 진입점 (sh → tclsh)
├── runtest-cluster            ← 클러스터 테스트 진입점
├── runtest-sentinel           ← Sentinel 테스트 진입점
├── runtest-moduleapi          ← 모듈 API 테스트 진입점
└── tests/
    ├── test_helper.tcl        ← 메인 오케스트레이터 (899줄)
    ├── support/               ← 테스트 프레임워크 핵심 라이브러리
    │   ├── server.tcl         ← 서버 생명주기 관리 (961줄)
    │   ├── util.tcl           ← 유틸리티 함수 모음 (1,288줄)
    │   ├── test.tcl           ← test/assert 기본 프리미티브 (366줄)
    │   ├── redis.tcl          ← Tcl Redis 클라이언트 (478줄)
    │   ├── cluster.tcl        ← 클러스터 헬퍼 (372줄)
    │   ├── cluster_util.tcl   ← 클러스터 유틸리티
    │   ├── aofmanifest.tcl    ← AOF 매니페스트 헬퍼
    │   ├── tmpfile.tcl        ← 임시 파일 관리
    │   └── response_transformers.tcl ← RESP 응답 변환
    ├── unit/                  ← 단위 테스트 (~50개 파일)
    │   ├── type/              ← 자료구조 타입 테스트
    │   ├── moduleapi/         ← 모듈 API 테스트 (Tcl + C)
    │   └── cluster/           ← 클러스터 단위 테스트
    ├── integration/           ← 통합 테스트 (복제, AOF 등)
    ├── cluster/               ← 클러스터 전용 테스트
    ├── sentinel/              ← Sentinel 전용 테스트
    ├── modules/               ← 모듈 API 테스트용 .so 소스 (C)
    ├── assets/                ← 기본 설정 파일 (default.conf 등)
    └── tmp/                   ← 테스트 임시 파일 (런타임 생성)
```

---

## 2. 실행 진입점

### 2-1. runtest / runtest-cluster / runtest-sentinel

```sh
#!/bin/sh
TCL_VERSIONS="8.5 8.6 8.7 9.0"
for VERSION in $TCL_VERSIONS; do
    TCL=`which tclsh$VERSION 2>/dev/null` && TCLSH=$TCL
done
$TCLSH tests/test_helper.tcl "${@}"
```

Tcl 8.5~9.0 중 사용 가능한 버전을 자동 감지한다. 각 runtest 스크립트는 테스트 목적에 맞는 Tcl 진입 파일로 위임한다.

| 스크립트 | Tcl 진입점 | 대상 |
|---|---|---|
| `runtest` | `tests/test_helper.tcl` | 단위 + 통합 테스트 전체 |
| `runtest-cluster` | `tests/cluster/run.tcl` | 클러스터 전용 |
| `runtest-sentinel` | `tests/sentinel/run.tcl` | Sentinel 전용 |
| `runtest-moduleapi` | `tests/test_helper.tcl --single unit/moduleapi/...` | 모듈 API |

---

## 3. 서버-클라이언트 병렬 실행 모델

테스트 프레임워크는 **1 서버 프로세스 + N 클라이언트 프로세스** 모델로 병렬 실행한다.

```
test_helper.tcl (테스트 서버, 조정자)
    │
    ├── 클라이언트 #0  (tclsh test_helper.tcl --client PORT)
    ├── 클라이언트 #1
    ├── ...
    └── 클라이언트 #15  (기본 16개)

포트 범위: 21111 ~ 29111 (8000개 슬롯)
각 클라이언트: 8000 / 16 = 500개 포트 할당
```

### 조정 흐름

```
테스트 서버                    클라이언트 #N
─────────────                  ─────────────
소켓 리슨
클라이언트 N개 스폰 →           접속
테스트 파일 목록 보유

← "idle" 수신
"run <테스트파일>" 전송 →
                               테스트 파일 실행
                               redis-server 스폰/종료
← "done/err/skip" 수신
결과 집계·출력
다음 테스트 파일 분배 →
```

- `send_data_packet` 함수로 클라이언트↔서버 간 패킷 통신
- 프로그레스 타임아웃: 기본 **1200초(20분)** 무응답 시 종료
- `test_server_cron`이 100ms마다 호출되어 타임아웃·idle 클라이언트 관리

### 주요 전역 변수

```tcl
set ::numclients 16       # 병렬 클라이언트 수
set ::baseport 21111      # 시작 포트
set ::portcount 8000      # 포트 슬롯 수
set ::timeout 1200        # 전체 타임아웃 (초)
```

---

## 4. 테스트 파일 검색 및 필터링

```tcl
set test_dirs {unit  unit/type  unit/moduleapi  unit/cluster  integration}

foreach test_dir $test_dirs {
    set files [glob -nocomplain $dir/tests/$test_dir/*.tcl]
    foreach file [lsort $files] {
        lappend ::all_tests $test_dir/[file root [file tail $file]]
    }
}
```

모든 테스트는 `::all_tests` 리스트에 등록된 후 필터링된다.

### 필터 옵션

| 옵션 | 동작 |
|---|---|
| `--single <unit>` | 특정 테스트 파일만 실행 |
| `--only <name>` | 특정 테스트 이름 패턴만 실행 |
| `--skipunit <unit>` | 특정 파일 제외 |
| `--skiptest <name>` | 특정 테스트 이름 제외 |
| `--skipfile <file>` | 파일에 나열된 테스트 이름 전체 제외 |
| `--skip-till <unit>` | 지정 파일 이후부터 실행 |
| `--tags <tag>` | `+tag` = 허용, `-tag` = 거부 |
| `--list-tests` | 전체 테스트 목록만 출력 후 종료 |

---

## 5. 핵심 API: `start_server`

가장 중요한 프리미티브다. 테스트 격리 단위인 Redis 인스턴스를 시작하고, 테스트 블록 실행 후 자동 종료한다.

```tcl
start_server {tags {"string"} overrides {maxmemory 10mb}} {
    test {SET and GET an item} {
        r set x foobar
        r get x
    } {foobar}

    test {Empty value} {
        r set x {}
        r get x
    } {}
}
```

### start_server 옵션

| 옵션 | 설명 |
|---|---|
| `tags` | 이 서버 블록의 태그 목록 |
| `overrides` | 기본 설정에서 덮어쓸 설정 값 |
| `config` | 기본 설정 파일 선택 (기본: `default.conf`) |
| `config_lines` | 추가 설정 줄 (dict 미지원 다중 값용) |
| `omit` | 제거할 설정 지시자 |
| `args` | redis-server 추가 인자 |
| `keep_persistence` | 테스트 후 RDB/AOF 파일 유지 여부 |
| `wait_ready` | 서버 준비 대기 여부 (기본 true) |

### start_server 내부 흐름

```
1. tags_acceptable() 확인 → 거부 태그 있으면 건너뜀
2. tests/assets/default.conf 로드
3. TLS 모드면 인증서 설정 추가
4. 임시 dir, port, unixsocket 할당 (find_available_port)
5. global_overrides + 옵션 overrides 적용
6. tmpfile redis.conf 생성 (create_server_config_file)
7. spawn_server() → redis-server 프로세스 백그라운드 실행
8. wait_server_started() → 로그에서 "PID: $pid.*Server initialized" 확인 (최대 2분)
9. Redis 클라이언트 연결 생성 → select 9 (격리 DB)
10. ::servers 스택에 {pid, host, port, client, stdout, stderr, ...} push
11. 코드 블록 실행
12. kill_server() → SIGCONT → SIGTERM → (타임아웃 시) SIGKILL
13. Valgrind/Sanitizer 오류 로그 검사
14. ::servers 스택에서 pop
```

### 중첩 start_server

```tcl
start_server {} {          # 마스터 서버
    start_server {} {      # 레플리카 서버
        # srv 0 = 레플리카 (innermost)
        # srv -1 = 마스터 (outer)
        r -1 set key value    # 마스터에 SET
        r get key             # 레플리카에서 GET
    }
}
```

`::servers` 스택 덕분에 중첩 서버에 인덱스(-1, -2 …)로 접근 가능하다.

---

## 6. 서버 스폰 및 도구 통합

```tcl
proc spawn_server {config_file stdout stderr args} {
    set cmd [list src/redis-server $config_file]

    if {$::valgrind} {
        # valgrind --track-origins=yes --leak-check=full ...
        set pid [exec valgrind ... {*}$cmd >> $stdout 2>> $stderr &]
    } elseif ($::stack_logging) {
        # macOS MallocStackLogging
        set pid [exec /usr/bin/env MallocStackLogging=1 ... {*}$cmd &]
    } else {
        # 일반 실행 (ASAN/MSAN/TSAN 환경 변수 설정)
        set env [list
            "ASAN_OPTIONS=allocator_may_return_null=1"
            "MSAN_OPTIONS=allocator_may_return_null=1"
            "TSAN_OPTIONS=allocator_may_return_null=1,detect_deadlocks=0,..."
        ]
        set pid [exec /usr/bin/env {*}$env {*}$cmd >> $stdout 2>> $stderr &]
    }
}
```

| 모드 | 활성화 옵션 | 동작 |
|---|---|---|
| 기본 | — | 직접 실행 + ASAN/MSAN/TSAN 환경 변수 |
| Valgrind | `--valgrind` | valgrind 래핑, 종료 시 메모리 오류 로그 검사 |
| ThreadSanitizer | `--tsan` | tsan.sup 억제 파일 + deadlock 감지 |
| macOS 메모리 | `--stack-logging` | MallocStackLogging 활성화 |
| TLS | `--tls` | tls 패키지 + 인증서 자동 설정 |
| 외부 서버 | `--host` / `--port` | 이미 실행 중인 서버에 연결 |

---

## 7. Tcl Redis 클라이언트 (`redis.tcl`)

```tcl
set r [redis 127.0.0.1 6379]    # 연결
$r set key value                 # 명령 실행
$r get key                       # → "value"
$r close                         # 연결 종료
```

테스트에서는 `r` 명령어를 통해 현재 스택 최상단 서버에 바로 접근한다.

```tcl
proc r {args} {
    set level 0
    if {[string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set args [lrange $args 1 end]
    }
    [srv $level "client"] {*}$args
}
```

### 클라이언트 헬퍼 함수

| 함수 | 설명 |
|---|---|
| `r <cmd>` | 현재 서버(srv 0)에 명령 실행 |
| `r -1 <cmd>` | 외부 서버(srv -1)에 명령 실행 |
| `R <n> <cmd>` | n번째 서버에 명령 실행 (양수 인덱스) |
| `Rn <n>` | n번째 서버 클라이언트 객체 반환 |
| `s <field>` | 현재 서버 INFO 필드 반환 |
| `S <n> <field>` | n번째 서버 INFO 필드 반환 |
| `CI <n> <field>` | n번째 서버 CLUSTER INFO 필드 반환 |
| `redis_client` | 새 블로킹 클라이언트 생성 |
| `redis_deferring_client` | 비동기(지연 읽기) 클라이언트 생성 |
| `reconnect` | 현재 서버 클라이언트 재연결 |

**클라이언트 특성:**
- `blocking` / `deferred` 모드 지원 (비동기 응답 테스트용)
- RESP3 지원 (`testing_resp3` 플래그)
- TLS 지원 (tls 패키지 연동)
- `readraw` 모드: 응답 변환 없이 원시 바이트 반환

---

## 8. test / assert 프리미티브 (`test.tcl`)

### test 명령어

```tcl
test {테스트 이름} {
    # 실행할 코드
    r set x hello
    r get x
} {hello}    # 기대 반환값 (생략 가능, 패턴 매칭 가능)
```

**test 프로시저 내부 흐름:**
1. `::skiptests` / `::only_tests` 필터 확인
2. `tags_acceptable()` 검사 (태그 기반 필터)
3. `config:restore` 태그 있으면 서버 설정 저장
4. 코드 블록 실행
5. `assertion:` 접두사 오류 → 우아한 실패 처리
6. 기타 오류 → 스택 위로 재전파
7. 반환값을 `okpattern`과 비교 (문자열 매칭 또는 glob)
8. 결과 패킷(`ok`/`err`/`skip`) 테스트 서버로 전송
9. `config:restore` 태그면 서버 설정 복원

### assert 함수 목록

| 함수 | 검증 내용 |
|---|---|
| `assert {expr}` | Tcl 표현식이 참 |
| `assert_equal val expected` | 값 동일 (`ne` 비교) |
| `assert_not_equal val expected` | 값 상이 |
| `assert_match pattern value` | glob 패턴 매칭 |
| `assert_no_match pattern value` | glob 패턴 불일치 |
| `assert_lessthan val expected` | 미만 |
| `assert_lessthan_equal val expected` | 이하 |
| `assert_morethan val expected` | 초과 |
| `assert_morethan_equal val expected` | 이상 |
| `assert_range val min max` | 범위 내 |
| `assert_error pattern code` | 오류 발생 + 패턴 일치 |
| `assert_encoding enc key` | 키 인코딩 일치 (ignoreencoding 모드 시 스킵) |
| `assert_type type key` | 키 타입 일치 |
| `assert_refcount ref key` | 객체 참조 카운트 일치 |
| `fail {msg}` | 즉시 실패 |

---

## 9. 태그 시스템 (`tags`)

```tcl
start_server {tags {"repl"}} {
    tags {"slow"} {
        test {Replication with big payload} { ... }
    }

    test {Fast replication test} { ... } {} {fast}
}
```

태그는 스택처럼 **누적**된다. `start_server`의 태그 + `tags {}` 블록 태그 + `test`의 4번째 인자 태그가 합쳐진다.

### 예약 태그

| 태그 | 의미 |
|---|---|
| `slow` | 느린 테스트 (`--no-latency` 모드에서 유용) |
| `fast` | 빠른 테스트 |
| `large-memory` | 대용량 메모리 필요 (`--large-memory` 없으면 스킵) |
| `needs:debug` | DEBUG 명령어 필요 |
| `external:skip` | 외부 서버 모드에서 스킵 |
| `tls:skip` | TLS 모드에서 스킵 |
| `tsan:skip` | ThreadSanitizer 모드에서 스킵 |
| `cluster:skip` | 클러스터 모드에서 스킵 |
| `singledb:skip` | 단일 DB 모드에서 스킵 |
| `debug_defrag:skip` | defrag 디버그 빌드에서 스킵 |
| `logreqres:skip` | 요청·응답 로깅 모드에서 스킵 |
| `experimental` | 실험적 테스트 (명시적 허용 필요) |
| `config:restore` | 테스트 전후 서버 설정 자동 저장/복원 |
| `leaks` | macOS에서 leaks 도구로 메모리 누수 검사 |

### 필터 동작

```tcl
proc tags_acceptable {tags err_return} {
    # allowtags 있으면 최소 1개 교집합 필요
    # denytags 있으면 교집합 없어야 함
    # 모드별(tls, tsan, cluster, ...) 특수 태그 확인
}
```

---

## 10. 유틸리티 함수 (`util.tcl`)

### 상태 조회

```tcl
proc status {r property}              # INFO 속성 값 반환
proc getInfoProperty {infostr field}  # INFO 문자열에서 필드 추출
proc s {field}                        # 현재 서버 INFO 필드 단축어
```

### 비동기 대기

```tcl
# 조건이 참이 될 때까지 최대 maxtries × delay(ms) 대기
proc wait_for_condition {maxtries delay expr else elsescript} {
    while {[incr maxtries -1] >= 0} {
        if {[uplevel 1 [list expr $expr]]} break
        after $delay
    }
    if {$maxtries == -1} { uplevel 1 $elsescript }
}

# 사용 예
wait_for_condition 50 100 {
    [status r rdb_bgsave_in_progress] == 0
} else {
    fail "BGSAVE did not complete"
}
```

### 백그라운드 저장 대기

```tcl
proc waitForBgsave r       # RDB bgsave 완료 대기
proc waitForBgrewriteaof r # AOF rewrite 완료 대기
proc waitForCondition r field value  # INFO 필드 값 대기
```

### 무작위 데이터 생성

```tcl
proc randstring {min max {type binary}}
# type: binary(0-255), alpha(48-122), simplealpha(알파벳), compr(48-52)
```

### 서버 설정 저장/복원 (config:restore 태그용)

```tcl
proc save_server_configs {}           # 모든 스택 서버 설정 저장
proc restore_server_configs saved     # 변경된 설정만 복원 (diff 기반)
proc save_single_server_config client  # 단일 서버 CONFIG GET * 스냅샷
proc restore_single_server_config client saved {diff_based 0}
```

### 크래시·오류 로그 분석

```tcl
proc crashlog_from_file {filename}    # "REDIS BUG REPORT" 이후 라인 추출
proc sanitizer_errors_from_file {filename}  # ASAN/MSAN/UBSAN 오류 감지
proc find_valgrind_errors {stderr}   # Valgrind 오류 감지
```

### 복제 스트림 테스트

```tcl
proc attach_to_replication_stream_on_connection {conn}
proc read_from_replication_stream {s}
proc assert_replication_stream {s patterns}
proc close_replication_stream {s}
```

---

## 11. 테스트 디렉토리 구성

### 11-1. 단위 테스트 (`tests/unit/`)

기능별로 파일이 분리되어 있으며 각각 `start_server`로 독립 인스턴스에서 실행된다.

| 파일 | 테스트 내용 |
|---|---|
| `acl.tcl` / `acl-v2.tcl` | ACL 규칙, 사용자 관리 |
| `expire.tcl` | TTL, 만료 메커니즘 |
| `maxmemory.tcl` | 메모리 한계, 축출 정책 |
| `scripting.tcl` | Lua 스크립트, EVAL |
| `functions.tcl` | Redis Functions |
| `protocol.tcl` | RESP 프로토콜 |
| `networking.tcl` | 클라이언트 연결, 버퍼 |
| `tracking.tcl` | 클라이언트 사이드 캐싱 |
| `pubsub.tcl` / `pubsubshard.tcl` | Pub/Sub |
| `multi.tcl` | 트랜잭션 |
| `slowlog.tcl` | 슬로우 로그 |
| `latency-monitor.tcl` | 레이턴시 모니터 |
| `scan.tcl` | SCAN 계열 명령 |
| `sort.tcl` | SORT 명령 |
| `geo.tcl` | Geo 명령 |
| `hyperloglog.tcl` | HyperLogLog |
| `lazyfree.tcl` | 비동기 삭제 |
| `introspection.tcl` | INFO, CONFIG, DEBUG |
| `type/string.tcl` | String 자료구조 |
| `type/list.tcl` ~ `list-3.tcl` | List 자료구조 |
| `type/hash.tcl` | Hash 자료구조 |
| `type/set.tcl` | Set 자료구조 |
| `type/zset.tcl` | Sorted Set |
| `type/stream.tcl` | Streams |
| `type/hash-field-expire.tcl` | Hash 필드 TTL |

### 11-2. 통합 테스트 (`tests/integration/`)

| 파일 | 테스트 내용 |
|---|---|
| `replication.tcl` ~ `replication-4.tcl` | 마스터-레플리카 복제 |
| `replication-psync.tcl` | PSYNC 부분 재동기화 |
| `replication-buffer.tcl` | 복제 버퍼 관리 |
| `replication-rdbchannel.tcl` | RDB 채널 복제 |
| `aof.tcl` | AOF 기본 동작 |
| `aof-multi-part.tcl` | Redis 7 다중 파일 AOF |
| `aof-race.tcl` | AOF 경쟁 조건 |
| `rdb.tcl` | RDB 스냅샷 |
| `psync2.tcl` ~ `psync2-reg.tcl` | PSYNC v2 |
| `redis-cli.tcl` | redis-cli 도구 |
| `redis-benchmark.tcl` | redis-benchmark 도구 |
| `failover.tcl` | 페일오버 시나리오 |
| `logging.tcl` | 로깅 기능 |
| `corrupt-dump.tcl` | 손상된 RDB 복구 |

### 11-3. 모듈 API 테스트 (`tests/modules/` + `tests/unit/moduleapi/`)

C로 작성된 테스트용 모듈(`.c` → `.so`)과 이를 검증하는 Tcl 테스트 파일 쌍으로 구성된다.

```
tests/modules/basics.c       ← C 모듈 소스
tests/unit/moduleapi/basics.tcl  ← 해당 모듈 테스트
```

모듈 빌드는 `tests/modules/Makefile`로 처리된다. 주요 모듈:
`basics`, `blockedclient`, `datatype`, `defragtest`, `auth`, `crash`, `eventloop`, `fork`, `hash`, `blockonkeys`, `commandfilter`, `configaccess` 등 40여 종

---

## 12. 특수 실행 모드

### 12-1. 외부 서버 모드

```bash
./runtest --host 127.0.0.1 --port 6379
```

이미 실행 중인 서버를 대상으로 테스트한다. `external:skip` 태그가 붙은 테스트는 자동 스킵된다.

### 12-2. Valgrind 모드

```bash
./runtest --valgrind
```

- 각 redis-server를 valgrind로 감싸 실행
- 종료 시 메모리 누수/오류 자동 검사
- 타임아웃 10초 → 120초로 확장

### 12-3. TLS 모드

```bash
./runtest --tls        # 정적 링크 TLS
./runtest --tls-module # 모듈 방식 TLS
```

`tests/tls/` 디렉토리의 자체 서명 인증서를 사용한다.

### 12-4. 반복 실행

```bash
./runtest --loop       # 무한 반복
./runtest --loops 10   # 10회 반복
```

### 12-5. 단일 DB 모드

```bash
./runtest --singledb    # DB 0만 사용
./runtest --cluster-mode # 클러스터 모드 + singledb
```

### 12-6. 기타 유용한 옵션

| 옵션 | 동작 |
|---|---|
| `--verbose` | 상세 출력 (2번 지정 시 더 상세) |
| `--stop` | 첫 실패 시 일시 정지 (Enter로 계속) |
| `--dump-logs` | 실패 시 서버 로그 전체 출력 |
| `--dont-clean` | 테스트 후 tmp 디렉토리 보존 |
| `--accurate` | 퍼즈 테스트 반복 수 증가 |
| `--no-latency` | 레이턴시 민감 테스트 스킵 |
| `--large-memory` | 대용량 메모리 필요 테스트 허용 |
| `--force-resp3` | 모든 클라이언트를 RESP3로 강제 |
| `--log-req-res` | 요청·응답 로깅 활성화 |
| `--debug-defrag` | activedefrag 강제 활성화 |
| `--clients <n>` | 병렬 클라이언트 수 변경 |
| `--timeout <sec>` | 전체 타임아웃 변경 |

---

## 13. 서버 시작 확인 메커니즘

```tcl
proc wait_server_started {config_file stdout pid} {
    set checkperiod 100   ;# 100ms 간격 폴링
    set maxiter [expr {120*1000/$checkperiod}]  ;# 최대 2분

    while 1 {
        # 로그에서 "PID: $pid.*Server initialized" 패턴 탐색
        if {[regexp -- " PID: $pid.*Server initialized" [exec cat $stdout]]} {
            break
        }
        # 포트 충돌 감지
        if {[regexp {Failed listening on port} [exec cat $stdout]]} {
            set port_busy 1; break
        }
        after $checkperiod
    }
}
```

포트가 충돌하면 `find_available_port`로 다시 탐색한다. 각 클라이언트는 독립 포트 범위를 사용하므로 충돌이 최소화된다.

---

## 14. 테스트 코드 패턴 예시

### 기본 패턴

```tcl
start_server {tags {"string"}} {
    test {SET and GET an item} {
        r set x foobar
        r get x
    } {foobar}

    test {Expire precision} {
        r set x val
        r pexpire x 100
        after 200
        r exists x
    } {0}
}
```

### 복제 테스트 패턴

```tcl
start_server {tags {"repl"}} {       # 마스터
    start_server {} {                 # 레플리카
        r replicaof [srv -1 host] [srv -1 port]
        wait_for_condition 50 100 {
            [status r master_link_status] eq "up"
        } else {
            fail "Replication link not established"
        }

        r -1 set testkey testval           # 마스터에 쓰기
        wait_for_condition 50 100 {
            [r get testkey] eq "testval"   # 레플리카에서 확인
        } else {
            fail "Key not replicated"
        }
    }
}
```

### 설정 자동 복원 패턴

```tcl
test {Maxmemory eviction} {
    r config set maxmemory 1mb
    # ... 테스트 ...
} {} {config:restore}    # 테스트 후 maxmemory 자동 복원
```

### 에러 검증 패턴

```tcl
test {WRONGTYPE error} {
    r set mykey hello
    catch {r lpush mykey world} err
    assert_match {WRONGTYPE*} $err
}
```

---

## 15. 테스트 결과 집계 및 출력

```tcl
set ::num_tests   0   # 전체 실행 수
set ::num_passed  0   # 성공
set ::num_failed  0   # 실패
set ::num_skipped 0   # 스킵 (태그 필터)
set ::num_aborted 0   # 중단 (태그 미허용)
```

테스트 종료 시 `Passed/Failed/Skipped` 요약과 함께 모든 실패 테스트 이름·상세가 출력된다. 실패 시 종료 코드 1 반환으로 CI 파이프라인 연동이 가능하다.

---

## 16. 설계 특징 요약

| 특징 | 내용 |
|---|---|
| **자체 완결 프레임워크** | 외부 테스트 라이브러리 없이 Tcl만으로 서버 관리·검증·병렬화 구현 |
| **완전한 격리** | 테스트마다 독립 redis-server 인스턴스, 독립 포트, 독립 임시 디렉토리 |
| **서버-클라이언트 병렬** | 기본 16개 병렬 클라이언트로 전체 테스트 수평 확장 |
| **도구 투명 통합** | Valgrind / ASAN / MSAN / TSan을 동일 테스트 코드로 커버 |
| **중첩 서버 스택** | 마스터-레플리카, 3중 계층 복제 등 복잡한 토폴로지를 스택 인덱스로 자연스럽게 표현 |
| **태그 기반 필터** | 세밀한 실행 범위 조정으로 CI 환경별 맞춤 실행 가능 |
| **설정 자동 복원** | `config:restore` 태그로 설정 변경 사이드이펙트 없는 테스트 작성 |
| **외부 서버 지원** | 이미 실행 중인 프로덕션 유사 서버에 대한 통합 테스트 가능 |
