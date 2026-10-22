# Redis `server.c` / `server.h` 분석 요약

> 대상: `redis/src/server.c` (8196줄) · `redis/src/server.h` (4534줄)  
> 분석 일자: 2026-04-13

---

## 목차

1. [server.h 개요](#serverh-개요)
2. [주요 #define 상수](#주요-define-상수)
3. [핵심 구조체](#핵심-구조체)
4. [열거형 (enum / typedef)](#열거형-enum--typedef)
5. [server.c 개요](#serverc-개요)
6. [주요 전역 변수](#주요-전역-변수)
7. [주요 함수 목록](#주요-함수-목록)
8. [요청 처리 데이터 흐름](#요청-처리-데이터-흐름)
9. [주목할 알고리즘/로직](#주목할-알고리즘로직)
10. [파일 간 관계](#파일-간-관계)

---

## server.h 개요

Redis 서버의 핵심 헤더 파일. 전역 상태(`redisServer`), 클라이언트(`client`), DB(`redisDb`), Redis 객체(`redisObject`) 등 서버 운영에 필요한 모든 데이터 구조, 상수, 함수 프로토타입을 정의한다. 다른 모든 `.c` 파일이 `#include "server.h"`로 공유한다.

---

## 주요 #define 상수

### 서버 설정 기본값

| 상수 | 줄 | 값/의미 |
|------|----|---------|
| `CONFIG_DEFAULT_HZ` | 116 | `10` — `serverCron()` 호출 빈도(Hz) |
| `CONFIG_MIN_HZ` | 117 | `1` |
| `CONFIG_MAX_HZ` | 118 | `500` |
| `CONFIG_RUN_ID_SIZE` | 130 | `40` — 서버 런 ID 크기 |
| `CONFIG_REPL_BACKLOG_MIN_SIZE` | 132 | `16KB` — 복제 백로그 최소 크기 |
| `CONFIG_BGSAVE_RETRY_DELAY` | 133 | `5초` |
| `CONFIG_BINDADDR_MAX` | 141 | `16` — 바인딩 주소 최대 개수 |
| `CONFIG_MIN_RESERVED_FDS` | 142 | `32` — 예약 파일 디스크립터 |
| `CRON_DBS_PER_CALL` | 120 | `16` — 크론 1회당 처리 DB 수 |
| `OBJ_SHARED_INTEGERS` | 124 | `10000` — 공유 정수 객체 최대 값 |
| `LOG_MAX_LEN` | 127 | `1024` — 로그 최대 길이 |
| `IO_THREADS_MAX_NUM` | 223 | `128` — 최대 IO 스레드 수 |
| `ZSKIPLIST_MAXLEVEL` | 633 | `32` — 스킵 리스트 최대 레벨 |

### 객체 타입 (`OBJ_*`)

| 상수 | 줄 | 값 |
|------|----|----|
| `OBJ_STRING` | 842 | `0` |
| `OBJ_LIST` | 843 | `1` |
| `OBJ_SET` | 844 | `2` |
| `OBJ_ZSET` | 845 | `3` |
| `OBJ_HASH` | 846 | `4` |
| `OBJ_MODULE` | 860 | `5` |
| `OBJ_STREAM` | 861 | `6` |
| `OBJ_TYPE_MAX` | 862 | `7` |

### 명령어 플래그 (`CMD_*`)

| 상수 | 줄 | 의미 |
|------|----|------|
| `CMD_WRITE` | 236 | 쓰기 명령어 |
| `CMD_READONLY` | 237 | 읽기 전용 |
| `CMD_DENYOOM` | 238 | OOM 상태에서 거부 |
| `CMD_ADMIN` | 240 | 관리 명령어 |
| `CMD_PUBSUB` | 241 | Pub/Sub 관련 |
| `CMD_BLOCKING` | 243 | 블로킹 가능 |
| `CMD_FAST` | 249 | O(1) 또는 O(log N) |
| `CMD_NO_AUTH` | 250 | 인증 불필요 |
| `CMD_MAY_REPLICATE` | 251 | 복제 트래픽 생성 가능 |
| `CMD_SENTINEL` | 252 | Sentinel 모드에서 사용 가능 |
| `CMD_INTERNAL` | 266 | 내부 명령어 |

### 명령어 키 접근 플래그 (`CMD_KEY_*`)

| 상수 | 줄 | 의미 |
|------|----|------|
| `CMD_KEY_RO` | 297 | 읽기 전용 접근 |
| `CMD_KEY_RW` | 298 | 읽기-쓰기 |
| `CMD_KEY_OW` | 301 | 덮어쓰기 |
| `CMD_KEY_RM` | 303 | 삭제 |
| `CMD_KEY_ACCESS` | 310 | 데이터 반환/복사 |
| `CMD_KEY_UPDATE` | 312 | 데이터 업데이트 |
| `CMD_KEY_INSERT` | 314 | 데이터 추가 |
| `CMD_KEY_DELETE` | 316 | 데이터 삭제 |

### 클라이언트 상태 플래그 (`CLIENT_*`)

| 상수 | 줄 | 의미 |
|------|----|------|
| `CLIENT_SLAVE` | 371 | 레플리카 클라이언트 |
| `CLIENT_MASTER` | 372 | 마스터 클라이언트 |
| `CLIENT_MONITOR` | 373 | MONITOR 클라이언트 |
| `CLIENT_MULTI` | 374 | MULTI 트랜잭션 중 |
| `CLIENT_BLOCKED` | 375 | 블로킹 중 |
| `CLIENT_PUBSUB` | 390 | Pub/Sub 모드 |
| `CLIENT_TRACKING` | 409 | 클라이언트 사이드 캐싱 활성화 |
| `CLIENT_PROTECTED` | 401 | 메모리 보호됨 |
| `CLIENT_INTERNAL` | 441 | 내부 클라이언트 |

### AOF / RDB 상태

| 상수 | 줄 | 의미 |
|------|----|------|
| `AOF_OFF` | 347 | AOF 비활성화 |
| `AOF_ON` | 348 | AOF 활성화 |
| `AOF_WAIT_REWRITE` | 349 | AOF 리라이트 대기 중 |
| `RDB_OK` | 361 | 저장 성공 |
| `RDB_NOT_EXIST` | 362 | RDB 파일 없음 |
| `RDB_FAILED` | 363 | 로드 실패 |

### 복제 레플리카 전송 상태

| 상수 | 줄 | 의미 |
|------|----|------|
| `SLAVE_STATE_WAIT_BGSAVE_START` | 566 | BGSAVE 시작 대기 |
| `SLAVE_STATE_WAIT_BGSAVE_END` | 567 | BGSAVE 완료 대기 |
| `SLAVE_STATE_SEND_BULK` | 568 | RDB 전송 중 |
| `SLAVE_STATE_ONLINE` | 569 | 온라인 |
| `SLAVE_STATE_RDB_TRANSMITTED` | 570 | RDB 전송 완료 |

### 메모리 제거 정책 (`MAXMEMORY_*`)

| 상수 | 줄 | 의미 |
|------|----|------|
| `MAXMEMORY_VOLATILE_LRU` | 687 | volatile-lru |
| `MAXMEMORY_VOLATILE_LFU` | 688 | volatile-lfu |
| `MAXMEMORY_VOLATILE_TTL` | 689 | volatile-ttl |
| `MAXMEMORY_VOLATILE_RANDOM` | 690 | volatile-random |
| `MAXMEMORY_ALLKEYS_LRU` | 691 | allkeys-lru |
| `MAXMEMORY_ALLKEYS_LFU` | 692 | allkeys-lfu |
| `MAXMEMORY_ALLKEYS_RANDOM` | 693 | allkeys-random |
| `MAXMEMORY_NO_EVICTION` | 694 | noeviction |

### 키스페이스 이벤트 알림 (`NOTIFY_*`)

| 상수 | 줄 | 의미 |
|------|----|------|
| `NOTIFY_KEYSPACE` | 780 | `K` — 키스페이스 이벤트 |
| `NOTIFY_KEYEVENT` | 781 | `E` — 키 이벤트 |
| `NOTIFY_EXPIRED` | 788 | `x` — 만료 키 |
| `NOTIFY_EVICTED` | 789 | `e` — 제거된 키 |
| `NOTIFY_ALL` | 798 | 모든 알림 |

---

## 핵심 구조체

### `redisServer` (1936–2530)

> Redis 서버 전역 상태. 모든 설정·통계·상태를 포함한다.

| 필드 | 줄 | 타입 | 역할 |
|------|----|------|------|
| `pid` | 1938 | `pid_t` | 메인 프로세스 PID |
| `db` | 1950 | `redisDb*` | 데이터베이스 배열 |
| `commands` | 1951 | `dict*` | 명령어 테이블 |
| `el` | 1953 | `aeEventLoop*` | 이벤트 루프 |
| `hz` | 1948 | `int` | `serverCron()` 호출 빈도 |
| `clients` | 2001 | `list*` | 활성 클라이언트 리스트 |
| `dirty` | 2232 | `long long` | 마지막 저장 이후 변경사항 수 |
| `aof_enabled` | 2192 | `int` | AOF 활성화 상태 |
| `repl_backlog` | 2299 | `replBacklog*` | 복제 백로그 |
| `hotkeys` | 2147 | `hotkeyStats*` | 핫키 추적 통계 |
| `dbnum` | 2173 | `int` | 데이터베이스 개수 |
| `maxmemory` | 2374 | `unsigned long long` | 최대 메모리(바이트) |
| `slowlog` | 2089 | `list*` | Slowlog 리스트 |
| `stat_numcommands` | 2053 | `long long` | 처리된 총 명령어 수 |
| `stat_expiredkeys` | 2060 | `long long` | 만료된 키 수 |
| `stat_keyspace_hits` | 2062 | `long long` | 키스페이스 히트 수 |
| `stat_keyspace_misses` | 2063 | `long long` | 키스페이스 미스 수 |

---

### `redisDb` (1167–1182)

> 각 Redis 데이터베이스 인스턴스.

| 필드 | 줄 | 타입 | 역할 |
|------|----|------|------|
| `keys` | 1168 | `kvstore*` | 키스페이스 저장소 |
| `expires` | 1169 | `kvstore*` | 만료 시간 저장소 |
| `blocking_keys` | 1171 | `dict*` | BLPOP 대기 키 |
| `watched_keys` | 1178 | `dict*` | WATCH된 키 |
| `id` | 1179 | `int` | DB 번호 |
| `avg_ttl` | 1180 | `long long` | 평균 TTL |

---

### `client` (1436–1624)

> 클라이언트 연결 상태 전체.

| 필드 | 줄 | 타입 | 역할 |
|------|----|------|------|
| `id` | 1437 | `uint64_t` | 고유 ID |
| `flags` | 1438 | `uint64_t` | 상태 플래그 |
| `conn` | 1439 | `connection*` | 연결 객체 |
| `db` | 1445 | `redisDb*` | 선택된 DB |
| `querybuf` | 1449 | `sds` | 쿼리 버퍼 |
| `argc / argv` | 1452 | `int / robj**` | 명령어 인자 |
| `reply` | 1476 | `list*` | 응답 객체 리스트 |
| `user` | 1470 | `user*` | ACL 사용자 |
| `mstate` | 1532 | `multiState` | MULTI/EXEC 트랜잭션 상태 |
| `bstate` | 1533 | `blockingState` | 블로킹 상태 |
| `watched_keys` | 1535 | `list*` | WATCH 키 목록 |
| `pubsub_channels` | 1536 | `dict*` | 구독 채널 |
| `lastinteraction` | 1490 | `time_t` | 마지막 상호작용 시간 |
| `replstate` | 1501 | `int` | 복제 상태 |
| `reploff` | 1521 | `long long` | 복제 오프셋 |

---

### `redisCommand` (2875–2927)

> Redis 명령어 메타데이터.

| 필드 | 줄 | 역할 |
|------|----|------|
| `declared_name` | 2877 | 명령어 이름 |
| `summary` | 2879 | 설명 문자열 |
| `proc` | 2890 | 구현 함수 포인터 |
| `arity` | 2891 | 인자 수 (`-N` → `>= N`) |
| `flags` | 2892 | 명령어 플래그 |
| `key_specs` | 2894 | 키 스펙 배열 |
| `subcommands` | 2901 | 서브명령어 배열 |

---

### 기타 주요 구조체

| 구조체 | 줄 | 역할 |
|--------|----|------|
| `multiState` | 1227–1240 | MULTI/EXEC 트랜잭션 — 큐된 명령어 배열(`commands`), 개수(`count`) |
| `blockingState` | 1244–1270 | BLPOP·WAIT 등 블로킹 상태 — 타입(`btype`), 타임아웃, 대기 키 |
| `replBacklog` | 1359–1369 | 부분 재동기화(PSYNC) 백로그 — 오프셋, 블록 인덱스(`rax*`) |
| `zskiplist` | 1709–1740 | Sorted Set용 스킵 리스트 — header/tail/length/level |
| `hotkeyStats` | 2567–2600 | 핫키 추적 — CPU/네트워크 TopK, 샘플링 비율 |
| `rdbSaveInfo` | 1832–1840 | RDB 저장/로드 메타데이터 — 복제 ID·오프셋 |
| `redisTLSContextConfig` | 1862–1880 | TLS 설정 — 인증서·키·CA 경로 |
| `pendingCommand` | 2615–2631 | 파싱된 명령어 상태 — argc/argv, cmd 포인터, 키 결과 |

---

## 열거형 (enum / typedef)

### `blocking_type` (480–493)

```c
BLOCKED_NONE       // 블로킹 없음
BLOCKED_LIST       // BLPOP 등
BLOCKED_WAIT       // WAIT 명령
BLOCKED_WAITAOF    // WAITAOF 명령
BLOCKED_MODULE     // 모듈에 의한 블로킹
BLOCKED_STREAM     // XREAD
BLOCKED_ZSET       // BZPOP 등
BLOCKED_POSTPONE   // 처리 연기
BLOCKED_SHUTDOWN   // SHUTDOWN
BLOCKED_LAZYFREE   // 지연 해제
BLOCKED_NUM        // 총 타입 수
```

### `repl_state` (513–530)

```c
REPL_STATE_NONE
REPL_STATE_CONNECT
REPL_STATE_CONNECTING
REPL_STATE_RECEIVE_PING_REPLY
REPL_STATE_SEND_HANDSHAKE
REPL_STATE_TRANSFER      // RDB 수신 중
REPL_STATE_CONNECTED     // 복제 완료
```

### `pause_purpose` (753–759)

```c
PAUSE_BY_CLIENT_COMMAND
PAUSE_DURING_SHUTDOWN
PAUSE_DURING_FAILOVER
PAUSE_DURING_SLOT_HANDOFF
NUM_PAUSE_PURPOSES
```

### `redisCommandGroup` (2755–2774)

```c
COMMAND_GROUP_GENERIC, COMMAND_GROUP_STRING, COMMAND_GROUP_LIST,
COMMAND_GROUP_SET, COMMAND_GROUP_SORTED_SET, COMMAND_GROUP_HASH,
COMMAND_GROUP_PUBSUB, COMMAND_GROUP_TRANSACTIONS, COMMAND_GROUP_CLUSTER, ...
```

### `configType` (3868–3875)

```c
BOOL_CONFIG, NUMERIC_CONFIG, STRING_CONFIG,
SDS_CONFIG, ENUM_CONFIG, SPECIAL_CONFIG
```

---

## server.c 개요

Redis 서버의 핵심 이벤트 루프, 명령어 처리, 메모리 관리, 지속성(RDB/AOF), 복제 기능을 담당하는 메인 엔진. 클라이언트 요청 수신 → 명령 실행 → 응답 전송의 전체 생명주기와 백그라운드 작업(RDB/AOF, 복제, 클러스터 동기화)을 관리한다.

---

## 주요 전역 변수

| 변수 | 줄 | 타입 | 역할 |
|------|----|------|------|
| `server` | 84 | `struct redisServer` | 서버 전역 상태 저장소 |
| `shared` | 73 | `struct sharedObjectsStruct` | 공유 문자열 객체 모음 |
| `R_Zero / R_PosInf / R_NegInf / R_Nan` | 78 | `double` | IEEE 754 특수값 (직렬화용) |
| `ProcessingEventsWhileBlocked` | 1868 | `int` | AOF/RDB 로딩 중 이벤트 처리 플래그 |
| `dbDictType` | 624 | `dictType` | DB 주 저장소용 해시테이블 타입 |
| `dbExpiresDictType` | 638 | `dictType` | 만료 저장소용 해시테이블 타입 |
| `commandTableDictType` | 652 | `dictType` | 명령어 테이블 (대소문자 무시) |
| `keylistDictType` | 688 | `dictType` | 차단 작업용 키→클라이언트 리스트 |
| `clientDictType` | 770 | `dictType` | 클라이언트 ID 세트 |
| `kvstoreBaseType` | 779 | `kvstoreType` | 기본 kvstore (메타데이터 없음) |
| `kvstoreExType` | 787 | `kvstoreType` | 확장 kvstore (통계 포함) |
| `ClientsPeakMemInput[8]` | 1009 | `size_t[]` | 슬롯별 클라이언트 최대 입력 버퍼 |
| `ClientsPeakMemOutput[8]` | 1010 | `size_t[]` | 슬롯별 클라이언트 최대 출력 버퍼 |
| `CurrentPeakMemUsageSlot` | 1011 | `int` | 현재 활성 메모리 슬롯 |

---

## 주요 함수 목록

### A. 서버 초기화/설정

| 함수 | 줄 | 설명 |
|------|----|------|
| `initServerConfig()` | 2327 | 서버 기본 설정값 초기화 (포트·DB 수·타임아웃 등) |
| `initServer()` | 2913 | 서버 런타임 초기화 (메모리 구조·리스너·이벤트루프) |
| `InitServerLast()` | 3230 | 초기화 최종 단계 (시작 로그·통계 리셋) |
| `createSharedObjects()` | 2136 | 메모리 절약용 공유 문자열 객체 생성 |
| `resetServerStats()` | 2829 | 서버 통계 전체 초기화 |
| `resetCommandTableStats()` | 3448 | 명령어 통계 테이블 리셋 |

### B. 이벤트 루프

| 함수 | 줄 | 설명 |
|------|----|------|
| `serverCron()` | 1506 | 100ms 주기 타이머 콜백 — 메인 주기 작업 |
| `beforeSleep()` | 1884 | 이벤트루프 슬립 전 — AOF 플러시·만료·추적 등 |
| `afterSleep()` | 2094 | 이벤트루프 슬립 후 — 모듈 처리 준비 |
| `whileBlockedCron()` | 1818 | RDB/AOF 로딩·스크립트 실행 중 주기 작업 |
| `blockingOperationStarts()` | 1800 | 장시간 작업 시작 알림 |
| `blockingOperationEnds()` | 1807 | 장시간 작업 종료 알림 |

### C. 클라이언트 관리

| 함수 | 줄 | 설명 |
|------|----|------|
| `clientsCron()` | 1218 | 클라이언트 타임아웃·메모리 주기 관리 |
| `clientsCronRunClient()` | 1161 | 개별 클라이언트 정기 유지보수 |
| `clientsCronResizeQueryBuffer()` | 894 | 쿼리 버퍼 크기 최적화 |
| `clientsCronResizeOutputBuffer()` | 947 | 출력 버퍼 동적 리사이징 |
| `clientsCronTrackExpansiveClients()` | 1013 | 메모리 대량 사용 클라이언트 추적 |
| `updateClientMemoryUsage()` | 1059 | 클라이언트 메모리 사용량 갱신 |
| `updateClientMemUsageAndBucket()` | 1112 | 메모리 버킷 이동 및 제거 추적 |
| `getMemUsageBucket()` | 1035 | 메모리 사용량 → 버킷 인덱스 계산 |
| `clientEvictionAllowed()` | 1072 | 클라이언트 축출 가능 여부 확인 |
| `getExpansiveClientsInfo()` | 1149 | 최근 최대 클라이언트 메모리 조회 |

### D. 명령어 처리

| 함수 | 줄 | 설명 |
|------|----|------|
| `processCommand()` | 4356 | 명령어 전처리·검증·실행 (ACL·클러스터·트랜잭션 체크) |
| `call()` | 3893 | 명령어 실제 실행 및 AOF/복제 전파 |
| `preprocessCommand()` | 4306 | 명령어 전처리 및 모듈 필터링 |
| `rejectCommand()` | 4178 | 명령어 거부 — 에러 응답 전송 |
| `rejectCommandFormat()` | 4203 | 형식 문자열 기반 거부 |
| `afterCommand()` | 4215 | 명령어 후처리 — 추적·메시지 플러시 |
| `commandCheckExistence()` | 4237 | 명령어 존재 확인 |
| `commandCheckArity()` | 4277 | 명령어 인자 수 검증 |
| `areCommandKeysInSameSlot()` | 4742 | 클러스터: 모든 키가 같은 슬롯인지 확인 |
| `populateCommandTable()` | 3425 | 명령어 테이블 생성/등록 |
| `populateCommandStructure()` | 3382 | 개별 명령어 구조 초기화 |

### E. DB 주기 작업 (만료/RDB/AOF)

| 함수 | 줄 | 설명 |
|------|----|------|
| `databasesCron()` | 1270 | DB 주기 작업 — 만료 처리·리해시·재정렬 |

### F. 복제 (Replication)

| 함수 | 줄 | 설명 |
|------|----|------|
| `sendGetackToReplicas()` | 1860 | 레플리카에 ACK 요청 전송 |

### G. 메모리 관리

| 함수 | 줄 | 설명 |
|------|----|------|
| `updatePeakMemory()` | 1433 | 피크 메모리 사용량 기록 |
| `cronUpdateMemoryStats()` | 1448 | 메모리 통계 주기 갱신 (RSS·할당량) |
| `dictResizeAllowed()` | 561 | 딕셔너리 리사이징 가능 여부 판단 |
| `updateDictResizePolicy()` | 801 | 자식 프로세스 상태에 따른 리사이징 정책 갱신 |

### H. 메트릭/통계

| 함수 | 줄 | 설명 |
|------|----|------|
| `trackInstantaneousMetric()` | 867 | 순간 메트릭 샘플 기록 (초당 명령어·네트워크) |
| `getInstantaneousMetric()` | 881 | 순간 메트릭 평균값 계산 |
| `statsUpdateActiveClients()` | 6047 | 활성 클라이언트 통계 갱신 |
| `getActiveClientsInWindow()` | 6085 | 512ms 슬라이딩 윈도우 활성 클라이언트 수 |

### I. 시간 관리

| 함수 | 줄 | 설명 |
|------|----|------|
| `ustime()` | 236 | UNIX 마이크로초 반환 |
| `mstime()` | 247 | UNIX 밀리초 반환 |
| `commandTimeSnapshot()` | 256 | 명령 실행 시작 시점 스냅샷 |
| `updateCachedTime()` | 1356 | 캐시된 시간 갱신 |
| `updateCachedTimeWithUs()` | 1327 | 마이크로초 기반 캐시 시간 갱신 |
| `enterExecutionUnit()` | 1367 | 실행 단위 진입 (중첩 추적) |
| `exitExecutionUnit()` | 1377 | 실행 단위 퇴출 |

### J. 자식 프로세스 관리

| 함수 | 줄 | 설명 |
|------|----|------|
| `checkChildrenDone()` | 1381 | 자식 프로세스 종료 감지 및 처리 |
| `resetChildState()` | 826 | 자식 프로세스 상태 리셋 |
| `hasActiveChildProcess()` | 822 | 활성 자식 존재 여부 확인 |
| `isMutuallyExclusiveChildType()` | 843 | 상호배타적 자식 타입 확인 (RDB/AOF/모듈) |
| `redisFork()` | 7369 | 목적별 포크 생성 |

### K. 로깅

| 함수 | 줄 | 설명 |
|------|----|------|
| `serverLogRaw()` | 129 | 로그 메시지 파일/stdout 기록 |
| `_serverLog()` | 177 | printf 스타일 로깅 |
| `serverLogFromHandler()` | 224 | 신호 핸들러 안전 로깅 |

### L. 시작/종료

| 함수 | 줄 | 설명 |
|------|----|------|
| `main()` | 7839 | Redis 서버 메인 진입점 |
| `prepareForShutdown()` | 4880 | 종료 전 준비 — AOF 플러시·통계 저장 |
| `finishShutdown()` | 4969 | 최종 종료 처리 |
| `isReadyToShutdown()` | 4926 | 종료 가능 여부 확인 |
| `abortShutdown()` | 4951 | 종료 프로세스 중단 |
| `listenToPort()` | 2783 | 포트 바인딩 및 리스너 생성 |
| `initListeners()` | 3153 | 모든 리스너 초기화 |
| `daemonize()` | 7130 | 데몬 프로세스로 전환 |
| `createPidFile()` | 7115 | PID 파일 생성 |

---

## 요청 처리 데이터 흐름

```
1. [네트워크 수신]
   readQueryFromClient()  ← networking.c
   └─> 클라이언트 querybuf에 데이터 축적

2. [RESP 파싱]
   processInlineBuffer() / processMultibulkBuffer()  ← networking.c
   └─> argc / argv 배열 구성

3. [전처리]
   preprocessCommand()  ← server.c:4306
   └─> moduleCallCommandFilters()

4. [검증 및 권한 확인]  processCommand()  ← server.c:4356
   ├─ 명령어 존재 확인              (4418-4421)
   ├─ 인수 개수 확인                (4422-4425)
   ├─ 보호 명령어 확인              (4429-4441)
   ├─ ACL 권한 확인                 (4479-4486)
   ├─ 클러스터 리다이렉트           (4492-4511)
   ├─ 클라이언트 메모리 축출        (4531)
   ├─ maxmemory 확인                (4543-4567)
   ├─ 쓰기 명령어 검증              (4576-4606)
   ├─ Pub/Sub 모드 제약             (4640-4655)
   └─ 레플리카 키스페이스 제약      (4704-4707)

5. [명령어 실행]  call()  ← server.c:3893
   ├─ 플래그 초기화                 (3912)
   ├─ enterExecutionUnit()          (3951)
   ├─ 시간 측정 시작                (3929-3946)
   ├─ c->cmd->proc(c)  ← 실제 실행  (3959)
   ├─ 시간 측정 종료                (3970-3973)
   ├─ 오류 통계 갱신                (3981-3987)
   ├─ slowlog 기록                  (4013-4014)
   ├─ MONITOR 전송                  (4020-4026)
   ├─ 명령어/핫키 통계 갱신         (4031-4054)
   └─ AOF/복제 전파                 (4062-4098)

6. [응답 전송]
   addReply*()  →  beforeSleep()에서 플러시

7. [beforeSleep()]  ← server.c:1884  (매 이벤트루프 슬립 전)
   ├─ 피크 메모리 갱신
   ├─ TLS 처리
   ├─ 클러스터 전처리
   ├─ 차단 클라이언트 처리
   ├─ 활성 만료 사이클
   ├─ 레플리카 ACK 송신
   ├─ 키 추적/캐시 무효화
   └─ AOF 플러시

8. [serverCron()]  ← server.c:1506  (100ms 간격)
   ├─ 메모리 통계 갱신
   ├─ 종료 신호 처리
   ├─ clientsCron()   — 클라이언트 타임아웃
   ├─ databasesCron() — DB 만료/리해시
   ├─ BGSAVE/AOF 리라이트 체크
   ├─ replicationCron()
   ├─ clusterCron()
   └─ 모듈 cron
```

---

## 주목할 알고리즘/로직

### 1. 클라이언트 메모리 버킷 관리 (1013–1145)

log₂(메모리) 기반 버킷 인덱싱으로 클라이언트를 분류한다. 메모리가 2배 증가 시 다음 버킷으로 이동하며, 축출(eviction) 시 버킷 단위로 빠른 탐색이 가능하다.

### 2. 이벤트 루프 빈도 동적 조정 (1516–1529)

연결 수가 늘어날수록 `serverCron` 빈도를 자동으로 2배씩 증가(상한: `CONFIG_MAX_HZ = 500Hz`). 초당 클라이언트당 균등 처리 시간을 보장한다.

```c
while (len(clients) / hz > MAX_CLIENTS_PER_CLOCK_TICK) {
    hz *= 2;
    if (hz > CONFIG_MAX_HZ) break;
}
```

### 3. 전파(Propagation) 메커니즘 (4062–4098)

명령어 실행 전후로 `server.dirty` 차이를 측정한다. `dirty > 0`이면 AOF 기록(`PROPAGATE_AOF`)·레플리카 전송(`PROPAGATE_REPL`)을 수행하며, `CLIENT_FORCE_AOF / CLIENT_FORCE_REPL / CLIENT_PREVENT_PROP` 플래그로 오버라이드 가능하다.

### 4. 명령어 검증 순서 최적화 (4356–4737)

저비용 검증(명령어 존재·인수 수·ACL)을 먼저 수행하고, 고비용 작업(메모리 축출·클러스터 리다이렉트)을 뒤에 배치하여 불필요한 연산을 최소화한다.

### 5. 캐시된 시간 활용 (255–270, 1327–1359)

`gettimeofday()` 시스템 콜 횟수를 줄이기 위해 명령어 실행 단위마다 `server.cmd_time_snapshot`을 캐시한다. 스크립트·트랜잭션 내부에서 시간 동결 효과를 제공하여 만료 판정의 일관성을 보장한다.

### 6. 활성 클라이언트 슬라이딩 윈도우 (6037–6085)

| 상수 | 줄 | 값 |
|------|----|-----|
| `WINDOW_SLOTS` | 6037 | `4` |
| `SLOT_DURATION_MS` | 6039 | `128ms` |
| `WINDOW_DURATION_MS` | 6040 | `512ms` |

4개 × 128ms = 512ms 윈도우. 각 슬롯에 클라이언트 이벤트를 기록하고 최근 512ms 내 이벤트가 있는 클라이언트를 '활성'으로 카운트한다.

---

## 파일 간 관계

```
server.h
  ├─ 모든 구조체·상수·enum·함수 프로토타입 선언
  ├─ extern 전역 변수 노출 (server, shared, dictType 인스턴스 등)
  └─ 다른 모든 .c 파일이 #include "server.h"로 공유

server.c
  ├─ server.h 포함
  ├─ 전역 변수 정의 (server, shared, R_Zero 등)
  ├─ 이벤트 루프 진입점: main() → aeMain() → beforeSleep / serverCron
  ├─ 명령어 처리 파이프라인: processCommand() → call() → cmd->proc()
  └─ 자식 프로세스 관리: redisFork(), checkChildrenDone()

server.c 의존 모듈:
  networking.c  — 클라이언트 읽기/쓰기 I/O
  aof.c         — AOF 쓰기/리라이트
  rdb.c         — RDB 저장/로드
  replication.c — 복제 로직
  cluster.c     — 클러스터 로직
  acl.c         — ACL 권한 관리
  object.c      — redisObject 생성/해제
  db.c          — DB CRUD 연산
  expire.c      — 키 만료 처리
  module.c      — 모듈 로딩
```
