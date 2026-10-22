# Redis 퍼시스턴스 메커니즘 (Persistence Mechanisms) 소스 분석

## 개요

Redis는 두 가지 주요 퍼시스턴스 메커니즘을 제공한다.

| 방식 | 파일 | 특징 |
|------|------|------|
| **RDB** (Redis Database) | `rdb.c`, `rdb.h` | 특정 시점의 전체 DB 스냅샷(바이너리) |
| **AOF** (Append-Only File) | `aof.c` | 모든 쓰기 커맨드를 순서대로 기록 |

두 방식을 동시에 사용하거나, 각각 단독으로 쓰거나, 모두 비활성화할 수 있다. RDB + AOF 혼합 모드에서는 AOF 재작성 시 RDB 프리앰블을 BASE 파일로 사용한다.

---

## 1. RDB 퍼시스턴스

### 1.1 파일 형식

**매직 헤더** (`rdb.c:1749`)

```
REDIS0014   (= "REDIS" + 4자리 RDB_VERSION)
```

`RDB_VERSION = 14` (`rdb.h`)

**전체 파일 구조**

```
[REDIS버전헤더 9bytes]
[AUX 필드들 (redis-ver, redis-bits, ctime, used-mem ...)]
[FUNCTIONS 블록]
[DB 0 데이터]
  SELECTDB 0
  RESIZEDB <key_count> <expire_count>
  [SLOT_INFO 클러스터용]
  [EXPIRETIME_MS] [FREQ] [IDLE]   ← 키 메타 opcodes
  <type_byte> <key> <value>
  ...
[DB 1 데이터]
  ...
[EOF opcode (0xFF)]
[CRC64 체크섬 8bytes]
```

### 1.2 길이 인코딩 (Length Encoding)

`rdb.h`에 정의된 6가지 인코딩 타입:

| 상수 | 값 | 의미 |
|------|----|------|
| `RDB_6BITLEN` | 0 | 첫 1바이트의 하위 6비트가 길이 (0~63) |
| `RDB_14BITLEN` | 1 | 2바이트, 14비트 길이 (0~16383) |
| `RDB_32BITLEN` | 0x80 | 다음 4바이트(big-endian) = 길이 |
| `RDB_64BITLEN` | 0x81 | 다음 8바이트(big-endian) = 길이 |
| `RDB_ENCVAL` | 3 | 특수 인코딩 (정수/LZF) |

특수 인코딩 (`RDB_ENCVAL` 안에서):

| 상수 | 값 | 의미 |
|------|----|------|
| `RDB_ENC_INT8` | 0 | 8비트 정수 |
| `RDB_ENC_INT16` | 1 | 16비트 정수 |
| `RDB_ENC_INT32` | 2 | 32비트 정수 |
| `RDB_ENC_LZF` | 3 | LZF 압축 문자열 |

### 1.3 객체 타입 (28종)

`rdb.h`에 RDB_TYPE_* 상수로 정의:

```
0   RDB_TYPE_STRING
1   RDB_TYPE_LIST
2   RDB_TYPE_SET
3   RDB_TYPE_ZSET         (구형)
4   RDB_TYPE_HASH
5   RDB_TYPE_ZSET_2       (double 직렬화 개선)
10  RDB_TYPE_LIST_QUICKLIST
11  RDB_TYPE_SET_INTSET
12  RDB_TYPE_ZSET_ZIPLIST (구형)
13  RDB_TYPE_HASH_ZIPLIST (구형)
14  RDB_TYPE_LIST_QUICKLIST_2
15  RDB_TYPE_STREAM_LISTPACKS
16  RDB_TYPE_HASH_LISTPACK
17  RDB_TYPE_ZSET_LISTPACK
18  RDB_TYPE_LIST_LISTPACK
19  RDB_TYPE_SET_LISTPACK
20  RDB_TYPE_STREAM_LISTPACKS_2
21  RDB_TYPE_STREAM_LISTPACKS_3
...
```

### 1.4 특수 Opcode 목록 (15종)

```
255  RDB_OPCODE_EOF
254  RDB_OPCODE_SELECTDB
253  RDB_OPCODE_EXPIRETIME
252  RDB_OPCODE_EXPIRETIME_MS
251  RDB_OPCODE_RESIZEDB
250  RDB_OPCODE_AUX
249  RDB_OPCODE_MODULE_AUX
248  RDB_OPCODE_IDLE          ← LRU idle time
247  RDB_OPCODE_FREQ          ← LFU frequency
246  RDB_OPCODE_FUNCTION2
245  RDB_OPCODE_FUNCTION      (구형)
244  RDB_OPCODE_SLOT_INFO     ← 클러스터 슬롯 메타
243  RDB_OPCODE_KEY_META      ← 키 메타 (expire+LRU/LFU 통합)
```

### 1.5 RDBFLAGS 비트 플래그

```c
#define RDBFLAGS_NONE           0
#define RDBFLAGS_AOF_PREAMBLE   (1<<0)   // AOF BASE 파일로 저장
#define RDBFLAGS_REPLICATION    (1<<1)   // 복제용 전송
#define RDBFLAGS_ALLOW_DUP      (1<<2)   // 중복 키 허용
#define RDBFLAGS_FEED_REPL      (1<<3)   // 슬레이브 피드
#define RDBFLAGS_KEEP_CACHE     (1<<4)   // 페이지 캐시 유지
```

### 1.6 저장 함수 계층 구조

```
rdbSaveBackground()           ← BGSAVE 커맨드 / 자동 저장 진입점
  └─ redisFork(CHILD_TYPE_RDB)
       └─ (child) rdbSave()
            └─ rdbSaveInternal()     temp-<PID>.rdb 에 쓰기
                 └─ rdbSaveRio()     실제 직렬화
            └─ rename(tmpfile, filename)   atomic 교체
            └─ fsyncFileDir()
```

**`rdbSaveRio()` 내부 순서** (`rdb.c:1740`):

1. `rdb->update_cksum = rioGenericUpdateChecksum` — CRC64 활성화
2. `"REDIS0014"` 헤더 9바이트 쓰기
3. `rdbSaveInfoAuxFields()` — redis-ver/bits/ctime/used-mem AUX 필드
4. `rdbSaveModulesAux(BEFORE_RDB)` — 모듈 AUX
5. `rdbSaveFunctions()` — Lua/Valkey 함수 블록
6. `rdbSaveDb()` × server.dbnum — 각 DB
7. `rdbSaveModulesAux(AFTER_RDB)`
8. `RDB_OPCODE_EOF` 쓰기
9. CRC64 체크섬 8바이트 기록

**`rdbSaveBackground()`** (`rdb.c:1926`):

```c
if (hasActiveChildProcess()) return C_ERR;   // 하나의 child만 허용
childpid = redisFork(CHILD_TYPE_RDB);
if (childpid == 0) {
    // child: rdbSave() → exitFromChild()
    sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
}
// parent: server.rdb_child_type = RDB_CHILD_TYPE_DISK
```

**Copy-On-Write 최적화**: fork 후 부모는 계속 서비스, 자식은 COW 페이지로 스냅샷 저장. 자식이 완료하면 `sendChildCowInfo()`로 COW 통계 전달.

**Incremental fsync**: `server.rdb_save_incremental_fsync`가 설정되면 `REDIS_AUTOSYNC_BYTES`마다 자동 fsync하여 I/O 스파이크를 분산.

### 1.7 로딩 함수 계층 구조

```
rdbLoad()
  └─ rioInitWithFile()
  └─ rdbLoadRio()
       └─ rdbLoadRioWithLoadingCtx()   실제 역직렬화
```

**`rdbLoadRioWithLoadingCtx()` 내부 루프** (`rdb.c:3781`):

1. `"REDIS"` 매직 + 버전 확인 (`rdbver >= 1 && rdbver <= RDB_VERSION`)
2. 무한 루프로 type byte 읽기:
   - `RDB_OPCODE_EXPIRETIME` / `_MS` → expiretime 설정
   - `RDB_OPCODE_FREQ` → lfu_freq 설정
   - `RDB_OPCODE_IDLE` → lru_idle 설정
   - `RDB_OPCODE_EOF` → 루프 종료
   - `RDB_OPCODE_SELECTDB` → 현재 DB 전환
   - `RDB_OPCODE_RESIZEDB` → dict pre-expand (rehash 절감)
   - `RDB_OPCODE_SLOT_INFO` → 클러스터 슬롯별 dict expand
   - `RDB_OPCODE_AUX` → 메타데이터 파싱 (repl-id, repl-offset 등)
   - 일반 type → `rdbLoadObject()` → DB에 삽입

**버전 호환성**: `rdbver`이 현재 `RDB_VERSION(14)`보다 낮은 파일도 로드 가능. 높은 버전은 거부.

### 1.8 Diskless 복제 (Replication)

```c
int rdbSaveRioWithEOFMark(...)  // rdb.c:1791
```

- 슬레이브에 직접 소켓으로 스트리밍 시 사용
- 형식: `$EOF:<40-hex-random>\r\n[RDB data]<40-hex-random>`
- 길이를 알 수 없어도 EOF 마크로 스트림 종료 감지

### 1.9 자동 저장 트리거

`serverCron()`에서 `server.saveparams[]` 배열을 순회:

```c
// redis.conf: save 3600 1    → 3600초 안에 1번 변경
//             save 300 100   → 300초 안에 100번 변경
//             save 60 10000  → 60초 안에 10000번 변경
if (server.dirty >= sp->changes &&
    server.unixtime - server.lastsave > sp->seconds) {
    rdbSaveBackground(...)
}
```

---

## 2. AOF 퍼시스턴스

### 2.1 멀티파일 매니페스트 구조

Redis 7.0부터 AOF는 단일 파일이 아닌 **디렉토리 + 매니페스트** 방식:

```
appendonlydir/
  appendonly.aof.1.base.rdb      ← BASE (RDB 스냅샷 or 순수 AOF)
  appendonly.aof.1.incr.aof      ← INCR (BGREWRITEAOF 이후 새 커맨드)
  appendonly.aof.2.incr.aof      ← 이전 INCR (여러 개 가능)
  appendonly.aof.manifest        ← 매니페스트 파일
```

**파일 타입** (`aof.c`):
```
AOF_FILE_TYPE_BASE  = 'b'   // 기본 스냅샷
AOF_FILE_TYPE_INCR  = 'i'   // 증분 파일
AOF_FILE_TYPE_HIST  = 'h'   // 구 파일 (삭제 예정)
```

**매니페스트 항목 필드**:
```
file <filename>
seq  <sequence_number>
type <b|i|h>
```

**임시 파일 접두어**: `TEMP_FILE_NAME_PREFIX = "temp-"`

### 2.2 커맨드 기록: `feedAppendOnlyFile()` (`aof.c:1409`)

```c
void feedAppendOnlyFile(int dictid, robj **argv, int argc)
```

실행 순서:
1. 타임스탬프 어노테이션 (`aof_timestamp_enabled` 설정 시)
2. DB 변경 시 `SELECT <n>` 커맨드 삽입 (aof_selected_db 비교)
3. `catAppendOnlyGenericCommand()` — RESP3 프로토콜로 직렬화
4. `server.aof_buf`에 append

`aof_buf`는 이벤트 루프 재진입 직전(`beforeSleep`)에 실제 파일로 flush됨.

**RESP 직렬화 형식**:
```
*3\r\n           ← 인자 3개
$3\r\nSET\r\n    ← "SET"
$3\r\nfoo\r\n    ← "foo"
$3\r\nbar\r\n    ← "bar"
```

### 2.3 버퍼 Flush: `flushAppendOnlyFile()` (`aof.c:1147`)

**fsync 정책 3가지**:

| 정책 | 동작 | 내구성 | 성능 |
|------|------|--------|------|
| `AOF_FSYNC_ALWAYS` | 쓰기마다 즉시 fsync | 최대 (1커맨드 손실 없음) | 최저 |
| `AOF_FSYNC_EVERYSEC` | 1초마다 백그라운드 fsync | 최대 1초치 손실 가능 | 중간 |
| `AOF_FSYNC_NO` | OS에 맡김 | OS flush 주기 | 최고 |

**`AOF_FSYNC_EVERYSEC` 지연 처리 로직**:

```c
if (sync_in_progress && !force) {
    if (aof_flush_postponed_start == 0) {
        // 첫 지연: 기억만 하고 return
        aof_flush_postponed_start = mstime;
        return;
    } else if (mstime - aof_flush_postponed_start < 2000) {
        // 2초 미만: 추가 대기
        return;
    }
    // 2초 초과 → 강제 write (aof_delayed_fsync++)
}
```

**쓰기 오류 처리**:
- `AOF_FSYNC_ALWAYS` 모드에서 fsync 실패 → `exit(1)` (데이터 정합성 보장 불가)
- 다른 모드 → `aof_last_write_status = C_ERR`, 다음 기회에 재시도
- 쓰기 부족(short write) 시 → `ftruncate()`로 마지막 유효 오프셋까지 되돌림

**버퍼 재사용 최적화**:
```c
if ((sdslen(aof_buf) + sdsavail(aof_buf)) < 4000)
    sdsclear(aof_buf);   // 작으면 재사용
else
    sdsfree(aof_buf); aof_buf = sdsempty();  // 크면 해제
```

### 2.4 AOF 로딩: `loadSingleAppendOnlyFile()` (`aof.c:1507`)

1. `createAOFClient()` — 가짜 클라이언트 생성 (블로킹 금지 `CLIENT_DENY_BLOCKING`)
2. RESP 파서로 커맨드 파싱 → `processCommandAndResetClient()` 실행
3. `valid_up_to` 포인터 추적 — 완전한 커맨드의 마지막 오프셋
4. `valid_before_multi` — MULTI 트랜잭션 시작 전 오프셋
5. 파일 끝이 잘린(truncated) 경우:
   - `aof-use-rdb-preamble`이 꺼진 경우 `truncateAppendOnlyFile()`로 정리
   - `AOF_TRUNCATED` 반환 (에러 아님)

**반환 코드**:
```
AOF_OK         ← 정상 완료
AOF_TRUNCATED  ← 끝이 잘렸지만 복구 가능
AOF_OPEN_ERR   ← 파일 오픈 실패
AOF_NOT_EXIST  ← 파일 없음
AOF_EMPTY      ← 빈 파일
AOF_FAILED     ← 로드 실패
```

### 2.5 AOF 재작성: BGREWRITEAOF

**`rewriteAppendOnlyFileBackground()`** (`aof.c:2718`):

```
BGREWRITEAOF 커맨드
  └─ flushAppendOnlyFile(force=1)       현재 버퍼 flush
  └─ openNewIncrAofForAppend()          새 INCR 파일 열기 (부모는 계속 여기에 씀)
  └─ redisFork(CHILD_TYPE_AOF)
       └─ (child) rewriteAppendOnlyFile(tmpfile)
            ├─ if aof_use_rdb_preamble:
            │    rdbSaveRio(RDBFLAGS_AOF_PREAMBLE)   ← RDB로 BASE 생성
            └─ else:
                 rewriteAppendOnlyFileRio()           ← 순수 AOF 재생성
  └─ (parent) 완료 신호 수신 후:
       4a. 새 BASE 파일명 생성, 이전 BASE → HISTORY
       4b. rename(tmpfile, new_base_filename)
       4c. 이전 INCR → HISTORY
       4d. 매니페스트 파일 갱신
       4e. BIO로 HISTORY 파일 삭제
```

**RDB 프리앰블 (`aof_use_rdb_preamble = yes`)**: BASE 파일에 RDB 형식을 사용해 로딩 속도 대폭 향상. 로딩 시 파일 헤더가 `"REDIS"`면 `rdbLoadRio()`로, 그 외엔 RESP 파서로 처리.

**Incremental fsync**: `aof_rewrite_incremental_fsync`가 설정되면 `REDIS_AUTOSYNC_BYTES`마다 자동 fsync.

### 2.6 AOF 상태 머신

```
AOF_OFF         ← 비활성
AOF_ON          ← 활성 (정상 기록 중)
AOF_WAIT_REWRITE ← BGREWRITEAOF 완료 후 새 BASE 전환 대기
```

---

## 3. 혼합 모드와 상호작용

### 3.1 시작 시 로딩 우선순위

```
서버 시작
  ├─ AOF 활성화 AND aof 파일 존재?
  │    └─ loadAppendOnlyFiles()
  │         ├─ BASE 파일 (RDB or AOF)
  │         └─ INCR 파일들 (순서대로)
  └─ else
       └─ rdbLoad()
```

### 3.2 BGSAVE vs BGREWRITEAOF 충돌 방지

```c
if (hasActiveChildProcess()) return C_ERR;
```

두 함수 모두 `hasActiveChildProcess()` 체크로 동시 실행 방지. `server.child_type`으로 현재 활성 child 종류 추적.

### 3.3 복제(Replication)와의 연계

- RDB는 슬레이브 풀 동기화에 사용 (`rdbSaveRioWithEOFMark()`)
- AOF의 `fsynced_reploff_pending` → `WAITAOF` 커맨드 지원 (fsync 완료 오프셋 추적)
- `RDBFLAGS_FEED_REPL`: RDB 저장 중에도 복제 스트림에 피드

---

## 4. 핵심 I/O 추상화: rio

RDB/AOF 모두 `rio` (Redis I/O) 추상화 레이어를 사용:

```c
// rio.h
typedef struct rio {
    size_t (*read)(struct rio *, void *buf, size_t len);
    size_t (*write)(struct rio *, const void *buf, size_t len);
    off_t  (*tell)(struct rio *);
    int    (*flush)(struct rio *);
    void   (*update_cksum)(struct rio *, const void *buf, size_t len);
    uint64_t cksum;              // 현재 CRC64
    size_t processed_bytes;      // 처리된 바이트
    size_t max_processing_chunk; // 로딩 중 이벤트 처리 간격
    // ...
} rio;
```

**백엔드 종류**:
- `rioInitWithFile()` — 일반 파일 I/O
- `rioInitWithBuffer()` — 메모리 버퍼
- `rioInitWithFdset()` — 복수 소켓(diskless 복제)
- `rioInitWithConn()` — 연결 스트림

---

## 5. BIO 스레드와의 연계

퍼시스턴스 관련 BIO 작업 (``bio.c``):

| Job Type | Worker | 역할 |
|----------|--------|------|
| `BIO_AOF_FSYNC` | bio_aof (worker 1) | AOF fsync 백그라운드 수행 |
| `BIO_CLOSE_AOF` | bio_aof (worker 1) | AOF INCR/HISTORY 파일 닫기 |
| `BIO_CLOSE_FILE` | bio_close_file (worker 0) | 구 RDB 파일 닫기 (fd) |
| `BIO_LAZY_FREE` | bio_lazy_free (worker 2) | RDB 로드 후 구 데이터 해제 |

`AOF_FSYNC_EVERYSEC` 모드에서 `flushAppendOnlyFile()`은 `aofFsyncInProgress()`로 BIO 큐에 fsync 작업이 있는지 확인하여 중복 fsync를 방지.

---

## 6. 주요 설정 파라미터

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `save <seconds> <changes>` | (복수) | RDB 자동 저장 조건 |
| `rdbcompression yes` | yes | RDB LZF 압축 |
| `rdbchecksum yes` | yes | RDB CRC64 체크섬 |
| `dbfilename dump.rdb` | - | RDB 파일명 |
| `appendonly no` | no | AOF 활성화 |
| `appendfsync everysec` | - | fsync 정책 |
| `aof-use-rdb-preamble yes` | yes | BASE 파일에 RDB 형식 사용 |
| `aof-rewrite-incremental-fsync yes` | yes | 재작성 시 점진적 fsync |
| `rdb-save-incremental-fsync yes` | yes | RDB 저장 시 점진적 fsync |
| `no-appendfsync-on-rewrite no` | no | child 작업 중 fsync 스킵 |

---

## 7. 요약: 저장 흐름 한눈에

```
쓰기 커맨드 실행
      │
      ├─→ AOF: feedAppendOnlyFile()
      │         aof_buf에 RESP 기록
      │         ↓
      │    beforeSleep: flushAppendOnlyFile()
      │         ↓
      │    aofWrite() → fsync (정책에 따라)
      │
      └─→ RDB (자동/수동): rdbSaveBackground()
                redisFork()
                  └─ child: rdbSaveRio()
                       헤더 → AUX → DB 데이터 → EOF → CRC64
                  └─ parent: 계속 서비스 (COW)
                rename(temp, final)  ← atomic
```

---

**참조 파일**:
- [src/rdb.h](redis/src/rdb.h) — RDB 상수 및 타입 정의
- [src/rdb.c](redis/src/rdb.c) — RDB 저장/로딩 구현 (4611줄)
- [src/aof.c](redis/src/aof.c) — AOF 구현 (3035줄)
- [src/bio.c](redis/src/bio.c) — 백그라운드 I/O 스레드
- [src/rio.h](redis/src/rio.h) — I/O 추상화 레이어
