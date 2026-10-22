# Redis 퍼시스턴스 소스 분석

> 분석 대상: `redis/src/` — rdb.h/c, aof.c, rio.h/c, bio.h/c, childinfo.c, lazyfree.c
> 총 분석 라인: ~9,703줄

---

## 목차

1. [퍼시스턴스 전체 아키텍처](#1-퍼시스턴스-전체-아키텍처)
2. [RIO — I/O 추상화 계층](#2-rio--io-추상화-계층-riohc)
3. [RDB — 스냅샷 저장](#3-rdb--스냅샷-저장-rdbhc)
4. [AOF — Append Only File](#4-aof--append-only-file-aofc)
5. [BIO — 백그라운드 I/O 스레드](#5-bio--백그라운드-io-스레드-biohc)
6. [Lazy Free — 비동기 메모리 해제](#6-lazy-free--비동기-메모리-해제)
7. [RDB vs AOF 비교](#7-rdb-vs-aof-비교)
8. [핵심 설계 포인트 요약](#8-핵심-설계-포인트-요약)

---

## 1. 퍼시스턴스 전체 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                        Redis Main Thread                        │
│                                                                 │
│  Write Command → feedAppendOnlyFile() → aof_buf (SDS)         │
│                                              │                  │
│                  beforeSleep() ──────────────┤                  │
│                       │         flushAppendOnlyFile()           │
│                       │              │  write() → AOF 파일     │
│                       │              │  BIO_AOF_FSYNC → fsync  │
│                       │                                         │
│  BGSAVE / BGREWRITEAOF                                         │
│       │                                                         │
│   fork()                                                        │
│    ├── Child: rdbSaveRio() → temp RDB file → rename()         │
│    └── Child: rewriteAppendOnlyFile() → BASE AOF → manifest   │
│                                                                 │
│  BIO Threads (3개)                                             │
│    ├── bio_close_file: deferred close(2)                       │
│    ├── bio_aof:        deferred fsync + AOF 닫기               │
│    └── bio_lazy_free:  비동기 메모리 해제                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. RIO — I/O 추상화 계층 (`rio.h` / `rio.c`)

### 2.1 설계 목적

RDB / AOF 코드가 파일·소켓·메모리 버퍼를 **동일한 인터페이스**로 읽고 쓸 수 있도록
vtable 기반 추상화 레이어 제공.

### 2.2 구조체 정의

```c
// rio.h:32
struct _rio {
    /* vtable — 백엔드별 구현 */
    size_t (*read) (struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t  (*tell) (struct _rio *);
    int    (*flush)(struct _rio *);
    void   (*update_cksum)(struct _rio *, const void *buf, size_t len);

    uint64_t cksum;            // CRC64 체크섬 누적값
    uint64_t flags;            // RIO_FLAG_READ_ERROR | RIO_FLAG_WRITE_ERROR
    size_t   processed_bytes;  // 총 처리 바이트 수
    size_t   max_processing_chunk; // 단일 read/write 최대 크기

    union {
        struct { sds ptr; off_t pos; } buffer;       // 인메모리 SDS 버퍼
        struct {
            FILE *fp;
            off_t buffered;  // 마지막 fsync 이후 기록된 바이트
            off_t autosync;  // N 바이트마다 자동 fsync
            unsigned reclaim_cache:1; // fsync 후 페이지 캐시 반납
        } file;
        struct { connection *conn; off_t pos; sds buf;
                 size_t read_limit; size_t read_so_far; } conn;
        struct { int fd; off_t pos; sds buf; } fd;
        struct { /* N개 커넥션 동시 쓰기 (diskless 복제용) */
            struct { connection *conn; int failed; } *dst;
            size_t n_dst; off_t pos; sds buf;
        } connset;
    } io;
};
```

### 2.3 5가지 백엔드

| 백엔드 | 초기화 함수 | 용도 |
|--------|-----------|------|
| `buffer` | `rioInitWithBuffer()` | 인메모리 SDS — AOF preamble, RESP 직렬화 |
| `file` | `rioInitWithFile()` | 파일 포인터 — RDB 저장/로드 |
| `conn` | `rioInitWithConn()` | 단일 커넥션 — 레플리케이션 수신 |
| `fd` | `rioInitWithFd()` | 파이프 FD — AOF 리라이트 파이프 |
| `connset` | `rioInitWithConnset()` | N개 커넥션 — diskless 복제 동시 전송 |

### 2.4 autosync — 페이지 캐시 폭증 방지

```c
// server.h:194
#define REDIS_AUTOSYNC_BYTES (1024*1024*4)  // 4MB마다 자동 fsync

// rio.c:100 — rioFileWrite() 내부
// 4MB 쓸 때마다:
//   Linux: sync_file_range(SYNC_FILE_RANGE_WRITE) → 비동기 writeback 요청
//          2번째 4MB부터: 이전 4MB에 WAIT_BEFORE|WRITE|WAIT_AFTER → 완료 보장
//   기타:  redis_fsync() → 블로킹 fsync

if (r->io.file.reclaim_cache) {
    // posix_fadvise(POSIX_FADV_DONTNEED) → 이미 flush된 페이지 캐시 반납
    // RDB 저장 후 메모리 낭비 방지
}
```

### 2.5 인라인 핵심 함수

```c
// rioWrite: 에러 플래그 체크 → max_processing_chunk 단위 분할 → 체크섬 업데이트 → write
// rioRead:  에러 플래그 체크 → max_processing_chunk 단위 분할 → read → 체크섬 업데이트
// rioAbort: READ_ERROR | WRITE_ERROR 동시 설정 → 이후 모든 I/O 무시
```

---

## 3. RDB — 스냅샷 저장 (`rdb.h` / `rdb.c`)

### 3.1 RDB 포맷 버전

```c
// rdb.h:21
#define RDB_VERSION 14  // 형식 비호환 변경 시 증가
// 파일 헤더: "REDIS0014" (ASCII)
```

### 3.2 길이 인코딩 (rdbSaveLen / rdbLoadLen)

RDB의 모든 길이값은 가변 길이 인코딩으로 저장 (공간 절약):

```
첫 바이트 상위 2비트 해석:
  00|xxxxxx → 6비트 길이 (0~63),       1바이트
  01|xxxxxx xxxxxxxx → 14비트 길이,    2바이트
  10|000000 [32비트] → 32비트 길이,    5바이트
  10|000001 [64비트] → 64비트 길이,    9바이트
  11|OBKIND → 특수 인코딩 (RDB_ENCVAL)
```

```c
// rdb.h:37
#define RDB_6BITLEN  0   // 상위 2비트 = 00
#define RDB_14BITLEN 1   // 상위 2비트 = 01
#define RDB_32BITLEN 0x80
#define RDB_64BITLEN 0x81
#define RDB_ENCVAL   3   // 상위 2비트 = 11 → 특수 인코딩
```

### 3.3 문자열 특수 인코딩

```c
// rdb.h:47
#define RDB_ENC_INT8  0  // 8비트 정수로 저장
#define RDB_ENC_INT16 1  // 16비트 정수로 저장
#define RDB_ENC_INT32 2  // 32비트 정수로 저장
#define RDB_ENC_LZF   3  // LZF 압축 문자열
```

### 3.4 RDB 오브젝트 타입 (디스크 고정값)

```c
// rdb.h:55 — 인메모리 OBJ_* 타입과 별개로 디스크용 타입 고정
#define RDB_TYPE_STRING             0
#define RDB_TYPE_LIST               1
#define RDB_TYPE_SET                2
#define RDB_TYPE_ZSET               3   // score를 텍스트로 저장 (구버전)
#define RDB_TYPE_HASH               4
#define RDB_TYPE_ZSET_2             5   // score를 binary double로 저장
#define RDB_TYPE_LIST_QUICKLIST_2  18   // quicklist + listpack 노드
#define RDB_TYPE_HASH_LISTPACK     16
#define RDB_TYPE_ZSET_LISTPACK     17
#define RDB_TYPE_SET_LISTPACK      20
#define RDB_TYPE_STREAM_LISTPACKS_5 27  // Stream + XNACK 지원 (최신)
// 총 0~27 범위 (중간 일부 미사용)
```

### 3.5 RDB 특수 오프코드

```c
// rdb.h:89 — 키-값 데이터 사이에 삽입되는 메타 오프코드
#define RDB_OPCODE_AUX          250  // 보조 필드 (redis-ver, redis-bits 등)
#define RDB_OPCODE_RESIZEDB     251  // DB 해시 테이블 크기 힌트
#define RDB_OPCODE_EXPIRETIME_MS 252 // 밀리초 단위 만료 시간
#define RDB_OPCODE_EXPIRETIME   253  // 초 단위 만료 (레거시)
#define RDB_OPCODE_SELECTDB     254  // DB 번호 선택
#define RDB_OPCODE_EOF          255  // 파일 종료 마커
#define RDB_OPCODE_IDLE         248  // LRU idle time
#define RDB_OPCODE_FREQ         249  // LFU frequency
#define RDB_OPCODE_FUNCTION2    245  // Lua 함수 라이브러리
#define RDB_OPCODE_SLOT_INFO    244  // 클러스터 슬롯 정보
```

### 3.6 RDB 파일 전체 구조

```
┌─────────────────────────────────────────────────────┐
│  "REDIS0014"  (9 bytes, ASCII magic + version)      │
├─────────────────────────────────────────────────────┤
│  OPCODE_AUX "redis-ver" "7.x.x"                    │
│  OPCODE_AUX "redis-bits" 64                         │
│  OPCODE_AUX "ctime" <unix timestamp>                │
│  OPCODE_AUX "used-mem" <bytes>                      │
├─────────────────────────────────────────────────────┤
│  [DB 0]                                             │
│   OPCODE_SELECTDB 0                                 │
│   OPCODE_RESIZEDB <db_size> <expires_size>          │
│   [key-value pairs...]                              │
│    ├── OPCODE_EXPIRETIME_MS <ms>   (만료 있는 경우) │
│    ├── OPCODE_IDLE <seconds>       (LRU 있는 경우)  │
│    ├── <RDB_TYPE_*>                                 │
│    ├── <encoded key string>                         │
│    └── <encoded value object>                       │
│  [DB 1] ...                                         │
├─────────────────────────────────────────────────────┤
│  OPCODE_EOF (0xFF)                                  │
│  CRC64 checksum (8 bytes, little-endian)            │
└─────────────────────────────────────────────────────┘
```

### 3.7 rdbSaveRio — 핵심 저장 함수

```c
// rdb.c:1740
int rdbSaveRio(int req, rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {
    // 1. 헤더 작성: "REDIS" + 4자리 버전
    snprintf(magic, sizeof(magic), "REDIS%04d", RDB_VERSION);
    rioWrite(rdb, magic, 9);

    // 2. AUX 필드 작성 (redis-ver, redis-bits, ctime, used-mem 등)
    rdbSaveAuxField(rdb, "redis-ver", REDIS_VERSION);
    // ...

    // 3. 각 DB 순회
    for (int j = 0; j < server.dbnum; j++) {
        // SELECTDB 오프코드 + DB 번호
        rdbSaveType(rdb, RDB_OPCODE_SELECTDB);
        rdbSaveLen(rdb, j);
        // RESIZEDB 힌트
        rdbSaveType(rdb, RDB_OPCODE_RESIZEDB);
        // 모든 키-값 저장
        // rdbSaveKeyValuePair() → 만료시간, 인코딩 타입, 키, 값
    }

    // 4. EOF 오프코드
    rdbSaveType(rdb, RDB_OPCODE_EOF);

    // 5. CRC64 체크섬 (little-endian 8바이트)
    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    rioWrite(rdb, &cksum, 8);
}
```

### 3.8 rdbSaveBackground — 백그라운드 저장

```c
// rdb.c:1926
int rdbSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    if (hasActiveChildProcess()) return C_ERR;  // 다른 자식 프로세스 있으면 거부
    server.stat_rdb_saves++;

    if ((childpid = redisFork(CHILD_TYPE_RDB)) == 0) {
        // ── 자식 프로세스 ──
        // CoW(Copy-on-Write) 덕분에 현재 메모리 스냅샷 사용
        // temp 파일에 rdbSave() → 완료 후 rename() (원자적 교체)
        rdbSave(req, filename, rsi, rdbflags);
        exitFromChild(exitcode);
    } else {
        // ── 부모(메인) 프로세스 ──
        server.rdb_child_pid = childpid;
        // 계속 클라이언트 요청 처리
    }
}
```

### 3.9 diskless 복제 모드

```c
// rdb.c:1791 — 네트워크로 직접 전송 (파일 저장 없음)
int rdbSaveRioWithEOFMark(int req, rio *rdb, int *error, rdbSaveInfo *rsi) {
    // 프로토콜: "$EOF:<40자리 랜덤 hex>\r\n" + RDB 데이터 + "<40자리 hex>"
    // 수신자가 파일 크기를 모르더라도 종료 시점 인식 가능
    getRandomHexChars(eofmark, RDB_EOF_MARK_SIZE);
    rioWrite(rdb, "$EOF:", 5);
    rioWrite(rdb, eofmark, RDB_EOF_MARK_SIZE);
    rioWrite(rdb, "\r\n", 2);
    rdbSaveRio(req, rdb, error, RDBFLAGS_REPLICATION, rsi);
    rioWrite(rdb, eofmark, RDB_EOF_MARK_SIZE);  // EOF 마커 (40바이트)
}
```

### 3.10 RDBFLAGS

```c
// rdb.h:118
#define RDBFLAGS_NONE           0
#define RDBFLAGS_AOF_PREAMBLE  (1<<0) // AOF 파일 앞부분에 RDB 포함
#define RDBFLAGS_REPLICATION   (1<<1) // 복제 동기화용
#define RDBFLAGS_ALLOW_DUP     (1<<2) // 중복 키 허용 (RESTORE 명령)
#define RDBFLAGS_FEED_REPL     (1<<3) // 로드 중 복제 스트림 피드
#define RDBFLAGS_KEEP_CACHE    (1<<4) // 생성 후 페이지 캐시 유지
```

---

## 4. AOF — Append Only File (`aof.c`)

### 4.1 AOF 다중 파일 구조 (Manifest 시스템)

Redis 7.0+에서 단일 AOF 파일 대신 **Manifest 기반 다중 파일** 시스템 도입.

```
appendonly.aof.manifest          ← 전체 파일 목록 관리
appendonly.aof.2.base.rdb        ← BASE: 마지막 리라이트 시점의 스냅샷
appendonly.aof.4.incr.aof        ← INCR: 이후 쓰기 명령 누적
appendonly.aof.5.incr.aof        ← INCR: 복수 가능
appendonly.aof.1.incr.aof        ← HISTORY: 이전 BASE/INCR (GC 대상)
```

**파일 타입:**

| 타입 | 코드 | 설명 |
|------|------|------|
| BASE | `b` | 마지막 AOF 리라이트 시점의 전체 스냅샷 |
| INCR | `i` | 리라이트 이후 누적된 명령어 로그 |
| HISTORY | `h` | 리라이트 성공 후 이전 BASE+INCR → 자동 삭제 예정 |

**Manifest 형식:**
```
file appendonly.aof.2.base.rdb seq 2 type b
file appendonly.aof.4.incr.aof seq 4 type i startoffset 1024
```

### 4.2 aofInfo 구조체

```c
typedef struct aofInfo {
    sds        file_name;
    long long  file_seq;      // 순번 (단조 증가)
    char       file_type;     // 'b'|'i'|'h'
    long long  start_offset;  // 복제 오프셋 시작
    long long  end_offset;    // 복제 오프셋 끝
} aofInfo;
```

### 4.3 AOF 쓰기 흐름

```
Write 명령 실행
  → feedAppendOnlyFile(cmd)
      → 명령을 RESP 형식으로 직렬화
      → server.aof_buf (SDS)에 추가

beforeSleep()
  → flushAppendOnlyFile(force=0)
      → aofWrite(server.aof_fd, server.aof_buf, len)
          (write(2) — 커널 페이지 캐시에 기록)
      → fsync 정책에 따라 처리 (아래 참조)
      → server.aof_buf = sdsempty() (버퍼 초기화)
```

### 4.4 fsync 3가지 정책

```c
// server.h — AOF_FSYNC_* 상수
// aof.c:1147 — flushAppendOnlyFile()

// 1. AOF_FSYNC_NO (가장 빠름, 위험)
//    OS가 결정 (보통 30초마다 또는 메모리 압박 시)
//    Redis 장애 시 최대 30초치 데이터 손실 가능

// 2. AOF_FSYNC_ALWAYS (가장 안전, 가장 느림)
//    매 write() 직후 즉시 fsync()
//    모든 명령이 디스크에 보장됨

// 3. AOF_FSYNC_EVERYSEC (기본값, 권장)
//    1초마다 BIO 스레드에서 fsync
//    최대 1초치 데이터 손실 가능
if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
    if (aofFsyncInProgress()) {
        // fsync 진행 중이면 최대 2초 지연 허용
        // 2초 초과 시 강제 write
    }
    bioCreateFsyncJob(server.aof_fd, ...);  // BIO 스레드로 위임
}
```

### 4.5 AOF 리라이트 (BGREWRITEAOF)

```c
// aof.c:2718
int rewriteAppendOnlyFileBackground(void) {
    if (hasActiveChildProcess()) return C_ERR;

    if ((childpid = redisFork(CHILD_TYPE_AOF)) == 0) {
        // ── 자식 프로세스 ──
        // 1. temp BASE 파일에 현재 DB 전체를 RDB 형식으로 저장
        rewriteAppendOnlyFile(temp_filename);
        exitFromChild(0);
    } else {
        // ── 부모(메인) 프로세스 ──
        // 2. 새 INCR AOF 파일 오픈 (리라이트 중 발생하는 쓰기 수용)
        openNewIncrAofForAppend();
        server.aof_child_pid = childpid;
    }
}

// 자식 완료 후 (backgroundRewriteDoneHandler):
// 1. temp BASE를 실제 BASE 파일명으로 rename()
// 2. 이전 BASE + INCR들을 HISTORY 타입으로 변경
// 3. manifest 파일 동기적 업데이트
// 4. HISTORY 파일들 BIO로 비동기 삭제
```

### 4.6 AOF 리라이트 중 데이터 손실 방지 구조

```
리라이트 시작
  │
  ├── 자식: DB 스냅샷 → temp BASE 파일 기록
  │
  └── 부모: 새 INCR AOF 파일에 계속 쓰기
               (리라이트 기간 동안 누적된 쓰기 보존)

리라이트 완료
  ├── BASE = temp → 새 BASE (rename)
  ├── INCR = 새로 시작한 INCR (리라이트 이후 명령)
  └── 이전 파일들 → HISTORY → 비동기 삭제
```

### 4.7 AOF 로드 (서버 시작)

```
1. manifest 파일 읽기
2. BASE 파일 로드 (RDB 형식)
3. INCR 파일들을 seq 순서대로 로드 (RESP 형식 재실행)
4. HISTORY 파일들 정리
```

---

## 5. BIO — 백그라운드 I/O 스레드 (`bio.h` / `bio.c`)

### 5.1 설계 목적

메인 이벤트 루프를 블로킹시킬 수 있는 I/O 작업을 별도 스레드로 위임.

### 5.2 3개 워커 스레드

```c
// bio.h:16
typedef enum bio_worker_t {
    BIO_WORKER_CLOSE_FILE = 0, // "bio_close_file"
    BIO_WORKER_AOF_FSYNC  = 1, // "bio_aof"
    BIO_WORKER_LAZY_FREE  = 2, // "bio_lazy_free"
} bio_worker_t;
```

### 5.3 작업 타입 → 워커 매핑

```c
// bio.c:59
static unsigned int bio_job_to_worker[] = {
    [BIO_CLOSE_FILE]       = 0,  // 파일 닫기 (fsync 옵션 포함)
    [BIO_AOF_FSYNC]        = 1,  // AOF fsync
    [BIO_CLOSE_AOF]        = 1,  // AOF 파일 닫기 (fsync 포함)
    [BIO_LAZY_FREE]        = 2,  // 메모리 비동기 해제
    [BIO_COMP_RQ_CLOSE_FILE] = 0,// 완료 알림 요청 (close 워커)
    [BIO_COMP_RQ_AOF_FSYNC]  = 1,// 완료 알림 요청 (aof 워커)
    [BIO_COMP_RQ_LAZY_FREE]  = 2,// 완료 알림 요청 (lazyfree 워커)
};
```

### 5.4 bio_job 유니온 구조체

```c
// bio.c:90
typedef union bio_job {
    struct { int type; } header;  // 모든 타입 공통 헤더

    // 파일 작업 (CLOSE_FILE, AOF_FSYNC, CLOSE_AOF)
    struct {
        int type;
        int fd;
        long long offset;
        unsigned need_fsync:1;         // 닫기 전 fsync 필요
        unsigned need_reclaim_cache:1; // 닫기 전 페이지 캐시 반납
    } fd_args;

    // 메모리 해제 작업 (LAZY_FREE)
    struct {
        int type;
        lazy_free_fn *free_fn; // 실제 해제 함수
        void *free_args[];     // 가변 인수 배열
    } free_args;

    // 완료 알림 (COMP_RQ_*)
    struct {
        int type;
        comp_fn *fn;    // 완료 콜백
        uint64_t arg;
        void *ptr;
    } comp_rq;
} bio_job;
```

### 5.5 동기화 메커니즘

```c
// 워커별 독립 동기화
static pthread_t      bio_threads[BIO_WORKER_NUM];
static pthread_mutex_t bio_mutex[BIO_WORKER_NUM];
static pthread_cond_t  bio_newjob_cond[BIO_WORKER_NUM];
static list           *bio_jobs[BIO_WORKER_NUM];  // FIFO 큐

// 완료 알림
static list           *bio_comp_list;     // 완료 작업 결과 목록
static pthread_mutex_t bio_mutex_comp;
static int             job_comp_pipe[2];  // 이벤트 루프 깨우기용 파이프
```

### 5.6 완료 알림 흐름

```
BIO 워커 스레드
  → COMP_RQ 작업 처리
  → bio_comp_list에 결과 추가
  → job_comp_pipe에 1바이트 write()  // 이벤트 루프 깨움

메인 이벤트 루프
  → aeCreateFileEvent(job_comp_pipe[0], bioPipeReadJobCompList)
  → bioPipeReadJobCompList() 호출
  → bio_comp_list에서 결과 읽기
  → 콜백 함수 실행
```

### 5.7 주요 API

```c
void bioCreateCloseJob(int fd, int need_fsync, int need_reclaim_cache);
void bioCreateCloseAofJob(int fd, long long offset, int need_reclaim_cache);
void bioCreateFsyncJob(int fd, long long offset, int need_reclaim_cache);
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...);
void bioCreateCompRq(bio_worker_t worker, comp_fn *func, uint64_t arg, void *ptr);

unsigned long bioPendingJobsOfType(int type); // 대기 중 작업 수 조회
void bioDrainWorker(int job_type);            // 해당 워커 큐 완전 소진 대기
```

---

## 6. Lazy Free — 비동기 메모리 해제

### 6.1 필요성

대형 객체(수백만 항목 Set 등) 동기 삭제 시 메인 스레드 수십ms 블로킹 발생.
→ BIO_LAZY_FREE 워커에 위임해 비동기 처리.

### 6.2 Lazy Free 트리거 시나리오

| 명령 / 상황 | lazyfree 사용 조건 |
|-------------|-------------------|
| `DEL` | `lazyfree-lazy-user-del yes` |
| `UNLINK` | 항상 |
| `FLUSHDB ASYNC` | 항상 |
| `FLUSHALL ASYNC` | 항상 |
| Key 만료 | `lazyfree-lazy-expire yes` |
| 서버 eviction | `lazyfree-lazy-eviction yes` |
| Replica 스트림 교체 | `lazyfree-lazy-user-flush yes` |

### 6.3 구현 패턴

```c
// lazyfree.c
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);  // 실제 해제는 여기서 발생 (BIO 스레드)
}

// DEL 명령 처리 시
if (server.lazyfree_lazy_user_del) {
    dbDeleteAsync(db, key);  // BIO에 위임
} else {
    dbDelete(db, key);       // 동기 삭제
}
```

---

## 7. RDB vs AOF 비교

| 항목 | RDB | AOF |
|------|-----|-----|
| **저장 형식** | 바이너리 스냅샷 | RESP 텍스트 명령 로그 |
| **저장 시점** | 주기적 (BGSAVE, save 설정) | 매 쓰기 명령마다 |
| **파일 크기** | 작음 (압축 인코딩) | 큼 (RESP 텍스트) |
| **복구 속도** | 빠름 (직접 로드) | 느림 (명령 재실행) |
| **데이터 손실** | 마지막 스냅샷 이후 손실 | fsync 정책에 따라 0~1초 |
| **포크 오버헤드** | 있음 (BGSAVE) | 있음 (리라이트 시) |
| **CPU 사용** | 낮음 | 약간 높음 (fsync) |
| **기본 설정** | 비활성화 (7.0+) | 비활성화 |
| **파일 구조** | 단일 RDB 파일 | Manifest + BASE + INCR |

### 권장 조합

```
# 최고 내구성 (금융 등)
appendonly yes
appendfsync always

# 균형 (일반 운영, 권장)
appendonly yes
appendfsync everysec
save 3600 1 300 100 60 10000  # RDB도 병행

# 최고 성능 (캐시 전용)
appendonly no
# RDB도 비활성화 가능
```

---

## 8. 핵심 설계 포인트 요약

### fork() + CoW 스냅샷

```
메인 프로세스 fork() → 자식 프로세스
  ├── 자식: fork 시점 메모리를 그대로 사용 (CoW)
  │         쓰기 발생 시 OS가 페이지 복사 → 부모/자식 독립
  └── 부모: 계속 클라이언트 처리 (쓰기 발생 시 CoW 페이지 분리)

장점: 스냅샷 중 서비스 중단 없음
단점: 쓰기 많을수록 CoW 복사 증가 → 메모리 사용량 일시 증가
```

### 점진적 fsync (autosync 4MB)

```
RDB 저장 / AOF 리라이트 중 4MB마다 자동 fsync:
  → 커널 더티 페이지가 한꺼번에 쌓이지 않음
  → I/O 스파이크 방지
  → Linux: sync_file_range() 비동기 → 성능 최적
  → 완료 후 posix_fadvise(DONTNEED) → 페이지 캐시 반납
```

### AOF Manifest 다중 파일 설계

```
리라이트 중에도 INCR 파일로 계속 쓰기 보장
  → 리라이트 실패 시 기존 BASE + INCR로 완전 복구
  → 리라이트 성공 시 원자적 manifest 교체
  → HISTORY 파일들 BIO로 비동기 정리
```

### 계층별 역할 분리

| 계층 | 파일 | 역할 |
|------|------|------|
| I/O 추상화 | `rio.c` | 파일/소켓/메모리를 단일 인터페이스로 |
| 스냅샷 | `rdb.c` | 바이너리 포맷 직렬화/역직렬화 |
| 명령 로그 | `aof.c` | RESP 명령 누적 + Manifest 관리 |
| 백그라운드 I/O | `bio.c` | fsync/close/lazyfree 비동기 처리 |
| 비동기 해제 | `lazyfree.c` | 대형 객체 삭제 지연 처리 |

---

*분석 기준: Redis `redis/src/` 소스, 2024-present 버전 (RDB_VERSION 14)*
