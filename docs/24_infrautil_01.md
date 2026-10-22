# Redis 3-9. 인프라 & 유틸리티 소스 분석

> 분석 대상: `sds.h/c`, `ae.h/c`, `ae_epoll/kqueue/evport/select.c`, `bio.h/c`, `iothread.c`, `acl.c`, `config.c`, `sentinel.c`, `latency.c`

---

## 1. SDS (Simple Dynamic Strings) — `sds.h / sds.c`

### 1-1. 설계 원칙

SDS는 Redis의 모든 문자열 표현에 사용되는 C 동적 문자열 라이브러리다.  
`sds` 타입은 단순히 `char *`이며, 포인터가 가리키는 위치 **앞**에 헤더가 붙어 있다.

```
메모리 레이아웃:
  [ sdshdr8 header ] [ 실제 문자열 데이터 '\0' ]
                     ^--- sds 포인터가 여기를 가리킴
```

이 설계로 인해:
- `strlen(s)` 대신 `sdslen(s)` → O(1) 조회 (헤더의 len 필드)
- `printf("%s", s)` 같은 C 함수와 직접 호환 (null terminator 유지)
- 이진 안전 (null 문자 포함 가능, len 기반 길이 추적)

### 1-2. 헤더 타입 5종 (`__attribute__((__packed__))`)

| 타입 | 상수 | len/alloc 크기 | 최대 문자열 길이 | 헤더 크기 |
|------|------|--------------|---------------|---------|
| `sdshdr5`  | `SDS_TYPE_5=0`  | 5비트 (flags 상위 5bit) | 31 bytes | 1 byte |
| `sdshdr8`  | `SDS_TYPE_8=1`  | uint8_t × 2 | 255 bytes | 3 bytes |
| `sdshdr16` | `SDS_TYPE_16=2` | uint16_t × 2 | 65535 bytes | 5 bytes |
| `sdshdr32` | `SDS_TYPE_32=3` | uint32_t × 2 | 4GB | 9 bytes |
| `sdshdr64` | `SDS_TYPE_64=4` | uint64_t × 2 | 2^64 bytes | 17 bytes |

```c
struct __attribute__ ((__packed__)) sdshdr8 {
    uint8_t len;          /* 현재 사용 길이 */
    uint8_t alloc;        /* 할당 크기 (헤더+null 제외) */
    unsigned char flags;  /* 하위 3비트: 타입, 상위 5비트: 여유분 */
    char buf[];           /* 실제 문자열 데이터 */
};
```

**sdshdr5 특수성**: len/alloc 필드 없이 flags 상위 5비트에 길이 직접 인코딩.  
별도 alloc 정보가 없으므로 `sdsavail(s)` == 0, 확장 불가 (읽기 전용 용도).

### 1-3. 타입 선택 및 접근 매크로

```c
// 타입 판별: s[-1] & SDS_TYPE_MASK (하위 3비트)
static inline unsigned char sdsType(sds s) { return s[-1] & SDS_TYPE_MASK; }

// 헤더 포인터 획득
#define SDS_HDR(T, s)     ((struct sdshdr##T *)((s)-(sizeof(struct sdshdr##T))))
#define SDS_HDR_VAR(T, s) struct sdshdr##T *sh = (void*)((s)-(sizeof(struct sdshdr##T)));

// 타입 5 전용: flags 상위 5비트에서 len 추출
#define SDS_TYPE_5_LEN(s) (((unsigned char)(s[-1])) >> SDS_TYPE_BITS)
```

주요 인라인 함수:
- `sdslen(s)` — 현재 문자열 길이
- `sdsavail(s)` — 남은 여유 공간 (alloc - len)
- `sdsalloc(s)` — 총 할당 크기
- `sdsAllocSize(s)` — 헤더 + alloc + null terminator 전체 크기

### 1-4. AuxBit (보조 비트)

flags 바이트의 `[3..7]` 비트 (타입 비트 위)를 사용자 정의 용도로 사용 가능:

```c
// bit 인덱스 0~4, SDS_TYPE_5는 미지원
static inline int  sdsGetAuxBit(sds s, int bit);
static inline void sdsSetAuxBit(sds s, int bit, int value);
```

다른 구조체에 SDS를 임베드할 때 불변(immutable) 표시 등에 활용.  
단, SDS 자동 resize 시 aux bits는 소실된다.

### 1-5. 핵심 상수 및 정책

```c
#define SDS_MAX_PREALLOC (1024*1024)  // 1MB: 이 이상이면 정확히 필요한 만큼만 할당
```

**선행 할당 정책** (`sdsMakeRoomFor`):
- 새 길이 < 1MB → 2배 할당
- 새 길이 ≥ 1MB → 필요량 + SDS_MAX_PREALLOC (1MB) 추가

### 1-6. 주요 API

| 함수 | 설명 |
|------|------|
| `sdsnew(init)` | C 문자열로 SDS 생성 |
| `sdsnewlen(init, len)` | 길이 지정 생성 (이진 안전) |
| `sdsnewplacement(buf, ...)` | 제공된 버퍼에 SDS 배치 (제로카피) |
| `sdscatprintf(s, fmt, ...)` | printf 스타일 추가 |
| `sdscatfmt(s, fmt, ...)` | Redis 자체 경량 포맷 (printf 의존 없음) |
| `sdstrim(s, cset)` | 양쪽 문자 제거 |
| `sdssplitlen(s, len, sep, ...)` | 구분자로 분리 → `sds[]` |
| `sdstemplate(tmpl, cb, arg)` | `{variable}` 템플릿 치환 |
| `sdsMakeRoomFor(s, addlen)` | 공간 확보 (greedy, 2배 전략) |
| `sdsMakeRoomForNonGreedy(s, addlen)` | 최소 공간만 확보 |
| `sdsRemoveFreeSpace(s, would_regrow)` | 여유 공간 제거 (메모리 절약) |
| `sdsResize(s, size, would_regrow)` | 크기 조정 |

---

## 2. 이벤트 루프 (ae.c / ae.h) — Reactor 패턴

### 2-1. 설계 개요

Redis의 단일 스레드 이벤트 루프. Jim Tcl 인터프리터용으로 시작, Redis에 이식됨.  
플랫폼 최적 I/O 다중화 API를 컴파일 타임에 선택:

```c
// ae.c - 우선순위 순서
#ifdef HAVE_EVPORT   → ae_evport.c   (Solaris)
#elif HAVE_EPOLL     → ae_epoll.c    (Linux)
#elif HAVE_KQUEUE    → ae_kqueue.c   (macOS/BSD)
#else                → ae_select.c   (범용 fallback)
```

각 백엔드는 동일한 내부 API를 구현:
- `aeApiCreate()`, `aeApiFree()`
- `aeApiAddEvent()`, `aeApiDelEvent()`
- `aeApiPoll(tvp)` — 이벤트 대기 핵심 함수
- `aeApiName()` — 백엔드 이름 반환

### 2-2. 핵심 자료구조

```c
typedef struct aeEventLoop {
    int maxfd;              /* 현재 등록된 최고 fd */
    int setsize;            /* 추적 가능한 최대 fd 수 */
    long long timeEventNextId;
    int nevents;            /* 현재 할당된 이벤트 배열 크기 */
    aeFileEvent *events;    /* 등록된 파일 이벤트 배열 [fd 인덱스] */
    aeFiredEvent *fired;    /* aeApiPoll()이 반환한 발생 이벤트 */
    aeTimeEvent *timeEventHead; /* 시간 이벤트 doubly-linked list */
    int stop;
    void *apidata;          /* epoll_fd / kqueue fd 등 백엔드 전용 데이터 */
    aeBeforeSleepProc *beforesleep;  /* poll 전 호출 */
    aeBeforeSleepProc *aftersleep;   /* poll 후 호출 */
    int flags;              /* AE_DONT_WAIT 등 */
    void *privdata[2];
} aeEventLoop;

typedef struct aeFileEvent {
    int mask;               /* AE_NONE / AE_READABLE / AE_WRITABLE / AE_BARRIER */
    aeFileProc *rfileProc;  /* 읽기 핸들러 */
    aeFileProc *wfileProc;  /* 쓰기 핸들러 */
    void *clientData;
} aeFileEvent;

typedef struct aeTimeEvent {
    long long id;
    monotime when;          /* 다음 실행 시각 (마이크로초, 모노토닉) */
    aeTimeProc *timeProc;   /* 핸들러 — AE_NOMORE 반환 시 삭제 */
    aeEventFinalizerProc *finalizerProc;
    void *clientData;
    struct aeTimeEvent *prev, *next;
    int refcount;           /* 재귀 호출 중 삭제 방지 */
} aeTimeEvent;
```

### 2-3. 이벤트 마스크 플래그

```c
#define AE_NONE      0   /* 등록 없음 */
#define AE_READABLE  1   /* fd 읽기 가능 시 발생 */
#define AE_WRITABLE  2   /* fd 쓰기 가능 시 발생 */
#define AE_BARRIER   4   /* WRITABLE과 함께 사용: 같은 루프에서
                            READABLE이 이미 발화했으면 WRITABLE 억제 */
```

**AE_BARRIER 사용 사례**: AOF fsync — 디스크에 먼저 저장한 후 클라이언트에 응답  
→ beforesleep()에서 fsync 후 쓰기 이벤트 발화, 읽기 응답 순서 역전 방지.

### 2-4. 동적 이벤트 배열 리사이즈

```c
// INITIAL_EVENT = 1024
// fd가 nevents 초과 시 자동 2배 확장 (단, setsize 상한)
if (unlikely(fd >= eventLoop->nevents)) {
    int newnevents = eventLoop->nevents;
    newnevents = (newnevents * 2 > fd + 1) ? newnevents * 2 : fd + 1;
    newnevents = (newnevents > eventLoop->setsize) ? eventLoop->setsize : newnevents;
    eventLoop->events = zrealloc(eventLoop->events, sizeof(aeFileEvent) * newnevents);
    eventLoop->fired  = zrealloc(eventLoop->fired,  sizeof(aeFiredEvent) * newnevents);
}
```

### 2-5. aeProcessEvents() 핵심 흐름

```
aeProcessEvents(flags)
  ├─ beforesleep(eventLoop)          [AE_CALL_BEFORE_SLEEP]
  ├─ usUntilEarliestTimer()          타임 이벤트 헤드 O(N) 스캔
  ├─ aeApiPoll(tvp)                  epoll_wait / kevent / select
  ├─ aftersleep(eventLoop)           [AE_CALL_AFTER_SLEEP]
  ├─ 파일 이벤트 dispatch:
  │   ├─ AE_BARRIER 없으면: READABLE → WRITABLE 순
  │   └─ AE_BARRIER 있으면: WRITABLE 억제 (역순 방지)
  └─ processTimeEvents()
      ├─ AE_DELETED_EVENT_ID 정리
      └─ te->when ≤ now 이면 timeProc() 호출
          └─ AE_NOMORE 반환 시 자동 삭제
```

### 2-6. 시간 이벤트 구현 특성

- 헤드에 새 이벤트 삽입 (prepend), doubly-linked list
- 삭제: id를 `AE_DELETED_EVENT_ID(-1)`로 마킹 → 다음 루프에서 실제 해제
- `refcount` 로 재귀 호출 중 freed 방지
- O(N) 스캔 (Redis는 현재 시간 이벤트가 1개뿐 — serverCron)

---

## 3. 백그라운드 I/O — `bio.h / bio.c`

### 3-1. 설계

블로킹 I/O를 메인 스레드에서 분리하기 위한 스레드 풀.  
현재 **3개 워커 스레드**, FIFO 처리 보장.

```
워커 0 (bio_close_file): BIO_CLOSE_FILE, BIO_COMP_RQ_CLOSE_FILE
워커 1 (bio_aof):        BIO_AOF_FSYNC, BIO_CLOSE_AOF, BIO_COMP_RQ_AOF_FSYNC
워커 2 (bio_lazy_free):  BIO_LAZY_FREE, BIO_COMP_RQ_LAZY_FREE
```

```c
static unsigned int bio_job_to_worker[] = {
    [BIO_CLOSE_FILE]          = 0,
    [BIO_AOF_FSYNC]           = 1,
    [BIO_CLOSE_AOF]           = 1,
    [BIO_LAZY_FREE]           = 2,
    [BIO_COMP_RQ_CLOSE_FILE]  = 0,
    [BIO_COMP_RQ_AOF_FSYNC]   = 1,
    [BIO_COMP_RQ_LAZY_FREE]   = 2
};
```

### 3-2. bio_job 유니온

```c
typedef union bio_job {
    struct { int type; } header;  // 모든 멤버의 첫 필드

    struct {                      // 파일 기반 작업
        int type;
        int fd;
        long long offset;
        unsigned need_fsync:1;
        unsigned need_reclaim_cache:1;
    } fd_args;

    struct {                      // lazyfree 작업
        int type;
        lazy_free_fn *free_fn;
        void *free_args[];        // 가변 인자 (bioCreateLazyFreeJob의 arg_count개)
    } free_args;

    struct {                      // 완료 통지 요청
        int type;
        comp_fn *fn;
        uint64_t arg;
        void *ptr;
    } comp_rq;
} bio_job;
```

### 3-3. 완료 통지 파이프라인

```
BIO 워커 스레드
  └─ 완료 job 처리 후 → bio_comp_list에 bio_comp_item 삽입
                        → job_comp_pipe[1]에 1바이트 write (이벤트 루프 깨우기)

메인 스레드 이벤트 루프
  └─ job_comp_pipe[0] 읽기 이벤트 발생
       → bioPipeReadJobCompList()
           → bio_comp_list에서 항목 소비
           → comp_fn(user_data, user_ptr) 콜백 호출
```

**설계 의도**: FLUSHALL 같이 하나의 커맨드가 여러 BIO job을 제출할 때,  
completion job을 추가로 큐에 넣으면 FIFO 순서 보장 덕분에 정확한 완료 시점 알림 가능.

### 3-4. 초기화 및 스택 크기

```c
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)  // 4MB

void bioInit(void) {
    // 뮤텍스 + condvar 초기화
    // completion pipe 생성 (O_CLOEXEC|O_NONBLOCK)
    // aeCreateFileEvent()로 pipe 읽기 이벤트 등록
    // pthread_attr setstacksize → 4MB 보장
    // BIO_WORKER_NUM(3)개 스레드 생성
}
```

### 3-5. 주요 API

```c
void bioCreateCloseJob(int fd, int need_fsync, int need_reclaim_cache);
void bioCreateCloseAofJob(int fd, long long offset, int need_reclaim_cache);
void bioCreateFsyncJob(int fd, long long offset, int need_reclaim_cache);
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...);
void bioCreateCompRq(bio_worker_t worker, comp_fn *func, uint64_t user_data, void *user_ptr);
void bioDrainWorker(int job_type);       // 해당 워커 작업 완료 대기 (블로킹)
unsigned long bioPendingJobsOfType(int type); // 대기 중인 작업 수 조회
```

---

## 4. IO 스레딩 — `iothread.c`

### 4-1. 아키텍처

Redis 7.x+에서 도입된 멀티스레드 I/O.  
메인 스레드: 커맨드 처리 (싱글 스레드 유지)  
IO 스레드: 네트워크 읽기/쓰기 처리 (병렬)

```c
// server.h
#define IO_THREADS_MAX_NUM  128
#define IOTHREAD_MAIN_THREAD_ID  0  // 메인 스레드도 "IO 스레드 0"으로 취급

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
    uint8_t id;
    pthread_t tid;
    redisAtomic int paused;   // IO 스레드 일시 정지 플래그
    redisAtomic int running;  // 실행 중 여부
    aeEventLoop *el;          // IO 스레드 전용 이벤트 루프
    list *pending_clients;              // 쓰기 대기 클라이언트
    list *processing_clients;           // 처리 중 클라이언트
    eventNotifier *pending_clients_notifier;
    pthread_mutex_t pending_clients_mutex;
    list *pending_clients_to_main_thread; // 메인 스레드로 이관 대기
    list *clients;            // 이 IO 스레드가 관리하는 클라이언트
} IOThread;
```

**CACHE_LINE_SIZE 정렬**: 여러 CPU 코어 간 false sharing 방지.

### 4-2. 클라이언트 이관 큐 체계

클라이언트 하나가 동시에 있을 수 있는 6개 큐:

```
IO 스레드 측:
  IOThreads[id].clients                     — IO 스레드가 관리 중
  IOThreads[id].pending_clients             — 쓰기 대기
  IOThreads[id].pending_clients_to_main_thread — 메인으로 이관 예정

메인 스레드 측:
  mainThreadPendingClientsToIOThreads[id]   — IO 스레드로 이관 예정
  mainThreadProcessingClients[id]           — 처리 중
  mainThreadPendingClients[id]              — IO 스레드에서 돌아온 클라이언트
```

### 4-3. 메인 스레드 전용 강제 처리 조건

```c
int isClientMustHandledByMainThread(client *c) {
    // 다음 플래그 중 하나라도 있으면 메인 스레드 전용
    if (c->flags & (CLIENT_CLOSE_ASAP |
                    CLIENT_PUBSUB | CLIENT_MONITOR | CLIENT_BLOCKED |
                    CLIENT_UNBLOCKED | CLIENT_TRACKING | CLIENT_LUA_DEBUG |
                    CLIENT_LUA_DEBUG_SYNC | CLIENT_ASM_MIGRATING |
                    CLIENT_ASM_IMPORTING))
        return 1;

    // Master 클라이언트: RDB 완료 후 + failover 없을 때만 IO 스레드 가능
    if (c->flags & CLIENT_MASTER &&
        server.repl_state == REPL_STATE_CONNECTED &&
        server.repl_rdb_ch_state == REPL_RDB_CH_STATE_NONE &&
        server.failover_state == NO_FAILOVER)
        return 0;
    ...
}
```

### 4-4. IO 스레드 ↔ 메인 스레드 이관 흐름

**메인 → IO 스레드** (`enqueuePendingClienstToIOThreads`):
```
1. CLIENT_PENDING_WRITE 해제
2. 슬레이브 클라이언트: io_repl_ack_time, io_curr_repl_node 복사
3. 마스터 클라이언트: io_read_reploff, io_lastinteraction 복사
4. c->running_tid = c->tid
5. mainThreadPendingClientsToIOThreads[tid] 큐에 추가
```

**IO 스레드 → 메인** (`enqueuePendingClientsToMainThread`):
```
1. connUnbindEventLoop() 로 이벤트 루프에서 제거
2. IO 스레드 clients 리스트에서 제거
3. pending_clients_to_main_thread에 추가
4. sendPendingClientsToMainThreadIfNeeded():
     배치 크기(IO_THREAD_MAX_PENDING_CLIENTS) 도달 또는 check_size=0
     → mainThreadPendingClients에 listJoin
     → triggerEventNotifier (메인 스레드 idle 시에만)
```

### 4-5. 크론 및 통계

```c
int runClientCronFromIOThread(client *c) {
    // 마스터 클라이언트: 1초마다 replication cron
    if (c->flags & CLIENT_MASTER &&
        c->io_last_repl_cron + 1000 <= server.mstime)
        if (replicationCronRunMasterClient()) return 1; // 클라이언트 종료 필요

    // 일반 클라이언트: 1초마다 또는 CLIENT_IO_PENDING_CRON 플래그
    if (c->io_last_client_cron + 1000 <= server.mstime ||
        c->io_flags & CLIENT_IO_PENDING_CRON)
        if (clientsCronRunClient(c)) return 1;
    else
        updateClientMemUsageAndBucket(c); // cron 미실행 시 메모리 사용량 갱신
}
```

통계 배열:
```c
redisAtomic long long stat_io_reads_processed[IO_THREADS_MAX_NUM];
redisAtomic long long stat_io_writes_processed[IO_THREADS_MAX_NUM];
```

---

## 5. ACL (Access Control List) — `acl.c`

### 5-1. 자료구조 계층

```
user (server.h)
  ├─ redisAtomic uint32_t flags   USER_FLAG_ENABLED/DISABLED/NOPASS/...
  ├─ list *selectors              aclSelector 목록 (v7.0+ 다중 선택자)
  └─ list *passwords              SHA256 해시 문자열 목록

aclSelector (acl.c 내부)
  ├─ uint32_t flags               SELECTOR_FLAG_ALLKEYS/ALLCHANNELS/ALLCOMMANDS
  ├─ uint64_t allowed_commands[USER_COMMAND_BITS_COUNT/64]  비트맵
  ├─ sds **allowed_firstargs      명령별 허용 첫 번째 인자 목록
  ├─ list *patterns               허용 키 패턴 (keyPattern)
  ├─ list *channels               허용 Pub/Sub 채널 패턴
  └─ sds command_rules            원본 ACL 규칙 문자열 (표시용)
```

### 5-2. USER_FLAG 및 SELECTOR_FLAG

```c
// USER 플래그
#define USER_FLAG_ENABLED            (1<<0)  /* 활성화 */
#define USER_FLAG_DISABLED           (1<<1)  /* 비활성화 */
#define USER_FLAG_NOPASS             (1<<2)  /* 비밀번호 불필요 */
#define USER_FLAG_SANITIZE_PAYLOAD   (1<<3)  /* 깊은 RESTORE 검증 */
#define USER_FLAG_SANITIZE_PAYLOAD_SKIP (1<<4)

// SELECTOR 플래그
// SELECTOR_FLAG_ALLKEYS      — 모든 키 허용
// SELECTOR_FLAG_ALLCHANNELS  — 모든 채널 허용
// SELECTOR_FLAG_ALLCOMMANDS  — 모든 명령 허용
```

### 5-3. ACL 카테고리

```c
#define ACL_MAX_CATEGORIES  64   // 최대 카테고리 수

// 21개 내장 카테고리 (server.h)
ACL_CATEGORY_KEYSPACE    (1ULL<<0)
ACL_CATEGORY_READ        (1ULL<<1)
ACL_CATEGORY_WRITE       (1ULL<<2)
ACL_CATEGORY_SET         (1ULL<<3)
ACL_CATEGORY_SORTEDSET   (1ULL<<4)
ACL_CATEGORY_LIST        (1ULL<<5)
ACL_CATEGORY_HASH        (1ULL<<6)
ACL_CATEGORY_STRING      (1ULL<<7)
ACL_CATEGORY_BITMAP      (1ULL<<8)
ACL_CATEGORY_HYPERLOGLOG (1ULL<<9)
ACL_CATEGORY_GEO         (1ULL<<10)
ACL_CATEGORY_STREAM      (1ULL<<11)
ACL_CATEGORY_PUBSUB      (1ULL<<12)
ACL_CATEGORY_ADMIN       (1ULL<<13)
ACL_CATEGORY_FAST        (1ULL<<14)
ACL_CATEGORY_SLOW        (1ULL<<15)
ACL_CATEGORY_BLOCKING    (1ULL<<16)
ACL_CATEGORY_DANGEROUS   (1ULL<<17)
ACL_CATEGORY_CONNECTION  (1ULL<<18)
ACL_CATEGORY_TRANSACTION (1ULL<<19)
ACL_CATEGORY_SCRIPTING   (1ULL<<20)
```

### 5-4. 비밀번호 처리

```c
#define HASH_PASSWORD_LEN (SHA256_BLOCK_SIZE*2)  // 64 hex chars

sds ACLHashPassword(unsigned char *cleartext, size_t len) {
    SHA256_CTX ctx;
    unsigned char hash[SHA256_BLOCK_SIZE];  // 32 bytes
    // sha256_init → sha256_update → sha256_final
    // 결과를 hex 인코딩 → 64자 SDS 반환
}
```

**타이밍 공격 방지**: `time_independent_strcmp()` — XOR 누적으로 일정 시간 비교

```c
int time_independent_strcmp(char *a, char *b, int len) {
    int diff = 0;
    for (int j = 0; j < len; j++) diff |= (a[j] ^ b[j]);
    return diff;  // 0이면 동일
}
```

### 5-5. ACL 권한 확인 체인

```
ACLCheckAllPerm(client *c, int *idxptr)
  ├─ ACLCheckAllUserCommandPerm()   — 명령 권한
  │    └─ ACLSelectorCheckCmd()
  │         ├─ ACLGetSelectorCommandBit()   비트맵 조회
  │         ├─ allowed_firstargs 확인
  │         └─ key 권한: ACLSelectorCheckKey()
  └─ ACLUserCheckChannelPerm()     — 채널 권한 (Pub/Sub)
```

**ACLLog 그루핑**: 60,000ms 이내 동일한 위반은 그룹핑하여 중복 로그 방지  
```c
#define ACL_LOG_GROUPING_MAX_TIME_DELTA 60000
```

### 5-6. ACL 규칙 문법 (`ACLSetUser / ACLSetSelector`)

| 규칙 | 의미 |
|------|------|
| `on` / `off` | 계정 활성/비활성 |
| `nopass` | 비밀번호 없이 허용 |
| `>password` | 비밀번호 추가 |
| `<password` | 비밀번호 제거 |
| `+command` / `-command` | 명령 허용/거부 |
| `+@category` / `-@category` | 카테고리 전체 허용/거부 |
| `~pattern` | 키 패턴 허용 (읽기+쓰기) |
| `%R~pattern` / `%W~pattern` | 키 읽기/쓰기 전용 허용 |
| `&pattern` | Pub/Sub 채널 패턴 허용 |
| `allkeys` / `resetkeys` | 모든 키 허용/모든 키 제한 초기화 |
| `allchannels` / `resetchannels` | 모든 채널 허용/초기화 |
| `allcommands` / `nocommands` | 모든 명령 허용/거부 |
| `reset` | 기본값으로 초기화 |

---

## 6. 런타임 설정 — `config.c`

### 6-1. standardConfig 구조

```c
typedef struct standardConfig {
    const char *name;       // 설정 이름
    const char *alias;      // 별칭 (호환성)
    int flags;              // CONFIG_NOTMODIFIABLE 등
    typeData data;          // 타입별 데이터 (아래 union)
    typeInterface interface; // 타입별 vtable (init/set/get/rewrite/apply)
} standardConfig;

typedef union typeData {
    boolConfigData yesno;    // bool
    stringConfigData string; // char*
    sdsConfigData sds;       // sds
    enumConfigData enumd;    // enum ↔ 문자열 매핑
    numericConfigData numeric; // long long (범위/단위)
} typeData;

typedef struct typeInterface {
    void (*init)(standardConfig *config);
    int  (*set)(standardConfig *config, sds *argv, int argc, const char **err);
    void (*get)(client *c, standardConfig *config);
    void (*rewrite)(standardConfig *config, const char *name, struct rewriteConfigState *state);
    int  (*apply)(const char **err);  // NULL이면 set() 즉시 적용
} typeInterface;
```

### 6-2. CONFIG REWRITE

```c
#define REDIS_CONFIG_REWRITE_SIGNATURE "# Generated by CONFIG REWRITE"

struct rewriteConfigState {
    dict *option_to_line_nums; // 옵션명 → 원본 파일 라인 번호들
    int    numlines;
    sds   *lines;              // 원본 파일 전체 라인 배열
    dict  *rewritten;          // 이미 처리된 옵션들
};
```

**동작**: 기존 설정 파일 파싱 → 변경된 설정만 인플레이스 수정 → atomic overwrite

```
rewriteConfig(path, force_write)
  ├─ rewriteConfigReadOldFile()     원본 파일 파싱
  ├─ 각 standardConfig 별 rewritefn 호출  변경 여부 확인
  ├─ rewriteConfigRemoveOrphaned()  불필요한 라인 제거
  └─ rewriteConfigOverwriteFile()   임시 파일 → rename() 원자적 교체
```

### 6-3. 모듈 설정 통합

```c
int performModuleConfigSetFromName(sds name, sds value, const char **err);
int performModuleConfigSetDefaultFromName(sds name, const char **err);
```

모듈도 표준 CONFIG GET/SET 인터페이스에 통합 가능.

### 6-4. 설정 로딩 흐름

```
loadServerConfig(filename, config_from_stdin, options)
  ├─ 파일 읽기 또는 stdin
  ├─ loadServerConfigFromString()
  │   ├─ 줄 단위 파싱
  │   ├─ include 지시어 재귀 처리
  │   ├─ bind/save/slaveof 등 특수 처리
  │   └─ configSetCommand() 경로로 표준 설정 적용
  └─ 커맨드라인 options 추가 적용 (최고 우선순위)
```

---

## 7. Sentinel — `sentinel.c`

### 7-1. 핵심 자료구조

```
sentinelState (전역 singleton)
  ├─ myid[CONFIG_RUN_ID_SIZE+1]  — 이 Sentinel의 고유 ID
  ├─ current_epoch               — 설정 epoch (단조 증가)
  ├─ dict *masters               — 모니터링 중인 마스터 인스턴스
  ├─ int tilt                    — TILT 모드 여부 (시계 왜곡 감지)
  ├─ list *scripts_queue         — 실행 대기 알림 스크립트
  └─ announce_ip/port            — 다른 Sentinel에 광고할 주소

sentinelRedisInstance (마스터/슬레이브/다른 Sentinel 각각)
  ├─ flags        SRI_MASTER|SRI_SLAVE|SRI_SENTINEL + 상태 플래그
  ├─ instanceLink *link          — hiredis 연결 (Sentinel 간 공유 가능)
  ├─ dict *sentinels             — 같은 마스터 모니터링 중인 다른 Sentinel들
  ├─ dict *slaves                — 이 마스터의 슬레이브들
  ├─ unsigned int quorum         — ODOWN 판정에 필요한 Sentinel 수
  └─ int failover_state          — 7단계 페일오버 상태머신

instanceLink (Sentinel 인스턴스 간 공유)
  ├─ int refcount                — 공유 참조 수
  ├─ redisAsyncContext *cc       — 커맨드 연결 (hiredis async)
  ├─ redisAsyncContext *pc       — Pub/Sub 연결
  └─ act_ping_time / last_pong_time / last_avail_time  — 장애 감지용 타이밍
```

**instanceLink 공유**: N개 Sentinel이 M개 마스터 모니터링 시 N×M 연결 대신  
Sentinel 인스턴스는 연결을 공유 → N개 연결만 유지, ping도 N번만 전송.

### 7-2. SRI 인스턴스 플래그

```c
#define SRI_MASTER              (1<<0)
#define SRI_SLAVE               (1<<1)
#define SRI_SENTINEL            (1<<2)
#define SRI_S_DOWN              (1<<3)   // 주관적 다운 (자신만의 판단)
#define SRI_O_DOWN              (1<<4)   // 객관적 다운 (쿼럼 합의)
#define SRI_MASTER_DOWN         (1<<5)   // 이 Sentinel이 마스터 다운으로 판단
#define SRI_FAILOVER_IN_PROGRESS (1<<6)
#define SRI_PROMOTED            (1<<7)   // 승격 대상 슬레이브로 선택됨
#define SRI_RECONF_SENT         (1<<8)   // SLAVEOF <newmaster> 전송됨
#define SRI_RECONF_INPROG       (1<<9)   // 슬레이브 동기화 진행 중
#define SRI_RECONF_DONE         (1<<10)  // 새 마스터와 동기화 완료
#define SRI_FORCE_FAILOVER      (1<<11)  // 마스터 살아있어도 강제 페일오버
#define SRI_SCRIPT_KILL_SENT    (1<<12)
#define SRI_MASTER_REBOOT       (1<<13)  // 마스터 재부팅 감지됨
```

### 7-3. 장애 감지 — SDOWN → ODOWN

**SDOWN (Subjective Down)**:
```c
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    elapsed = mstime() - ri->link->act_ping_time;  // 마지막 미응답 ping 기준

    // 추가: 연결 반응 없으면 자동 재연결 판단
    // cc 링크: down_after_period/2 초과 → cc 끊기
    // pc 링크: sentinel_publish_period*3 초과 → pc 끊기

    // SDOWN 판정:
    // elapsed > down_after_period (기본 30초)
    // OR: 마스터인데 스스로 슬레이브 보고 && 충분한 시간 경과
    // OR: 재부팅 감지 && master_reboot_down_after_period 경과
    if (조건) flags |= SRI_S_DOWN;
}
```

**ODOWN (Objective Down)**:
```c
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    if (master->flags & SRI_S_DOWN) {
        quorum = 1;  // 자신 포함
        // 다른 Sentinel 중 SRI_MASTER_DOWN 플래그 있는 것 카운트
        if (quorum >= master->quorum)
            master->flags |= SRI_O_DOWN;  // → 페일오버 조건 충족
    }
}
```

### 7-4. 페일오버 상태 머신 (7단계)

```
SENTINEL_FAILOVER_STATE_NONE          (0) — 페일오버 없음
  ↓ sentinelStartFailoverIfNeeded()
SENTINEL_FAILOVER_STATE_WAIT_START    (1) — 리더 선출 대기
  ↓ sentinelVoteLeader() — Raft 스타일
SENTINEL_FAILOVER_STATE_SELECT_SLAVE  (2) — 승격할 슬레이브 선택
  ↓ sentinelSelectSlave()
SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE (3) — 슬레이브 → 마스터 전환
  ↓ SLAVEOF NO ONE 전송
SENTINEL_FAILOVER_STATE_WAIT_PROMOTION (4) — 마스터 역할 확인 대기
  ↓ INFO 응답에서 role:master 확인
SENTINEL_FAILOVER_STATE_RECONF_SLAVES (5) — 나머지 슬레이브 재설정
  ↓ SLAVEOF <newmaster> 전송 (parallel_syncs개씩)
SENTINEL_FAILOVER_STATE_UPDATE_CONFIG  (6) — 설정 업데이트 완료
```

**슬레이브 선택 기준** (`sentinelSelectSlave`):
1. 연결 가능한 슬레이브만
2. 낮은 slave_priority 우선 (0이면 제외)
3. 동일 priority면 높은 replication offset 우선 (더 최신 데이터)
4. 동일 offset이면 run ID lexicographic 비교 (결정론적)

**리더 선출** (`sentinelVoteLeader`):
- Raft 스타일: req_epoch > leader_epoch이면 투표
- 자신의 current_epoch 업데이트 → config epoch 단조 증가 보장

### 7-5. 타이밍 상수

```c
static mstime_t sentinel_info_period         = 10000; // 10초: INFO 갱신 주기
static mstime_t sentinel_ping_period         = 1000;  // 1초: PING 전송
static mstime_t sentinel_ask_period          = 1000;  // 1초: 장애 쿼리
static mstime_t sentinel_publish_period      = 2000;  // 2초: Hello 발행
static mstime_t sentinel_default_down_after  = 30000; // 30초: SDOWN 타임아웃
static mstime_t sentinel_tilt_trigger        = 2000;  // 2초 차이 → TILT 진입
static mstime_t sentinel_tilt_period         = 30000; // 30초: TILT 지속
static mstime_t sentinel_slave_reconf_timeout= 10000; // 10초: 슬레이브 재설정
static mstime_t sentinel_election_timeout    = 10000; // 10초: 선출 타임아웃
static mstime_t sentinel_script_max_runtime  = 60000; // 60초: 스크립트 최대 실행
static mstime_t sentinel_script_retry_delay  = 30000; // 30초: 재시도 간격
static mstime_t sentinel_default_failover_timeout = 180000; // 3분: 페일오버 타임아웃

#define SENTINEL_HELLO_CHANNEL          "__sentinel__:hello"
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MAX_PENDING_COMMANDS   100
#define SENTINEL_SCRIPT_MAX_QUEUE       256
#define SENTINEL_SCRIPT_MAX_RUNNING     16
#define SENTINEL_SCRIPT_MAX_RETRY       10
```

### 7-6. TILT 모드

시계 왜곡 감지 안전장치:
```
previous_time 대비 현재 시간이 2000ms 이상 앞서거나 음수 → TILT 진입
TILT 동안: 장애 감지 로직 수행 안 함 (오판 방지)
30초 후 자동 해제
```

---

## 8. 지연 시간 모니터링 — `latency.c`

### 8-1. 구조

```
latencyTimeSeries
  ├─ time_t  last_event_time   최근 이벤트 시각
  ├─ uint32_t idx              링 버퍼 현재 인덱스
  ├─ struct latencySample samples[LATENCY_TS_LEN]   링 버퍼
  └─ uint32_t max              기록된 최대 지연시간

latencySample
  ├─ uint32_t time    Unix 타임스탬프
  └─ uint32_t latency 밀리초 단위 지연
```

### 8-2. 샘플 추가

```c
void latencyAddSample(const char *event, mstime_t latency) {
    latencyTimeSeries *ts = dictFetchValue(server.latency_events, event);
    // 이벤트 없으면 새로 생성
    // 링 버퍼에 (time, latency) 추가
    // ts->max 갱신
    // 최소 지연시간 기준 미달이면 추가 안 함 (server.latency_monitor_threshold)
}
```

### 8-3. 분석 및 출력

```c
void analyzeLatencyForEvent(char *event, struct latencyStats *ls);
// → min/max/avg/percentiles 계산

// HDR 히스토그램 기반 CDF (Cumulative Distribution Function)
void fillCommandCDF(client *c, struct hdr_histogram* histogram);
void latencyAllCommandsFillCDF(client *c, dict *commands, int *command_with_data);
void latencySpecificCommandsFillCDF(client *c);

#define LATENCY_GRAPH_COLS 80  // ASCII 그래프 너비

// 지속 시간 샘플링 (모노토닉 시계 기반)
void durationAddSample(int type, monotime duration);
```

### 8-4. 모니터링 매크로 패턴

```c
// server.c 등에서 사용 패턴:
latency_tracker_init(latency);
// ... 작업 수행 ...
latency_tracker_stop(latency);
latencyAddSampleIfNeeded("event-name", latency);
```

---

## 9. 주요 상수 요약

| 상수 | 값 | 위치 | 의미 |
|------|----|------|------|
| `SDS_MAX_PREALLOC` | 1MB | sds.h | SDS 선행 할당 최대치 |
| `SDS_TYPE_5/8/16/32/64` | 0~4 | sds.h | SDS 헤더 타입 |
| `INITIAL_EVENT` | 1024 | ae.c | 이벤트 루프 초기 배열 크기 |
| `AE_BARRIER` | 4 | ae.h | 쓰기 이벤트 순서 역전 방지 |
| `BIO_WORKER_NUM` | 3 | bio.c | BIO 워커 스레드 수 |
| `REDIS_THREAD_STACK_SIZE` | 4MB | bio.c | BIO 스레드 스택 크기 |
| `IO_THREADS_MAX_NUM` | 128 | server.h | 최대 IO 스레드 수 |
| `IOTHREAD_MAIN_THREAD_ID` | 0 | server.h | 메인 스레드 ID |
| `ACL_MAX_CATEGORIES` | 64 | acl.c | ACL 카테고리 최대 수 |
| `HASH_PASSWORD_LEN` | 64 | acl.c | SHA256 hex 비밀번호 길이 |
| `ACL_LOG_GROUPING_MAX_TIME_DELTA` | 60000ms | acl.c | ACL 로그 그루핑 시간 창 |
| `SENTINEL_PING_PERIOD` | 1000ms | sentinel.c | Sentinel PING 주기 |
| `SENTINEL_HELLO_CHANNEL` | `__sentinel__:hello` | sentinel.c | Sentinel Pub/Sub 채널 |
| `SENTINEL_SCRIPT_MAX_QUEUE` | 256 | sentinel.c | 알림 스크립트 큐 최대 |
| `SENTINEL_SCRIPT_MAX_RUNNING` | 16 | sentinel.c | 동시 실행 스크립트 최대 |
| `LATENCY_GRAPH_COLS` | 80 | latency.c | 레이턴시 그래프 너비 |

---

## 10. 컴포넌트 상호작용 다이어그램

```
클라이언트 연결
      │
      ▼
  IO Thread (iothread.c)          ← aeEventLoop (ae.c) 로 읽기/쓰기
      │ 커맨드 완성
      ▼
  Main Thread (server.c)
      │ 실행 + AOF 기록
      ├──► BIO Worker 1 (bio_aof)  AOF fsync
      ├──► BIO Worker 0 (bio_close_file)  fd 닫기
      └──► BIO Worker 2 (bio_lazy_free)  메모리 비동기 해제
               │ 완료 시
               └──► pipe → aeEventLoop 깨움 → callback

Sentinel (독립 프로세스)
      │ hiredis async (instanceLink)
      ├── cc (커맨드 연결): PING, INFO, CONFIG
      └── pc (Pub/Sub 연결): __sentinel__:hello
              │
              ▼
      SDOWN → ODOWN (quorum) → sentinelVoteLeader (Raft)
              → sentinelSelectSlave → SLAVEOF NO ONE
              → SLAVEOF newmaster (병렬 슬레이브 재설정)
```

---

## 11. 설계 패턴 & 핵심 인사이트

### SDS
- **음수 인덱스 트릭**: `s[-1]`로 헤더 flags 접근 — C 표준상 합법 (동일 할당 블록 내)
- **타입 자동 업그레이드**: 길이 초과 시 sdshdr8 → sdshdr16 자동 승격 (sdsMakeRoomFor)
- **`SDS_NOINIT`**: 초기화 없이 할당만 (호출자가 직접 채울 때 성능 최적화)

### 이벤트 루프
- **serverCron만 시간 이벤트**: Redis는 시간 이벤트를 사실상 1개만 사용, O(N) 스캔 문제 없음
- **AE_BARRIER**: AOF 안전성을 이벤트 루프 레벨에서 보장하는 엘레강트한 설계
- **beforesleep/aftersleep**: 플러시, 슬레이브 동기화, 클라이언트 처리 등 핵심 작업 배치

### BIO
- **파이프 기반 완료 통지**: IO 다중화와 통합 — 별도 폴링 불필요
- **comp_rq job 타입**: 완료 순서가 보장되므로 여러 job을 묶은 flush 완료 감지에 활용

### IO Thread
- **CACHE_LINE_SIZE 정렬**: false sharing 방지로 락 없는 원자 변수 성능 보장
- **조건부 메인 전용**: Pub/Sub, 블로킹, 트래킹 클라이언트는 메인 전용 — 레이스 컨디션 완전 차단
- **배치 전송**: IO_THREAD_MAX_PENDING_CLIENTS 임계값으로 과도한 컨텍스트 스위치 방지

### Sentinel
- **instanceLink 공유**: N Sentinel × M 마스터에서 O(N×M) → O(N) 연결 절감
- **TILT 모드**: NTP, VM 이주 등으로 시계가 튀는 경우 오판을 원천 차단
- **Raft 유사 선출**: epoch 기반 단조 투표로 split-brain 방지
