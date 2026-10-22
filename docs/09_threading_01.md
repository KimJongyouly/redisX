# Redis 스레딩 모델 상세 분석

## 개요

Redis는 **싱글 스레드 커맨드 실행** 원칙을 유지하면서, 네트워크 I/O와 백그라운드 작업을 별도 스레드로 분리하는 하이브리드 모델을 사용한다.

```
┌─────────────────────────────────────────────────────────┐
│                    Redis 스레드 구조                      │
│                                                          │
│  메인 스레드(1개)         IO 스레드(N개)                   │
│  ─────────────           ────────────                    │
│  커맨드 실행             네트워크 읽기/쓰기               │
│  이벤트 루프             각자의 aeEventLoop               │
│                                                          │
│  BIO 워커(3개)           스레드 매니저(진단용)             │
│  ─────────────           ──────────────────              │
│  파일 close             SIGUSR2 시그널 기반               │
│  AOF fsync              전체 스레드 콜백                  │
│  Lazy free                                               │
└─────────────────────────────────────────────────────────┘
```

---

## 1. 메인 스레드 — 싱글 스레드 커맨드 실행

### 역할

- **모든 Redis 커맨드를 단독 실행** — 데이터 경쟁(Data Race) 없음
- 클라이언트 연결 수락 (`accept()`)
- `server.el` 이벤트 루프 구동 (`aeMain()`)
- IO 스레드로부터 받은 완성된 커맨드 처리
- `serverCron()` 타이머 이벤트 처리

### 메인 루프

```c
void aeMain(aeEventLoop *eventLoop) {
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop,
            AE_ALL_EVENTS | AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
    }
}
```

**beforeSleep**에서 매 루프마다:
1. `processClientsOfAllIOThreads()` — IO 스레드에서 넘어온 클라이언트 처리
2. `sendPendingClientsToIOThreads()` — 처리 완료 클라이언트를 IO 스레드로 반환
3. AOF 버퍼 플러시, 복제 데이터 전송 등

---

## 2. IO 스레드 (`src/iothread.c`)

### 설계 목적

네트워크 읽기·파싱·쓰기를 병렬화하여 메인 스레드의 I/O 부하를 줄인다.  
**메인 스레드는 오직 커맨드 실행만 담당**한다.

### 핵심 상수

```c
#define IO_THREADS_MAX_NUM         128   /* 최대 IO 스레드 수 */
#define IO_THREAD_MAX_PENDING_CLIENTS 16 /* 배치 전송 임계값 */
#define IOTHREAD_MAIN_THREAD_ID    0     /* 메인 스레드 ID */
```

### IOThread 구조체 (`server.h:1626`)

```c
typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
    uint8_t  id;                          /* 스레드 고유 ID (1~N) */
    pthread_t tid;                        /* pthread ID */
    redisAtomic int paused;               /* 일시정지 상태 플래그 */
    redisAtomic int running;              /* 실행 중 여부 */
    aeEventLoop *el;                      /* 이 스레드 전용 이벤트 루프 */
    list *pending_clients;                /* 메인→IO 전달 대기 클라이언트 */
    list *processing_clients;             /* 현재 처리 중인 클라이언트 */
    list *pending_clients_to_main_thread; /* IO→메인 전달 대기 클라이언트 */
    list *clients;                        /* 이 IO 스레드가 관리하는 클라이언트 */
    eventNotifier *pending_clients_notifier; /* 메인→IO 알림 (pipe/eventfd) */
    pthread_mutex_t pending_clients_mutex;
} IOThread;
/* CACHE_LINE_SIZE 정렬: False Sharing 방지 */
```

### IO 스레드 초기화 (`initThreadedIO`)

```c
void initThreadedIO(void) {
    if (server.io_threads_num <= 1) return;  /* 1이면 메인 스레드만 사용 */

    for (int i = 1; i < server.io_threads_num; i++) {
        IOThread *t = &IOThreads[i];
        t->id = i;
        t->el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR);
        /* 메인→IO 알림 이벤트 등록 */
        aeCreateFileEvent(t->el, ..., handleClientsFromMainThread, t);
        /* IO 스레드 타이머 (1초 주기 cron) */
        aeCreateTimeEvent(t->el, 1, IOThreadCron, t, NULL);
        /* IO 스레드 시작 */
        pthread_create(&t->tid, NULL, IOThreadMain, t);

        /* 메인 스레드 측 IO→메인 알림 이벤트 등록 */
        aeCreateFileEvent(server.el, ..., handleClientsFromIOThread, t);
    }
}
```

### IO 스레드 메인 함수

```c
void *IOThreadMain(void *ptr) {
    IOThread *t = ptr;
    snprintf(thdname, 16, "io_thd_%d", t->id);
    redis_set_thread_title(thdname);
    redisSetCpuAffinity(server.server_cpulist);
    aeSetBeforeSleepProc(t->el, IOThreadBeforeSleep);
    aeSetAfterSleepProc(t->el, IOThreadAfterSleep);
    aeMain(t->el);  /* 자체 이벤트 루프 구동 */
}
```

### 클라이언트 ↔ 스레드 이동 흐름

```
새 연결 수락 (메인 스레드)
        │
        ▼
assignClientToIOThread()  ← 클라이언트 수가 가장 적은 IO 스레드 선택
        │
        ▼
IO 스레드: 읽기·파싱 (readQueryFromClient)
        │  완성된 커맨드 감지
        ▼
enqueuePendingClientsToMainThread()  ← IO→메인 대기 큐에 추가
        │
        ▼
메인 스레드: processClientsFromIOThread()
        │  커맨드 실행 (call())
        ▼
enqueuePendingClienstToIOThreads()   ← 메인→IO 대기 큐에 추가
        │
        ▼
IO 스레드: 응답 쓰기 (writeToClient / sendReplyToClient)
        │
        └── 다음 요청 대기
```

### 클라이언트 부하 분산

```c
void assignClientToIOThread(client *c) {
    /* 클라이언트 수 가장 적은 스레드 선택 */
    int min_id = 0, min = INT_MAX;
    for (int i = 1; i < server.io_threads_num; i++) {
        if (server.io_threads_clients_num[i] < min) {
            min = server.io_threads_clients_num[i];
            min_id = i;
        }
    }
    c->tid = min_id;
    server.io_threads_clients_num[min_id]++;
}
```

### 메인 스레드에서만 처리해야 하는 클라이언트

```c
int isClientMustHandledByMainThread(client *c) {
    /* 다음 플래그를 가진 클라이언트는 메인 스레드 전담 */
    if (c->flags & (CLIENT_CLOSE_ASAP |
                    CLIENT_PUBSUB | CLIENT_MONITOR |
                    CLIENT_BLOCKED | CLIENT_UNBLOCKED |
                    CLIENT_TRACKING | CLIENT_LUA_DEBUG |
                    CLIENT_ASM_MIGRATING | CLIENT_ASM_IMPORTING)) {
        return 1;
    }
    /* Master/Slave는 RDB 복제 완료 후 IO 스레드 이동 가능 */
    ...
}
```

---

## 3. IO 스레드 일시정지/재개 메커니즘

메인 스레드가 IO 스레드의 클라이언트 데이터에 접근할 때 안전한 컨텍스트를 확보하기 위해 **Busy-wait 기반 일시정지** 사용.

### 상태 전이

```
IO_THREAD_UNPAUSED → IO_THREAD_PAUSING → IO_THREAD_PAUSED
        ↑                                        │
        └────── IO_THREAD_RESUMING ←─────────────┘
```

```c
/* 메인 스레드: 일시정지 요청 */
void pauseIOThread(int id) {
    atomicSetWithSync(IOThreads[id].paused, IO_THREAD_PAUSING);
    triggerEventNotifier(IOThreads[id].pending_clients_notifier);
    /* Busy-wait: IO 스레드가 PAUSED 상태가 될 때까지 대기 */
    while (paused != IO_THREAD_PAUSED) { atomicGetWithSync(...); }
}

/* IO 스레드: IOThreadBeforeSleep에서 확인 */
void handlePauseAndResume(IOThread *t) {
    if (paused == IO_THREAD_PAUSING) {
        atomicSetWithSync(t->paused, IO_THREAD_PAUSED);
        /* Busy-wait: RESUMING 신호 대기 */
        while (paused != IO_THREAD_RESUMING) { atomicGetWithSync(...); }
        atomicSetWithSync(t->paused, IO_THREAD_UNPAUSED);
    }
}
```

**사용 시나리오:**
- `unbindClientFromIOThreadEventLoop()` — fd를 이벤트 루프에서 분리
- `resizeAllIOThreadsEventLoops()` — maxclients 변경 시 이벤트 루프 리사이즈
- `fetchClientFromIOThread()` — 특정 클라이언트를 강제로 메인 스레드로 이관

### 중첩 일시정지 지원

```c
static int PausedIOThreads[IO_THREADS_MAX_NUM] = {0};
/* 재진입 허용: 참조 카운팅으로 관리 */
/* PausedIOThreads[i]++ 로 중첩 가능, 0이 될 때만 실제 재개 */
```

---

## 4. IO 스레드 beforeSleep / afterSleep

```c
void IOThreadBeforeSleep(aeEventLoop *el) {
    IOThread *t = el->privdata[0];

    /* TLS 같은 pending 데이터 처리 */
    connTypeProcessPendingData(el);

    /* 메인 스레드에서 배달된 클라이언트 처리 */
    if (processClientsFromMainThread(t) > 0)
        dont_sleep = 1;  /* 즉시 다음 루프 */

    if (!dont_sleep) {
        atomicSetWithSync(t->running, 0);  /* sleeping 상태 표시 */
        processClientsFromMainThread(t);   /* 레이스 방지 재확인 */
    }
    aeSetDontWait(t->el, dont_sleep);

    /* 일시정지 요청 처리 */
    handlePauseAndResume(t);

    /* 처리 완료 클라이언트를 메인 스레드로 전송 */
    sendPendingClientsToMainThreadIfNeeded(t, 0);
}

void IOThreadAfterSleep(aeEventLoop *el) {
    /* 슬립에서 깨어나면 running=1 → 메인이 별도 알림 없이 직접 전송 가능 */
    atomicSetWithSync(t->running, 1);
}
```

### 불필요한 알림 최소화 최적화

```c
/* running=1이면 메인이 알림 없이 직접 클라이언트 전송 가능 */
/* pending > 0이면 이미 알림 발송됨 → 중복 알림 생략 */
if (!running && !pending) {
    triggerEventNotifier(t->pending_clients_notifier);
}
```

### 명령 프리패치 (`prefetchIOThreadCommands`)

```c
/* Two-phase prefetch for cache optimization */
/* Phase 1: client 구조체와 pending_cmds에 prefetch 힌트 발행 */
redis_prefetch_read(c[i]);
redis_prefetch_read(&c[i]->pending_cmds);

/* Phase 2: 캐시에 올라온 데이터를 실제로 읽어 배치에 추가 */
addCommandToBatch(c[i]);
prefetchCommands();  /* 배치 전체 프리패치 */
```

---

## 5. BIO 스레드 (`src/bio.c`)

### 역할

메인 스레드를 블로킹시킬 수 있는 **느린 I/O 작업**을 백그라운드로 위임.

```c
static char *bio_worker_title[] = {
    "bio_close_file",  /* Worker 0 */
    "bio_aof",         /* Worker 1 */
    "bio_lazy_free",   /* Worker 2 */
};
#define BIO_WORKER_NUM 3
```

### 7가지 Job 타입과 워커 매핑

```c
static unsigned int bio_job_to_worker[] = {
    [BIO_CLOSE_FILE]         = 0,  /* close(fd) — 대용량 파일 삭제 */
    [BIO_AOF_FSYNC]          = 1,  /* fsync(aof_fd) */
    [BIO_CLOSE_AOF]          = 1,  /* fsync + close(aof_fd) */
    [BIO_LAZY_FREE]          = 2,  /* 대용량 자료구조 비동기 해제 */
    [BIO_COMP_RQ_CLOSE_FILE] = 0,  /* 완료 알림 (파일 close 워커) */
    [BIO_COMP_RQ_AOF_FSYNC]  = 1,  /* 완료 알림 (AOF 워커) */
    [BIO_COMP_RQ_LAZY_FREE]  = 2,  /* 완료 알림 (lazy free 워커) */
};
```

### Job 구조체

```c
typedef union bio_job {
    struct { int type; } header;            /* 공통 타입 태그 */

    struct {
        int type;
        int fd;
        long long offset;
        unsigned need_fsync:1;
        unsigned need_reclaim_cache:1;
    } fd_args;                              /* CLOSE_FILE / AOF_FSYNC */

    struct {
        int type;
        lazy_free_fn *free_fn;
        void *free_args[];                  /* 가변 인수 */
    } free_args;                            /* LAZY_FREE */

    struct {
        int type;
        comp_fn *fn;                        /* 완료 콜백 */
        uint64_t arg;
        void *ptr;
    } comp_rq;                              /* COMP_RQ_* */
} bio_job;
```

### 워커 스레드 루프

```c
void *bioProcessBackgroundJobs(void *arg) {
    unsigned long worker = (unsigned long)arg;
    redis_set_thread_title(bio_worker_title[worker]);
    redisSetCpuAffinity(server.bio_cpulist);  /* CPU 친화성 설정 */

    /* SIGALRM 차단: watchdog 시그널은 메인 스레드만 수신 */
    sigaddset(&sigset, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &sigset, NULL);

    pthread_mutex_lock(&bio_mutex[worker]);
    while (1) {
        if (listLength(bio_jobs[worker]) == 0) {
            pthread_cond_wait(&bio_newjob_cond[worker], &bio_mutex[worker]);
            continue;
        }
        /* 잡 꺼내기 */
        job = listFirst(bio_jobs[worker])->value;
        pthread_mutex_unlock(&bio_mutex[worker]);

        /* 타입별 처리 */
        if (job_type == BIO_CLOSE_FILE) { ... close(fd); }
        else if (job_type == BIO_AOF_FSYNC) { redis_fsync(fd); }
        else if (job_type == BIO_LAZY_FREE) { job->free_args.free_fn(args); }
        else if (job_type == BIO_COMP_RQ_*) {
            /* 완료 응답을 bio_comp_list에 추가 + pipe에 쓰기 */
            write(job_comp_pipe[1], "A", 1);  /* 메인 스레드 깨우기 */
        }
        pthread_mutex_lock(&bio_mutex[worker]);
    }
}
```

### 완료 알림 파이프

```c
/* BIO → 메인 스레드 완료 알림 경로 */
static int job_comp_pipe[2];   /* 비차단 파이프 */

/* BIO 워커: 완료 시 파이프에 1바이트 쓰기 */
write(job_comp_pipe[1], "A", 1);

/* 메인 스레드: 이벤트 루프에서 readable 이벤트 수신 */
aeCreateFileEvent(server.el, job_comp_pipe[0], AE_READABLE,
                  bioPipeReadJobCompList, NULL);

/* 콜백 실행 */
void bioPipeReadJobCompList(...) {
    /* bio_comp_list에서 완료 항목 가져오기 */
    rsp->func(rsp->arg, rsp->ptr);  /* 콜백 호출 */
}
```

### Job 제출 API

```c
/* 파일 닫기 */
void bioCreateCloseJob(int fd, int need_fsync, int need_reclaim_cache);

/* AOF fsync */
void bioCreateFsyncJob(int fd, long long offset, int need_reclaim_cache);

/* Lazy free (UNLINK, FLUSHDB ASYNC 등) */
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...);

/* 완료 알림 요청 */
void bioCreateCompRq(bio_worker_t worker, comp_fn *func, uint64_t arg, void *ptr);
```

### 스택 크기

```c
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)  /* 4MB */
/* 기본 스택이 작은 시스템(Solaris 등)을 위해 명시적으로 설정 */
while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
```

---

## 6. 스레드 매니저 (`src/threads_mngr.c`)

### 역할

Linux 전용. **전체 스레드에 동시에 콜백을 실행**시키는 매커니즘. 주로 크래시 시 메모리 덤프와 진단 정보 수집에 사용.

### 구현 방식

```c
/* SIGUSR2 시그널 핸들러 등록 */
void ThreadsManager_init(void) {
    sigaction(SIGUSR2, &act, NULL);  /* invoke_callback을 시그널 핸들러로 */
}

/* 전체 스레드 콜백 실행 */
int ThreadsManager_runOnThreads(pid_t *tids, size_t tids_len,
                                 run_on_thread_cb callback) {
    atomicSet(g_callback, callback);
    /* 각 스레드에 SIGUSR2 시그널 전송 (Linux tgkill) */
    for (size_t i = 0; i < tids_len; i++) {
        syscall(SYS_tgkill, pid, tids[i], THREADS_SIGNAL);
    }
    /* 전체 완료 대기 (최대 2초 타임아웃) */
    wait_threads();
}
```

**특징:**
- `g_in_progress` 원자 플래그로 중복 실행 방지
- 타임아웃: `RUN_ON_THREADS_TIMEOUT = 2`초
- Linux 이외 플랫폼에서는 no-op

---

## 7. 원자 변수 시스템 (`src/atomicvar.h`)

### 추상화 레이어

```c
/* 3가지 구현을 조건부 컴파일로 선택 */

/* 1. C11 _Atomic (권장) */
#define redisAtomic _Atomic
#define atomicGet(var, dst)  atomic_load_explicit(&var, memory_order_relaxed)
#define atomicSet(var, val)  atomic_store_explicit(&var, val, memory_order_relaxed)
#define atomicGetWithSync(var, dst) atomic_load_explicit(&var, memory_order_seq_cst)
#define atomicSetWithSync(var, val) atomic_store_explicit(&var, val, memory_order_seq_cst)

/* 2. GCC __atomic builtins */
#define atomicGet(var, dst)  __atomic_load_n(&var, __ATOMIC_RELAXED)
#define atomicGetWithSync(var, dst) __atomic_load_n(&var, __ATOMIC_SEQ_CST)

/* 3. GCC __sync (레거시 fallback) */
#define atomicIncr(var, count) __sync_add_and_fetch(&var, count)
```

### memory_order 전략

| 함수 | Memory Order | 사용 시나리오 |
|------|-------------|-------------|
| `atomicGet/Set` | `relaxed` | 단순 카운터, 통계 | 
| `atomicGetWithSync/SetWithSync` | `seq_cst` | 스레드 간 동기화 신호 (paused, running) |
| `atomicIncr` | `relaxed` | 통계 누적 |
| `atomicFlagGetSet` | `seq_cst` | mutex 대체 플래그 |

### False Sharing 방지

```c
/* zmalloc.c: 스레드별 메모리 통계 */
typedef struct used_memory_entry {
    redisAtomic long long used_memory;
    redisAtomic long long last_peak_check;
    char padding[CACHE_LINE_SIZE - sizeof(long long) - sizeof(long long)];
} used_memory_entry;

static __attribute__((aligned(CACHE_LINE_SIZE)))
    used_memory_entry used_memory[MAX_THREADS];
/* 각 스레드 항목이 별도 캐시 라인 점유 → 불필요한 캐시 무효화 방지 */

/* IOThread 구조체도 CACHE_LINE_SIZE 정렬 */
typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) { ... } IOThread;
```

---

## 8. 스레드별 역할 요약

| 스레드 | 이름 | 개수 | 주요 역할 |
|--------|------|------|---------|
| 메인 스레드 | `redis-server` | 1 | 커맨드 실행, 이벤트 루프, 연결 수락 |
| IO 스레드 | `io_thd_N` | 0~127 | 네트워크 읽기/파싱/쓰기 |
| BIO 워커 0 | `bio_close_file` | 1 | close(fd), page cache reclaim |
| BIO 워커 1 | `bio_aof` | 1 | fsync(aof_fd) |
| BIO 워커 2 | `bio_lazy_free` | 1 | 대용량 메모리 비동기 해제 |

---

## 9. 스레드 간 통신 채널

```
메인 스레드 ←→ IO 스레드
─────────────────────────
방향        채널              설명
메인→IO:    eventNotifier     클라이언트 전달 알림 (pipe/eventfd)
IO→메인:    eventNotifier     커맨드 완료 알림
공유:       pthread_mutex     클라이언트 큐 접근 보호
동기화:     atomicVar(paused) 일시정지/재개 핸드셰이크

메인 스레드 ←→ BIO 워커
─────────────────────────
메인→BIO:   pthread_mutex + pthread_cond  잡 큐에 추가 + 시그널
BIO→메인:   pipe(job_comp_pipe)           완료 알림 (이벤트 루프 활성화)
```

---

## 10. 설정 파라미터

```
io-threads 1           # IO 스레드 수 (1=비활성, 2~128=활성)
io-threads-do-reads yes # IO 스레드가 읽기도 처리할지 여부
                        # (no면 쓰기만 IO 스레드 처리)
bio-cpulist ""          # BIO 워커의 CPU 친화성
server-cpulist ""       # IO 스레드의 CPU 친화성
```

### io-threads 활성화 조건

```
io-threads=1      → 메인 스레드만 (기본)
io-threads=N(>1)  → N-1개 IO 스레드 생성
                     (인덱스 1~N-1, 0은 메인 스레드)
```

---

## 11. 동시성 안전 보장 요약

| 메커니즘 | 적용 대상 | 방식 |
|---------|---------|------|
| 단일 커맨드 실행 | 메인 스레드 독점 실행 | 설계적 격리 |
| pause/resume | 메인이 IO 스레드 데이터 접근 | Busy-wait + atomic |
| mutex + cond | BIO 잡 큐 | POSIX 동기화 |
| CACHE_LINE 정렬 | per-thread 카운터 | False Sharing 방지 |
| memory_order_relaxed | 통계 카운터 | 성능 우선 |
| memory_order_seq_cst | 스레드 상태 신호 | 정확성 우선 |
| CLIENT 플래그 검사 | 특수 클라이언트 격리 | 메인 스레드 전담 |
```
