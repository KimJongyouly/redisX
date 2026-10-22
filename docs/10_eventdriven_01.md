# Redis 이벤트드리븐 아키텍처 상세 분석

## 개요

Redis의 이벤트 루프는 `src/ae.c`에 구현된 **단일 파일 경량 이벤트 라이브러리**로,  
원래 Tcl 인터프리터(Jim)의 이벤트 루프에서 파생된 코드다.  
플랫폼별 I/O 멀티플렉서를 추상화하고, 파일 이벤트·타이머 이벤트·슬립 콜백을 통합 관리한다.

---

## 1. 핵심 자료구조 (`src/ae.h`)

### aeEventLoop — 이벤트 루프 상태

```c
typedef struct aeEventLoop {
    int maxfd;              /* 현재 등록된 최대 fd 번호 */
    int setsize;            /* 추적 가능한 최대 fd 수 (상한) */
    long long timeEventNextId; /* 다음 타이머 이벤트 ID */
    int nevents;            /* events/fired 배열의 현재 할당 크기 */
    aeFileEvent  *events;   /* fd → 핸들러 매핑 배열 */
    aeFiredEvent *fired;    /* aeApiPoll 결과 임시 저장 배열 */
    aeTimeEvent  *timeEventHead; /* 타이머 이벤트 연결 리스트 (정렬 없음) */
    int stop;               /* aeMain 종료 플래그 */
    void *apidata;          /* 백엔드별 상태 (epoll/kqueue 등) */
    aeBeforeSleepProc *beforesleep; /* poll 진입 전 콜백 */
    aeBeforeSleepProc *aftersleep;  /* poll 반환 후 콜백 */
    int flags;              /* AE_DONT_WAIT 등 */
    void *privdata[2];      /* 사용자 데이터 (IO 스레드에서 IOThread* 저장) */
} aeEventLoop;
```

### aeFileEvent — fd별 이벤트 핸들러

```c
typedef struct aeFileEvent {
    int mask;               /* AE_READABLE | AE_WRITABLE | AE_BARRIER */
    aeFileProc *rfileProc;  /* 읽기 핸들러 */
    aeFileProc *wfileProc;  /* 쓰기 핸들러 */
    void *clientData;       /* 핸들러에 전달할 사용자 데이터 */
} aeFileEvent;
```

### aeTimeEvent — 타이머 이벤트

```c
typedef struct aeTimeEvent {
    long long id;           /* 이벤트 식별자 */
    monotime when;          /* 발화 시각 (마이크로초 단위) */
    aeTimeProc *timeProc;   /* 타이머 핸들러 */
    aeEventFinalizerProc *finalizerProc; /* 이벤트 삭제 시 정리 콜백 */
    void *clientData;
    struct aeTimeEvent *prev, *next;  /* 이중 연결 리스트 */
    int refcount;           /* 재귀 호출 중 삭제 방지 */
} aeTimeEvent;
```

### aeFiredEvent — poll 결과

```c
typedef struct aeFiredEvent {
    int fd;
    int mask;   /* AE_READABLE | AE_WRITABLE */
} aeFiredEvent;
```

### 이벤트 마스크 상수

```c
#define AE_NONE      0  /* 등록된 이벤트 없음 */
#define AE_READABLE  1  /* fd 읽기 가능 */
#define AE_WRITABLE  2  /* fd 쓰기 가능 */
#define AE_BARRIER   4  /* WRITABLE을 READABLE 이후에 발화 금지
                          (fsync-before-reply 패턴에 사용) */
```

---

## 2. 플랫폼별 I/O 멀티플렉서 백엔드

컴파일 타임에 성능 순서대로 선택:

```c
/* ae.c — 백엔드 선택 (내림차순 우선순위) */
#ifdef HAVE_EVPORT
    #include "ae_evport.c"   /* Solaris (최고 성능) */
#elif HAVE_EPOLL
    #include "ae_epoll.c"    /* Linux */
#elif HAVE_KQUEUE
    #include "ae_kqueue.c"   /* macOS / FreeBSD */
#else
    #include "ae_select.c"   /* 범용 fallback */
#endif
```

### 공통 백엔드 인터페이스 (6개 함수)

모든 백엔드가 동일한 정적 함수를 구현:

| 함수 | 역할 |
|------|------|
| `aeApiCreate(el)` | 백엔드 초기화 (epollfd/kqfd 생성) |
| `aeApiResize(el, size)` | 이벤트 배열 재할당 |
| `aeApiFree(el)` | 백엔드 자원 해제 |
| `aeApiAddEvent(el, fd, mask)` | fd + 마스크 등록/수정 |
| `aeApiDelEvent(el, fd, mask)` | fd + 마스크 삭제 |
| `aeApiPoll(el, tvp)` | I/O 대기 → fired[] 채우기 |
| `aeApiName()` | 백엔드 이름 문자열 반환 |

---

## 3. 백엔드별 구현 상세

### epoll (`ae_epoll.c` — Linux)

```c
typedef struct aeApiState {
    int epfd;                    /* epoll 파일 디스크립터 */
    struct epoll_event *events;  /* 결과 배열 */
} aeApiState;

/* 등록/수정: 기존 mask 유무로 ADD/MOD 자동 선택 */
int op = eventLoop->events[fd].mask == AE_NONE ?
        EPOLL_CTL_ADD : EPOLL_CTL_MOD;
mask |= eventLoop->events[fd].mask;  /* 기존 마스크와 병합 */
epoll_ctl(state->epfd, op, fd, &ee);

/* 대기 */
retval = epoll_wait(state->epfd, state->events, eventLoop->setsize,
                    timeout_ms);  /* timeout=-1이면 무한 대기 */

/* 에러 매핑 */
if (e->events & EPOLLERR) mask |= AE_WRITABLE | AE_READABLE;
if (e->events & EPOLLHUP) mask |= AE_WRITABLE | AE_READABLE;
```

### kqueue (`ae_kqueue.c` — macOS/FreeBSD)

```c
typedef struct aeApiState {
    int kqfd;
    struct kevent *events;
    char *eventsMask;   /* fd별 2비트 마스크 (4개 fd를 1바이트에 압축) */
} aeApiState;

/* 메모리 절약: fd당 2비트 */
#define EVENT_MASK_MALLOC_SIZE(sz) (((sz) + 3) / 4)  /* ceil(sz/4) bytes */
#define EVENT_MASK_OFFSET(fd) ((fd) % 4 * 2)

/* kqueue 특이점: 읽기/쓰기가 별도 이벤트로 분리됨 */
/* → eventsMask에 누적 후 합병하여 fired[]에 기록 */
for (j = 0; j < retval; j++) {
    int fd = state->events[j].ident;
    addEventMask(state->eventsMask, fd, mask);  /* 1단계: 누적 */
}
for (j = 0; j < retval; j++) {
    int mask = getEventMask(state->eventsMask, fd);
    if (mask) {
        eventLoop->fired[numevents++] = {fd, mask};  /* 2단계: 합병 */
        resetEventMask(state->eventsMask, fd);
    }
}
```

### select (`ae_select.c` — 범용 fallback)

```c
typedef struct aeApiState {
    fd_set rfds, wfds;    /* 등록된 fd 집합 */
    fd_set _rfds, _wfds;  /* select() 호출 시 복사본 (select가 덮어씀) */
} aeApiState;

/* 한계: FD_SETSIZE(보통 1024) 초과 불가 */
if (setsize >= FD_SETSIZE) return -1;

/* O(maxfd) 스캔 필요 */
for (j = 0; j <= eventLoop->maxfd; j++) {
    if (FD_ISSET(j, &state->_rfds)) mask |= AE_READABLE;
    if (FD_ISSET(j, &state->_wfds)) mask |= AE_WRITABLE;
}
```

---

## 4. 이벤트 루프 생성과 동적 배열

```c
#define INITIAL_EVENT 1024  /* 초기 배열 크기 */

aeEventLoop *aeCreateEventLoop(int setsize) {
    eventLoop->nevents  = MIN(setsize, INITIAL_EVENT);  /* 초기 1024 */
    eventLoop->setsize  = setsize;                       /* 상한 */
    eventLoop->events   = zmalloc(sizeof(aeFileEvent)  * nevents);
    eventLoop->fired    = zmalloc(sizeof(aeFiredEvent) * nevents);
    /* 모든 슬롯을 AE_NONE으로 초기화 */
    for (i = 0; i < nevents; i++) events[i].mask = AE_NONE;
    aeApiCreate(eventLoop);
    return eventLoop;
}
```

### fd 등록 시 동적 확장

```c
int aeCreateFileEvent(aeEventLoop *el, int fd, int mask, ...) {
    if (fd >= el->setsize) { errno = ERANGE; return AE_ERR; }

    /* fd > nevents이면 배열 2배 확장 (setsize 상한 내) */
    if (unlikely(fd >= el->nevents)) {
        int newnevents = MAX(el->nevents * 2, fd + 1);
        newnevents = MIN(newnevents, el->setsize);
        el->events = zrealloc(el->events, sizeof(aeFileEvent) * newnevents);
        el->fired  = zrealloc(el->fired,  sizeof(aeFiredEvent) * newnevents);
        /* 새 슬롯 초기화 */
        for (i = el->nevents; i < newnevents; i++)
            el->events[i].mask = AE_NONE;
        el->nevents = newnevents;
    }
    ...
}
```

---

## 5. 파일 이벤트 처리 순서와 AE_BARRIER

### 기본 순서 (readable → writable)

```c
/* 쿼리 읽기 후 즉시 응답 — 파이프라인 효율 최대화 */
if (!invert && fe->mask & mask & AE_READABLE) {
    fe->rfileProc(eventLoop, fd, fe->clientData, mask);
    fired++;
}
if (fe->mask & mask & AE_WRITABLE) {
    fe->wfileProc(eventLoop, fd, fe->clientData, mask);
    fired++;
}
```

### AE_BARRIER: 순서 역전 (writable → readable)

```c
/* AOF fsync=always 시: 응답 전에 반드시 디스크 flush */
int invert = fe->mask & AE_BARRIER;

if (invert) {
    /* 쓰기(응답 전송)를 먼저 처리하지 않고, readable 이후로 미룸 */
    /* writable → readable 순서로 실행 */
}
```

**사용 시나리오**: `appendfsync always` 설정 시, `beforeSleep`에서 AOF fsync 완료 후 클라이언트에 응답하기 위해 `connSetWriteHandlerWithBarrier()` 호출.

---

## 6. 타이머 이벤트 시스템

### 등록과 삭제

```c
/* 등록: head에 삽입 (O(1)) */
long long aeCreateTimeEvent(aeEventLoop *el, long long milliseconds, ...) {
    te->when = getMonotonicUs() + milliseconds * 1000;
    te->next = el->timeEventHead;
    el->timeEventHead = te;
    return id;
}

/* 삭제: AE_DELETED_EVENT_ID 마킹 (지연 삭제) */
int aeDeleteTimeEvent(aeEventLoop *el, long long id) {
    te->id = AE_DELETED_EVENT_ID;  /* 다음 루프에서 실제 제거 */
}
```

### 다음 발화까지 대기 시간 계산 (O(N))

```c
/* 타이머는 정렬되지 않으므로 전체 탐색 필요 */
static int64_t usUntilEarliestTimer(aeEventLoop *el) {
    aeTimeEvent *earliest = NULL;
    for (te = el->timeEventHead; te; te = te->next) {
        if (!earliest || te->when < earliest->when)
            earliest = te;
    }
    return (now >= earliest->when) ? 0 : earliest->when - now;
}
```

### 타이머 처리 로직

```c
static int processTimeEvents(aeEventLoop *el) {
    long long maxId = el->timeEventNextId - 1;  /* 이번 루프에서 생성된 이벤트 제외 */

    for (te = el->timeEventHead; te; te = te->next) {
        /* 지연 삭제된 이벤트 처리 */
        if (te->id == AE_DELETED_EVENT_ID) {
            if (te->refcount == 0) { /* 재귀 호출 중 아닐 때만 해제 */
                te->finalizerProc(...);
                zfree(te);
            }
            continue;
        }
        if (te->id > maxId) continue;  /* 이번 루프에서 새로 생성된 이벤트 건너뜀 */

        if (te->when <= now) {
            te->refcount++;
            int retval = te->timeProc(el, te->id, te->clientData);
            te->refcount--;
            if (retval == AE_NOMORE)
                te->id = AE_DELETED_EVENT_ID;  /* 반환값 -1: 이벤트 삭제 */
            else
                te->when = now + (monotime)retval * 1000;  /* 양수: 재스케줄 */
        }
    }
}
```

---

## 7. aeProcessEvents — 메인 디스패치 루프

```c
int aeProcessEvents(aeEventLoop *el, int flags) {
    /* 1. beforesleep 콜백 */
    if (el->beforesleep && (flags & AE_CALL_BEFORE_SLEEP))
        el->beforesleep(el);

    /* 2. 대기 시간 결정 */
    if (flags & AE_DONT_WAIT || el->flags & AE_DONT_WAIT) {
        tvp = &tv;  /* tv_sec = tv_usec = 0: 즉시 반환 */
    } else if (flags & AE_TIME_EVENTS) {
        usUntilTimer = usUntilEarliestTimer(el);
        /* 다음 타이머까지 대기, 없으면 NULL(무한 대기) */
    }

    /* 3. I/O 멀티플렉서 호출 (여기서 실제로 슬립) */
    numevents = aeApiPoll(el, tvp);

    /* 4. aftersleep 콜백 */
    if (el->aftersleep && (flags & AE_CALL_AFTER_SLEEP))
        el->aftersleep(el);

    /* 5. 발화된 파일 이벤트 처리 */
    for (j = 0; j < numevents; j++) {
        int fd   = el->fired[j].fd;
        int mask = el->fired[j].mask;
        aeFileEvent *fe = &el->events[fd];

        int invert = fe->mask & AE_BARRIER;

        /* readable 처리 (AE_BARRIER 없을 때) */
        if (!invert && fe->mask & mask & AE_READABLE) {
            fe->rfileProc(el, fd, fe->clientData, mask);
            fe = &el->events[fd];  /* resize 후 포인터 갱신 */
        }
        /* writable 처리 */
        if (fe->mask & mask & AE_WRITABLE) {
            fe->wfileProc(el, fd, fe->clientData, mask);
        }
        /* readable 처리 (AE_BARRIER 있을 때 — 역전) */
        if (invert && fe->mask & mask & AE_READABLE) {
            fe->rfileProc(el, fd, fe->clientData, mask);
        }
    }

    /* 6. 타이머 이벤트 처리 */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(el);

    return processed;
}
```

---

## 8. 메인 루프 (`aeMain`)

```c
void aeMain(aeEventLoop *el) {
    el->stop = 0;
    while (!el->stop) {
        aeProcessEvents(el,
            AE_ALL_EVENTS |         /* 파일 + 타이머 이벤트 모두 */
            AE_CALL_BEFORE_SLEEP |  /* beforesleep 콜백 활성화 */
            AE_CALL_AFTER_SLEEP);   /* aftersleep 콜백 활성화 */
    }
}
```

---

## 9. beforeSleep — poll 진입 전 처리 (`server.c:1884`)

매 이벤트 루프 반복에서 `aeApiPoll` 호출 **직전**에 실행되는 집약적 작업 목록:

```
① 피크 메모리 업데이트 (updatePeakMemory)
② TLS pending 데이터 처리 (connTypeProcessPendingData)
③ 클러스터 상태 업데이트 (clusterBeforeSleep)
④ 블로킹 클라이언트 처리 (blockedBeforeSleep)
⑤ 빠른 만료 사이클 (activeExpireCycle FAST)
⑥ 모듈 BEFORE_SLEEP 이벤트 발생
⑦ 슬레이브에 ACK 요청 전송 (sendGetackToReplicas)
⑧ 클라이언트 사이드 캐싱 BCAST 무효화 (trackingBroadcastInvalidationMessages)
⑨ AOF 버퍼 플러시 (flushAppendOnlyFile)
⑩ 클라이언트 쓰기 처리 (handleClientsWithPendingWrites)
⑪ IO 스레드 클라이언트 처리 (processClientsOfAllIOThreads)
⑫ IO 스레드에 클라이언트 전송 (sendPendingClientsToIOThreads)
⑬ 비동기 해제 큐 처리 (freeClientsInAsyncFreeQueue)
⑭ 복제 전파 처리 (replicationCron 관련)
```

### ProcessingEventsWhileBlocked 모드

스크립트/모듈이 블로킹 중에 이벤트를 처리할 때 호출 경로:

```c
if (ProcessingEventsWhileBlocked) {
    /* 핵심 작업만 수행 (최소 집합) */
    connTypeProcessPendingData(server.el);
    flushAppendOnlyFile(0);
    handleClientsWithPendingWrites();
    freeClientsInAsyncFreeQueue();
    processClientsOfAllIOThreads();
    sendPendingClientsToIOThreads();
    server.events_processed_while_blocked += processed;
    return;  /* 일찍 반환 */
}
```

---

## 10. afterSleep — poll 반환 후 처리 (`server.c:2094`)

`aeApiPoll` 반환 직후, 이벤트 핸들러 실행 전 실행:

```c
void afterSleep(aeEventLoop *el) {
    if (!ProcessingEventsWhileBlocked) {
        /* 모듈 GIL 획득 (모듈 스레드가 데이터 접근 못하도록 잠금) */
        moduleAcquireGIL();
        moduleFireServerEvent(REDISMODULE_EVENT_EVENTLOOP,
                              REDISMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP, NULL);
        /* 이벤트 루프 시작 시각 기록 */
        server.el_start = getMonotonicUs();
    }
    /* IO 스레드에 running=1 신호: 메인 스레드가 활성 상태임을 알림 */
    if (server.io_threads_num > 1)
        atomicSetWithSync(server.running, 1);
    /* 캐시된 시간 갱신 */
    updateCachedTime(1);
    server.cmd_time_snapshot = server.mstime;
}
```

---

## 11. serverCron — 주기적 타이머 (`server.c:1506`)

`aeCreateTimeEvent(server.el, 1, serverCron, ...)` 로 등록.  
반환값 = 다음 발화까지 ms → 자동 재스케줄.

```c
int serverCron(aeEventLoop *el, long long id, void *clientData) {
    /* 동적 hz 조정: 클라이언트 수에 따라 1~500 Hz */
    if (server.dynamic_hz) {
        while (clients_per_hz > MAX_CLIENTS_PER_CLOCK_TICK)
            server.hz = MIN(server.hz * 2, CONFIG_MAX_HZ);
    }
    return 1000 / server.hz;  /* 다음 호출까지 ms */
}
```

### `run_with_period(ms)` 매크로

```c
#define run_with_period(_ms_) \
    if ((_ms_ <= 1000/server.hz) || \
        !(server.cronloops % ((_ms_)/(1000/server.hz))))
```

**주요 작업별 실행 주기:**

| 주기 | 작업 |
|------|------|
| 100ms | 네트워크 통계, EL cycle 메트릭, 활성 클라이언트 슬라이딩 윈도우 |
| 매 호출 | LRU 클록 갱신, 메모리 통계, SIGTERM/SIGINT 처리 |
| 1초 | 클라이언트 타임아웃, 슬로우 클라이언트, DB 키 만료 (적극 만료) |
| 5초 | 복제 버퍼 트리밍 |
| server.hz | RDB/AOF 상태 확인, 복제 크론 |

---

## 12. 주요 이벤트 핸들러 등록

### 서버 초기화 시 (`initServer`)

```c
/* TCP 연결 수락 핸들러 */
aeCreateFileEvent(server.el, fd, AE_READABLE, acceptTcpHandler, sfd);

/* 메인 타이머 (serverCron) */
aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL);

/* 모듈 pipe 읽기 핸들러 */
aeCreateFileEvent(server.el, server.module_pipe[0], AE_READABLE,
                  modulePipeReadable, NULL);

/* IO 스레드 완료 알림 핸들러 */
aeCreateFileEvent(server.el, notifier_fd, AE_READABLE,
                  handleClientsFromIOThread, t);

/* BIO 완료 알림 핸들러 */
aeCreateFileEvent(server.el, job_comp_pipe[0], AE_READABLE,
                  bioPipeReadJobCompList, NULL);
```

### 클라이언트 연결 시 (`acceptTcpHandler` → `createClient`)

```c
/* 읽기 핸들러: 클라이언트 요청 읽기 */
connSetReadHandler(conn, readQueryFromClient);
```

### 응답 대기 중인 클라이언트

```c
/* 쓰기 핸들러: 응답 전송 */
connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_barrier);
/* ae_barrier: AOF_FSYNC_ALWAYS 시 1, 일반 시 0 */
```

---

## 13. 이벤트 처리 전체 흐름

```
aeMain() ─────────────────────────────────────────────────────────
        │
        └─ aeProcessEvents(AE_ALL_EVENTS | BEFORE_SLEEP | AFTER_SLEEP)
                │
                ├─ [beforesleep] ← poll 전 집약 작업 (AOF, 응답쓰기, IO스레드 등)
                │
                ├─ usUntilEarliestTimer() ← 타이머 대기 시간 계산 (O(N))
                │
                ├─ aeApiPoll(tvp) ════════════════════════════
                │    epoll_wait / kevent / select             │
                │    (여기서 실제 슬립 발생)                    │
                │    ← 이벤트 발생 또는 타임아웃              │
                │ ════════════════════════════════════════════
                │
                ├─ [aftersleep] ← GIL 획득, 시간 캐시, running=1
                │
                ├─ fired[] 순회: 파일 이벤트 처리
                │    ├─ readable: readQueryFromClient()
                │    │            acceptTcpHandler()
                │    │            handleClientsFromIOThread()
                │    └─ writable: sendReplyToClient()
                │
                └─ processTimeEvents(): serverCron(), IOThreadCron(), ...
```

---

## 14. AE_DONT_WAIT 플래그

```c
/* 대기 없이 즉시 반환: 처리할 이벤트가 있을 때 사용 */
void aeSetDontWait(aeEventLoop *el, int noWait) {
    if (noWait) el->flags |= AE_DONT_WAIT;
    else        el->flags &= ~AE_DONT_WAIT;
}

/* IOThreadBeforeSleep에서 pending data 있을 때 활성화 */
aeSetDontWait(t->el, dont_sleep);

/* beforeSleep에서 TLS pending data 있을 때 */
int dont_sleep = connTypeHasPendingData(server.el);
```

---

## 15. 설계 특성 요약

| 특성 | 내용 |
|------|------|
| **단순성** | 약 500줄, 자체 완결 라이브러리 |
| **이식성** | 4개 백엔드로 모든 Unix 플랫폼 지원 |
| **타이머** | 비정렬 연결 리스트 O(N) — 타이머가 소수라 실용적 |
| **확장성** | fd 배열 동적 확장 (INITIAL_EVENT=1024 → setsize) |
| **AE_BARRIER** | fsync-before-reply 보장을 이벤트 레벨에서 지원 |
| **통합** | beforesleep/aftersleep으로 메인 루프에 다양한 작업 삽입 |
| **IO 스레드** | 각 IO 스레드도 동일한 ae 이벤트 루프 사용 |
