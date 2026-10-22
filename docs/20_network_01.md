# Redis 네트워킹 & 이벤트 루프 소스 분석

> 분석 대상: `redis/src/` — ae.h/c, ae_epoll.c, ae_kqueue.c, ae_select.c,
> networking.c, connection.h/c, anet.h/c, iothread.c, server.c (관련 부분)
> 총 분석 라인: ~9,783줄

---

## 목차

1. [전체 아키텍처 개요](#1-전체-아키텍처-개요)
2. [AE 이벤트 루프](#2-ae-이벤트-루프-aeh--aec)
3. [플랫폼별 I/O 멀티플렉싱 백엔드](#3-플랫폼별-io-멀티플렉싱-백엔드)
4. [Connection 추상화 계층](#4-connection-추상화-계층-connectionhc)
5. [anet — 네트워크 유틸리티](#5-anet--네트워크-유틸리티-anethc)
6. [RESP 프로토콜 파싱](#6-resp-프로토콜-파싱-networkingc)
7. [클라이언트 읽기 / 쓰기 흐름](#7-클라이언트-읽기--쓰기-흐름)
8. [IO 스레드 아키텍처](#8-io-스레드-아키텍처-iothreadc)
9. [서버 메인 루프 & 타이머](#9-서버-메인-루프--타이머-serverc)
10. [핵심 설계 요약](#10-핵심-설계-요약)

---

## 1. 전체 아키텍처 개요

```
┌──────────────────────────────────────────────────────────────┐
│                        Redis Server                          │
│                                                              │
│  ┌─────────────────────────────────────────────┐            │
│  │           aeMain() — 이벤트 루프             │            │
│  │                                             │            │
│  │  beforeSleep() ──► aeApiPoll() ──► afterSleep()         │
│  │       │               │(epoll/kqueue/select)  │          │
│  │       │           fired events                │          │
│  │       ▼               ▼                       │          │
│  │  [AOF flush]   [File Event 핸들러 실행]        │          │
│  │  [Write 큐]    [Time Event 처리]              │          │
│  └─────────────────────────────────────────────┘            │
│                                                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │  IOThread1 │  │  IOThread2 │  │  IOThreadN │            │
│  │ (read/write│  │            │  │            │            │
│  │  RESP parse│  │            │  │            │            │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘            │
│        │               │               │                     │
│        └───────────────┴───────────────┘                     │
│                        │ (mutex + notifier)                  │
│               Main Thread (command execution)                │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. AE 이벤트 루프 (`ae.h` / `ae.c`)

### 2.1 핵심 구조체

```c
// ae.h:52 - 파일(소켓) 이벤트
typedef struct aeFileEvent {
    int mask;               // AE_READABLE | AE_WRITABLE | AE_BARRIER
    aeFileProc *rfileProc;  // 읽기 콜백
    aeFileProc *wfileProc;  // 쓰기 콜백
    void *clientData;
} aeFileEvent;

// ae.h:60 - 타이머 이벤트 (이중 연결 리스트)
typedef struct aeTimeEvent {
    long long id;
    monotime when;                      // 절대 시각 (마이크로초, 단조 시계)
    aeTimeProc *timeProc;
    aeEventFinalizerProc *finalizerProc;
    void *clientData;
    struct aeTimeEvent *prev;
    struct aeTimeEvent *next;
    int refcount;                       // 재귀 호출 방지
} aeTimeEvent;

// ae.h:79 - 이벤트 루프 상태
typedef struct aeEventLoop {
    int maxfd;                  // 현재 등록된 최대 fd
    int setsize;                // 최대 추적 fd 수 (상한)
    long long timeEventNextId;
    int nevents;                // 현재 할당된 events 배열 크기
    aeFileEvent *events;        // fd 인덱스 → 등록 이벤트
    aeFiredEvent *fired;        // 발화된 이벤트 임시 배열
    aeTimeEvent *timeEventHead; // 타이머 연결 리스트
    int stop;
    void *apidata;              // epoll/kqueue/select 내부 상태
    aeBeforeSleepProc *beforesleep;
    aeBeforeSleepProc *aftersleep;
    int flags;
    void *privdata[2];
} aeEventLoop;
```

### 2.2 이벤트 마스크 플래그

| 상수 | 값 | 설명 |
|------|----|------|
| `AE_NONE` | 0 | 미등록 |
| `AE_READABLE` | 1 | 읽기 가능 시 발화 |
| `AE_WRITABLE` | 2 | 쓰기 가능 시 발화 |
| `AE_BARRIER` | 4 | WRITABLE보다 READABLE을 나중에 처리 |

```
AE_BARRIER 사용 이유 (ae.h:24):
  AOF fsync를 클라이언트에 응답 보내기 전에 완료하고 싶을 때.
  기본 순서: READ → WRITE
  BARRIER 설정 시: WRITE → READ (이미 READ가 발화했으면 WRITE 생략)
```

### 2.3 이벤트 루프 메인 루프

```c
// ae.c:492
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop,
            AE_ALL_EVENTS | AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
    }
}
```

### 2.4 aeProcessEvents 흐름

```
aeProcessEvents()
  │
  ├─ 1. beforesleep() 콜백 호출
  │       (AOF flush, write 큐 처리, IO 스레드 클라이언트 수집 등)
  │
  ├─ 2. usUntilEarliestTimer() → 다음 타이머까지 남은 시간 계산
  │       (타이머 없으면 tvp=NULL → 무한 대기)
  │
  ├─ 3. aeApiPoll(tvp) → epoll_wait / kevent / select 블로킹
  │       (이벤트 또는 타임아웃 시 반환)
  │
  ├─ 4. aftersleep() 콜백 호출
  │
  ├─ 5. 발화된 파일 이벤트 순차 처리
  │       for each fired[j]:
  │         if !AE_BARRIER: READ → WRITE
  │         if  AE_BARRIER: WRITE → READ (READ를 나중에)
  │
  └─ 6. processTimeEvents() → 만료 타이머 실행
```

### 2.5 동적 이벤트 배열 크기 조정

```c
// ae.c:47 - 초기 1024 슬롯, fd 초과 시 2배씩 확장 (setsize 상한)
#define INITIAL_EVENT 1024

// ae.c:155
if (unlikely(fd >= eventLoop->nevents)) {
    int newnevents = eventLoop->nevents;
    newnevents = (newnevents * 2 > fd + 1) ? newnevents * 2 : fd + 1;
    newnevents = (newnevents > eventLoop->setsize) ? eventLoop->setsize : newnevents;
    // events[], fired[] 배열 realloc
}
```

### 2.6 타이머 이벤트 특성

```c
// ae.c:263 - O(N) 스캔으로 최초 만료 타이머 탐색 (미정렬 연결 리스트)
static int64_t usUntilEarliestTimer(aeEventLoop *eventLoop) { ... }

// timeProc 반환값:
//   AE_NOMORE(-1) → 이 타이머 삭제
//   N > 0         → N 밀리초 후 재호출
```

---

## 3. 플랫폼별 I/O 멀티플렉싱 백엔드

### 3.1 백엔드 선택 (컴파일 타임)

```c
// ae.c:32 - 성능 내림차순으로 자동 선택
#ifdef HAVE_EVPORT   → ae_evport.c  (Solaris)
#elif HAVE_EPOLL    → ae_epoll.c   (Linux)
#elif HAVE_KQUEUE   → ae_kqueue.c  (macOS / BSD)
#else               → ae_select.c  (범용 fallback)
```

모든 백엔드는 동일한 5개 함수를 구현:

| 함수 | 역할 |
|------|------|
| `aeApiCreate()` | 멀티플렉서 초기화 |
| `aeApiResize()` | 배열 크기 조정 |
| `aeApiFree()` | 자원 해제 |
| `aeApiAddEvent()` | fd 등록/수정 |
| `aeApiDelEvent()` | fd 해제 |
| `aeApiPoll()` | 이벤트 대기 및 발화 목록 반환 |
| `aeApiName()` | 백엔드 이름 문자열 |

### 3.2 epoll 백엔드 (`ae_epoll.c`)

```c
typedef struct aeApiState {
    int epfd;                    // epoll 파일 디스크립터
    struct epoll_event *events;  // 발화 이벤트 수신 버퍼
} aeApiState;

// epoll_create(1024) - 1024는 커널 힌트일 뿐 (실제 제한 없음)
// anetCloexec(epfd) - fork 시 자동 닫힘 설정

// 이벤트 등록/수정:
// fd가 처음이면 EPOLL_CTL_ADD, 이미 있으면 EPOLL_CTL_MOD
int op = eventLoop->events[fd].mask == AE_NONE ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;

// 이벤트 매핑:
// EPOLLIN  → AE_READABLE
// EPOLLOUT → AE_WRITABLE
// EPOLLERR → AE_WRITABLE | AE_READABLE  (에러 → 양방향 트리거로 처리)
// EPOLLHUP → AE_WRITABLE | AE_READABLE
```

### 3.3 kqueue 백엔드 (`ae_kqueue.c`)

```c
typedef struct aeApiState {
    int kqfd;
    struct kevent *events;
    char *eventsMask;  // 2비트/fd, 4fd/바이트로 READ+WRITE 병합용
} aeApiState;

// kqueue는 READ/WRITE를 별도 이벤트로 반환
// → 동일 fd의 이벤트를 2패스로 병합해야 AE_BARRIER 순서 제어 가능
//
// eventsMask 비트 레이아웃:
#define EVENT_MASK_MALLOC_SIZE(sz) (((sz) + 3) / 4)  // 바이트 수
#define EVENT_MASK_OFFSET(fd)      ((fd) % 4 * 2)    // 바이트 내 오프셋

// 1패스: EVFILT_READ/WRITE → eventsMask에 기록
// 2패스: 같은 fd의 READ+WRITE 마스크 합산 후 fired[]에 단일 항목으로 추가
```

### 3.4 백엔드 비교

| 항목 | epoll | kqueue | select |
|------|-------|--------|--------|
| 플랫폼 | Linux | macOS/BSD | 범용 |
| fd 상한 | 없음 | 없음 | FD_SETSIZE(1024) |
| 이벤트 반환 | 변경된 fd만 | 변경된 fd만 | 전체 스캔 |
| 레벨/엣지 | 레벨 트리거 | 레벨 트리거 | 레벨 트리거 |
| READ+WRITE 병합 | 단일 이벤트 가능 | 2패스 병합 필요 | 단일 처리 |
| 시간 복잡도 | O(ready) | O(ready) | O(maxfd) |

---

## 4. Connection 추상화 계층 (`connection.h` / `connection.c`)

### 4.1 설계 의도

Redis는 TCP / Unix 소켓 / TLS를 단일 인터페이스로 다루기 위해
**vtable 기반 다형성** 을 C로 구현했다.

```c
// connection.h:47 - 모든 연결 타입이 구현해야 하는 vtable
typedef struct ConnectionType {
    const char *(*get_type)(connection *conn);
    void (*init)(void);
    void (*cleanup)(void);
    int  (*configure)(void *priv, int reconfigure);

    // 이벤트 루프 연동
    void (*ae_handler)(aeEventLoop *el, int fd, void *clientData, int mask);
    aeFileProc *accept_handler;

    // 연결 생성/해제
    connection* (*conn_create)(aeEventLoop *el);
    connection* (*conn_create_accepted)(aeEventLoop *el, int fd, void *priv);
    void (*shutdown)(connection *conn);
    void (*close)(connection *conn);

    // 연결/수락
    int (*connect)(connection *conn, const char *addr, int port,
                   const char *source_addr, ConnectionCallbackFunc handler);
    int (*blocking_connect)(connection *conn, const char *addr, int port,
                            long long timeout);
    int (*accept)(connection *conn, ConnectionCallbackFunc accept_handler);

    // I/O
    int (*write)(connection *conn, const void *data, size_t data_len);
    int (*writev)(connection *conn, const struct iovec *iov, int iovcnt);
    int (*read)(connection *conn, void *buf, size_t buf_len);
    int (*set_write_handler)(connection *conn, ConnectionCallbackFunc h, int barrier);
    int (*set_read_handler)(connection *conn, ConnectionCallbackFunc h);

    // 동기 I/O (레플리케이션용)
    ssize_t (*sync_write)(connection *conn, char *ptr, ssize_t size, long long timeout);
    ssize_t (*sync_read)(connection *conn, char *ptr, ssize_t size, long long timeout);
    ssize_t (*sync_readline)(connection *conn, char *ptr, ssize_t size, long long timeout);

    // 이벤트 루프 이동 (IO 스레드 ↔ 메인 스레드)
    void (*unbind_event_loop)(connection *conn);
    int  (*rebind_event_loop)(connection *conn, aeEventLoop *el);

    // TLS 전용
    sds (*get_peer_cert)(connection *conn);
    sds (*get_peer_username)(connection *conn);
} ConnectionType;
```

### 4.2 connection 구조체

```c
// connection.h:100
struct connection {
    ConnectionType   *type;          // vtable 포인터
    ConnectionState   state;         // NONE/CONNECTING/ACCEPTING/CONNECTED/CLOSED/ERROR
    int               last_errno;
    int               fd;            // 소켓 fd
    short int         flags;         // CONN_FLAG_CLOSE_SCHEDULED | CONN_FLAG_WRITE_BARRIER
    short int         refs;
    unsigned short    iovcnt;        // writev 사용 시 iov 수
    void             *private_data;  // 클라이언트 포인터 등
    struct aeEventLoop *el;
    ConnectionCallbackFunc conn_handler;
    ConnectionCallbackFunc write_handler;
    ConnectionCallbackFunc read_handler;
};
```

### 4.3 연결 타입 등록 시스템

```c
// 3가지 타입 상수
#define CONN_TYPE_SOCKET  "tcp"    // TCP 소켓
#define CONN_TYPE_UNIX    "unix"   // Unix 도메인 소켓
#define CONN_TYPE_TLS     "tls"    // TLS (OpenSSL)
#define CONN_TYPE_MAX     8        // 최대 확장 가능 타입 수

// 등록 함수 (server 초기화 시 호출)
int RedisRegisterConnectionTypeSocket(void);
int RedisRegisterConnectionTypeUnix(void);
int RedisRegisterConnectionTypeTLS(void);
```

### 4.4 connListener 구조체

```c
// connection.h:118
struct connListener {
    int fd[CONFIG_BINDADDR_MAX]; // 최대 16개 bind 주소별 fd
    int count;
    char **bindaddr;
    int   bindaddr_count;
    int   port;
    ConnectionType *ct;
    void *priv;
};
```

### 4.5 인라인 래퍼 (connection.h)

모든 I/O 함수는 vtable을 통해 호출되는 인라인 래퍼로 제공:

```c
static inline int connWrite(connection *conn, const void *data, size_t len) {
    return conn->type->write(conn, data, len);
}
static inline int connRead(connection *conn, void *buf, size_t len) {
    return conn->type->read(conn, buf, len);
}
// writev, set_read_handler, set_write_handler, close, shutdown 등 동일 패턴
```

---

## 5. anet — 네트워크 유틸리티 (`anet.h` / `anet.c`)

저수준 소켓 시스템 콜 래퍼 라이브러리.

### 5.1 서버 생성

```c
int anetTcpServer (char *err, int port, char *bindaddr, int backlog); // IPv4
int anetTcp6Server(char *err, int port, char *bindaddr, int backlog); // IPv6
int anetUnixServer(char *err, char *path, mode_t perm, int backlog);  // Unix
```

### 5.2 연결 처리

```c
int anetTcpAccept (char *err, int serversock, char *ip, size_t ip_len, int *port);
int anetUnixAccept(char *err, int serversock);
int anetTcpNonBlockConnect(char *err, const char *addr, int port);
int anetTcpNonBlockBestEffortBindConnect(...); // 소스 IP 바인딩 포함
```

### 5.3 소켓 옵션

```c
int anetNonBlock(char *err, int fd);          // O_NONBLOCK 설정
int anetBlock(char *err, int fd);             // 블로킹 모드 복원
int anetCloexec(int fd);                      // FD_CLOEXEC (fork 시 닫힘)
int anetEnableTcpNoDelay(char *err, int fd);  // TCP_NODELAY (Nagle 비활성화)
int anetDisableTcpNoDelay(char *err, int fd);
int anetKeepAlive(char *err, int fd, int interval); // SO_KEEPALIVE
int anetSendTimeout(char *err, int fd, long long ms);
int anetRecvTimeout(char *err, int fd, long long ms);
```

### 5.4 주소 관련

```c
// DNS 해석 (IPv4/IPv6 선택 플래그)
int anetResolve(char *err, char *host, char *ipbuf, size_t len, int flags);
// flags: ANET_IP_ONLY | ANET_PREFER_IPV4 | ANET_PREFER_IPV6

int anetFdToString(int fd, char *ip, size_t len, int *port, int remote);
int anetFormatAddr(char *fmt, size_t len, char *ip, int port);
// IPv6는 [::1]:6379 형식으로 자동 포맷
int anetPipe(int fds[2], int read_flags, int write_flags);
```

---

## 6. RESP 프로토콜 파싱 (`networking.c`)

### 6.1 프로토콜 상수

```c
// server.h:189
#define PROTO_REPLY_CHUNK_BYTES (16*1024)  // 16KB: 출력 버퍼 청크 크기
#define PROTO_INLINE_MAX_SIZE   (64*1024)  // 64KB: 인라인 명령 최대 크기
#define PROTO_MBULK_BIG_ARG     (32*1024)  // 32KB: 대형 인수 임계값
#define PROTO_RESIZE_THRESHOLD  (32*1024)  // 32KB: 쿼리 버퍼 리사이즈 임계값
```

### 6.2 RESP 버전

```c
// server.h:1444
int resp; // 클라이언트별 RESP 버전 (2 또는 3)
// HELLO 명령으로 RESP3 협상 가능
// RESP3: Map, Set, Double, BigNumber 등 추가 타입 지원
// Null Array는 RESP2에만 존재, RESP3은 Null 타입("_\r\n") 사용
```

### 6.3 두 가지 파싱 경로

```
클라이언트 첫 바이트:
  '*' → processMultibulkBuffer() — RESP 프로토콜
  그 외 → processInlineBuffer()  — 인라인 (telnet 호환)
```

**인라인 프로토콜 (processInlineBuffer)**:
```
SET key value\r\n
→ 공백으로 분리 → argc/argv 구성
→ 최대 PROTO_INLINE_MAX_SIZE(64KB) 제한
```

**RESP 멀티벌크 프로토콜 (processMultibulkBuffer)**:
```
*3\r\n          ← 인수 개수
$3\r\n          ← 인수 1 바이트 길이
SET\r\n         ← 인수 1 데이터
$3\r\n
key\r\n
$5\r\n
value\r\n

인수가 PROTO_MBULK_BIG_ARG(32KB) 초과 시:
  → 직접 SDS에 임시 저장 (쿼리 버퍼 복사 없음)
```

### 6.4 pendingCommand 풀

```c
// IO 스레드가 파싱 완료한 명령을 메인 스레드로 넘기는 구조체
// pendingCommand 풀: zmalloc 오버헤드 감소
// serverCron에서 pendingCommandPoolCron()으로 풀 크기 주기적 관리
```

---

## 7. 클라이언트 읽기 / 쓰기 흐름

### 7.1 연결 수락

```
클라이언트 연결 요청
  → accept_handler() (connListener에 등록)
  → anetTcpAccept() → 새 fd 획득
  → connCreateAccepted() → connection 객체 생성
  → connAccept() → 핸들셰이크(TLS) 또는 즉시 완료
  → connSetReadHandler(conn, readQueryFromClient)
  → connKeepAlive(), connEnableTcpNoDelay()
  → 클라이언트를 IO 스레드에 배정 (assignClientToIOThread)
```

### 7.2 읽기 흐름 (readQueryFromClient)

```
aeApiPoll() → AE_READABLE 발화
  → readQueryFromClient()
     → connRead() → 소켓에서 쿼리 버퍼로 읽기
     → processInputBuffer()
         → reqtype 판별 (INLINE / MULTIBULK)
         → processInlineBuffer() 또는 processMultibulkBuffer()
         → 완성된 pendingCommand → 메인 스레드로 전달
```

### 7.3 쓰기 흐름 (writeToClient)

```c
// networking.c:2676
int writeToClient(client *c, int handler_installed) {
    // CLIENT_IO_WRITE_ENABLED 플래그 확인
    // 슬레이브 클라이언트: _writeToClientSlave()
    //   → replication buffer 노드 직접 참조 (zero-copy)
    // 일반 클라이언트: _writeToClientNonSlave()
    //   → reply 리스트가 비어있지 않으면 connWritev() (scatter-gather I/O)
    //   → reply 리스트가 비어있으면 connWrite()로 buf 전송
    // 모두 보내면 write handler 제거
}
```

### 7.4 출력 버퍼 구조

```
client
  ├── buf[PROTO_REPLY_CHUNK_BYTES=16KB]  ← 소형 응답 인라인 버퍼
  ├── bufpos / sentlen                   ← 전송 진행 위치
  └── reply (list)                       ← 대형 응답 연결 리스트
        ├── clientReplyBlock (16KB)
        ├── clientReplyBlock
        └── ...
```

---

## 8. IO 스레드 아키텍처 (`iothread.c`)

### 8.1 IOThread 구조체

```c
// server.h:1626 - 캐시 라인 정렬 (false sharing 방지)
typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
    uint8_t   id;                           // 스레드 ID (0 = 메인)
    pthread_t tid;                          // pthread ID
    // ... (event loop, mutex, notifier 등)
    list *pending_clients_to_main_thread;   // 메인 → 메인으로 보낼 클라이언트
    list *clients;                          // 이 IO 스레드가 관리하는 클라이언트
} IOThread;

// 최대 스레드 수
#define IO_THREADS_MAX_NUM       128
#define IOTHREAD_MAIN_THREAD_ID  0    // 메인 스레드 = ID 0
#define IO_THREAD_MAX_PENDING_CLIENTS 16  // 배치 전송 임계값
```

### 8.2 클라이언트 이동 흐름

```
메인 스레드
  ├── sendPendingClientsToIOThreads()
  │     → enqueuePendingClienstToIOThreads(c)
  │     → IO 스레드 이벤트 루프에 connRebindEventLoop()
  │
IO 스레드 (read/write 담당)
  ├── readQueryFromClient() → RESP 파싱
  ├── writeToClient() → 응답 전송
  └── 완료 시: enqueuePendingClientsToMainThread(c, unbind)
        → pending list에 추가
        → len >= 16(IO_THREAD_MAX_PENDING_CLIENTS)이면 즉시 전송
        → triggerEventNotifier() → 메인 스레드 깨움

메인 스레드
  ├── processClientsOfAllIOThreads()
  │     → 각 IO 스레드의 pending_clients 수집
  │     → 커맨드 실행 (single-threaded)
  └── 다시 IO 스레드로 배정 또는 직접 처리
```

### 8.3 동기화 메커니즘

```c
// 뮤텍스: IO 스레드 → 메인 전달 시
pthread_mutex_t mainThreadPendingClientsMutexes[IO_THREADS_MAX_NUM];

// eventNotifier: 메인 스레드가 epoll 대기 중일 때 깨우기
eventNotifier* mainThreadPendingClientsNotifiers[IO_THREADS_MAX_NUM];

// atomic: running 상태 확인 (깨움 중복 방지)
atomicGetWithSync(server.running, running);
if (!running && !pending)
    triggerEventNotifier(notifier);  // 메인 스레드가 쉬고 있을 때만 알림
```

### 8.4 스레드별 역할 분리

| 작업 | IO 스레드 | 메인 스레드 |
|------|-----------|------------|
| 소켓 읽기 (recv) | O | O (스레드 없을 때) |
| RESP 파싱 | O | O (스레드 없을 때) |
| **커맨드 실행** | **X** | **O (항상)** |
| 응답 쓰기 (send) | O | O (스레드 없을 때) |
| Key 만료 처리 | X | O |
| 레플리케이션 | X (읽기만) | O |

### 8.5 Master/Slave 클라이언트 특수 처리

```c
// iothread.c:49
void updateClientDataFromIOThread(client *c) {
    // IO 스레드가 변경한 값을 메인 스레드 클라이언트 구조체에 동기화
    // io_repl_ack_time → repl_ack_time
    // io_lastinteraction → lastinteraction
    // io_read_reploff → read_reploff
    // replication buffer 노드 참조 카운트 업데이트
}
```

---

## 9. 서버 메인 루프 & 타이머 (`server.c`)

### 9.1 beforeSleep 콜백

```c
// server.c:1884
void beforeSleep(struct aeEventLoop *eventLoop) {
    updatePeakMemory();
    // 재진입 중이면 최소 작업만 수행

    // 1. handleClientsWithPendingWritesUsingThreads()
    //    → 쓰기 대기 클라이언트를 IO 스레드로 배분
    // 2. processClientsOfAllIOThreads()
    //    → IO 스레드 완료 클라이언트 처리
    // 3. flushAppendOnlyFile(0) → AOF 버퍼 플러시
    // 4. freeClientsInAsyncFreeQueue()
    // 5. sendPendingClientsToIOThreads()
    // 6. tracking invalidation 처리
    // ...
}
```

### 9.2 afterSleep 콜백

```c
// server.c:2094
void afterSleep(struct aeEventLoop *eventLoop) {
    // moduleAcquireGIL() — Lua/모듈 GIL 획득
    // (경고: GIL 획득 전 코드 추가 금지)
}
```

### 9.3 serverCron 타이머

```c
// server.c:1506
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    // server.hz 빈도로 실행 (기본 10Hz = 100ms 간격)
    // dynamic_hz: 클라이언트 수 많을수록 hz 증가
    //   (listLength(clients) / hz > MAX_CLIENTS_PER_CLOCK_TICK 시)

    // run_with_period(100)  { ... } → 100ms마다
    // run_with_period(1000) { ... } → 1000ms마다

    // 주요 작업:
    // - 통계 업데이트 (메모리, 연결 수 등)
    // - activeExpireCycle() → 랜덤 키 만료 검사
    // - clientsCron() → 클라이언트 타임아웃, 버퍼 크기 조정
    // - databasesCron() → 키 공간 축소, Rehash 진행
    // - replicationCron() → 레플리카 핸드셰이크, backlog 관리
    // - clusterCron() → 클러스터 gossip
    // - AOF rewrite 스케줄링
    // - RDB 백그라운드 저장 모니터링
}
```

### 9.4 이벤트 루프 전체 생명주기

```
main()
  → initServer()
      → server.el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)
      → aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL)
      → 각 connListener에 accept_handler 등록
  → aeSetBeforeSleepProc(server.el, beforeSleep)
  → aeSetAfterSleepProc(server.el, afterSleep)
  → aeMain(server.el)  ← 여기서 무한 루프 진입
```

---

## 10. 핵심 설계 요약

### 단일 스레드 커맨드 실행 + 멀티 스레드 I/O

```
┌─────────────────────────────────────────────────────┐
│ Redis 동시성 모델                                    │
│                                                     │
│  I/O 병렬화 (IO 스레드): 소켓 읽기/쓰기, RESP 파싱  │
│  명령 직렬화 (메인 스레드): 실제 데이터 조작         │
│                                                     │
│  → Race condition 없는 데이터 모델 유지              │
│  → Lock-free 커맨드 실행 경로                        │
└─────────────────────────────────────────────────────┘
```

### 계층별 역할

| 계층 | 파일 | 역할 |
|------|------|------|
| 이벤트 루프 | `ae.c` | fd 이벤트 + 타이머 통합 관리 |
| I/O 멀티플렉서 | `ae_epoll.c` / `ae_kqueue.c` | OS별 고성능 이벤트 감지 |
| 연결 추상화 | `connection.h` | TCP/Unix/TLS 다형성 |
| 네트워크 유틸 | `anet.c` | 소켓 생성/옵션 래퍼 |
| 프로토콜 파싱 | `networking.c` | RESP2/3 파싱, 버퍼 관리 |
| I/O 스레딩 | `iothread.c` | 멀티 스레드 I/O 분산 |
| 메인 루프 | `server.c` | 커맨드 실행, 주기적 작업 |

### AE_BARRIER를 이용한 AOF 안전 쓰기

```
일반 순서:   [READ: 커맨드 처리] → [WRITE: 응답 전송]
BARRIER 시:  [WRITE 시도] → (READ 발화 전이면 생략)
             [READ: 커맨드 처리]
→ beforeSleep에서 AOF fsync 완료 후 응답 전송 보장
```

### writev를 활용한 Zero-copy 응답

```c
// networking.c:2588
// reply 리스트에 여러 청크가 있을 때 connWritev() (scatter-gather)
// → 여러 iovec를 단일 시스템 콜로 전송
// → 레플리케이션 버퍼: 노드 포인터 공유로 복사 없음
```

---

*분석 기준: Redis `redis/src/` 소스, 2024-present 버전*
