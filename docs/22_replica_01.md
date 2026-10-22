# Redis 복제 & 클러스터 소스 분석

> 분석 대상: `redis/src/replication.c`, `cluster.h`, `cluster_legacy.h`, `cluster_legacy.c`, `cluster_asm.h`, `server.h`

---

## 1. 복제 (Replication)

### 1.1 아키텍처 개요

`replication.c`는 Redis 비동기 복제의 핵심 파일로, 다음을 담당한다.

| 역할 | 내용 |
|------|------|
| Master → Replica 동기화 | 명령 스트림 전파 |
| Full / Partial 재동기화 | PSYNC 프로토콜 |
| 복제 백로그 관리 | 부분 재동기화용 링 버퍼 |
| 상태 머신 | connectWithMaster / syncWithMaster |
| RDB 채널 | 전용 연결로 RDB 전송 |

복제는 **비동기**가 기본이며, 마스터는 커맨드를 전파한 뒤 복제본의 확인을 기다리지 않는다. `WAIT` 커맨드로 동기 대기를 선택적으로 적용할 수 있다.

---

### 1.2 Replica 관점의 상태 머신

복제본이 마스터에 연결되는 과정은 `server.repl_state`로 추적된다.

```c
// server.h (enum repl_state)
REPL_STATE_NONE = 0,           /* 복제 비활성 */
REPL_STATE_CONNECT,            /* 연결 시도 예정 */
REPL_STATE_CONNECTING,         /* 연결 중 */
REPL_STATE_RECEIVE_PING_REPLY, /* PING 응답 대기 */
REPL_STATE_SEND_HANDSHAKE,     /* 핸드셰이크 전송 */
REPL_STATE_RECEIVE_AUTH_REPLY, /* AUTH 응답 대기 */
REPL_STATE_RECEIVE_PORT_REPLY, /* REPLCONF port 응답 대기 */
REPL_STATE_RECEIVE_IP_REPLY,   /* REPLCONF ip 응답 대기 */
REPL_STATE_RECEIVE_REQ_REPLY,  /* REPLCONF rdb-only 응답 대기 */
REPL_STATE_RECEIVE_CAPA_REPLY, /* REPLCONF capa 응답 대기 */
REPL_STATE_SEND_PSYNC,         /* PSYNC 전송 */
REPL_STATE_RECEIVE_PSYNC_REPLY,/* PSYNC 응답 대기 */
REPL_STATE_TRANSFER,           /* RDB 수신 중 */
REPL_STATE_CONNECTED,          /* 정상 연결, 명령 스트리밍 */
```

상태 전이 흐름:

```
NONE → CONNECT → CONNECTING
  → RECEIVE_PING_REPLY
  → SEND_HANDSHAKE
  → RECEIVE_AUTH_REPLY
  → RECEIVE_PORT_REPLY → RECEIVE_IP_REPLY → RECEIVE_REQ_REPLY → RECEIVE_CAPA_REPLY
  → SEND_PSYNC → RECEIVE_PSYNC_REPLY
  ┌── +CONTINUE  → CONNECTED (부분 동기화)
  └── +FULLRESYNC → TRANSFER → CONNECTED (전체 동기화)
```

---

### 1.3 Master 관점의 Slave 상태 (replstate)

마스터는 각 복제본 클라이언트의 `replstate` 필드로 상태를 추적한다.

```c
// server.h
#define SLAVE_STATE_WAIT_BGSAVE_START       6  /* RDB 생성 대기 중 */
#define SLAVE_STATE_WAIT_BGSAVE_END         7  /* RDB 생성 완료 대기 */
#define SLAVE_STATE_SEND_BULK               8  /* RDB 전송 중 */
#define SLAVE_STATE_ONLINE                  9  /* 스트리밍 중 */
#define SLAVE_STATE_RDB_TRANSMITTED         10 /* RDB 전송 완료 */
#define SLAVE_STATE_WAIT_RDB_CHANNEL        11 /* RDB 채널 연결 대기 */
#define SLAVE_STATE_SEND_BULK_AND_STREAM    12 /* RDB 채널 복제 중 */
```

---

### 1.4 Replication ID 이중 체계

```
server.replid          : 현재 복제 ID (40자리 hex)
server.replid2         : 이전 복제 ID (페일오버 후 보존)
server.second_replid_offset : replid2의 유효 상한 오프셋
```

#### ID 관리 함수

| 함수 | 동작 |
|------|------|
| `changeReplicationId()` | 새 무작위 ID 생성, replid2 초기화 |
| `shiftReplicationId()` | 현재 replid → replid2로 이동, 새 replid 생성 |
| `clearReplicationId2()` | replid2를 '0' * 40으로 초기화 |

페일오버 후 새 마스터는 `shiftReplicationId()`를 호출하여, 이전 복제본들이 **replid2 + second_replid_offset 범위 내에서** 부분 재동기화를 시도할 수 있게 한다.

---

### 1.5 PSYNC 프로토콜 (부분 / 전체 재동기화)

#### 요청 형식

```
PSYNC <replid> <offset>   /* 부분 재동기화 시도 */
PSYNC ? -1                /* 전체 재동기화 강제 */
```

#### masterTryPartialResynchronization() 판단 로직

```c
// replication.c:943
int masterTryPartialResynchronization(client *c, long long psync_offset) {
    // 1. replid 일치 여부 확인
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) ||
         psync_offset > server.second_replid_offset))
    {
        goto need_full_resync;   // ID 불일치
    }

    // 2. 요청 오프셋이 백로그 범위 내인지 확인
    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog->offset ||
        psync_offset > (server.repl_backlog->offset + server.repl_backlog->histlen))
    {
        goto need_full_resync;   // 백로그 범위 초과
    }

    // 3. 부분 재동기화 수행
    // +CONTINUE <replid> 응답 후 백로그 데이터 전송
    return C_OK;

need_full_resync:
    return C_ERR;  // 전체 재동기화 필요
}
```

#### 응답 형식

| 상황 | 마스터 응답 |
|------|-------------|
| 부분 재동기화 가능 | `+CONTINUE <replid>` |
| 전체 재동기화 필요 | `+FULLRESYNC <replid> <offset>` |
| PSYNC 미지원 | `+OK` (구형 프로토콜) |

PSYNC v2 (`SLAVE_CAPA_PSYNC2`)를 지원하는 복제본은 `+CONTINUE` 응답에 replid를 포함시켜 페일오버 후 ID 변경을 감지한다.

---

### 1.6 복제 백로그 (Replication Backlog)

부분 재동기화의 핵심 자료구조. 최근 복제 스트림을 메모리에 유지한다.

```c
// server.h
typedef struct replBacklog {
    listNode *ref_repl_buf_node; /* 첫 번째 replBufBlock 참조 */
    long long unindexed_count;   /* 미인덱스 블록 수 */
    rax *blocks_index;           /* 오프셋 → listNode 빠른 검색 */
    long long histlen;           /* 유효 데이터 길이 */
    long long offset;            /* 백로그 시작 오프셋 */
} replBacklog;
```

```c
// server.h (전역)
list *repl_buffer_blocks;    /* replBufBlock 리스트 (모든 복제본과 공유) */
long long repl_buffer_mem;   /* 총 메모리 사용량 */
```

#### 주요 특성

| 항목 | 값 |
|------|-----|
| 기본 크기 | 1MB (`repl-backlog-size`) |
| 최소 크기 | 16KB (`CONFIG_REPL_BACKLOG_MIN_SIZE`) |
| 인덱스 자료구조 | rax (Radix Tree) |
| 블록 공유 | 모든 복제본이 동일 블록을 ref-count로 공유 |

`createReplicationBacklogIndex()`: `REPL_BACKLOG_INDEX_PER_BLOCKS` 블록마다 rax에 오프셋 인덱스 삽입 → 복제본의 부분 재동기화 요청 시 O(log N) 탐색.

---

### 1.7 Full Resync 흐름

```c
// replication.c:1060
int startBgsaveForReplication(int mincapa, int req) {
    if (server.repl_diskless_sync) {
        // 소켓 직접 전송 (diskless)
        retval = rdbSaveToSlavesSockets(req, rsiptr);
    } else {
        // 디스크 저장 후 전송
        retval = rdbSaveBackground(req, rsiptr);
    }
}
```

전체 재동기화 단계:

```
1. masterTryPartialResynchronization() → C_ERR
2. startBgsaveForReplication():
   ├── diskless=0: fork() → rdbSaveBackground() → 파일 전송
   └── diskless=1: fork() → rdbSaveToSlavesSockets() → 소켓 직접 스트리밍
3. SLAVE_STATE_WAIT_BGSAVE_END → SLAVE_STATE_SEND_BULK
4. RDB 전송 완료
5. replicaStartCommandStream(slave) → SLAVE_STATE_ONLINE
6. 이후 replication buffer에서 명령 스트리밍
```

#### Diskless 로드 모드 (복제본 측)

| 모드 | 동작 |
|------|------|
| `REPL_DISKLESS_LOAD_ALWAYS` | 항상 메모리 로드 |
| `REPL_DISKLESS_LOAD_SWAPDB` | 임시 DB에 로드 후 원자적 스왑 |
| `REPL_DISKLESS_LOAD_WHEN_DB_EMPTY` | DB가 비어 있을 때만 |

---

### 1.8 RDB 채널 (rdbchannel) - 신기능

기존 단일 연결 복제의 성능 한계를 극복하기 위해, **두 개의 TCP 연결**을 사용한다.

```
[Replica] ─── main channel ───▶ [Master]  명령 스트림
[Replica] ─── rdb channel  ───▶ [Master]  RDB 전송 전용
```

#### 핵심 함수

| 함수 | 역할 |
|------|------|
| `rdbChannelFullSyncWithMaster()` | RDB 채널에서 스냅샷 수신 |
| `rdbChannelBufferReplData()` | RDB 수신 중 main channel 명령 버퍼링 |
| `rdbChannelStreamReplDataToDb()` | RDB 완료 후 버퍼된 명령 적용 |
| `rdbChannelCleanup()` | 정리 |
| `rdbChannelAbort()` | 비동기 중단 |

```c
// replication.c
static rio *disklessLoadingRio = NULL;  /* 비동기 취소용 전역 포인터 */
```

main channel에서 네트워크 오류 감지 시 `rioAbort(disklessLoadingRio)`를 호출하여 RDB 채널 로드를 안전하게 중단한다.

`CLIENT_REPL_RDB_CHANNEL` 플래그로 RDB 채널 복제본을 식별하며, `replicationLogicalReplicaCount()`는 같은 복제본의 두 연결을 하나로 카운트한다.

---

### 1.9 connectWithMaster() / syncWithMaster()

```c
// replication.c:3385
int connectWithMaster(void) {
    // TCP 또는 TLS 연결 생성
    // syncWithMaster를 read handler로 등록
}

// replication.c:3023
void syncWithMaster(connection *conn) {
    // 이벤트 루프 기반 상태 머신
    // repl_state에 따라 다음 단계 처리
    switch (server.repl_state) {
        case REPL_STATE_CONNECTING:       /* PING 전송 */
        case REPL_STATE_RECEIVE_PING_REPLY: /* PONG 확인 */
        case REPL_STATE_SEND_HANDSHAKE:   /* REPLCONF 전송 */
        case REPL_STATE_SEND_PSYNC:       /* PSYNC 전송 */
        case REPL_STATE_RECEIVE_PSYNC_REPLY: /* 응답 처리 */
        case REPL_STATE_TRANSFER:         /* RDB 수신 */
    }
}
```

TLS 복제는 `server.tls_replication` 설정으로 제어:
```c
ConnectionType *connTypeOfReplication(void) {
    if (server.tls_replication) return connectionTypeTls();
    return connectionTypeTcp();
}
```

---

### 1.10 replicationCron() - 주기적 관리

`serverCron()`에서 10Hz로 호출된다.

```c
// replication.c:4889
void replicationCron(void) {
    // 1. 연결이 끊긴 경우 재연결 시도
    connectWithMaster();

    // 2. 마스터 클라이언트 timeout 감지 및 ACK 전송
    replicationCronRunMasterClient();

    // 3. 복제본 타임아웃 감지 (min-slaves-to-write 위반)
    // 4. 백로그 정리 (incremental trim)
}

// 마스터로 주기적 ACK 전송
void replicationSendAck(void) {
    // REPLCONF ACK <offset>
}
```

---

### 1.11 IO Thread와 복제 통합

```c
// replication.c:112
void putReplicasInPendingClientsToIOThreads(void) {
    // 복제본 중 pending write/read가 있는 것을 IO thread로 이동
    if (replica->flags & CLIENT_PENDING_WRITE ||
        clientHasPendingReplies(replica) ||
        replicaFromIOThreadHasPendingRead(replica))
    {
        enqueuePendingClienstToIOThreads(replica);
    }
}
```

복제 데이터 전송의 I/O 부분을 IO thread로 오프로드하여 마스터 메인 스레드 부하를 줄인다.

---

### 1.12 min-slaves-to-write 보호

| 설정 | 의미 |
|------|------|
| `repl-min-slaves-to-write` | write 허용을 위한 최소 온라인 복제본 수 |
| `repl-min-slaves-max-lag` | 최대 허용 복제 지연 (초) |

복제본 수가 부족하면 마스터가 write를 거부 → 데이터 유실 방지.

---

## 2. 클러스터 (Cluster)

### 2.1 아키텍처 개요

| 항목 | 내용 |
|------|------|
| 해시 슬롯 수 | 16384 (`CLUSTER_SLOTS = 1 << 14`) |
| 해시 알고리즘 | CRC16 & 0x3FFF |
| 멤버십 프로토콜 | Gossip 기반 분산 합의 |
| 클러스터 버스 포트 | `base_port + 10000` (`CLUSTER_PORT_INCR`) |
| 버스 프로토콜 버전 | 1 (`CLUSTER_PROTO_VER`) |

---

### 2.2 Hash Slot 계산 (keyHashSlot)

```c
// cluster.h (inline)
static inline unsigned int keyHashSlot(const char *key, int keylen) {
    int s, e;

    // { 검색
    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    // { 없으면 전체 키 해싱
    if (likely(s == keylen)) return crc16(key, keylen) & 0x3FFF;

    // } 검색
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    // } 없거나 {} 빈 경우 전체 키 해싱
    if (e == keylen || e == s+1) return crc16(key, keylen) & 0x3FFF;

    // {content} 사이 content만 해싱
    return crc16(key+s+1, e-s-1) & 0x3FFF;
}
```

Hash tag 예시:
```
{user.1000}.following  →  "user.1000" 해싱
{user.1000}.followers  →  "user.1000" 해싱 (동일 슬롯 보장)
order:12345            →  "order:12345" 전체 해싱
```

---

### 2.3 clusterNode 구조체

```c
// cluster_legacy.h
struct _clusterNode {
    mstime_t ctime;                  /* 노드 생성 시간 */
    char name[CLUSTER_NAMELEN];      /* 노드 ID: 40자리 hex (SHA1) */
    char shard_id[CLUSTER_NAMELEN];  /* 샤드 ID */
    int flags;                       /* CLUSTER_NODE_* 비트 플래그 */
    uint64_t configEpoch;            /* 충돌 해결용 단조 증가 버전 */
    unsigned char slots[CLUSTER_SLOTS/8]; /* 담당 슬롯 비트맵 (2048 bytes) */
    uint16_t *slot_info_pairs;       /* (start, end) 쌍 배열 */
    int slot_info_pairs_count;
    int numslots;                    /* 담당 슬롯 수 */
    int numslaves;                   /* 복제본 수 (마스터인 경우) */
    clusterNode **slaves;            /* 복제본 포인터 배열 */
    clusterNode *slaveof;            /* 마스터 포인터 */
    mstime_t ping_sent;              /* 최근 PING 전송 시각 */
    mstime_t pong_received;          /* 최근 PONG 수신 시각 */
    mstime_t fail_time;              /* FAIL 플래그 설정 시각 */
    mstime_t voted_time;             /* 마지막 투표 시각 */
    long long repl_offset;           /* 마지막 알려진 복제 오프셋 */
    char ip[NET_IP_STR_LEN];         /* IP 주소 */
    sds hostname;                    /* 호스트명 */
    sds human_nodename;              /* 사람이 읽기 쉬운 노드명 */
    int tcp_port, tls_port, cport;   /* 포트 */
    clusterLink *link;               /* 아웃바운드 링크 */
    clusterLink *inbound_link;       /* 인바운드 링크 */
    list *fail_reports;              /* 실패 신고 목록 (clusterNodeFailReport) */
};
```

---

### 2.4 clusterState 구조체

```c
// cluster_legacy.h
struct clusterState {
    clusterNode *myself;             /* 자기 자신 */
    uint64_t currentEpoch;           /* 클러스터 글로벌 epoch */
    int state;                       /* CLUSTER_OK(0) / CLUSTER_FAIL(1) */
    int size;                        /* 슬롯 보유 마스터 수 */
    dict *nodes;                     /* name → clusterNode */
    dict *shards;                    /* shard_id → node 리스트 */
    dict *nodes_black_list;          /* 잠시 재추가 금지 노드 */

    /* 슬롯 마이그레이션 상태 */
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];    /* 내보내는 슬롯 */
    clusterNode *importing_slots_from[CLUSTER_SLOTS];  /* 받아오는 슬롯 */
    clusterNode *slots[CLUSTER_SLOTS];                 /* slot → 담당 노드 */

    char internal_secret[CLUSTER_INTERNALSECRETLEN];   /* 내부 인증 비밀 */

    /* 자동 페일오버 상태 */
    mstime_t failover_auth_time;     /* 선거 예정 시각 */
    int failover_auth_count;         /* 수신한 투표 수 */
    int failover_auth_sent;          /* 투표 요청 전송 여부 */
    int failover_auth_rank;          /* 이 복제본의 순위 */
    uint64_t failover_auth_epoch;    /* 현재 선거 epoch */
    int cant_failover_reason;        /* 페일오버 불가 이유 */

    /* 수동 페일오버 상태 */
    mstime_t mf_end;                 /* 수동 페일오버 제한 시각 */
    clusterNode *mf_slave;           /* 페일오버 수행 복제본 */
    long long mf_master_offset;      /* 복제본이 도달해야 할 오프셋 */
    int mf_can_start;                /* 선거 시작 가능 여부 */

    uint64_t lastVoteEpoch;          /* 마지막 투표 epoch */
    int todo_before_sleep;           /* clusterBeforeSleep 작업 플래그 */

    /* 통계 */
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];
    long long stats_pfail_nodes;
    unsigned long long stat_cluster_links_buffer_limit_exceeded;

    unsigned char owner_not_claiming_slot[CLUSTER_SLOTS / 8]; /* 마이그레이션 중 슬롯 비트맵 */
};
```

---

### 2.5 clusterLink 구조체

```c
// cluster_legacy.h
typedef struct clusterLink {
    mstime_t ctime;                        /* 링크 생성 시각 */
    connection *conn;                      /* 연결 객체 */
    list *send_msg_queue;                  /* 전송 대기 메시지 큐 */
    size_t head_msg_send_offset;           /* 큐 헤드 메시지의 전송 위치 */
    unsigned long long send_msg_queue_mem; /* 큐 메모리 사용량 */
    char *rcvbuf;                          /* 수신 버퍼 */
    size_t rcvbuf_len;                     /* 실제 사용량 */
    size_t rcvbuf_alloc;                   /* 할당 크기 */
    clusterNode *node;                     /* 연결된 노드 (미상이면 NULL) */
    int inbound;                           /* 1 = 인바운드 연결 */
} clusterLink;
```

각 노드는 다른 노드마다 최대 2개의 링크(`link` + `inbound_link`)를 유지한다.

---

### 2.6 Gossip 메시지 프로토콜

#### clusterMsg 헤더

```c
// cluster_legacy.h
typedef struct {
    char sig[4];            /* "RCmb" 시그니처 */
    uint32_t totlen;        /* 전체 메시지 길이 */
    uint16_t ver;           /* 프로토콜 버전 (현재 1) */
    uint16_t port;          /* 기본 포트 */
    uint16_t type;          /* 메시지 타입 */
    uint16_t count;         /* gossip 섹션 항목 수 */
    uint64_t currentEpoch;  /* 발신자의 currentEpoch */
    uint64_t configEpoch;   /* 발신자의 configEpoch */
    uint64_t offset;        /* 발신자의 복제 오프셋 */
    char sender[CLUSTER_NAMELEN];          /* 발신자 노드명 */
    unsigned char myslots[CLUSTER_SLOTS/8]; /* 발신자 슬롯 비트맵 (2048 bytes) */
    char slaveof[CLUSTER_NAMELEN];         /* 마스터 노드명 */
    char myip[NET_IP_STR_LEN];             /* 발신자 IP */
    uint16_t extensions;                   /* 확장 데이터 수 */
    char notused1[30];                     /* 예약 */
    uint16_t pport;         /* 보조 포트 */
    uint16_t cport;         /* 클러스터 버스 포트 */
    uint16_t flags;         /* 발신자 노드 플래그 */
    unsigned char state;    /* 발신자 클러스터 상태 */
    unsigned char mflags[3];/* 메시지 플래그 */
    union clusterMsgData data; /* 페이로드 */
} clusterMsg;
```

**오프셋 고정 (static_assert 보장)**:

| 필드 | 오프셋 |
|------|--------|
| `sig` | 0 |
| `totlen` | 4 |
| `ver` | 8 |
| `data` | 2256 |

#### 메시지 타입 (CLUSTERMSG_TYPE_*)

| 타입 | 값 | 용도 |
|------|-----|------|
| PING | 0 | 헬스체크 + 노드 정보 피기백 |
| PONG | 1 | PING 응답 |
| MEET | 2 | 새 노드 추가 요청 |
| FAIL | 3 | 노드 실패 브로드캐스트 |
| PUBLISH | 4 | Pub/Sub 전파 |
| FAILOVER_AUTH_REQUEST | 5 | 투표 요청 |
| FAILOVER_AUTH_ACK | 6 | 투표 승인 |
| UPDATE | 7 | 슬롯 구성 업데이트 |
| MFSTART | 8 | 수동 페일오버 시작 |
| MODULE | 9 | 모듈 메시지 |
| PUBLISHSHARD | 10 | Pub/Sub 샤드 전파 |

#### clusterMsgDataGossip - 피기백 노드 정보

```c
typedef struct {
    char nodename[CLUSTER_NAMELEN]; /* 노드 ID */
    uint32_t ping_sent;             /* 마지막 PING 전송 시각 */
    uint32_t pong_received;         /* 마지막 PONG 수신 시각 */
    char ip[NET_IP_STR_LEN];        /* IP */
    uint16_t port;                  /* 기본 포트 */
    uint16_t cport;                 /* 클러스터 버스 포트 */
    uint16_t flags;                 /* 노드 플래그 */
    uint16_t pport;                 /* 보조 포트 */
    uint16_t notused1;
} clusterMsgDataGossip;
```

PING/PONG 메시지에 랜덤 노드 정보를 피기백하여, 별도의 브로드캐스트 없이 전체 노드 상태가 자연스럽게 전파된다.

#### Ping Extension 타입

```c
typedef enum {
    CLUSTERMSG_EXT_TYPE_HOSTNAME,         /* 호스트명 */
    CLUSTERMSG_EXT_TYPE_HUMAN_NODENAME,   /* 사람이 읽기 쉬운 노드명 */
    CLUSTERMSG_EXT_TYPE_FORGOTTEN_NODE,   /* 블랙리스트 노드 */
    CLUSTERMSG_EXT_TYPE_SHARDID,          /* 샤드 ID */
    CLUSTERMSG_EXT_TYPE_INTERNALSECRET,   /* 내부 인증 비밀 */
} clusterMsgPingtypes;
```

---

### 2.7 노드 플래그 (CLUSTER_NODE_*)

```c
// cluster_legacy.h
#define CLUSTER_NODE_MASTER      1    /* 마스터 노드 */
#define CLUSTER_NODE_SLAVE       2    /* 복제본 노드 */
#define CLUSTER_NODE_PFAIL       4    /* 단독 의심 실패 (Probable FAIL) */
#define CLUSTER_NODE_FAIL        8    /* 공식 실패 (quorum 동의) */
#define CLUSTER_NODE_MYSELF      16   /* 자기 자신 */
#define CLUSTER_NODE_HANDSHAKE   32   /* 핸드셰이크 중 */
#define CLUSTER_NODE_NOADDR      64   /* 주소 미상 */
#define CLUSTER_NODE_MEET        128  /* MEET 메시지 전송 예정 */
#define CLUSTER_NODE_MIGRATE_TO  256  /* 마이그레이션 대상 마스터 */
#define CLUSTER_NODE_NOFAILOVER  512  /* 페일오버 비활성화 */
#define CLUSTER_NODE_EXTENSIONS_SUPPORTED 1024 /* 확장 메시지 지원 */
```

편의 매크로:
```c
#define nodeIsMaster(n)    ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n)     ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeTimedOut(n)    ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n)      ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n)((n)->flags & CLUSTER_NODE_NOFAILOVER)
```

---

### 2.8 clusterCron() - 주기적 관리

`serverCron()`에서 ~100ms마다 호출된다.

```c
// cluster_legacy.c:4731
void clusterCron(void) {
    // 1. 핸드셰이크 타임아웃 정리
    // 2. pong_received가 가장 오래된 노드에 PING 전송
    // 3. 연결이 끊긴 노드 재연결
    // 4. PFAIL 노드 감지 (ping_delay > cluster_node_timeout)
    // 5. clusterHandleSlaveFailover() 호출
    // 6. 수동 페일오버 처리
    // 7. clusterUpdateState() 호출
}
```

PING 전송 주기:
```
cluster_ping_interval 설정이 있으면 해당 주기 사용
없으면: cluster_node_timeout / 2 마다 전송
조건: ping_delay > cluster_node_timeout / 2 AND data_delay > cluster_node_timeout / 2
```

---

### 2.9 장애 감지: PFAIL → FAIL 전이

```
단계 1: 자신이 노드를 의심
  ├── node.ping_sent > 0
  └── mstime() - node.ping_sent > cluster_node_timeout
  → CLUSTER_NODE_PFAIL 설정

단계 2: 다른 노드들도 같은 노드를 PFAIL로 gossip
  → fail_reports에 추가
  → fail_reports 유효 기간: cluster_node_timeout * CLUSTER_FAIL_REPORT_VALIDITY_MULT(2)

단계 3: fail_reports 수 ≥ (size/2 + 1)
  → CLUSTER_NODE_FAIL 설정
  → FAIL 메시지 클러스터 전체 브로드캐스트

단계 4: 복구
  → 마스터 재연결 AND cluster_node_timeout * CLUSTER_FAIL_UNDO_TIME_MULT(2) 경과
  → FAIL / PFAIL 플래그 제거
```

---

### 2.10 페일오버 알고리즘 (clusterHandleSlaveFailover)

```c
// cluster_legacy.c:4298
void clusterHandleSlaveFailover(void) {
    int needed_quorum = (server.cluster->size / 2) + 1;
    // auth_timeout = MAX(cluster_node_timeout * 2, 2000ms)
    // auth_retry_time = auth_timeout * 2
```

#### 전제 조건 (자동 페일오버)

1. 나는 slave
2. 마스터가 FAIL 상태
3. CLUSTER_NODE_NOFAILOVER 플래그 없음
4. 마스터가 슬롯을 보유 중

#### 선거 시작 지연 계산

```
failover_auth_time = now
    + 500ms           // FAIL 메시지 전파 대기 (고정)
    + random(500ms)   // split-brain 방지 (랜덤)
    + rank * 1000ms   // 순위에 따른 추가 지연
```

#### 복제본 순위 (rank)

`clusterGetSlaveRank()`: 현재 복제본의 `repl_offset`을 다른 복제본들과 비교한다.  
→ **repl_offset이 클수록 rank 낮음** (rank=0이 가장 먼저 선출).

#### 투표 수집 과정

```
1. FAILOVER_AUTH_REQUEST 브로드캐스트 (모든 마스터에게)
2. 마스터 측 조건 확인 (clusterSendFailoverAuthIfNeeded):
   ├── 마스터인가?
   ├── 요청 epoch > lastVoteEpoch?
   ├── 이전 투표로부터 cluster_node_timeout * 2 경과?
   └── 요청자가 실패한 마스터의 복제본인가?
3. 조건 충족 시 FAILOVER_AUTH_ACK 전송
4. failover_auth_count >= needed_quorum
   → clusterPromoteSelfToMaster() 호출
   → 슬롯 소유권 인수
   → clusterUpdateState()
```

#### 수동 페일오버 (CLUSTER FAILOVER)

- `mf_can_start = 1`이면 지연 없이 즉시 선거 시작
- `failover_auth_rank = 0` (항상 최고 우선순위)
- 마스터가 일시 정지되어 복제본이 오프셋 따라잡기를 기다림

---

### 2.11 configEpoch

단조 증가하는 버전 번호로, 슬롯 소유권 충돌을 해결한다.

| 상황 | 동작 |
|------|------|
| 정상 운영 | 마스터마다 고유한 configEpoch 유지 |
| 페일오버 | 새 마스터: `currentEpoch + 1`로 설정 |
| 충돌 감지 | 더 큰 epoch가 승리 |
| UPDATE 메시지 | 구식 노드에게 최신 슬롯 정보 전파 |

---

### 2.12 슬롯 마이그레이션

#### 마이그레이션 상태 설정

```
소스 노드: CLUSTER SETSLOT <slot> MIGRATING <dst-node-id>
  → migrating_slots_to[slot] = dst

목적지 노드: CLUSTER SETSLOT <slot> IMPORTING <src-node-id>
  → importing_slots_from[slot] = src
```

#### 리다이렉션 종류

| 에러 | 의미 | 클라이언트 처리 |
|------|------|----------------|
| `-MOVED <slot> <ip:port>` | 영구 이동 완료 | 새 노드에 직접 재전송 |
| `-ASK <slot> <ip:port>` | 마이그레이션 중 임시 | `ASKING` 선행 후 재전송 |
| `-TRYAGAIN` | 슬롯 불안정 | 잠시 후 재시도 |
| `-CROSSSLOT` | 키가 다른 슬롯에 걸침 | 클라이언트 로직 수정 필요 |
| `-CLUSTERDOWN` | 클러스터 장애 상태 | 복구 대기 |

#### 마이그레이션 완료

```
두 노드 모두: CLUSTER SETSLOT <slot> NODE <new-owner-id>
  → 슬롯 비트맵 업데이트
  → configEpoch 증가
  → migrating/importing 상태 해제
```

---

### 2.13 Atomic Slot Migration (ASM) - 신기능

`cluster_asm.h`에 정의된 플러그인 가능한 슬롯 마이그레이션 API.  
대안적 클러스터 구현체를 위한 원자적 슬롯 이전 프레임워크.

#### 이벤트 흐름

```
목적지 구현체                   Redis(목적지)           Redis(소스)               소스 구현체
     │                              │                       │                          │
     │──ASM_EVENT_IMPORT_START─────▶│                       │                          │
     │                              │──CLUSTER SYNCSLOTS──▶│                          │
     │                              │◀─── 스냅샷(RDB) ───────│                          │
     │                              │◀─── 복제 스트림 ────────│                          │
     │                              │                       │──ASM_EVENT_HANDOFF_PREP─▶│
     │                              │                       │◀─ASM_EVENT_HANDOFF───────│
     │                              │◀─── 스트림 드레인 ──────│                          │
     │◀──ASM_EVENT_TAKEOVER─────────│                       │                          │
     │──ASM_EVENT_DONE─────────────▶│                       │──ASM_EVENT_DONE─────────▶│
```

#### API

```c
// 구현체 → Redis: ASM 이벤트 요청
int clusterAsmProcess(const char *task_id, int event, void *arg, char **err);

// Redis → 구현체: 상태 변경 알림
int clusterAsmOnEvent(const char *task_id, int event, void *arg);
```

---

### 2.14 clusterUpdateState() - 클러스터 상태 계산

```c
// cluster_legacy.c:5151
void clusterUpdateState(void) {
    new_state = CLUSTER_OK;

    // FAIL 조건 확인:
    // 1. 슬롯이 없는 마스터 (unbound slot)
    // 2. FAIL 상태인 마스터
    // 3. 슬롯이 커버되지 않음
    if (조건 충족) new_state = CLUSTER_FAIL;

    // 재참여 지연: cluster_node_timeout (최소 1500ms)
    // CLUSTER_FAIL → CLUSTER_OK 전이 시 로그 출력
    server.cluster->state = new_state;
}
```

`clusterDoBeforeSleep()` 플래그:
```c
CLUSTER_TODO_HANDLE_FAILOVER    // 페일오버 처리
CLUSTER_TODO_UPDATE_STATE       // 상태 재계산
CLUSTER_TODO_SAVE_CONFIG        // 설정 저장
CLUSTER_TODO_FSYNC_CONFIG       // 설정 fsync
CLUSTER_TODO_HANDLE_MANUALFAILOVER // 수동 페일오버
CLUSTER_TODO_BROADCAST_PONG     // PONG 브로드캐스트
```

---

## 3. 핵심 설정 파라미터 요약

### 복제 관련

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `repl-backlog-size` | 1MB | 부분 재동기화용 백로그 크기 |
| `repl-backlog-ttl` | 3600s | 백로그 보존 시간 (복제본 없을 때) |
| `repl-diskless-sync` | no | RDB를 디스크 없이 소켓으로 전송 |
| `repl-diskless-sync-delay` | 5s | diskless 시작 전 대기 (복수 replica 대기) |
| `repl-diskless-sync-max-replicas` | 0 | diskless BGSAVE당 최대 replica 수 |
| `repl-timeout` | 60s | 복제 타임아웃 |
| `repl-ping-replica-period` | 10s | 마스터 → 복제본 PING 주기 |
| `min-replicas-to-write` | 0 | write 허용 최소 복제본 수 |
| `min-replicas-max-lag` | 10s | 최대 허용 복제 지연 |

### 클러스터 관련

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `cluster-node-timeout` | 15000ms | 노드 타임아웃 (PFAIL 기준) |
| `cluster-slave-validity-factor` | 10 | 페일오버 가능 data_age 계수 |
| `cluster-slave-no-failover` | no | 자동 페일오버 비활성화 |
| `cluster-require-full-coverage` | yes | 모든 슬롯 커버 필요 시 CLUSTER_OK |
| `cluster-migration-barrier` | 1 | 슬롯 없는 마스터 최소 복제본 수 |
| `cluster-announce-hostname` | "" | 공개 호스트명 |
| `cluster-ping-interval` | 0 | PING 전송 주기 (0=자동) |

---

## 4. 데이터 흐름 요약

### 복제 스트림 흐름

```
클라이언트 write 명령
    │
    ▼
마스터 실행 + AOF 기록
    │
    ├──▶ feedReplicationBuffer()
    │         │
    │         ▼
    │    repl_buffer_blocks (replBufBlock 리스트)
    │         │
    │         ├──▶ repl_backlog (histlen 관리)
    │         │
    │         └──▶ 각 SLAVE_STATE_ONLINE 복제본
    │                   │
    │                   ├── IO thread로 전송 오프로드
    │                   └── connWrite() → 복제본 소켓
    │
    ▼
복제본: 수신 → 실행 → REPLCONF ACK <offset> 전송
```

### 클러스터 명령 처리 흐름

```
클라이언트 요청
    │
    ▼
getNodeByQuery() → 슬롯 계산
    │
    ├── 내 슬롯? → 실행
    ├── migrating? → ASKING 없으면 ASK redirect
    ├── 다른 노드 슬롯? → MOVED redirect
    └── 슬롯 미배정? → CLUSTERDOWN
```

---

## 5. 정리: 복제 vs 클러스터 역할 비교

| 항목 | 복제 (Replication) | 클러스터 (Cluster) |
|------|-------------------|--------------------|
| 목적 | 데이터 이중화, HA | 수평 확장, 샤딩 |
| 위상 | 마스터-복제본 계층 | 마스터들의 P2P 메시 |
| 동기화 | PSYNC (비동기) | 슬롯 기반 파티셔닝 |
| 페일오버 | 수동 또는 Sentinel | 자동 (Gossip + 투표) |
| 데이터 분산 | 전체 데이터 복제 | CRC16 슬롯 분산 |
| 포트 | 기본 포트만 | 기본 포트 + 버스 포트 |
| 구현 | replication.c | cluster_legacy.c |
