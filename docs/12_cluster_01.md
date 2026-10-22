# Redis 클러스터 & 센티널 (Cluster & Sentinel) 소스 분석

## 개요

Redis는 고가용성(HA)과 수평 확장을 위해 두 가지 독립적인 메커니즘을 제공한다.

| 메커니즘 | 주요 파일 | 목적 |
|----------|----------|------|
| **Redis Cluster** | `cluster.c`, `cluster_legacy.c`, `cluster.h`, `cluster_legacy.h` | 데이터 샤딩 + 자체 페일오버 |
| **Sentinel** | `sentinel.c` | 단일 마스터 모니터링 + 자동 페일오버 |

---

## 1. Redis Cluster

### 1.1 해시 슬롯 (Hash Slot)

**기본 상수** (`cluster.h`):

```c
#define CLUSTER_SLOT_MASK_BITS 14
#define CLUSTER_SLOTS          (1<<14)   // = 16384
#define CLUSTER_SLOT_MASK      0x3FFF
```

**슬롯 계산** (`cluster.h:60`):

```c
static inline unsigned int keyHashSlot(const char *key, int keylen) {
    // Hash tag 지원: {tag} 부분만 해싱
    if (key contains '{...}' with non-empty content)
        return crc16(tag_part) & 0x3FFF;
    return crc16(key, keylen) & 0x3FFF;
}
```

**해시 태그 규칙**:
- `{user}.1`, `{user}.2` → 동일 슬롯 (multi-key 연산 가능)
- `{` 없거나 `{}` 비어있으면 전체 키 해싱

**리다이렉션 에러 코드**:

| 상수 | 값 | 에러 | 의미 |
|------|----|------|------|
| `CLUSTER_REDIR_NONE` | 0 | — | 이 노드가 처리 가능 |
| `CLUSTER_REDIR_CROSS_SLOT` | 1 | `-CROSSSLOT` | 여러 슬롯에 걸친 요청 |
| `CLUSTER_REDIR_UNSTABLE` | 2 | `-TRYAGAIN` | 슬롯 마이그레이션 중 |
| `CLUSTER_REDIR_ASK` | 3 | `-ASK` | 마이그레이션 임시 리다이렉션 |
| `CLUSTER_REDIR_MOVED` | 4 | `-MOVED` | 영구 슬롯 이동 |
| `CLUSTER_REDIR_DOWN_STATE` | 5 | `-CLUSTERDOWN` | 클러스터 전체 장애 |
| `CLUSTER_REDIR_DOWN_UNBOUND` | 6 | `-CLUSTERDOWN` | 슬롯 미할당 |
| `CLUSTER_REDIR_TRIMMING` | 8 | `-TRYAGAIN` | 슬롯 트리밍 중 |

### 1.2 핵심 자료구조

#### `_clusterNode` (`cluster_legacy.h:299`)

```c
struct _clusterNode {
    char name[CLUSTER_NAMELEN];        // 40자 hex SHA1 노드 ID
    char shard_id[CLUSTER_NAMELEN];    // 샤드 ID (40자 hex)
    int flags;                         // CLUSTER_NODE_* 플래그
    uint64_t configEpoch;              // 마지막 config epoch
    unsigned char slots[CLUSTER_SLOTS/8]; // 슬롯 비트맵 (2048 bytes)
    int numslots;                      // 보유 슬롯 수
    int numslaves;
    clusterNode **slaves;
    clusterNode *slaveof;
    mstime_t ping_sent, pong_received; // 핑/퐁 타임스탬프
    mstime_t fail_time;                // FAIL 플래그 설정 시각
    mstime_t voted_time;               // 마지막 투표 시각
    long long repl_offset;             // 복제 오프셋
    char ip[NET_IP_STR_LEN];
    sds hostname, human_nodename;
    int tcp_port, tls_port, cport;
    clusterLink *link;                 // 아웃바운드 연결
    clusterLink *inbound_link;         // 인바운드 연결
    list *fail_reports;                // 장애 보고 목록
};
```

**노드 플래그 비트**:

| 플래그 | 값 | 의미 |
|--------|----|------|
| `CLUSTER_NODE_MASTER` | 1 | 마스터 노드 |
| `CLUSTER_NODE_SLAVE` | 2 | 슬레이브 노드 |
| `CLUSTER_NODE_PFAIL` | 4 | 주관적 장애 (확인 필요) |
| `CLUSTER_NODE_FAIL` | 8 | 객관적 장애 (확정) |
| `CLUSTER_NODE_MYSELF` | 16 | 자기 자신 |
| `CLUSTER_NODE_HANDSHAKE` | 32 | 핸드셰이크 미완료 |
| `CLUSTER_NODE_NOADDR` | 64 | 주소 미확인 |
| `CLUSTER_NODE_MEET` | 128 | MEET 메시지 전송 예정 |
| `CLUSTER_NODE_MIGRATE_TO` | 256 | 슬레이브 마이그레이션 대상 |
| `CLUSTER_NODE_NOFAILOVER` | 512 | 자동 페일오버 비활성 |

#### `clusterState` (`cluster_legacy.h:335`)

```c
struct clusterState {
    clusterNode *myself;
    uint64_t currentEpoch;          // 클러스터 논리 시계
    int state;                       // CLUSTER_OK / CLUSTER_FAIL
    int size;                        // 슬롯 보유 마스터 수
    dict *nodes;                     // name → clusterNode
    dict *shards;                    // shard_id → node list
    dict *nodes_black_list;
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];   // 슬롯 이동 목적지
    clusterNode *importing_slots_from[CLUSTER_SLOTS]; // 슬롯 수입 출처
    clusterNode *slots[CLUSTER_SLOTS]; // 슬롯 → 담당 노드 매핑
    char internal_secret[40];        // 내부 인증 시크릿
    /* 페일오버 투표 상태 */
    mstime_t failover_auth_time;
    int failover_auth_count;
    int failover_auth_rank;
    uint64_t failover_auth_epoch;
    int todo_before_sleep;           // beforeSleep에서 처리할 작업
    long long stats_bus_messages_sent[11];
    long long stats_bus_messages_received[11];
};
```

#### `clusterLink` (`cluster_legacy.h:45`)

```c
typedef struct clusterLink {
    mstime_t ctime;
    connection *conn;
    list *send_msg_queue;
    size_t head_msg_send_offset;
    char *rcvbuf;
    size_t rcvbuf_len, rcvbuf_alloc;
    clusterNode *node;
    int inbound;
} clusterLink;
```

### 1.3 클러스터 버스 프로토콜

**포트**: `cluster_bus_port = base_port + CLUSTER_PORT_INCR(10000)`

**메시지 헤더** (`clusterMsg`, `cluster_legacy.h:231`):

```c
typedef struct {
    char sig[4];         // "RCmb" (Redis Cluster message bus)
    uint32_t totlen;
    uint16_t ver;        // CLUSTER_PROTO_VER = 1
    uint16_t port;
    uint16_t type;       // CLUSTERMSG_TYPE_*
    uint16_t count;      // gossip 항목 수
    uint64_t currentEpoch;
    uint64_t configEpoch;
    uint64_t offset;     // 복제 오프셋
    char sender[40];     // 발신 노드 이름
    unsigned char myslots[CLUSTER_SLOTS/8];  // 2048 bytes 슬롯 비트맵
    char slaveof[40];
    char myip[NET_IP_STR_LEN];
    uint16_t extensions;
    // ...
    uint16_t cport;
    uint16_t flags;
    unsigned char state;
    unsigned char mflags[3];
    union clusterMsgData data;
} clusterMsg;
```

**`static_assert`로 오프셋 고정** (호환성 보장):
- `sig` @ 0, `totlen` @ 4, `type` @ 12, `currentEpoch` @ 16, `myslots` @ 80, `data` @ 2256

**메시지 타입 11종**:

| 타입 | 값 | 설명 |
|------|----|------|
| `CLUSTERMSG_TYPE_PING` | 0 | 주기적 상태 교환 |
| `CLUSTERMSG_TYPE_PONG` | 1 | PING 응답 |
| `CLUSTERMSG_TYPE_MEET` | 2 | 클러스터 참가 요청 |
| `CLUSTERMSG_TYPE_FAIL` | 3 | 노드 장애 브로드캐스트 |
| `CLUSTERMSG_TYPE_PUBLISH` | 4 | Pub/Sub 전파 |
| `CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST` | 5 | 페일오버 투표 요청 |
| `CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK` | 6 | 페일오버 투표 승인 |
| `CLUSTERMSG_TYPE_UPDATE` | 7 | 슬롯 설정 업데이트 |
| `CLUSTERMSG_TYPE_MFSTART` | 8 | 수동 페일오버 시작 |
| `CLUSTERMSG_TYPE_MODULE` | 9 | 모듈 메시지 |
| `CLUSTERMSG_TYPE_PUBLISHSHARD` | 10 | 샤드 Pub/Sub 전파 |

**Ping 확장 메시지 타입** (`CLUSTERMSG_FLAG0_EXT_DATA`):
- `CLUSTERMSG_EXT_TYPE_HOSTNAME` — 호스트명 공유
- `CLUSTERMSG_EXT_TYPE_HUMAN_NODENAME` — 사람이 읽을 수 있는 노드명
- `CLUSTERMSG_EXT_TYPE_FORGOTTEN_NODE` — 블랙리스트 노드 (TTL 포함)
- `CLUSTERMSG_EXT_TYPE_SHARDID` — 샤드 ID
- `CLUSTERMSG_EXT_TYPE_INTERNALSECRET` — 내부 인증 시크릿

**Gossip 데이터** (`clusterMsgDataGossip`): 각 PING/PONG에 랜덤 노드 목록 포함

```c
typedef struct {
    char nodename[40];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[NET_IP_STR_LEN];
    uint16_t port, cport, flags, pport;
} clusterMsgDataGossip;
```

### 1.4 `clusterCron()` 주기 작업 (`cluster_legacy.c:4731`)

매 100ms (`server.hz`) 호출. 주요 작업:

1. **링크 재연결**: `clusterNodeCronHandleReconnect()` — 끊긴 노드 재연결
2. **랜덤 PING** (10 iteration마다): 5개 랜덤 노드 중 pong_received 가장 오래된 노드에 PING
3. **PFAIL 감지**: `ping_sent > cluster_node_timeout` → `CLUSTER_NODE_PFAIL` 설정
4. **고아 마스터 감지**: 슬레이브 없는 마스터 카운트 → 슬레이브 마이그레이션 트리거
5. **슬레이브 페일오버**: `clusterHandleSlaveFailover()` 호출
6. **수동 페일오버**: `clusterHandleManualFailover()` 처리
7. **클러스터 상태 업데이트**: `clusterUpdateState()` — CLUSTER_OK/FAIL 재판단

**`todo_before_sleep` 플래그** (각 작업 완료 후 `clusterBeforeSleep()`에서 처리):

```c
#define CLUSTER_TODO_HANDLE_FAILOVER      (1<<0)
#define CLUSTER_TODO_UPDATE_STATE         (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG          (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG         (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)
#define CLUSTER_TODO_BROADCAST_PONG       (1<<5)
```

### 1.5 장애 감지 및 페일오버

#### PFAIL → FAIL 전파

```
노드 A가 노드 X에 ping_sent 후 cluster_node_timeout 초과
  → X에 CLUSTER_NODE_PFAIL 설정
  → 다른 노드들로부터 X가 PFAIL이라는 gossip 수집
  → quorum(size/2+1)개 PFAIL 보고 수집 시
  → clusterSendFail(X) 브로드캐스트
  → 모든 노드가 X에 CLUSTER_NODE_FAIL 설정
```

**FAIL 보고 유효시간**: `CLUSTER_FAIL_REPORT_VALIDITY_MULT(2) × cluster_node_timeout`

#### 슬레이브 페일오버 (`clusterHandleSlaveFailover()`, `cluster_legacy.c:4298`)

**사전 조건**:
1. 자신이 슬레이브
2. 마스터가 `CLUSTER_NODE_FAIL` (또는 수동 페일오버)
3. `cluster_slave_no_failover = 0`
4. 마스터가 슬롯 보유 중

**필요 정족수**: `needed_quorum = (cluster->size / 2) + 1`

**타임아웃**: `auth_timeout = max(cluster_node_timeout * 2, 2000ms)`

**랭크 기반 지연** (복제 뒤처진 슬레이브 페널티):
```
failover_auth_time = mstime()
    + 500ms (FAIL 메시지 전파 대기)
    + random() % 500ms (분산)
    + rank * 1000ms (랭크 낮을수록 = 오프셋 낮을수록 지연)
```

**투표 흐름**:
```
슬레이브 → FAILOVER_AUTH_REQUEST 브로드캐스트
마스터들 → configEpoch 검증 후 FAILOVER_AUTH_ACK 응답
슬레이브 → quorum 이상 ACK 수집 시 마스터로 승격
  → clusterFailoverReplaceYourMaster()
  → CLUSTER_NODE_MASTER 설정, slaveof 해제
  → 슬롯 비트맵 인수
  → PONG 브로드캐스트로 새 설정 전파
```

**수동 페일오버** (`CLUSTER FAILOVER`): `CLUSTER_MF_TIMEOUT=5000ms`, 랭크 지연 없음

**슬레이브 마이그레이션**: `CLUSTER_SLAVE_MIGRATION_DELAY=5000ms`, 고아 마스터에 슬레이브 자동 이전

### 1.6 슬롯 마이그레이션

```
CLUSTER SETSLOT <slot> MIGRATING <dst-node-id>  → migrating_slots_to[slot] = dst
CLUSTER SETSLOT <slot> IMPORTING <src-node-id>  → importing_slots_from[slot] = src

클라이언트가 마이그레이션 중 슬롯의 키에 접근:
  - 키가 src에 있으면: 정상 처리
  - 키가 src에 없으면: -ASK <slot> <dst-ip>:<dst-port> 응답
  - 클라이언트: ASKING → dst에 재전송
```

**Atomic Slot Migration (ASM)** (`cluster.h:276`): 대안 클러스터 구현을 위한 API. 14가지 이벤트 (`ASM_EVENT_*`)로 import/migrate 라이프사이클 관리.

### 1.7 슬롯 범위 구조 (`slotRangeArray`)

```c
typedef struct slotRange { unsigned short start, end; } slotRange;
typedef struct slotRangeArray {
    int num_ranges;
    slotRange ranges[];
} slotRangeArray;
```

압축된 슬롯 표현 (연속된 슬롯을 범위로 표현). `slotRangeArraySortAndMerge()`, `slotRangeArrayToString()` 등 헬퍼 함수 제공.

---

## 2. Sentinel

### 2.1 Sentinel 기본 정보

```c
#define REDIS_SENTINEL_PORT 26379
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
```

### 2.2 타이밍 상수

| 상수 | 기본값 | 의미 |
|------|--------|------|
| `sentinel_ping_period` | 1000ms | PING 간격 |
| `sentinel_info_period` | 10000ms | INFO 수집 간격 |
| `sentinel_ask_period` | 1000ms | 다른 Sentinel에 상태 질의 간격 |
| `sentinel_publish_period` | 2000ms | HELLO 채널 발행 간격 |
| `sentinel_default_down_after` | 30000ms | S_DOWN 판정 기준 |
| `sentinel_tilt_trigger` | 2000ms | TILT 모드 진입 기준 |
| `sentinel_tilt_period` | 30000ms | TILT 모드 유지 시간 |
| `sentinel_election_timeout` | 10000ms | 리더 선출 타임아웃 |
| `sentinel_slave_reconf_timeout` | 10000ms | 슬레이브 재설정 타임아웃 |
| `sentinel_default_failover_timeout` | 180000ms(3분) | 페일오버 전체 타임아웃 |

### 2.3 핵심 자료구조

#### `instanceLink` (`sentinel.c:135`)

```c
typedef struct instanceLink {
    int refcount;           // 공유 참조 카운트 (Sentinel 간 연결 공유)
    int disconnected;
    int pending_commands;
    redisAsyncContext *cc;  // 커맨드용 hiredis 비동기 컨텍스트
    redisAsyncContext *pc;  // Pub/Sub용 hiredis 비동기 컨텍스트
    mstime_t cc_conn_time, pc_conn_time;
    mstime_t last_avail_time;  // 마지막 유효 응답
    mstime_t act_ping_time;    // 응답 대기 중인 PING 전송 시각
    mstime_t last_ping_time;
    mstime_t last_pong_time;
    mstime_t last_reconn_time;
} instanceLink;
```

**연결 공유**: N개 Sentinel이 M개 마스터를 감시할 때, Sentinel 간 연결은 `refcount`로 공유 → N×M 대신 N개 연결

#### `sentinelRedisInstance` (`sentinel.c:162`)

```c
typedef struct sentinelRedisInstance {
    int flags;            // SRI_* 플래그
    char *name;           // 마스터 이름
    char *runid;
    uint64_t config_epoch;
    sentinelAddr *addr;
    instanceLink *link;
    // ...
    dict *sentinels;      // 같은 마스터를 감시하는 다른 Sentinel들
    dict *slaves;         // 마스터의 슬레이브들
    unsigned int quorum;  // O_DOWN 판정 필요 Sentinel 수
    int parallel_syncs;   // 동시 재설정 슬레이브 수
    // 페일오버 상태
    int failover_state;
    mstime_t failover_state_change_time;
    uint64_t failover_epoch;
    sentinelRedisInstance *promoted_slave;
} sentinelRedisInstance;
```

**SRI 플래그**:

| 플래그 | 값 | 의미 |
|--------|----|------|
| `SRI_MASTER` | 1 | 마스터 인스턴스 |
| `SRI_SLAVE` | 2 | 슬레이브 인스턴스 |
| `SRI_SENTINEL` | 4 | Sentinel 인스턴스 |
| `SRI_S_DOWN` | 8 | 주관적 다운 |
| `SRI_O_DOWN` | 16 | 객관적 다운 |
| `SRI_MASTER_DOWN` | 32 | 다른 Sentinel이 마스터 다운 보고 |
| `SRI_FAILOVER_IN_PROGRESS` | 64 | 페일오버 진행 중 |
| `SRI_PROMOTED` | 128 | 승격 선정된 슬레이브 |
| `SRI_RECONF_SENT` | 256 | SLAVEOF newmaster 전송 완료 |
| `SRI_RECONF_INPROG` | 512 | 슬레이브 동기화 진행 중 |
| `SRI_RECONF_DONE` | 1024 | 재설정 완료 |
| `SRI_FORCE_FAILOVER` | 2048 | 마스터가 살아있어도 강제 페일오버 |
| `SRI_MASTER_REBOOT` | 8192 | 마스터 재부팅 감지 |

### 2.4 장애 감지

#### 주관적 다운 (S_DOWN) (`sentinel.c:4526`)

```c
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    elapsed = mstime() - ri->link->act_ping_time;

    // S_DOWN 조건:
    // 1) elapsed > down_after_period (응답 없음)
    // 2) 마스터인데 슬레이브 역할 보고 (role 불일치)
    // 3) SRI_MASTER_REBOOT 플래그 + master_reboot_down_after_period 초과
    if (elapsed > down_after_period || role_mismatch || reboot_timeout) {
        ri->flags |= SRI_S_DOWN;
        sentinelEvent(LL_WARNING, "+sdown", ri, "%@");
    }
}
```

**링크 자동 재연결 조건**:
- 커맨드 링크: `act_ping_time` 이 `down_after_period/2` 초과 시
- Pub/Sub 링크: `pc_last_activity` 가 `publish_period * 3` 초과 시

#### 객관적 다운 (O_DOWN) (`sentinel.c:4600`)

```c
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    if (master->flags & SRI_S_DOWN) {
        quorum = 1;  // 자신
        // 다른 Sentinel들의 SRI_MASTER_DOWN 플래그 카운트
        foreach sentinel: if (SRI_MASTER_DOWN) quorum++;
        if (quorum >= master->quorum)
            master->flags |= SRI_O_DOWN;
    }
}
```

**O_DOWN은 약한 쿼럼**: 메시지 지연으로 동시 합의 보장 없음, Raft 리더 선출로 실제 페일오버 진행

### 2.5 페일오버 상태 머신 (`sentinel.c:5320`)

```
SENTINEL_FAILOVER_STATE_NONE (0)
         ↓ O_DOWN 감지
SENTINEL_FAILOVER_STATE_WAIT_START (1)
  - sentinelStartFailover(): current_epoch++, failover_start_time 설정
  - 다른 Sentinel에 is-master-down-by-addr 요청
  - sentinelGetLeader()로 리더 선출
         ↓ 리더로 선출됨
SENTINEL_FAILOVER_STATE_SELECT_SLAVE (2)
  - sentinelSelectSlave(): 승격할 슬레이브 선정
         ↓ 슬레이브 선정
SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE (3)
  - sentinelSendSlaveOf(ri, NULL): SLAVEOF NO ONE 전송
         ↓ 슬레이브가 마스터로 전환
SENTINEL_FAILOVER_STATE_WAIT_PROMOTION (4)
  - INFO로 role: master 확인
         ↓ 승격 확인
SENTINEL_FAILOVER_STATE_RECONF_SLAVES (5)
  - parallel_syncs 단위로 나머지 슬레이브에 SLAVEOF newmaster
         ↓ 모든 슬레이브 재설정
SENTINEL_FAILOVER_STATE_UPDATE_CONFIG (6)
  - 이 Sentinel의 마스터 설정 업데이트 (승격된 슬레이브 모니터링)
```

**`sentinelHandleRedisInstance()` 흐름** (`sentinel.c:5368`):
```
sentinelReconnectInstance()          ← 연결 유지
sentinelSendPeriodicCommands()       ← PING / INFO / HELLO 발행
if (TILT 모드) return                ← 시계 이상 시 감시만
sentinelCheckSubjectivelyDown()      ← S_DOWN 판정
if (마스터):
  sentinelCheckObjectivelyDown()     ← O_DOWN 판정
  sentinelStartFailoverIfNeeded()    ← 페일오버 시작 여부
  sentinelAskMasterStateToOtherSentinels() ← 상태 질의
  sentinelFailoverStateMachine()     ← FSM 진행
```

### 2.6 리더 선출: Raft 투표 (`sentinel.c:4794`)

```c
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch) {
    voters = dictSize(master->sentinels) + 1;  // 전체 Sentinel 수

    // 1. 다른 Sentinel들의 투표 집계
    // 2. 최다 득표자 찾기 (winner)
    // 3. 자신의 투표 캐스팅
    //    - winner 존재: winner에 투표
    //    - 없으면: 자신(sentinel.myid)에 투표

    voters_quorum = voters / 2 + 1;
    // 당선 조건: max_votes >= voters_quorum AND max_votes >= quorum
    if (winner && max_votes < voters_quorum || max_votes < master->quorum)
        winner = NULL;
    return winner;  // NULL이면 리더 없음 (다음 epoch에 재시도)
}
```

### 2.7 슬레이브 선정 기준 (`sentinel.c:5051`)

필터링 (제외 대상):
- S_DOWN / O_DOWN / DISCONNECTED
- 마지막 PING 응답이 `5 × ping_period` 초과
- info_refresh가 `3 × info_period` 초과
- `master_link_down_time` 이 `(now - s_down_since_time) + (down_after * 10)` 초과
- `slave_priority == 0`

**정렬 기준** (우선순위 순):
1. `slave_priority` 낮을수록 우선 (기본값 100)
2. 처리된 복제 오프셋 클수록 우선 (데이터 최신)
3. `runid` 사전순 (동점 결정)

### 2.8 `sentinelSendSlaveOf()` 트랜잭션

```
MULTI
  SLAVEOF <newmaster-host> <newmaster-port>  (또는 NO ONE)
  CONFIG REWRITE                              (설정 파일에 저장)
  CLIENT KILL TYPE normal                     (기존 클라이언트 끊기)
EXEC
```

응답은 무시 → 다음 INFO 출력으로 효과 확인

### 2.9 TILT 모드

시계가 비정상적으로 변경되었을 때 진입:
- `sentinel_tilt_trigger(2000ms)` 이상 시계가 뒤로 감 / 빠르게 앞으로 감
- TILT 중: 모니터링만, 페일오버 중단
- `sentinel_tilt_period(30000ms)` 후 자동 해제

### 2.10 스크립트 지원

외부 알림/자동화 스크립트:
- `sentinel notification-script` — 이벤트 발생 시 실행
- `sentinel client-reconfig-script` — 페일오버 후 클라이언트 재설정

제한: `SENTINEL_SCRIPT_MAX_RUNNING=16`, `SENTINEL_SCRIPT_MAX_QUEUE=256`, 실행 시간 60초, 재시도 10회, 재시도 간격 30초

---

## 3. Cluster vs Sentinel 비교

| 항목 | Redis Cluster | Sentinel |
|------|---------------|----------|
| **목적** | 샤딩 + HA | HA만 |
| **토폴로지** | 다중 마스터 | 단일 마스터 |
| **페일오버 주체** | 슬레이브 자체 투표 | Sentinel 리더 |
| **투표 방식** | `clusterSendFailoverAuthIfNeeded()` | Raft 리더 선출 |
| **클라이언트** | MOVED/ASK 리다이렉션 처리 필요 | 새 마스터 주소만 알면 됨 |
| **설정 파일** | nodes.conf (자동) | sentinel.conf |
| **최소 노드** | 마스터 3 + 슬레이브 3 | Sentinel 3 |
| **데이터 분산** | 16384 슬롯 | 없음 |

---

## 4. 전체 흐름 요약

### Cluster 페일오버 흐름

```
A → B ping, 응답 없음 > cluster_node_timeout
  → B에 PFAIL 설정
  → PING gossip으로 다른 노드들에게 전파
  → quorum(size/2+1)개 PFAIL 보고 수집
  → A가 B에 CLUSTER_NODE_FAIL 설정 + FAIL 브로드캐스트
B의 슬레이브 C:
  → 랭크별 지연 후 FAILOVER_AUTH_REQUEST 브로드캐스트
  → quorum 이상 ACK 수집
  → clusterFailoverReplaceYourMaster()
  → 슬롯 인수 + 마스터 전환 + PONG 브로드캐스트
```

### Sentinel 페일오버 흐름

```
Sentinel S1: master M에 PING → 응답 없음 > down_after_period
  → M에 SRI_S_DOWN 설정
  → 다른 Sentinel에 is-master-down-by-addr 질의
  → quorum 이상 SRI_MASTER_DOWN 응답 → SRI_O_DOWN 설정
  → sentinelStartFailover(): current_epoch++
  → sentinelGetLeader(): Raft 투표 → S1 리더 선출
  → sentinelSelectSlave(): 슬레이브 R 선정 (priority/offset/runid)
  → R에 SLAVEOF NO ONE (MULTI/EXEC 트랜잭션)
  → INFO로 role: master 확인
  → 나머지 슬레이브에 SLAVEOF R-ip R-port
  → HELLO 채널로 새 마스터 주소 공표
```

---

**참조 파일**:
- [src/cluster.h](redis/src/cluster.h) — 클러스터 공개 API (357줄)
- [src/cluster_legacy.h](redis/src/cluster_legacy.h) — 핵심 자료구조 및 메시지 형식 (386줄)
- [src/cluster_legacy.c](redis/src/cluster_legacy.c) — 클러스터 구현 (6581줄)
- [src/cluster.c](redis/src/cluster.c) — 클러스터 공통 구현 (2348줄)
- [src/cluster_asm.c](redis/src/cluster_asm.c) — Atomic Slot Migration (3809줄)
- [src/sentinel.c](redis/src/sentinel.c) — Sentinel 전체 구현 (5474줄)
