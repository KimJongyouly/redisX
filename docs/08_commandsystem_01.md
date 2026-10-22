# Redis 커맨드 시스템 상세 분석

## 개요

Redis의 커맨드 시스템은 JSON 선언 → 코드 자동생성 → 런타임 등록 → 요청 처리의 4단계 파이프라인으로 구성된다. 커맨드 하나를 추가하려면 JSON 파일만 작성하면 나머지는 자동화된다.

---

## 1. 커맨드 정의 파일 구조

### JSON 선언 (`src/commands/*.json`, 총 425개 파일)

각 커맨드는 하나의 JSON 파일로 선언된다. `src/commands/set.json` 예시:

```json
{
  "SET": {
    "summary": "Sets the string value of a key...",
    "complexity": "O(1)",
    "group": "string",
    "since": "1.0.0",
    "arity": -3,
    "function": "setCommand",
    "get_keys_function": "setGetKeys",
    "command_flags": ["WRITE", "DENYOOM"],
    "acl_categories": ["STRING"],
    "key_specs": [...],
    "reply_schema": {...},
    "arguments": [...]
  }
}
```

**주요 필드 설명:**

| 필드 | 의미 |
|------|------|
| `arity` | 인수 개수. 음수면 최소 개수 (예: -3 → argc ≥ 3) |
| `function` | C 함수 이름 (커맨드 구현체) |
| `get_keys_function` | 키 추출 함수 (클러스터 라우팅용, 선택사항) |
| `command_flags` | CMD_* 플래그 목록 |
| `acl_categories` | ACL 카테고리 목록 |
| `key_specs` | 키 위치 명세 (begin_search + find_keys) |
| `history` | 버전별 변경 이력 |
| `arguments` | 인수 스키마 (타입, 토큰, 옵션 여부 등) |

### 커맨드 그룹 (18개)

```c
/* src/commands.def에서 자동 생성 */
const char *COMMAND_GROUP_STR[] = {
    "generic", "string", "list", "set", "sorted-set", "hash",
    "pubsub", "transactions", "connection", "server", "scripting",
    "hyperloglog", "cluster", "sentinel", "geo", "stream", "bitmap", "module"
};
```

---

## 2. 코드 자동 생성 파이프라인

```
src/commands/*.json
        ↓
utils/generate-command-code.py
        ↓
src/commands.def    ← 커맨드별 정적 초기화 매크로
src/commands.c      ← redisCommandTable[] 배열 (include commands.def)
```

### `commands.def` 생성 구조

```c
/* BITCOUNT 커맨드 예시 */
commandHistory BITCOUNT_History[] = {
    {"7.0.0","Added the `BYTE|BIT` option."},
};

keySpec BITCOUNT_Keyspecs[1] = {
    {NULL, CMD_KEY_RO|CMD_KEY_ACCESS,
     KSPEC_BS_INDEX, .bs.index={1},
     KSPEC_FK_RANGE, .fk.range={0,1,0}}
};

struct COMMAND_ARG BITCOUNT_Args[] = {
    {MAKE_ARG("key", ARG_TYPE_KEY, 0, ...)},
    {MAKE_ARG("range", ARG_TYPE_BLOCK, -1, ...)},
    ...
};
```

### `commands.c`의 역할

```c
/* src/commands.c (13줄 — 단순히 include) */
#include "commands.def"
```

`commands.def`가 실제 커맨드 테이블 전체를 담고 있다.

---

## 3. `redisCommand` 구조체 (`src/server.h:2875`)

```c
struct redisCommand {
    /* ── 선언적 데이터 (JSON에서 생성) ── */
    const char *declared_name;    /* "SET", "CLIENT|INFO" 등 */
    const char *summary;          /* 한 줄 설명 */
    const char *complexity;       /* "O(1)" 등 */
    const char *since;            /* 도입 버전 */
    int doc_flags;                /* CMD_DOC_* */
    const char *replaced_by;      /* deprecated 대체 커맨드 */
    const char *deprecated_since;
    redisCommandGroup group;      /* 18개 그룹 중 하나 */
    commandHistory *history;      /* 버전별 변경 이력 */
    int num_history;
    const char **tips;            /* 클라이언트 힌트 */
    int num_tips;
    redisCommandProc *proc;       /* ★ 실제 C 함수 포인터 */
    int arity;                    /* 인수 개수 */
    uint64_t flags;               /* CMD_WRITE | CMD_READONLY | ... */
    uint64_t acl_categories;      /* ACL_CATEGORY_* 비트 마스크 */
    keySpec *key_specs;           /* 키 위치 명세 배열 */
    int key_specs_num;
    redisGetKeysProc *getkeys_proc; /* 동적 키 추출 함수 */
    int num_args;
    struct redisCommand *subcommands; /* 서브커맨드 배열 */
    struct redisCommandArg *args;     /* 인수 스키마 배열 */

    /* ── 런타임 데이터 (실행 시 갱신) ── */
    long long microseconds;       /* 총 실행 시간 누적 */
    long long calls;              /* 호출 횟수 */
    long long rejected_calls;     /* 거부된 호출 횟수 */
    long long failed_calls;       /* 실패한 호출 횟수 */
    long long slowlog_count;      /* slowlog 기록 횟수 */
    int id;                       /* ACL 비트맵 인덱스 */
    sds fullname;                 /* "CLIENT|INFO" 형태 풀네임 */
    struct hdr_histogram *latency_histogram; /* ns 단위 레이턴시 분포 */
    keySpec legacy_range_key_spec;
    dict *subcommands_dict;       /* 서브커맨드 빠른 조회용 dict */
    struct redisCommand *parent;  /* 부모 커맨드 포인터 */
    struct RedisModuleCommand *module_cmd; /* 모듈 커맨드용 */
};
```

---

## 4. CMD_* 플래그 (30개)

```c
/* 동작 관련 */
#define CMD_WRITE           (1ULL<<0)   /* 키 공간 수정 가능 */
#define CMD_READONLY        (1ULL<<1)   /* 키 읽기만 */
#define CMD_DENYOOM         (1ULL<<2)   /* OOM 시 실행 거부 */
#define CMD_ADMIN           (1ULL<<4)   /* 관리자 커맨드 */
#define CMD_PUBSUB          (1ULL<<5)   /* Pub/Sub 관련 */
#define CMD_NOSCRIPT        (1ULL<<6)   /* 스크립트 내 실행 불가 */
#define CMD_BLOCKING        (1ULL<<8)   /* 블로킹 가능 커맨드 */
#define CMD_LOADING         (1ULL<<9)   /* DB 로딩 중 실행 허용 */
#define CMD_STALE           (1ULL<<10)  /* stale 슬레이브에서 허용 */
#define CMD_SKIP_MONITOR    (1ULL<<11)  /* MONITOR에 기록 안 함 */
#define CMD_SKIP_SLOWLOG    (1ULL<<12)  /* slowlog에 기록 안 함 */
#define CMD_FAST            (1ULL<<14)  /* O(1)/O(log N) 빠른 커맨드 */
#define CMD_NO_AUTH         (1ULL<<15)  /* 인증 불필요 (AUTH, HELLO) */
#define CMD_MAY_REPLICATE   (1ULL<<16)  /* 복제 트래픽 발생 가능 */
#define CMD_NO_MULTI        (1ULL<<24)  /* MULTI/EXEC 내 사용 불가 */
#define CMD_MOVABLE_KEYS    (1ULL<<25)  /* 정적 키 스펙으로 커버 안 됨 */
#define CMD_ALLOW_BUSY      (1ULL<<26)  /* 바쁜 상태에서도 허용 */
#define CMD_INTERNAL        (1ULL<<29)  /* 내부 전용 (ACL 우회) */
```

---

## 5. ACL 카테고리 (21개)

```c
#define ACL_CATEGORY_KEYSPACE    (1ULL<<0)
#define ACL_CATEGORY_READ        (1ULL<<1)
#define ACL_CATEGORY_WRITE       (1ULL<<2)
#define ACL_CATEGORY_SET         (1ULL<<3)
#define ACL_CATEGORY_SORTEDSET   (1ULL<<4)
#define ACL_CATEGORY_LIST        (1ULL<<5)
#define ACL_CATEGORY_HASH        (1ULL<<6)
#define ACL_CATEGORY_STRING      (1ULL<<7)
#define ACL_CATEGORY_BITMAP      (1ULL<<8)
#define ACL_CATEGORY_HYPERLOGLOG (1ULL<<9)
#define ACL_CATEGORY_GEO         (1ULL<<10)
#define ACL_CATEGORY_STREAM      (1ULL<<11)
#define ACL_CATEGORY_PUBSUB      (1ULL<<12)
#define ACL_CATEGORY_ADMIN       (1ULL<<13)
#define ACL_CATEGORY_FAST        (1ULL<<14)
#define ACL_CATEGORY_SLOW        (1ULL<<15)
#define ACL_CATEGORY_BLOCKING    (1ULL<<16)
#define ACL_CATEGORY_DANGEROUS   (1ULL<<17)
#define ACL_CATEGORY_CONNECTION  (1ULL<<18)
#define ACL_CATEGORY_TRANSACTION (1ULL<<19)
#define ACL_CATEGORY_SCRIPTING   (1ULL<<20)
```

**자동 상속 규칙:**
- `CMD_READONLY` → `@read` 자동 포함
- `CMD_WRITE` → `@write` 자동 포함
- `CMD_FAST` → `@fast` 자동 포함
- `CMD_ADMIN` → `@admin` + `@dangerous` 자동 포함
- `CMD_PUBSUB` → `@pubsub` 자동 포함
- `CMD_FAST` 없음 → `@slow` 자동 포함

---

## 6. 키 명세 시스템 (`keySpec`)

클러스터 라우팅과 ACL 키 권한 검사를 위해 커맨드의 키 인수 위치를 명세한다.

```c
typedef struct keySpec {
    const char *notes;
    uint64_t flags;              /* CMD_KEY_RO/RW/OW/RM + ACCESS/UPDATE/INSERT/DELETE */
    kspec_bs_type begin_search_type;
    union {
        struct { int pos; } index;        /* BS_INDEX: argv[pos]부터 탐색 */
        struct { const char *keyword; int startfrom; } keyword; /* BS_KEYWORD: 토큰 위치 */
    } bs;
    kspec_fk_type find_keys_type;
    union {
        struct { int lastkey; int step; int limit; } range;  /* FK_RANGE */
        struct { int keynumidx; int firstkey; int step; } keynum; /* FK_KEYNUM */
    } fk;
} keySpec;
```

### 2단계 키 탐색

```
begin_search (BS)              find_keys (FK)
─────────────────────          ─────────────────
BS_INDEX   → argv[pos]         FK_RANGE  → start부터 step씩 lastkey까지
BS_KEYWORD → 토큰 검색         FK_KEYNUM → argv[keynumidx]의 값이 키 개수
```

**예시: `MSET key1 val1 key2 val2`**
- `BS_INDEX(pos=1)` + `FK_RANGE(lastkey=-1, step=2, limit=0)` → 홀수 위치 인수들이 키

### 키 접근 플래그

```c
CMD_KEY_RO       /* 읽기 전용 */
CMD_KEY_RW       /* 읽기 + 수정 */
CMD_KEY_OW       /* 덮어쓰기 */
CMD_KEY_RM       /* 삭제 */
CMD_KEY_ACCESS   /* 값 반환/사용 */
CMD_KEY_UPDATE   /* 값 갱신 */
CMD_KEY_INSERT   /* 새 데이터 추가 */
CMD_KEY_DELETE   /* 데이터 삭제 */
CMD_KEY_INCOMPLETE /* 모든 키를 커버하지 않음 */
CMD_KEY_VARIABLE_FLAGS /* 인수에 따라 플래그 다를 수 있음 */
```

---

## 7. 인수 스키마 (`redisCommandArg`)

```c
typedef struct redisCommandArg {
    const char *name;
    redisCommandArgType type;    /* 9가지 타입 */
    int key_spec_index;          /* keySpec 배열 인덱스 */
    const char *token;           /* 예: "EX", "NX", "GET" */
    const char *summary;
    const char *since;
    int flags;                   /* CMD_ARG_OPTIONAL | CMD_ARG_MULTIPLE | ... */
    const char *deprecated_since;
    int num_args;
    struct redisCommandArg *subargs; /* ONEOF/BLOCK의 하위 인수 */
    const char *display_text;
} redisCommandArg;
```

### 9가지 인수 타입

| 타입 | 설명 | 예시 |
|------|------|------|
| `ARG_TYPE_STRING` | 일반 문자열 | `value` in SET |
| `ARG_TYPE_INTEGER` | 정수 | `count` in LPOS |
| `ARG_TYPE_DOUBLE` | 부동소수점 | `longitude` in GEOADD |
| `ARG_TYPE_KEY` | 키 이름 | `key` in GET |
| `ARG_TYPE_PATTERN` | glob 패턴 | `pattern` in KEYS |
| `ARG_TYPE_UNIX_TIME` | Unix 타임스탬프 | `unix-time-seconds` in SET EXAT |
| `ARG_TYPE_PURE_TOKEN` | 단독 토큰 | `NX`, `XX`, `EX` |
| `ARG_TYPE_ONEOF` | 선택지 중 하나 | SET의 condition 그룹 |
| `ARG_TYPE_BLOCK` | 그룹화된 인수 | range block |

---

## 8. 커맨드 테이블 초기화 (`populateCommandTable`)

### 서버 시작 시 등록 흐름

```c
/* server.c: initServer() */
server.commands      = dictCreate(&commandTableDictType); /* 대소문자 무관 dict */
server.orig_commands = dictCreate(&commandTableDictType); /* rename-command 원본 보존 */
populateCommandTable();
```

```c
void populateCommandTable(void) {
    extern struct redisCommand redisCommandTable[];   /* commands.def 에서 생성 */

    for (j = 0; ; j++) {
        struct redisCommand *c = redisCommandTable + j;
        if (c->declared_name == NULL) break;          /* 배열 끝 sentinel */

        c->fullname = sdsnew(c->declared_name);
        if (populateCommandStructure(c) == C_ERR) continue;

        dictAdd(server.commands,      sdsdup(c->fullname), c);
        dictAdd(server.orig_commands, sdsdup(c->fullname), c);
    }
}
```

### `populateCommandStructure()` 재귀 처리

```c
int populateCommandStructure(struct redisCommand *c) {
    /* 1. sentinel 모드에서 CMD_SENTINEL 없으면 제외 */
    /* 2. 서브커맨드 재귀 처리 */
    for (j = 0; c->subcommands[j].declared_name; j++) {
        sub->fullname = catSubCommandFullname(c->declared_name, sub->declared_name);
        /* "CLIENT|INFO" 형태로 fullname 구성 */
        populateCommandStructure(sub);  /* 재귀 */
        commandAddSubcommand(c, sub, sub->declared_name);
    }
    /* 3. 런타임 id 부여 (ACL 비트맵 인덱스) */
    /* 4. legacy_range_key_spec 계산 */
}
```

---

## 9. 커맨드 조회 (`lookupCommand`)

```c
/* "CLIENT|INFO" 형태를 "|"로 분리해 재귀 조회 */
struct redisCommand *lookupCommandBySdsLogic(dict *commands, sds s) {
    int argc;
    sds *strings = sdssplitlen(s, sdslen(s), "|", 1, &argc);
    /* argc=1: 최상위 커맨드 직접 조회 */
    /* argc=2: 상위 커맨드 → subcommands_dict 에서 하위 조회 */
    ...
}

/* 최적화: 마지막 커맨드 재사용 */
if (isCommandReusable(c->lastcmd, c->argv[0]))
    cmd = c->lastcmd;   /* 동일 커맨드 반복 시 dict 조회 스킵 */
else
    cmd = lookupCommand(c->argv, c->argc);
```

---

## 10. 커맨드 처리 파이프라인 (`processCommand`)

`src/server.c:4356` — 요청 하나가 실행되기까지의 전체 검사 순서:

```
클라이언트 요청 도착
        │
        ▼
① 모듈 CommandFilter 호출 (moduleCallCommandFilters)
        │
        ▼
② 커맨드 조회 (lookupCommand)
        │  ├─ 미존재 → ERR unknown command
        │  └─ CMD_INTERNAL + 비내부 클라이언트 → not found 처리
        ▼
③ Arity 검사 (commandCheckArity)
        │  └─ argc != arity → ERR wrong number of arguments
        ▼
④ CMD_PROTECTED 검사 (DEBUG, MODULE 커맨드)
        │
        ▼
⑤ 인증 검사 (authRequired)
        │  └─ 미인증 + CMD_NO_AUTH 없음 → NOAUTH
        ▼
⑥ MULTI 내 CMD_NO_MULTI 검사
        │
        ▼
⑦ ACL 검사 (ACLCheckAllPerm)
        │  ├─ 커맨드 권한
        │  ├─ 키 접근 권한
        │  └─ 채널 권한
        ▼
⑧ 클러스터 리다이렉션 (getNodeByQuery)
        │  └─ 다른 노드의 슬롯 → MOVED/ASK 응답
        ▼
⑨ maxmemory 검사 → 필요 시 eviction 실행
        │  └─ OOM + CMD_DENYOOM → OOM error
        ▼
⑩ 복제/로딩/stale 상태 검사
        │
        ▼
⑪ SUBSCRIBE 모드 커맨드 제한
        │
        ▼
⑫ MULTI 큐잉 or 즉시 call()
```

---

## 11. 커맨드 실행 엔진 (`call`)

`src/server.c:3893`:

```c
void call(client *c, int flags) {
    /* 타이머 시작 */
    const long long call_timer = ustime();
    enterExecutionUnit(1, call_timer);

    c->flags |= CLIENT_EXECUTING_COMMAND;

    /* ★ 실제 커맨드 실행 */
    c->cmd->proc(c);

    exitExecutionUnit();

    ustime_t duration = ustime() - call_timer;
    c->duration += duration;

    /* dirty: 이번 커맨드로 변경된 키 수 */
    dirty = server.dirty - dirty;

    /* 통계 업데이트 */
    latencyAddSampleIfNeeded("command", duration/1000);
    real_cmd->microseconds += duration;
    real_cmd->calls++;

    /* Slowlog 기록 */
    if (duration/1000 >= server.slowlog_log_slower_than)
        slowlogPushCurrentCommand(c, real_cmd, duration);

    /* AOF/복제 전파 */
    if (flags & CMD_CALL_PROPAGATE)
        propagateNow(c->db->id, c->argv, c->argc, target);
}
```

### `call()` 호출 경로

```
processCommand()   ← 일반 클라이언트 요청
execCommand()      ← MULTI/EXEC 트랜잭션
evalCommand()      ← Lua 스크립트
RM_Call()          ← Redis Module
```

---

## 12. 서브커맨드 시스템

Redis 7.0+에서 도입된 계층형 커맨드 구조:

```
CLIENT
  ├── INFO         → clientInfoCommand()
  ├── LIST         → clientListCommand()
  ├── GETNAME      → clientGetNameCommand()
  ├── SETNAME      → clientSetNameCommand()
  └── ...

CONFIG
  ├── GET          → configGetCommand()
  ├── SET          → configSetCommand()
  ├── REWRITE      → configRewriteCommand()
  └── RESETSTAT    → configResetStatCommand()
```

**저장 구조:**
- 상위 커맨드의 `subcommands_dict`: `sds이름 → redisCommand*` 매핑
- `fullname = "CLIENT|INFO"` — `|` 구분자로 계층 표현
- `parent` 포인터: 서브커맨드 → 상위 커맨드 역참조

---

## 13. 런타임 통계 & 관찰성

### 커맨드별 통계 (`redisCommand` 런타임 필드)

| 필드 | 의미 |
|------|------|
| `calls` | 총 호출 횟수 |
| `microseconds` | 총 실행 시간 (μs) |
| `rejected_calls` | 거부된 호출 (OOM, 클러스터 리다이렉트) |
| `failed_calls` | 오류로 실패한 호출 |
| `slowlog_count` | slowlog 기록 횟수 |
| `slowlog_time_us_sum` | slowlog 기록된 총 시간 |
| `slowlog_time_us_max` | slowlog 기록된 최대 시간 |
| `latency_histogram` | 나노초 단위 레이턴시 분포 (HDR Histogram) |

### `COMMAND INFO` 응답 구조

`COMMAND INFO GET` 응답:
1. 커맨드 이름
2. arity
3. flags 배열
4. first_key, last_key, step (legacy)
5. ACL 카테고리 배열
6. tips 배열
7. key_specs 배열
8. subcommands 배열

---

## 14. 모듈 커맨드 통합

```c
struct redisCommand {
    ...
    uint64_t flags;                        /* CMD_MODULE 플래그 포함 */
    struct RedisModuleCommand *module_cmd; /* 모듈 구현체 포인터 */
};
```

- `RM_CreateCommand()`: 모듈이 새 커맨드 등록
- `RM_Call()`: 모듈 내에서 기존 커맨드 호출 → `call()` 진입
- `CMD_MODULE_GETKEYS`: 모듈이 키 추출 함수 제공
- `CMD_MODULE_NO_CLUSTER`: 클러스터에서 해당 모듈 커맨드 금지

---

## 전체 구조 요약

```
JSON 정의 (425개 파일)
        │ generate-command-code.py
        ▼
commands.def / commands.c
(redisCommandTable[] 정적 배열)
        │ populateCommandTable() @ 서버 시작
        ▼
server.commands (dict)
  key: SDS "GET", "CLIENT|INFO", ...
  val: redisCommand*
        │ lookupCommand()
        ▼
processCommand()
  ① 모듈 필터 → ② 조회 → ③ arity → ④ 보호 → ⑤ 인증
  → ⑥ ACL → ⑦ 클러스터 → ⑧ maxmemory → ⑨ 전처리
        │ call()
        ▼
c->cmd->proc(c)   ← 실제 C 함수 (setCommand, getCommand, ...)
        │
        ▼
통계 업데이트 / slowlog / AOF-복제 전파
```
