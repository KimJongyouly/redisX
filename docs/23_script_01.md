# Redis 스크립팅 & Lua 소스 분석

> 분석 대상: `redis/src/script.h`, `script.c`, `script_lua.h`, `script_lua.c`, `eval.c`, `functions.h`, `functions.c`, `function_lua.c`

---

## 1. 파일 구성과 역할

| 파일 | 역할 |
|------|------|
| `script.h / script.c` | scriptRunCtx 정의, 실행 전 검증, scriptCall, 타임아웃/킬 처리 |
| `script_lua.h / script_lua.c` | Redis-Lua API 등록, RESP↔Lua 타입 변환, luaCallFunction |
| `eval.c` | EVAL/EVALSHA 명령 처리, SHA1 캐싱, shebang 파싱, Lua 디버거 |
| `functions.h / functions.c` | Functions API: FUNCTION LOAD/LIST/CALL/DELETE/STATS/DUMP/RESTORE |
| `function_lua.c` | Lua 엔진 구현 (`luaEngineInitEngine`), `register_function` API |

### 두 가지 스크립팅 방식

| 방식 | 커맨드 | 도입 | 특성 |
|------|--------|------|------|
| **EVAL (Legacy)** | `EVAL`, `EVALSHA`, `SCRIPT` | Redis 2.6 | 일회성, SHA1 캐싱, 재시작 시 소멸 |
| **Functions** | `FCALL`, `FCALL_RO`, `FUNCTION` | Redis 7.0 | 영구 라이브러리, RDB/AOF 저장, 엔진 추상화 |

두 방식은 **별도의 Lua VM**을 사용하며, 공통 실행 인프라(`script.c`, `script_lua.c`)를 공유한다.

---

## 2. script.h / script.c — 공통 실행 계층

### 2.1 scriptRunCtx 구조체

```c
// script.h
struct scriptRunCtx {
    const char *funcname;         /* 실행 중인 함수/스크립트 이름 */
    client *c;                    /* 가짜 엔진 클라이언트 (명령 실행용) */
    client *original_client;      /* 실제 호출한 클라이언트 */
    int flags;                    /* 런타임 상태 플래그 */
    int repl_flags;               /* PROPAGATE_AOF | PROPAGATE_REPL */
    monotime start_time;          /* 실행 시작 시각 (타임아웃 계산) */
    int slot;                     /* 클러스터 슬롯 (cross-slot 감지) */
    int cluster_compatibility_check_slot; /* 구형 EVAL cross-slot 감지 */
};

/* 현재 실행 중인 컨텍스트 (전역) */
static scriptRunCtx *curr_run_ctx = NULL;
```

스크립트 실행 시 `curr_run_ctx`가 설정되어, 다른 모듈이 `scriptIsRunning()` / `scriptGetClient()` / `scriptGetCaller()`로 스크립트 실행 여부와 컨텍스트를 조회할 수 있다.

---

### 2.2 런타임 플래그 (runCtx flags)

```c
// script.h
#define SCRIPT_WRITE_DIRTY      (1ULL<<0) /* 이미 write 명령 실행됨 */
#define SCRIPT_TIMEDOUT         (1ULL<<3) /* 타임아웃 발생 */
#define SCRIPT_KILLED           (1ULL<<4) /* SCRIPT KILL 요청됨 */
#define SCRIPT_READ_ONLY        (1ULL<<5) /* 읽기 전용 스크립트 */
#define SCRIPT_ALLOW_OOM        (1ULL<<6) /* OOM 상태에서도 실행 허용 */
#define SCRIPT_EVAL_MODE        (1ULL<<7) /* EVAL 방식 (Functions와 구분) */
#define SCRIPT_ALLOW_CROSS_SLOT (1ULL<<8) /* 다중 슬롯 접근 허용 */
```

---

### 2.3 스크립트 선언 플래그 (Script flags)

```c
// script.h
#define SCRIPT_FLAG_NO_WRITES        (1ULL<<0) /* 쓰기 명령 없음 */
#define SCRIPT_FLAG_ALLOW_OOM        (1ULL<<1) /* OOM 허용 */
#define SCRIPT_FLAG_ALLOW_STALE      (1ULL<<2) /* stale replica에서 허용 */
#define SCRIPT_FLAG_NO_CLUSTER       (1ULL<<3) /* 클러스터 모드 금지 */
#define SCRIPT_FLAG_EVAL_COMPAT_MODE (1ULL<<4) /* 구형 EVAL 호환 모드 (shebang 없음) */
#define SCRIPT_FLAG_ALLOW_CROSS_SLOT (1ULL<<5) /* 다중 슬롯 접근 허용 */
```

플래그 문자열 매핑:
```c
scriptFlag scripts_flags_def[] = {
    {SCRIPT_FLAG_NO_WRITES,        "no-writes"},
    {SCRIPT_FLAG_ALLOW_OOM,        "allow-oom"},
    {SCRIPT_FLAG_ALLOW_STALE,      "allow-stale"},
    {SCRIPT_FLAG_NO_CLUSTER,       "no-cluster"},
    {SCRIPT_FLAG_ALLOW_CROSS_SLOT, "allow-cross-slot-keys"},
    {0, NULL},
};
```

---

### 2.4 scriptPrepareForRun() — 실행 전 검증

```c
// script.c:192
int scriptPrepareForRun(scriptRunCtx *run_ctx, client *engine_client,
                        client *caller, const char *funcname,
                        uint64_t script_flags, int ro)
```

**검증 단계** (shebang 있는 신형 스크립트):

| 단계 | 조건 | 에러 |
|------|------|------|
| 1 | `NO_CLUSTER` + 클러스터 활성화 | `Can not run script on cluster` |
| 2 | stale replica + `ALLOW_STALE` 미설정 | `-MASTERDOWN` |
| 3 | readonly replica + `NO_WRITES` 미설정 | `-READONLY` |
| 4 | 디스크 오류 + `NO_WRITES` 미설정 | `-MISCONF` |
| 5 | read-only 커맨드(`*_ro`) + write 플래그 | `Can not execute a script with write flag` |
| 6 | 복제본 부족 + min-slaves-to-write | `-NOREPLICASERR` |
| 7 | OOM + `ALLOW_OOM`/`NO_WRITES` 미설정 | `-OOM allow-oom flag is not set` |

**초기화 작업:**
```c
run_ctx->c = engine_client;           /* 가짜 클라이언트 설정 */
run_ctx->original_client = caller;    /* 실제 호출자 설정 */
selectDb(script_client, curr_client->db->id); /* DB 선택 동기화 */
script_client->resp = 2;              /* 기본 RESP2 */
run_ctx->start_time = getMonotonicUs();
run_ctx->repl_flags = PROPAGATE_AOF | PROPAGATE_REPL; /* 기본: 모두 전파 */
curr_run_ctx = run_ctx;               /* 전역에 현재 컨텍스트 등록 */
```

---

### 2.5 scriptCall() — Redis 명령 실행

```c
// script.c:633
void scriptCall(scriptRunCtx *run_ctx, sds *err) {
    client *c = run_ctx->c;

    /* 사용자 권한 전달 */
    c->user = run_ctx->original_client->user;
    moduleCallCommandFilters(c);  /* 모듈 필터 적용 */

    struct redisCommand *cmd = lookupCommand(c->argv, c->argc);

    /* 검증 순서: */
    scriptVerifyCommandArity(cmd, c->argc, err);    /* 인수 개수 */
    /* CMD_NOSCRIPT: 스크립트 내 금지 명령 */
    scriptVerifyACL(c, err);                        /* ACL 검사 */
    scriptVerifyWriteCommandAllow(run_ctx, err);    /* 쓰기 허용 여부 */
    scriptVerifyOOM(run_ctx, err);                  /* OOM 검사 */
    scriptVerifyAllowStale(c, err);                 /* stale 허용 여부 */
    scriptVerifyClusterState(run_ctx, c, ...);      /* 클러스터 슬롯 검사 */

    /* 실행 후 SCRIPT_WRITE_DIRTY 설정 */
    if (cmd->flags & CMD_WRITE)
        run_ctx->flags |= SCRIPT_WRITE_DIRTY;
}
```

---

### 2.6 scriptInterrupt() — 타임아웃 처리

Lua debug hook에서 주기적으로 호출되어 타임아웃과 킬 요청을 처리한다.

```c
// script.c:141
int scriptInterrupt(scriptRunCtx *run_ctx) {
    /* 이미 타임아웃 상태면 이벤트 처리 후 킬 여부 반환 */
    if (run_ctx->flags & SCRIPT_TIMEDOUT) {
        processEventsWhileBlocked();
        return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
    }

    long long elapsed = elapsedMs(run_ctx->start_time);
    if (elapsed < server.busy_reply_threshold) {  /* 기본 5000ms */
        return SCRIPT_CONTINUE;
    }

    /* 타임아웃 진입 */
    enterScriptTimedoutMode(run_ctx);   /* SCRIPT_TIMEDOUT 설정 */
    protectClient(run_ctx->original_client); /* 원본 클라이언트 이벤트 차단 */
    processEventsWhileBlocked();        /* 다른 클라이언트 응답 허용 */

    return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
}
```

타임아웃 상태에서 Redis는 `PING`, `SCRIPT KILL`, `FUNCTION KILL`, `SHUTDOWN NOSAVE`만 처리한다.

---

### 2.7 scriptKill() — 강제 종료

```c
// script.c:358
void scriptKill(client *c, int is_eval) {
    /* 킬 불가 조건 */
    if (!curr_run_ctx)
        → "-NOTBUSY No scripts in execution right now."

    if (mustObeyClient(curr_run_ctx->original_client))
        → "-UNKILLABLE: 마스터에서 복제된 스크립트"

    if (curr_run_ctx->flags & SCRIPT_WRITE_DIRTY)
        → "-UNKILLABLE: 이미 write 실행됨 (SHUTDOWN NOSAVE 사용)"

    /* EVAL 스크립트에 FUNCTION KILL 사용 → 거부 */
    if (!is_eval && (curr_run_ctx->flags & SCRIPT_EVAL_MODE))
        → slowevalerr

    /* FUNCTION에 SCRIPT KILL 사용 → 거부 */
    if (is_eval && !(curr_run_ctx->flags & SCRIPT_EVAL_MODE))
        → slowscripterr

    curr_run_ctx->flags |= SCRIPT_KILLED;  /* 다음 interrupt 호출 시 종료 */
}
```

---

### 2.8 복제 제어 (scriptSetRepl)

```c
int scriptSetRepl(scriptRunCtx *run_ctx, int repl) {
    /* repl: PROPAGATE_AOF | PROPAGATE_REPL 조합 */
    run_ctx->repl_flags = repl;
}
```

Lua에서:
```lua
redis.set_repl(redis.REPL_NONE)     -- 복제 안 함
redis.set_repl(redis.REPL_AOF)      -- AOF만 기록
redis.set_repl(redis.REPL_SLAVE)    -- 복제본에만 전파
redis.set_repl(redis.REPL_ALL)      -- AOF + 복제본 (기본값)
```

---

### 2.9 jemalloc 통합

```c
// script.c
#if defined(USE_JEMALLOC)

void luaEnvInit(void) {
    /* Lua 전용 jemalloc 아레나 생성 (조각 모음 방해 방지) */
    je_mallctl("arenas.create", &arena, &sz, NULL, 0);
    server.lua_arena = arena;
}

lua_State *createLuaState(void) {
    /* Lua VM마다 개인 tcache 생성 (스레드 캐시 충돌 방지) */
    je_mallctl("tcache.create", &tcache, &sz, NULL, 0);
    return lua_newstate(luaAlloc, (void *)(uintptr_t)tcache);
}
```

jemalloc 미사용 시: `lua_open()`으로 생성 (glibc 기본 할당자 사용).

---

## 3. eval.c — EVAL/EVALSHA 명령

### 3.1 EVAL 처리 흐름

```
EVAL <script> <numkeys> [key...] [arg...]
    │
    ▼
evalCommand(c) → evalGenericCommand(c, evalsha=0)
    │
    ├── evalCalcFunctionName(): SHA1 계산 → "f_<40자리sha1>"
    │
    ├── lctx.lua_scripts에서 캐시 조회
    │       │
    │       ├── 존재: 캐시 히트 → 기존 함수 사용
    │       └── 없음: evalCreateFunction() → Lua에 함수 정의
    │                  → lctx.lua_scripts에 저장
    │                  → lctx.lua_scripts_lru_list에 추가
    │
    ├── evalExtractShebangFlags(): shebang 파싱
    │       ├── shebang 없음 → SCRIPT_FLAG_EVAL_COMPAT_MODE
    │       └── #!lua flags=... → 명시적 플래그 파싱
    │
    ├── scriptPrepareForRun(): 실행 전 검증
    ├── rctx.flags |= SCRIPT_EVAL_MODE
    │
    └── luaCallFunction(): Lua 함수 호출
        → scriptResetRun(): 정리
```

### 3.2 함수명 규칙

```c
// eval.c:298
void evalCalcFunctionName(int evalsha, sds script, char *out_funcname) {
    out_funcname[0] = 'f';
    out_funcname[1] = '_';
    if (!evalsha) {
        sha1hex(out_funcname+2, script, sdslen(script));  /* SHA1 계산 */
    } else {
        /* EVALSHA: 이미 SHA1이 있으므로 소문자 변환만 */
        for (j = 0; j < 40; j++)
            out_funcname[j+2] = tolower(sha[j]);
    }
}
```

예시: `f_a42059b356c875f0717db19a51f6aaca9ae659ea`

### 3.3 shebang (#!) 파싱

```lua
-- 신형 EVAL 스크립트 (플래그 명시)
#!lua flags=no-writes,allow-oom
return redis.call('GET', KEYS[1])
```

```c
// eval.c:326
int evalExtractShebangFlags(sds body, uint64_t *out_flags, ...) {
    if (!strncmp(body, "#!", 2)) {
        /* "#!lua" 확인 */
        /* "flags=..." 파싱 → scripts_flags_def 조회 */
        script_flags &= ~SCRIPT_FLAG_EVAL_COMPAT_MODE;  /* 구형 호환 해제 */
    } else {
        script_flags = SCRIPT_FLAG_EVAL_COMPAT_MODE;    /* 기본: 구형 호환 */
    }
}
```

### 3.4 스크립트 캐싱과 LRU 축출

```c
lctx.lua_scripts          /* dict: SHA1 → luaScript */
lctx.lua_scripts_lru_list /* LRU 리스트 */
```

메모리 압박 시 `evalEvictScripts()`가 LRU 스크립트를 축출한다.
- EVALSHA가 축출된 스크립트를 요청하면 `-NOSCRIPT` 에러 반환
- `SCRIPT LOAD`: SHA1 사전 등록 (EVALSHA를 안전하게 사용 가능)

### 3.5 Lua 디버거 (LDB)

```c
// eval.c
void evalGenericCommandWithDebugging(client *c, int evalsha) {
    /* fork() 기반 디버그 세션 (서버에 영향 없음) */
    /* ldbIsEnabled(): 디버그 활성 여부 */
    /* ldbLog(): 실행 단계별 로깅 */
    /* ldbLogRedisReply(): Redis 응답 로깅 */
}
```

---

## 4. script_lua.c — Lua-Redis API 계층

### 4.1 Redis API 등록 (luaRegisterRedisAPI)

```c
// script_lua.c:1452
void luaRegisterRedisAPI(lua_State *lua) {
    luaSetAllowListProtection(lua);  /* 허용 목록 보호 */
    luaLoadLibraries(lua);           /* 허용된 라이브러리 로드 */

    lua_newtable(lua);               /* redis 테이블 생성 */

    /* 커맨드 실행 */
    lua_pushcfunction(lua, luaRedisCallCommand);   /* redis.call */
    lua_pushcfunction(lua, luaRedisPCallCommand);  /* redis.pcall */

    /* 유틸리티 */
    luaRegisterLogFunction(lua);     /* redis.log, redis.LOG_* */
    luaRegisterVersion(lua);         /* redis.REDIS_VERSION */

    /* RESP / 복제 제어 */
    lua_pushcfunction(lua, luaSetResp);            /* redis.setresp */
    lua_pushcfunction(lua, luaRedisSetReplCommand);/* redis.set_repl */

    /* 응답 헬퍼 */
    lua_pushcfunction(lua, luaRedisErrorReplyCommand);  /* redis.error_reply */
    lua_pushcfunction(lua, luaRedisStatusReplyCommand); /* redis.status_reply */

    /* 기타 */
    lua_pushcfunction(lua, luaRedisSha1hexCommand);     /* redis.sha1hex */
    lua_pushcfunction(lua, luaRedisAclCheckCmdPermissionsCommand); /* redis.acl_check_cmd */

    /* 복제 플래그 상수 */
    redis.REPL_NONE    = PROPAGATE_NONE
    redis.REPL_AOF     = PROPAGATE_AOF
    redis.REPL_SLAVE   = PROPAGATE_REPL
    redis.REPL_REPLICA = PROPAGATE_REPL
    redis.REPL_ALL     = PROPAGATE_AOF | PROPAGATE_REPL

    lua_setglobal(lua, "redis");     /* 전역 "redis" 테이블로 등록 */
}
```

### 4.2 Redis Lua API 전체 목록

| API | 동작 | 오류 처리 |
|-----|------|----------|
| `redis.call(cmd, ...)` | Redis 명령 실행 | 오류 시 Lua 예외 발생 |
| `redis.pcall(cmd, ...)` | Redis 명령 실행 | 오류 시 `{err="..."}` 테이블 반환 |
| `redis.log(level, msg)` | 서버 로그 기록 | - |
| `redis.setresp(n)` | RESP 버전 설정 (2 또는 3) | - |
| `redis.set_repl(flags)` | 복제 방법 제어 | - |
| `redis.error_reply(msg)` | 에러 응답 테이블 생성 | - |
| `redis.status_reply(msg)` | 상태 응답 테이블 생성 | - |
| `redis.sha1hex(str)` | SHA1 해시 (hex) 계산 | - |
| `redis.acl_check_cmd(cmd, ...)` | ACL 권한 확인 | - |
| `redis.REDIS_VERSION` | Redis 버전 문자열 | - |
| `redis.REDIS_VERSION_NUM` | Redis 버전 숫자 | - |

로그 레벨: `redis.LOG_DEBUG`, `redis.LOG_VERBOSE`, `redis.LOG_NOTICE`, `redis.LOG_WARNING`

---

### 4.3 luaRedisGenericCommand() — 핵심 실행 함수

`redis.call()` / `redis.pcall()` 모두 이 함수로 수렴한다.

```c
// script_lua.c:882
static int luaRedisGenericCommand(lua_State *lua, int raise_error) {
    scriptRunCtx *rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);

    /* Lua 스택에서 인수 추출 */
    c->argv = luaArgsToRedisArgv(lua, &c->argc, &c->argv_len);

    /* 재귀 호출 감지 (debug hook 남용 방지) */
    static int inuse = 0;
    if (inuse) { /* 경고 후 에러 반환 */ }
    inuse++;

    /* 디버거 활성 시 명령 로깅 */
    if (ldbIsEnabled()) ldbLog(...);

    /* Redis 명령 실행 */
    scriptCall(rctx, &err);

    /* 응답 파싱: RESP → Lua 타입 변환 */
    if (listLength(c->reply) == 0 && c->bufpos < c->buf_usable_size) {
        /* fast path: 정적 버퍼 직접 사용 */
        reply = c->buf;
    } else {
        /* slow path: reply 리스트 수집 → SDS 결합 */
        reply = sdsnewlen(c->buf, c->bufpos);
        /* c->reply 노드들 순차 수집 */
    }
    redisProtocolToLuaType(lua, reply);  /* 타입 변환 */

    inuse--;
}
```

---

### 4.4 RESP → Lua 타입 변환

`redisProtocolToLuaTypeCallbacks` vtable에 타입별 콜백이 등록된다.

| RESP 타입 | Lua 표현 |
|-----------|----------|
| Integer (`:`) | `number` |
| Bulk String (`$`) | `string` |
| Null Bulk String | `false` (RESP2) / `nil` (RESP3) |
| Status (`+`) | `{ok = "STATUS_MSG"}` |
| Error (`-`) | `{err = "ERR MSG"}` |
| Array (`*`) | `table` (1-indexed) |
| Null Array | `false` |
| Map (RESP3 `%`) | `table` (key-value 쌍) |
| Set (RESP3 `~`) | `table` |
| Bool (RESP3 `#`) | `boolean` |
| Double (RESP3 `,`) | `{double = value}` |
| Big Number (RESP3 `(`) | `{big_number = "str"}` |
| Verbatim String (RESP3 `=`) | `{verbatim_string = {format="txt", string="..."}}` |
| Null (RESP3 `_`) | `nil` |
| Attribute (RESP3 `|`) | 무시 (데이터만 반환) |

`redis.setresp(3)` 호출 시 RESP3 타입이 Lua로 직접 변환된다.

---

### 4.5 Lua → Redis 응답 변환 (luaReplyToRedisReply)

스크립트 반환값을 RESP로 변환하는 역방향 변환:

| Lua 타입 | Redis 응답 |
|----------|-----------|
| `number` (정수) | Integer |
| `number` (실수) | Bulk String (소수 표현) |
| `string` | Bulk String |
| `false` / `nil` | Null Bulk Reply |
| `true` | Integer 1 |
| `{ok = "..."}` | Status Reply |
| `{err = "..."}` | Error Reply |
| `{double = n}` | Double (RESP3) |
| `{big_number = "..."}` | Big Number (RESP3) |
| `{verbatim_string = {...}}` | Verbatim String (RESP3) |
| `table` (배열) | Array |
| `table` (map) | Map (RESP3) |

---

### 4.6 Lua 샌드박스 (허용 목록)

```c
// script_lua.c
/* 허용된 라이브러리 */
static char *libraries_allow_list[] = {
    "string", "cjson", "bit", "cmsgpack",
    "math", "table", "struct", "os", NULL
};

/* 허용된 Redis API */
static char *redis_api_allow_list[] = {
    "redis", "__redis__err__handler", NULL
};

/* 허용된 Lua 내장 함수 */
static char *lua_builtins_allow_list[] = {
    "xpcall", "tostring", "setmetatable", "next", "assert",
    "tonumber", "rawequal", "collectgarbage", "getmetatable",
    "rawset", "pcall", "coroutine", "type", "_G", "select",
    "unpack", "gcinfo", "pairs", "rawget", "loadstring",
    "ipairs", "_VERSION", "load", "error", NULL
};

/* 금지된 함수 (deny list) */
static char *deny_list[] = {
    "dofile", "loadfile", "print", NULL
};

/* deprecated (샌드박스 우려) */
static char *lua_builtins_deprecated[] = {
    "newproxy", "setfenv", "getfenv", NULL
};
```

초기화 후 `debug` 라이브러리는 `nil`로 설정 (에러 핸들러 생성 후 제거).

### 글로벌 보호 메커니즘

```c
luaSetAllowListProtection(lua);        /* 허용 목록 이외의 전역 변수 추가 차단 */
luaRegisterGlobalProtectionFunction(); /* __newindex 메타메서드로 전역 수정 방지 */
luaSetTableProtectionRecursively();    /* 중첩 테이블 재귀 보호 */
luaSetTableProtectionForBasicTypes();  /* string, number 등 기본 타입 메타테이블 보호 */
```

---

## 5. functions.h / functions.c — Functions API

### 5.1 engine vtable

플러그인 가능한 스크립팅 엔진 추상화.

```c
// functions.h
typedef struct engine {
    void *engine_ctx;   /* 엔진별 불투명 컨텍스트 */

    /* 라이브러리 로드/컴파일 */
    int (*create)(void *engine_ctx, functionLibInfo *li,
                  sds code, size_t timeout, sds *err);

    /* 함수 실행 */
    void (*call)(scriptRunCtx *r_ctx, void *engine_ctx,
                 void *compiled_function,
                 robj **keys, size_t nkeys,
                 robj **args, size_t nargs);

    /* 메모리 조회 */
    size_t (*get_used_memory)(void *engine_ctx);
    size_t (*get_function_memory_overhead)(void *compiled_function);
    size_t (*get_engine_memory_overhead)(void *engine_ctx);

    /* 해제 */
    void (*free_function)(void *engine_ctx, void *compiled_function);
    void (*free_ctx)(void *engine_ctx);
} engine;
```

현재 `lua` 엔진 하나만 구현되어 있으며, `luaEngineInitEngine()`으로 등록된다.

### 5.2 핵심 구조체

```c
// functions.h

/* 라이브러리 정보 */
struct functionLibInfo {
    sds name;        /* 라이브러리 이름 */
    dict *functions; /* 함수명 → functionInfo */
    engineInfo *ei;  /* 엔진 참조 */
    sds code;        /* 원본 라이브러리 코드 */
};

/* 개별 함수 정보 */
typedef struct functionInfo {
    sds name;            /* 함수 이름 */
    void *function;      /* 컴파일된 함수 (엔진 불투명 객체) */
    functionLibInfo *li; /* 소속 라이브러리 */
    sds desc;            /* 설명 */
    uint64_t f_flags;    /* 함수 플래그 */
} functionInfo;

/* 엔진 정보 */
typedef struct engineInfo {
    sds name;       /* 엔진 이름 ("LUA") */
    engine *engine; /* 엔진 vtable */
    client *c;      /* 명령 실행용 가짜 클라이언트 */
} engineInfo;
```

### 5.3 FUNCTION LOAD 흐름

```
FUNCTION LOAD [REPLACE] <code>
    │
    ▼
functionsCreateWithLibraryCtx(code, replace, err, lib_ctx, timeout)
    │
    ├── shebang 파싱: "#!lua name=<lib_name>"
    ├── lib_ctx에서 중복 라이브러리 확인
    │       └── REPLACE 없으면 에러
    │
    ├── luaEngineCreate(engine_ctx, li, code, timeout, err)
    │       │
    │       ├── 임시 Lua 환경 설정
    │       ├── 라이브러리 코드 실행
    │       └── redis.register_function() 콜백 수집
    │
    ├── functionLibCreateFunction(): functionInfo 등록
    │
    └── functionsLibCtxSwapWithCurrent(lib_ctx)
        → 원자적으로 현재 라이브러리 컨텍스트 교체
```

### 5.4 register_function API

```lua
-- 라이브러리 예시
#!lua name=mylib

local function helper(x) return x * 2 end

redis.register_function('double', function(keys, args)
    return helper(tonumber(args[1]))
end)

redis.register_function{
    function_name = 'greet',
    callback      = function(keys, args)
        return redis.call('SET', keys[1], 'hello ' .. args[1])
    end,
    flags         = {'no-writes'},
    description   = 'Greeting function'
}
```

`redis.register_function`은 `FUNCTION LOAD` 실행 중에만 호출 가능하다.

### 5.5 FCALL 실행 흐름

```
FCALL <func_name> <numkeys> [key...] [arg...]
    │
    ▼
fcallCommand(c) / fcallCommandWithFlags()
    │
    ├── functionsLibGet()에서 함수 탐색
    ├── scriptPrepareForRun()
    ├── engine->call(r_ctx, engine_ctx, compiled_function, keys, nkeys, args, nargs)
    │       └── luaEngineCall() → luaCallFunction()
    └── scriptResetRun()
```

### 5.6 RDB 저장/복원

```c
// functions.c
void createFunctionDumpPayload(rio *payload);  /* FUNCTION DUMP */
```

- RDB 타입 `RDB_TYPE_FUNCTION`: 라이브러리 코드 저장
- `FUNCTION RESTORE`: RDB 페이로드에서 라이브러리 복원
- 함수는 서버 재시작 후에도 자동 복원됨 (EVAL 스크립트와 차이)

---

## 6. function_lua.c — Lua 엔진 구현

### 6.1 luaEngineInitEngine() — 엔진 초기화

```c
// function_lua.c:428
int luaEngineInitEngine(void) {
    luaEngineCtx *lua_engine_ctx = zmalloc(...);
    lua_engine_ctx->lua = createLuaState();

    luaRegisterRedisAPI(lua_engine_ctx->lua);

    /* FUNCTION LOAD 전용 redis 테이블 생성 */
    lua_pushstring(lua, "register_function");
    lua_pushcfunction(lua, luaRegisterFunction);

    /* 에러 핸들러 등록 (debug 라이브러리 활용 후 nil 설정) */
    char *errh_func =
        "local dbg = debug\n"
        "debug = nil\n"
        "local error_handler = function(err)\n"
        "  local i = dbg.getinfo(2,'nSl')\n"
        "  if i and i.what == 'C' then i = dbg.getinfo(3,'nSl') end\n"
        "  if type(err) ~= 'table' then\n"
        "    err = {err='ERR ' .. tostring(err)}\n"
        "  end\n"
        "  if i then err['source'] = i.source; err['line'] = i.currentline end\n"
        "  return err\n"
        "end\n"
        "return error_handler";

    /* 전역 테이블 보호 (읽기 전용 메타테이블) */
    lua_newtable(lua);                    /* new globals */
    lua_newtable(lua);                    /* new globals metatable */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    lua_setfield(lua, -2, "__index");     /* 실제 globals를 __index로 */
    lua_enablereadonlytable(lua, -1, 1); /* 메타테이블 보호 */
    lua_setmetatable(lua, -2);
    lua_enablereadonlytable(lua, -1, 1); /* 새 globals 보호 */
    lua_replace(lua, LUA_GLOBALSINDEX);  /* 새 globals 설치 */

    luaSetTableProtectionForBasicTypes(lua); /* 기본 타입 보호 */

    /* engine vtable 설정 */
    engine *lua_engine = zmalloc(sizeof(*lua_engine));
    *lua_engine = (engine){
        .create                        = luaEngineCreate,
        .call                          = luaEngineCall,
        .get_used_memory               = luaEngineGetUsedMemoy,
        .get_function_memory_overhead  = luaEngineFunctionMemoryOverhead,
        ...
    };

    functionsRegisterEngine("LUA", lua_engine);
}
```

### 6.2 FUNCTION LOAD 타임아웃 훅

```c
// function_lua.c:64
/* Hook for FUNCTION LOAD execution */
static void luaEngineLoadHook(lua_State *lua, lua_Debug *ar) {
    functionLibInfo *li = ...;
    /* 타임아웃 체크 */
    if (elapsedMs(start_time) > timeout) {
        luaPushError(lua, "FUNCTION LOAD timeout");
        luaError(lua);
    }
}
```

---

## 7. 타임아웃 처리 전체 흐름

```
Lua VM 실행 중 debug hook 주기적 호출
    │
    ▼
scriptInterrupt(run_ctx)
    │
    ├── elapsed < busy_reply_threshold(5000ms): SCRIPT_CONTINUE
    │
    └── elapsed >= busy_reply_threshold:
            │
            ├── enterScriptTimedoutMode(): SCRIPT_TIMEDOUT 설정
            ├── blockingOperationStarts()
            ├── protectClient(original_client): 원본 클라이언트 이벤트 차단
            ├── processEventsWhileBlocked(): 제한적 이벤트 루프 재진입
            │       └── 허용: PING, SCRIPT/FUNCTION KILL, SHUTDOWN NOSAVE
            │
            ├── SCRIPT KILL 수신:
            │       └── scriptKill() → SCRIPT_KILLED 설정
            │
            └── 다음 scriptInterrupt():
                    └── SCRIPT_KILLED → SCRIPT_KILL 반환 → Lua 중단
                                         → scriptResetRun() → exitScriptTimedoutMode()
```

---

## 8. 클러스터와의 통합

### 8.1 Cross-slot 검증 (scriptVerifyClusterState)

```c
// script.c:491
static int scriptVerifyClusterState(scriptRunCtx *run_ctx, ...) {
    int hashslot = -1;
    getNodeByQuery(c, cmd, argv, argc, &hashslot, ..., &error_code);

    /* 다른 노드 슬롯 접근 → 에러 */
    if (result != getMyClusterNode()) {
        /* CLUSTER_REDIR_CROSS_SLOT: 다른 슬롯 키 접근 */
        /* CLUSTER_REDIR_MOVED: 다른 노드 슬롯 */
        /* CLUSTER_REDIR_DOWN_STATE: 클러스터 다운 */
        return C_ERR;
    }

    /* Cross-slot 감지 */
    if (hashslot != -1 && !(run_ctx->flags & SCRIPT_ALLOW_CROSS_SLOT)) {
        if (run_ctx->slot == -1)
            run_ctx->slot = hashslot;       /* 첫 슬롯 기록 */
        else if (run_ctx->slot != hashslot)
            → "Script attempted to access keys that do not hash to the same slot"
    }
}
```

### 8.2 EVAL 호환 모드와 클러스터

| 모드 | Cross-slot | 동작 |
|------|-----------|------|
| `SCRIPT_FLAG_EVAL_COMPAT_MODE` (shebang 없음) | 허용 (감지만) | `cluster_compatibility_check_slot`으로 통계만 수집 |
| 신형 (`#!lua flags=...`) | 기본 금지 | `allow-cross-slot-keys` 플래그 필요 |

---

## 9. EVAL vs Functions 비교

| 항목 | EVAL | Functions |
|------|------|-----------|
| 명령 | `EVAL`, `EVALSHA` | `FCALL`, `FCALL_RO` |
| 관리 | `SCRIPT LOAD/EXISTS/FLUSH/KILL` | `FUNCTION LOAD/LIST/DELETE/STATS/KILL/FLUSH/DUMP/RESTORE` |
| 영속성 | ✗ (재시작 시 소멸) | ✓ (RDB/AOF 저장) |
| 복수 함수 | ✗ | ✓ (라이브러리 단위) |
| 캐싱 | SHA1 기반, LRU 축출 | 서버 메모리에 영구 보관 |
| 타임아웃 킬 | `SCRIPT KILL` | `FUNCTION KILL` |
| Lua VM | eval 전용 | functions 전용 |
| 플래그 선언 | shebang 또는 기본 COMPAT | `register_function` flags 인수 |
| 배포 단위 | 스크립트 | 라이브러리 |

---

## 10. 핵심 설정 파라미터

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `lua-time-limit` | 5000ms | 스크립트 타임아웃 (`busy_reply_threshold`) |
| `busy-reply-threshold` | 5000ms | 동의어 |
| `lua-replicate-commands` | yes | 명령 단위 복제 (구형) |
| `lua-use-jemalloc` | 빌드 시 결정 | Lua VM jemalloc 사용 |
| `script-disable-deny-script` | no | CMD_NOSCRIPT 무시 |

---

## 11. 데이터 흐름 요약

### redis.call() 실행 경로

```
Lua 스크립트: redis.call('SET', KEYS[1], ARGV[1])
    │
    ▼
luaRedisCallCommand(lua)
    → luaRedisGenericCommand(lua, raise_error=1)
        → luaArgsToRedisArgv(): Lua 스택 → c->argv[]
        → scriptCall(rctx, &err)
            → lookupCommand() → 명령 탐색
            → scriptVerify*(): 검증 (ACL/OOM/cluster/stale/write)
            → call(): 명령 실행 → c->buf 또는 c->reply에 응답 기록
        → redisProtocolToLuaType(): RESP → Lua 타입
        → Lua 스택에 결과 push
    ← Lua 스크립트에 결과 반환
```

### FUNCTION LOAD + FCALL 라이프사이클

```
FUNCTION LOAD "#!lua name=mylib\n redis.register_function(...)"
    │
    ├── luaEngineCreate(): 코드 컴파일 + register_function 수집
    ├── functionLibInfo 생성 + functionInfo 등록
    └── RDB/AOF에 코드 저장 → 재시작 후 복원

FCALL myFunc 1 key1 arg1
    │
    ├── functionInfo 탐색
    ├── scriptPrepareForRun()
    ├── luaEngineCall() → luaCallFunction()
    │       → Lua 함수 실행 (keys, args 테이블 전달)
    │       → redis.call() 가능
    └── scriptResetRun()
```
