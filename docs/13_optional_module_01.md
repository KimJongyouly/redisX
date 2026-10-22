# Redis 옵셔널 모듈 (Optional Modules) 소스 분석

## 개요

Redis 모듈 시스템은 두 계층으로 구성된다.

| 계층 | 위치 | 설명 |
|------|------|------|
| **모듈 API 엔진** | `src/module.c`, `src/redismodule.h` | 동적 모듈 로딩 및 API 브리지 (15,589줄) |
| **옵셔널 모듈** | `modules/` | 외부 빌드 모듈 4종 + 내장 모듈 1종 |

---

## 1. 모듈 API 엔진 (`src/module.c`)

### 1.1 모듈 로딩 메커니즘

#### 외부 .so 로딩 (`moduleLoad()`, `module.c:13185`)

```c
int moduleLoad(const char *path, void **module_argv, int module_argc, int is_loadex) {
    // 실행 권한 확인 (best-effort)
    stat(path, &st); // S_IXUSR | S_IXGRP | S_IXOTH 체크

    handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);   // 동적 라이브러리 로드
    onload = dlsym(handle, "RedisModule_OnLoad");     // 진입점 심볼 탐색
    return moduleOnLoad(onload, path, handle, ...);
}
```

#### 공통 초기화 (`moduleOnLoad()`, `module.c:13217`)

```c
int moduleOnLoad(int (*onload)(void*, void**, int), ...) {
    RedisModuleCtx ctx;
    moduleCreateContext(&ctx, NULL, REDISMODULE_CTX_TEMP_CLIENT);

    if (onload((void*)&ctx, module_argv, module_argc) == REDISMODULE_ERR) {
        // 실패 시 cleanup: moduleUnregisterCleanup, moduleRemoveCateogires, dlclose
        return C_ERR;
    }
    // 성공 시 modules 딕셔너리에 등록
    dictAdd(modules, ctx.module->name, ctx.module);
    ctx.module->handle = handle;     // dlopen 핸들 보관
    return C_OK;
}
```

#### 내장 모듈 (`module.c:12973`)

```c
void moduleLoadInternalModules(void) {
#ifdef INCLUDE_VEC_SETS
    // Vector Sets는 Redis 8+에서 .so 없이 바이너리에 내장
    int retval = moduleOnLoad(
        (int (*)(void*, void**, int)) VectorSets_OnLoad,
        NULL, NULL, NULL, 0, 0
    );
    serverAssert(retval == C_OK);
#endif
}
```

**모듈 진입점 시그니처**:
```c
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
```
모든 커맨드 등록, 데이터 타입 등록, 이벤트 구독은 반드시 `OnLoad()` 내에서 수행.

### 1.2 API 등록 시스템 (`moduleRegisterCoreAPI()`)

Redis가 시작될 때 수백 개의 `RM_*` 함수 포인터를 `server.moduleapi` 딕셔너리에 등록.
모듈은 `RedisModule_GetApi(funcname, &ptr)`로 함수 포인터 획득.

매크로:
```c
#define REGISTER_API(name) \
    moduleRegisterApi("RedisModule_" #name, (void*)(unsigned long)RM_##name)
```

### 1.3 `RedisModuleCtx` 플래그 (27종)

`RedisModule_GetContextFlags()`로 조회:

| 플래그 | 비트 | 의미 |
|--------|------|------|
| `CTX_FLAGS_LUA` | 0 | Lua 스크립트 내에서 호출 |
| `CTX_FLAGS_MULTI` | 1 | MULTI/EXEC 트랜잭션 내 |
| `CTX_FLAGS_MASTER` | 2 | 마스터 노드 |
| `CTX_FLAGS_SLAVE` | 3 | 슬레이브 노드 |
| `CTX_FLAGS_CLUSTER` | 5 | 클러스터 모드 |
| `CTX_FLAGS_AOF` | 6 | AOF 활성화 |
| `CTX_FLAGS_RDB` | 7 | RDB 활성화 |
| `CTX_FLAGS_MAXMEMORY` | 8 | maxmemory 제한 도달 |
| `CTX_FLAGS_EVICT` | 9 | 메모리 부족으로 eviction 중 |
| `CTX_FLAGS_OOM` | 10 | OOM 상태 |
| `CTX_FLAGS_LOADING` | 13 | RDB/AOF 로딩 중 |
| `CTX_FLAGS_REPLICATED` | 12 | 마스터로부터 복제된 커맨드 |
| `CTX_FLAGS_DENY_BLOCKING` | 21 | 블로킹 커맨드 금지 컨텍스트 |
| `CTX_FLAGS_RESP3` | 22 | RESP3 프로토콜 |
| `CTX_FLAGS_TRIM_IN_PROGRESS` | 26 | 슬롯 트리밍 진행 중 |

### 1.4 핵심 API 카테고리

#### 커맨드 등록

```c
// 커맨드 생성
RM_CreateCommand(ctx, "mymod.set", MySetCommand, "write deny-oom", 1, 1, 1);
RM_CreateSubcommand(parent, "get", MyGetSub, "readonly", 1, 1, 1);

// 커맨드 메타정보 (문서화, keyspec, args)
RM_SetCommandInfo(command, &info);
RM_SetCommandACLCategories(command, "write dangerous");
RM_AddACLCategory(ctx, "mymodule");
```

#### 키 접근

```c
// 모드 플래그
REDISMODULE_READ        // 읽기 전용
REDISMODULE_WRITE       // 쓰기
REDISMODULE_OPEN_KEY_NOTOUCH    // LRU/LFU 갱신 안 함 (1<<16)
REDISMODULE_OPEN_KEY_NONOTIFY   // keyspace 이벤트 미발생 (1<<17)
REDISMODULE_OPEN_KEY_NOEXPIRE   // 만료 키 삭제 방지 (1<<19)
REDISMODULE_OPEN_KEY_ACCESS_EXPIRED  // 만료된 키도 접근 (1<<21)

RedisModuleKey *key = RM_OpenKey(ctx, keyname, REDISMODULE_READ);
int type = RM_KeyType(key);   // KEYTYPE_EMPTY/STRING/LIST/HASH/SET/ZSET/MODULE/STREAM
RM_CloseKey(key);
```

**키 타입 (8종)**:
`EMPTY(0)`, `STRING(1)`, `LIST(2)`, `HASH(3)`, `SET(4)`, `ZSET(5)`, `MODULE(6)`, `STREAM(7)`

#### 응답 (Reply)

**13종 reply 타입**: `STRING(0)`, `ERROR(1)`, `INTEGER(2)`, `ARRAY(3)`, `NULL(4)`, `MAP(5)`, `SET(6)`, `BOOL(7)`, `DOUBLE(8)`, `BIG_NUMBER(9)`, `VERBATIM_STRING(10)`, `ATTRIBUTE(11)`, `PROMISE(12)`

```c
RM_ReplyWithLongLong(ctx, 42);
RM_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);  // 길이 나중에 설정
RM_ReplySetArrayLength(ctx, len);
RM_ReplyWithMap(ctx, REDISMODULE_POSTPONED_LEN);    // RESP3 맵
```

#### 커스텀 데이터 타입

```c
RedisModuleTypeMethods tm = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,  // = 5
    .rdb_load = MyRdbLoad,
    .rdb_save = MyRdbSave,
    .aof_rewrite = MyAofRewrite,
    .free = MyFree,
    .copy = MyCopy,       // COPY 커맨드 지원
    .defrag = MyDefrag,   // 액티브 디프래그 지원
};
RedisModuleType *mytype = RM_CreateDataType(ctx, "MyType-v1", 1, &tm);
```

#### 블로킹 커맨드

```c
// 블로킹 후 백그라운드 스레드에서 처리
RedisModuleBlockedClient *bc = RM_BlockClient(ctx, ReplyFunc, TimeoutFunc, FreeFunc, 0);
// 별도 스레드에서 작업 완료 후:
RM_UnblockClient(bc, privdata);
```

#### 자동 메모리 관리 (Pool Allocator)

```c
RM_AutoMemory(ctx);  // 활성화 시 콜백 종료 시 자동 해제
```

블록 크기 `REDISMODULE_POOL_ALLOC_MIN_SIZE = 8192`, 정렬 `sizeof(void*)`.
`AutoMemEntry` 타입: `KEY(0)`, `STRING(1)`, `REPLY(2)`, `FREED(3)`, `DICT(4)`, `INFO(5)`, `CONFIG(6)`, `SLOTRANGEARRAY(7)`

#### 설정 등록

```c
RM_RegisterStringConfig(ctx, "mykey", default, flags, getFunc, setFunc, applyFunc, privdata);
RM_RegisterBoolConfig(ctx, ...);
RM_RegisterNumericConfig(ctx, ...);
RM_RegisterEnumConfig(ctx, ...);
```

**Config 플래그**: `IMMUTABLE(1)`, `SENSITIVE(2)`, `HIDDEN(16)`, `PROTECTED(32)`, `DENY_LOADING(64)`, `MEMORY(128)`, `BITFLAGS(256)`, `UNPREFIXED(512)`

#### 서버 이벤트 구독 (20종)

```c
RM_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence, MyCallback);
```

| 이벤트 ID | 값 | 의미 |
|-----------|-----|------|
| `EVENT_REPLICATION_ROLE_CHANGED` | 0 | 마스터↔슬레이브 전환 |
| `EVENT_PERSISTENCE` | 1 | RDB/AOF 저장 시작/종료 |
| `EVENT_FLUSHDB` | 2 | FLUSHDB/FLUSHALL |
| `EVENT_LOADING` | 3 | RDB/AOF 로딩 |
| `EVENT_CLIENT_CHANGE` | 4 | 클라이언트 연결/해제 |
| `EVENT_SHUTDOWN` | 5 | 서버 종료 |
| `EVENT_CRON_LOOP` | 8 | serverCron 주기 |
| `EVENT_MODULE_CHANGE` | 9 | MODULE LOAD/UNLOAD |
| `EVENT_FORK_CHILD` | 13 | fork 발생 |
| `EVENT_EVENTLOOP` | 15 | 이벤트 루프 before/after sleep |
| `EVENT_CONFIG` | 16 | CONFIG SET |
| `EVENT_KEY` | 17 | 키 생성/수정/삭제 |
| `EVENT_CLUSTER_SLOT_MIGRATION` | 18 | 슬롯 마이그레이션 |

#### 키스페이스 알림 구독 (18종)

```c
RM_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_STRING | REDISMODULE_NOTIFY_HASH, Callback);
```

주요 플래그: `KEYSPACE(1)`, `KEYEVENT(2)`, `GENERIC(4)`, `STRING(8)`, `LIST(16)`, `SET(32)`, `HASH(64)`, `ZSET(128)`, `EXPIRED(256)`, `EVICTED(512)`, `STREAM(1024)`, `KEY_MISS(2048)`, `LOADED(4096)`, `MODULE(8192)`, `NEW(16384)`, `OVERWRITTEN(32768)`, `TYPE_CHANGED(65536)`, `KEY_TRIMMED(131072)`

#### 모듈 간 API 공유

```c
// 제공 측
RM_ExportSharedAPI(ctx, "mymod.util.getvalue", MyGetValue);
// 사용 측
void *api = RM_GetSharedAPI(ctx, "mymod.util.getvalue");
```

---

## 2. 외부 옵셔널 모듈 (`modules/`)

### 2.1 빌드 시스템

`modules/common.mk`:
```makefile
$(SRC_DIR)/.prepared:
    git clone --recursive --depth 1 --branch $(MODULE_VERSION) $(MODULE_REPO) $(SRC_DIR)
```

빌드 시 자동 클론 (`git clone --depth 1`), 별도 `make` 후 `.so` 파일 생성.

**4종 외부 모듈 목록**:

| 모듈 | 버전 | 출력 파일 | 빌드 언어 |
|------|------|----------|----------|
| **RedisBloom** | v8.7.80 | `redisbloom.so` | C |
| **RediSearch** | v8.5.90 | `redisearch.so` | C |
| **RedisJSON** | v8.7.80 | `rejson.so` | Rust (cargo) |
| **RedisTimeSeries** | v8.7.80 | `redistimeseries.so` | C |

#### RedisBloom
- 확률적 데이터 구조: Bloom Filter, Cuckoo Filter, Count-Min Sketch, Top-K, t-digest
- 멤버십 테스트를 상수 공간에서 처리 (false positive 허용)
- `BF.ADD`, `BF.EXISTS`, `CF.ADD`, `CF.DEL`, `CMS.INCRBY`, `TOPK.ADD`

#### RediSearch
- 전문 검색 엔진: 역색인(Inverted Index), BM25 랭킹
- 필드 타입: TEXT, NUMERIC, TAG, GEO, VECTOR
- `FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE`, `FT.SUGADD`
- 클러스터 분산 쿼리 지원

#### RedisJSON
- JSON 문서 스토어: JSONPath 지원
- Rust 구현 (`rejson.so`), `cargo fetch` 의존성 관리
- `JSON.SET`, `JSON.GET`, `JSON.MGET`, `JSON.ARRAPPEND`, `JSON.NUMINCRBY`
- 도큐먼트 내 부분 업데이트 지원

#### RedisTimeSeries
- 시계열 데이터 타입: 압축 청크 저장, 다운샘플링
- 집계 함수: avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s
- `TS.ADD`, `TS.RANGE`, `TS.MRANGE`, `TS.CREATERULE` (집계 룰)

---

## 3. Vector Sets 내장 모듈 (`modules/vector-sets/`)

### 3.1 개요

Redis 8.0+에 **바이너리 내장** (`INCLUDE_VEC_SETS` 컴파일 플래그). `.so` 없이 `VectorSets_OnLoad()`가 서버 시작 시 자동 호출.

**소스 파일 구성**:

| 파일 | 줄 수 | 역할 |
|------|-------|------|
| `hnsw.c` | 3299 | HNSW 그래프 자료구조 |
| `vset.c` | 2593 | Redis 모듈 인터페이스 (커맨드 구현) |
| `expr.c` | 959 | 필터 표현식 파서/평가기 |
| `fastjson.c` | 441 | 빠른 JSON 속성 파서 |
| `w2v.c` | 539 | word2vec 테스트 유틸리티 |
| `hnsw.h` | — | HNSW API 헤더 |

### 3.2 HNSW 자료구조 (`hnsw.c`)

**Hierarchical Navigable Small World** 알고리즘 (Malkov & Yashunin 논문 기반):

**핵심 설계 결정**:

1. **양방향 링크(Bi-directional links)**: 단방향과 달리 삭제 시 고아 노드 없이 재연결 가능
2. **삽입 시 정규화**: 모든 벡터를 단위 벡터로 변환 → 코사인 유사도 = 내적(Dot Product)
3. **양자화(Quantization)**:
   - 기본 `Q8`: int8 범위 + 범위(range) 저장 → 4× 메모리 절감
   - `BIN`: 이진 양자화, 정규화된 해밍 거리 (`hamming_bits * 2 / dim`) → [0, 2] 범위
   - `NOQUANT`: float32 원본 유지
4. **진짜 삭제(True Deletion)**: 삭제 시 마킹이 아닌 실제 제거 + 링크 재연결

**SIMD 최적화** (런타임 CPU 감지):

```c
// GCC/Clang의 target attribute로 함수별 SIMD 코드 생성
#if defined(__x86_64__) && __GNUC__ >= 5
    #define HAVE_AVX2
    #define HAVE_AVX512
    #define HAVE_POPCNT
#endif

// 예: AVX2 + FMA 코사인 유사도 계산
__attribute__((target("avx2,fma")))
float cosine_avx2(float *a, float *b, int dim) { ... }

// 런타임 감지
if (VSET_USE_AVX2)      cosine = cosine_avx2(a, b, dim);
else if (VSET_USE_POPCNT) hamming = hamming_popcnt(a, b, dim);
else                     cosine = cosine_scalar(a, b, dim);
```

**HNSW 파라미터**:
- `M` (기본 16): 각 노드의 최대 연결 수. 레이어 0은 `M*2` 연결
- `EF` (탐색 인수, 기본 200): 삽입/검색 시 후보 탐색 깊이
- 레이어 수: 평균 `1.33층`, 확률적 레이어 할당 (`-ln(random()) * mL`)

### 3.3 커맨드 (`vset.c`)

**VADD** (삽입):
```
VADD key [REDUCE dim] FP32|VALUES vector element
     [CAS] [NOQUANT | Q8 | BIN]
     [EF build-ef] [SETATTR <json>] [M <links>]
```
- `REDUCE dim`: 랜덤 프로젝션으로 차원 축소 (프로젝션 행렬 저장)
- `CAS`: 이웃 탐색을 백그라운드 스레드에서 수행 (Check-And-Set 패턴)
- 내부: `hnsw_insert(vset->hnsw, vec, qvec, qrange, 0, nv, ef)`

**VSIM** (유사도 검색):
```
VSIM key [ELE|FP32|VALUES] <vector or element>
     [WITHSCORES] [WITHATTRIBS] [COUNT num]
     [EPSILON delta] [EF search-ef]
     [FILTER expr] [FILTER-EF max-effort]
     [TRUTH] [NOTHREAD]
```
- 기본 스레드 실행: `pthread_create` → `VSIM_thread` → `hnsw_search()`
- `FILTER`: 벡터 유사도 + 속성 필터 동시 처리 → `hnsw_search_with_filter()`
- `TRUTH`: 선형 스캔으로 정확한 결과 계산 (recall 측정용)
- `NOTHREAD`: 메인 스레드에서 동기 실행

**기타 커맨드**: `VDEL`, `VCARD`, `VEMB`, `VRANDMEMBER`, `VGETATTR`, `VSETATTR`

### 3.4 스레딩 모델 (`vset.c` 주석)

```
메인 스레드          백그라운드 스레드
     │                      │
     │ VSIM 커맨드 수신      │
     ├──── BlockClient ─────►│
     │                      │ hnsw_search()
     │                      │ (읽기 잠금 in_use_lock 획득)
     │                      │ 결과 계산
     │◄─── UnblockClient ───┤
     │ 클라이언트에 응답      │
```

**동기화 설계**:
- `in_use` 읽기-쓰기 뮤텍스: 백그라운드 스레드가 읽기 모드로 획득
- `vectorSetWaitAllBackgroundClients()`: 쓰기 모드로 획득 후 즉시 해제 → 모든 백그라운드 스레드 완료 대기
  - 호출 시점 A: 벡터셋 삭제 (DEL / expire)
  - 호출 시점 B: `VSETATTR` - 노드 속성 직접 수정
- 스레드 생성~잠금 사이의 race 방지: 원자적 카운터(`pending_bg_clients`)

**VADD CAS 패턴**:
```
메인 스레드 → BlockClient → 백그라운드: 이웃 후보 탐색(느린 부분)
           ← UnblockClient ← 완료
메인 스레드 VADD_CASReply(): 실제 hnsw_insert() (빠른 부분)
```

### 3.5 필터 표현식 (`expr.c`)

JSON 속성에 대한 수식 필터:
```
".year > 1950"
".genre == 'drama' && .rating >= 8.0"
```
- 파서: 재귀 하강 파서 (recursive descent)
- 피연산자: 점 표기법 속성 접근, 숫자 리터럴, 문자열 리터럴
- 연산자: `>`, `<`, `>=`, `<=`, `==`, `!=`, `&&`, `||`, `!`

---

## 4. 모듈 개발 패턴 요약

### 최소 모듈 구조

```c
#include "redismodule.h"

int MyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModule_AutoMemory(ctx);          // 자동 메모리 관리 활성화

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    // ... 작업 수행
    return RedisModule_ReplyWithLongLong(ctx, 42);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "mymod", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "mymod.cmd", MyCommand,
        "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
```

### 커스텀 데이터 타입 구조

```c
RedisModuleTypeMethods tm = {
    .version     = REDISMODULE_TYPE_METHOD_VERSION,  // 5
    .rdb_load    = Load,     // RDB 역직렬화
    .rdb_save    = Save,     // RDB 직렬화
    .aof_rewrite = Rewrite,  // AOF 재작성
    .free        = Free,     // 메모리 해제
    .copy        = Copy,     // COPY 커맨드
    .defrag      = Defrag,   // 액티브 디프래그
    .mem_usage   = MemUsage, // MEMORY USAGE
    .digest      = Digest,   // DEBUG DIGEST
};
RedisModuleType *type = RedisModule_CreateDataType(ctx, "TYPE-NAME", 1, &tm);
```

---

## 5. 전체 구조 요약

```
Redis 서버 시작
  └─ moduleLoadInternalModules()
       └─ VectorSets_OnLoad()  ← 내장 (INCLUDE_VEC_SETS)

loadmodule /path/to/module.so  (redis.conf)
  └─ moduleLoad()
       ├─ dlopen(path, RTLD_NOW | RTLD_LOCAL)
       ├─ dlsym(handle, "RedisModule_OnLoad")
       └─ moduleOnLoad()
            └─ onload(ctx, argv, argc)
                 ├─ RedisModule_Init()       → 이름/버전 등록
                 ├─ RedisModule_CreateCommand()  → 커맨드 테이블 등록
                 ├─ RedisModule_CreateDataType() → 커스텀 타입 등록
                 ├─ RedisModule_SubscribeToServerEvent() → 이벤트 구독
                 └─ RedisModule_RegisterStringConfig()  → 설정 등록
```

**외부 모듈 빌드 흐름**:
```
cd modules/redisbloom
make
  └─ git clone --depth 1 --branch v8.7.80 https://github.com/redisbloom/redisbloom src/
  └─ make -C src/
  └─ cp redisbloom.so ./
```

---

**참조 파일**:
- [src/module.c](redis/src/module.c) — 모듈 엔진 구현 (15,589줄)
- [src/redismodule.h](redis/src/redismodule.h) — 모듈 공개 API (1,889줄)
- [modules/vector-sets/hnsw.c](redis/modules/vector-sets/hnsw.c) — HNSW 그래프 (3,299줄)
- [modules/vector-sets/vset.c](redis/modules/vector-sets/vset.c) — Vector Sets 커맨드 (2,593줄)
- [modules/vector-sets/expr.c](redis/modules/vector-sets/expr.c) — 필터 파서 (959줄)
- [modules/common.mk](redis/modules/common.mk) — 외부 모듈 공통 빌드 규칙
