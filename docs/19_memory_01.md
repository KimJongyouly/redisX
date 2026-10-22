# Redis 메모리 관리 소스 분석

> 분석 대상: `redis/src/zmalloc.h`, `zmalloc.c`, `evict.c`, `expire.c`, `lazyfree.c`, `defrag.c`, `memory_prefetch.h/c`, `server.h`

---

## 1. 메모리 할당 추상화 (zmalloc)

### 1.1 할당자 선택 계층

```c
// zmalloc.h — 컴파일 타임 할당자 선택
#if defined(USE_TCMALLOC)
    // tcmalloc: tc_malloc / tc_free
    #define ZMALLOC_LIB "tcmalloc-X.Y"
    #define zmalloc_size(p) tc_malloc_size(p)

#elif defined(USE_JEMALLOC)
    // jemalloc: je_malloc / je_free (Redis 기본)
    #define ZMALLOC_LIB "jemalloc-X.Y.Z"
    #define zmalloc_size(p) je_malloc_usable_size(p)

#elif defined(__APPLE__)
    // macOS: malloc_size()
    #define zmalloc_size(p) malloc_size(p)

#else
    // glibc: malloc_usable_size()
    #define ZMALLOC_LIB "libc"
    #define zmalloc_size(p) malloc_usable_size(p)
#endif
```

`ZMALLOC_LIB` 문자열은 `INFO memory` 응답의 `mem_allocator` 필드에 노출된다.

### 1.2 PREFIX_SIZE — 크기 추적 방식

```c
#ifdef HAVE_MALLOC_SIZE
    #define PREFIX_SIZE (0)    /* 할당자가 크기 반환 → 헤더 불필요 */
#else
    #define PREFIX_SIZE (sizeof(size_t))  /* 블록 앞 8바이트에 크기 저장 */
#endif
```

`HAVE_MALLOC_SIZE`가 있으면 포인터로 `zmalloc_size(p)` 호출로 크기를 알 수 있다. 없으면 `zmalloc()`이 `size` 값을 반환된 포인터 앞 8바이트에 직접 기록한다.

### 1.3 멀티스레드 메모리 추적

```c
// zmalloc.c
#define MAX_THREADS 16           /* 최대 추적 스레드 (2의 거듭제곱) */
#define THREAD_MASK (MAX_THREADS - 1)
#define PEAK_CHECK_THRESHOLD (1024 * 100)  /* 100KB */

typedef struct used_memory_entry {
    redisAtomic long long used_memory;       /* 스레드별 사용량 */
    redisAtomic long long last_peak_check;   /* 마지막 peak 체크 시점 */
    char padding[CACHE_LINE_SIZE - sizeof(long long) * 2]; /* false sharing 방지 */
} used_memory_entry;

/* 캐시 라인 정렬로 false sharing 완전 제거 */
static __attribute__((aligned(CACHE_LINE_SIZE)))
    used_memory_entry used_memory[MAX_THREADS];

static __thread long my_thread_index = -1;  /* TLS 스레드 인덱스 */
```

스레드 인덱스 초기화:
```c
static inline void init_my_thread_index(void) {
    if (unlikely(my_thread_index == -1)) {
        atomicGetIncr(num_active_threads, my_thread_index, 1);
        my_thread_index &= THREAD_MASK;  /* 16 스레드 순환 */
    }
}
```

Peak 메모리 갱신 (CAS 루프):
```c
/* 100KB마다 전역 peak 확인 — 너무 잦은 전역 합산 방지 */
if (thread_used - thread_last_peak_check_used > PEAK_CHECK_THRESHOLD) {
    size_t used_mem = zmalloc_used_memory();  /* 모든 스레드 합산 */
    size_t old_peak = published_peak;
    while (used_mem > old_peak &&
           !atomicCompareExchange(size_t, zmalloc_peak, old_peak, used_mem)) {
        /* CAS 실패 시 old_peak 갱신 후 재시도 */
    }
    if (used_mem > old_peak)
        atomicSet(zmalloc_peak_time, time(NULL));
}
```

### 1.4 zmalloc API 전체 목록

| 함수 | 설명 |
|------|------|
| `zmalloc(size)` | 할당 + 통계 추적, OOM 시 서버 패닉 |
| `zcalloc(size)` | 0 초기화 + 통계 추적 |
| `zcalloc_num(num, size)` | `num * size` 0 초기화 |
| `zrealloc(ptr, size)` | 재할당 + 통계 갱신 |
| `zfree(ptr)` | 해제 + 통계 차감 |
| `ztrymalloc(size)` | 할당 실패 시 NULL 반환 (패닉 없음) |
| `ztrycalloc(size)` | try 버전 calloc |
| `ztryrealloc(ptr, size)` | try 버전 realloc |
| `zmalloc_usable(size, *usable)` | 실제 사용 가능한 크기도 반환 |
| `zrealloc_usable(ptr, size, *usable, *old_usable)` | realloc + usable 크기 |
| `zfree_usable(ptr, *usable)` | 해제 + 해제된 크기 반환 |
| `zstrdup(s)` | strdup + 통계 추적 |
| `zlibc_free(ptr)` | libc free() 직접 호출 (backtrace_symbols 해제용) |
| `zmadvise_dontneed(ptr)` | `MADV_DONTNEED`로 물리 메모리 OS 반환 |

### 1.5 jemalloc 전용 확장 API

```c
// flags: MALLOCX_ARENA(n) | MALLOCX_TCACHE(n) 조합

void *zmalloc_with_flags(size_t size, int flags);    /* 특정 아레나/tcache 할당 */
void *zrealloc_with_flags(void *ptr, size_t size, int flags);
void zfree_with_flags(void *ptr, int flags);

/* 조각 모음 전용 (tcache 우회로 정확한 단편화 힌트 획득) */
void *zmalloc_no_tcache(size_t size);   /* HAVE_DEFRAG 필요 */
void zfree_no_tcache(void *ptr);
```

Lua VM 전용 아레나 (`server.lua_arena`): `MALLOCX_ARENA(lua_arena)` 플래그로 Lua 할당을 별도 아레나에 격리 → 조각 모음 방해 방지.

### 1.6 메모리 통계 함수

```c
size_t zmalloc_used_memory(void);         /* 전체 스레드 used_memory 합산 */
size_t zmalloc_get_peak_memory(void);     /* 최대 사용량 */
time_t zmalloc_get_peak_memory_time(void);/* 최대 사용량 도달 시각 */
size_t zmalloc_get_rss(void);             /* RSS (/proc/self/status 파싱) */
size_t zmalloc_get_private_dirty(pid_t);  /* Private Dirty 페이지 크기 */
size_t zmalloc_get_memory_size(void);     /* 물리 메모리 전체 크기 */

/* jemalloc 상세 정보 */
int zmalloc_get_allocator_info(int refresh_stats,
    size_t *allocated, size_t *active,
    size_t *resident,  size_t *retained,
    size_t *muzzy,     size_t *frag_smallbins_bytes);
```

`zmalloc_get_private_dirty()`: fork() 후 CoW 실제 사용량 추적에 활용. `INFO memory`의 `rdb_changes_since_last_save`와 연관.

### 1.7 jemalloc 기능 플래그

```c
/* jemalloc + JEMALLOC_FRAG_HINT: 활성 조각 모음 가능 */
#if (defined(USE_JEMALLOC) && defined(JEMALLOC_FRAG_HINT)) || defined(DEBUG_DEFRAG_FORCE)
    #define HAVE_DEFRAG
#endif

/* jemalloc + JEMALLOC_ALLOC_WITH_USIZE: 할당 시 실제 크기 동시 반환 */
#if defined(USE_JEMALLOC) && defined(JEMALLOC_ALLOC_WITH_USIZE)
    #define HAVE_ALLOC_WITH_USIZE
#endif
```

---

## 2. 만료 처리 (expire.c)

### 2.1 만료 방식 두 가지

| 방식 | 동작 | 시점 |
|------|------|------|
| **Passive** | 키 접근 시 `expireIfNeeded()` 호출 | 매 명령 처리 시 |
| **Active** | `activeExpireCycle()` 주기적 스캔 | beforeSleep / serverCron |

### 2.2 Active Expire — 두 가지 모드

```c
// expire.c 상수
#define ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP 20    /* 루프당 검사 키 수 */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000  /* FAST 모드 최대 시간 (μs) */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25   /* SLOW 모드 최대 CPU % */
#define ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE 10 /* 허용 stale 키 비율 % */
```

| 모드 | 최대 지속 시간 | 트리거 |
|------|--------------|--------|
| **FAST** | 1000 μs | `beforeSleep()` (이벤트 루프 매 반복) |
| **SLOW** | CPU의 25% | `serverCron()` (10Hz) |

```
activeExpireCycle(FAST / SLOW):
    for each DB:
        1. expires dict를 랜덤 스캔 (KEYS_PER_LOOP = 20개)
        2. 만료된 키 즉시 삭제 (또는 lazyfree-lazy-expire 시 비동기)
        3. expired/sampled < ACCEPTABLE_STALE(10%): 다음 DB로
        4. elapsed >= time_limit: 전체 중단
```

`active-expire-effort` (1~10): 높을수록 KEYS_PER_LOOP +25%, 허용 stale % 감소.

### 2.3 expireScanCallback — 스캔 콜백

```c
typedef struct {
    redisDb *db;
    long long now;
    unsigned long sampled;    /* 검사한 키 수 */
    unsigned long expired;    /* 만료된 키 수 */
    long long ttl_sum;        /* 미만료 키 TTL 합계 */
    int ttl_samples;          /* 미만료 키 수 */
} expireScanData;

void expireScanCallback(void *privdata, const dictEntry *de, ...) {
    kvobj *kv = dictGetKV(de);
    long long ttl = kvobjGetExpire(kv) - data->now;
    if (activeExpireCycleTryExpire(data->db, kv, data->now))
        data->expired++;
    if (ttl > 0) {
        data->ttl_sum += ttl;    /* 평균 TTL 계산용 */
        data->ttl_samples++;
    }
    data->sampled++;
}
```

스캔 건너뜀 조건:
- 버킷 채움률 < 1% (sparse dict)
- ASM 슬롯 마이그레이션 중인 슬롯

### 2.4 복제본에서의 만료 처리

복제본은 만료를 감지해도 **직접 삭제하지 않는다**. 마스터에서 DEL 명령이 전파될 때까지 `expireIfNeeded()`가 해당 키를 "없는 것처럼" 응답한다. 이를 통해 복제본과 마스터의 데이터 일관성을 보장한다.

### 2.5 Hash Field Expiration (HFE)

Hash 타입 개별 필드의 만료 지원 (Redis 7.4+):

```c
#define HFE_DB_BASE_ACTIVE_EXPIRE_FIELDS_PER_SEC 10000  /* 초당 처리 필드 수 */

/* subexpires estore: 필드 만료가 있는 hash 추적 */
static ExpireAction activeSubexpiresCb(eItem item, void *ctx) {
    kvobj *kv = (kvobj *)item;
    /* hashTypeExpire(): 만료된 필드 삭제 */
    uint64_t nextExpTime = hashTypeExpire(subexCtx->db, kv, &quota, ...);

    if (nextExpTime == EB_EXPIRE_TIME_INVALID || nextExpTime == 0)
        return ACT_REMOVE_EXP_ITEM;  /* HFE DB에서 제거 */
    else {
        ebSetMetaExpTime(hashGetExpireMeta(item), nextExpTime);
        return ACT_UPDATE_EXP_ITEM;  /* 다음 만료 시간 갱신 */
    }
}
```

---

## 3. 축출 (evict.c)

### 3.1 maxmemory 정책

```c
// server.h — 정책 플래그
#define MAXMEMORY_FLAG_LRU     (1<<0)  /* LRU 기반 */
#define MAXMEMORY_FLAG_LFU     (1<<1)  /* LFU 기반 */
#define MAXMEMORY_FLAG_ALLKEYS (1<<2)  /* 모든 키 대상 */
#define MAXMEMORY_FLAG_LRM     (1<<3)  /* LRM (Least Recently Modified) */

/* 정책 열거 */
MAXMEMORY_VOLATILE_LRU     /* expire 있는 키 중 LRU */
MAXMEMORY_VOLATILE_LFU     /* expire 있는 키 중 LFU */
MAXMEMORY_VOLATILE_RANDOM  /* expire 있는 키 중 랜덤 */
MAXMEMORY_VOLATILE_TTL     /* expire 있는 키 중 TTL 가장 짧은 것 */
MAXMEMORY_ALLKEYS_LRU      /* 모든 키 중 LRU */
MAXMEMORY_ALLKEYS_LFU      /* 모든 키 중 LFU */
MAXMEMORY_ALLKEYS_RANDOM   /* 모든 키 중 랜덤 */
MAXMEMORY_VOLATILE_LRM     /* expire 있는 키 중 LRM */
MAXMEMORY_ALLKEYS_LRM      /* 모든 키 중 LRM */
MAXMEMORY_NO_EVICTION      /* 축출 없음 → OOM 에러 */
```

### 3.2 LRU 알고리즘 (근사 LRU)

`robj.lru`: 24비트 타임스탬프.

```c
// evict.c
unsigned int getLRUClock(void) {
    return (mstime() / LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
    /* LRU_CLOCK_RESOLUTION = 1000ms
       LRU_CLOCK_MAX = (1<<24)-1 = 약 194일 주기 */
}

unsigned int LRU_CLOCK(void) {
    /* serverCron에서 미리 계산된 server.lruclock 캐시값 반환 */
    if (1000/server.hz <= LRU_CLOCK_RESOLUTION)
        return server.lruclock;   /* 캐시 활용 (비용 없음) */
    else
        return getLRUClock();     /* 시스템 콜 필요 */
}

unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru)
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    else
        /* 오버플로우 처리 */
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) * LRU_CLOCK_RESOLUTION;
}
```

### 3.3 LFU 알고리즘

`robj.lru` 24비트를 두 부분으로 재사용:

```
[ 16비트: 분 단위 마지막 접근 시각 | 8비트: 빈도 카운터 (0~255) ]
```

**카운터 증가** (Morris counter — 로그 스케일):
```
p = 1 / ((counter - LFU_INIT_VAL) * lfu_log_factor + 1)
random() < p → counter++
```
- `LFU_INIT_VAL = 5`: 새 키 초기 카운터 (0 대신 5로 시작 → 즉각 축출 방지)
- `lfu_log_factor = 10` (기본): 높을수록 카운터 증가 더 느림

**카운터 감쇄** (시간 경과에 따른 자동 감소):
```
elapsed_minutes = now_minutes - (lru >> 8)
decay = elapsed_minutes / lfu_decay_time
counter = max(0, counter - decay)
```
- `lfu_decay_time = 1` (기본): 1분마다 카운터 1 감소

### 3.4 LRM (Least Recently Modified) — 신기능

```c
MAXMEMORY_FLAG_LRM (1<<3)
```
- 마지막 **쓰기** 작업 시각 기준 축출
- 읽기(`GET`, `LRANGE` 등)는 `lru` 필드를 갱신하지 않음
- 쓰기(`SET`, `LPUSH` 등)만 `lru` 갱신
- 읽기 빈도와 무관하게 오래된 데이터 축출

### 3.5 Eviction Pool (근사 LRU 핵심 자료구조)

```c
// evict.c
#define EVPOOL_SIZE 16           /* 풀 크기 (정렬된 후보 목록) */
#define EVPOOL_CACHED_SDS_SIZE 255 /* SDS 재사용 캐시 (재할당 방지) */

struct evictionPoolEntry {
    unsigned long long idle; /* LRU: idle time / LFU: 역 빈도 / TTL: -TTL */
    sds key;                 /* 키 이름 */
    sds cached;              /* SDS 버퍼 재사용 캐시 */
    int dbid;                /* DB 번호 */
    int slot;                /* 클러스터 슬롯 */
};

static struct evictionPoolEntry *EvictionPoolLRU; /* 전역 풀 */
```

`evictionPoolPopulate()` 동작:
1. `maxmemory-samples`(기본 5)개 랜덤 샘플링
2. idle time 기준 오름차순으로 풀에 삽입 (우측이 더 오래됨)
3. 풀은 호출 간 유지 → 시간이 지남에 따라 최적 후보 수렴

### 3.6 performEvictions() — 축출 실행 흐름

```c
// evict.c:532
int performEvictions(void) {
    // 반환값: EVICT_OK / EVICT_RUNNING / EVICT_FAIL

    if (getMaxmemoryState(&mem_reported, NULL, &mem_tofree, NULL) == C_OK)
        return EVICT_OK;  /* 메모리 여유 있음 */

    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        return EVICT_FAIL;

    while (mem_freed < mem_tofree) {
        /* LRU / LFU / LRM / TTL 정책 */
        if (policy & (LRU|LFU|LRM) || policy == VOLATILE_TTL) {
            /* 1. 모든 DB에서 evictionPoolPopulate() */
            for (i = 0; i < server.dbnum; i++) {
                kvs = ALLKEYS ? db->keys : db->expires;
                evictionPoolPopulate(db, kvs, pool);
            }
            /* 2. 풀 우측(가장 오래된)부터 탐색 → 유효한 키 선택 */
            for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                if (!pool[k].key) continue;
                de = kvstoreDictFind(kvs, pool[k].slot, pool[k].key);
                if (de) { bestkey = ...; break; }
                /* Ghost (이미 삭제됨): 계속 탐색 */
            }
        }
        /* RANDOM 정책 */
        else if (policy == ALLKEYS_RANDOM || policy == VOLATILE_RANDOM) {
            /* next_db 순환 방식으로 랜덤 키 선택 */
        }

        /* 선택된 키 삭제 */
        dbDeleteAsync(db, keyobj);   /* lazyfree_lazy_eviction=yes: 비동기 */
        /* 또는 dbDelete(db, keyobj); /* 동기 */

        /* 복제본에 DEL 전파 */
        propagateDeletion(db, keyobj, ...);
    }
}
```

---

## 4. 비동기 해제 (lazyfree.c)

### 4.1 lazyfreeGetFreeEffort() — 해제 비용 추정

```c
// lazyfree.c:168
size_t lazyfreeGetFreeEffort(robj *key, robj *obj, int dbid) {
    if (obj->type == OBJ_LIST && obj->encoding == OBJ_ENCODING_QUICKLIST)
        return ql->len;              /* quicklist 노드 수 */
    else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT)
        return dictSize(ht);         /* set 원소 수 */
    else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST)
        return zsl->length;          /* skiplist 길이 */
    else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT)
        return dictSize(ht);         /* hash 필드 수 */
    else if (obj->type == OBJ_STREAM) {
        effort += s->rax->numnodes;  /* rax 노드 수 */
        effort += raxSize(cgroups) * (1 + raxSize(first_cg->pel));
        return effort;
    }
    else if (obj->type == OBJ_MODULE)
        return moduleGetFreeEffort(key, obj, dbid);
    else
        return 1;                    /* 문자열 등 단일 할당 */
}
```

### 4.2 freeObjAsync() — 동기/비동기 분기

```c
// lazyfree.c:220
#define LAZYFREE_THRESHOLD 64   /* 이 값 초과 시 비동기 해제 */

void freeObjAsync(robj *key, robj *obj, int dbid) {
    size_t free_effort = lazyfreeGetFreeEffort(key, obj, dbid);

    if (free_effort > LAZYFREE_THRESHOLD && obj->refcount == 1) {
        /* 대형 객체: BIO 스레드로 오프로드 */
        atomicIncr(lazyfree_objects, 1);
        bioCreateLazyFreeJob(lazyfreeFreeObject, 1, obj);
    } else {
        /* 소형 객체: 동기 해제 (BIO 오버헤드가 더 큼) */
        decrRefCount(obj);
    }
}
```

### 4.3 lazyfree 대상 및 설정

| 작업 | BIO 콜백 | 관련 설정 |
|------|---------|----------|
| 키 만료 | `lazyfreeFreeObject` | `lazyfree-lazy-expire` |
| 키 축출 | `lazyfreeFreeObject` | `lazyfree-lazy-eviction` |
| DEL/UNLINK | `lazyfreeFreeObject` | `lazyfree-lazy-server-del` |
| FLUSHDB/FLUSHALL | `kvsLazyfreeFree` | `lazyfree-lazy-user-flush` |
| 트래킹 테이블 | `lazyFreeTrackingTable` | 자동 |
| 에러 통계 rax | `lazyFreeErrors` | 자동 |
| Lua 스크립트 캐시 | `lazyFreeLuaScripts` | 자동 |
| Functions 컨텍스트 | `lazyFreeFunctionsCtx` | 자동 |
| 복제 백로그 | `lazyFreeReplicationBacklogRefMem` | 자동 |

### 4.4 kvsLazyfreeFree() — DB 전체 비동기 해제

```c
// lazyfree.c:55
void kvsLazyfreeFree(void *args[]) {
    kvstore *da1 = args[0];   /* keys kvstore */
    kvstore *da2 = args[1];   /* expires kvstore */
    estore *subexpires = args[2];

    estoreRelease(subexpires);
    kvstoreRelease(da1);
    kvstoreRelease(da2);

#if defined(USE_JEMALLOC)
    /* 해제 후 tcache 플러시 + dirty 페이지 OS 반환 */
    je_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);
    jemalloc_purge();
#endif
}
```

`emptyDbAsync()`: `FLUSHDB ASYNC` 또는 `FLUSHALL ASYNC` 시 새 빈 DB와 교체 후 구 DB를 BIO로 해제. 메인 스레드는 즉시 반환.

### 4.5 lazyfree 추적 카운터

```c
static redisAtomic size_t lazyfree_objects = 0;   /* 해제 대기 중 객체 수 */
static redisAtomic size_t lazyfreed_objects = 0;  /* 해제 완료 객체 수 */
```

`INFO stats`의 `lazyfree_pending_objects` 필드로 노출.

---

## 5. 활성 조각 모음 (defrag.c)

### 5.1 조각 모음 개요

```c
#define DEFRAG_CYCLE_US 500  /* 표준 주기당 실행 시간 (μs) */
```

- **필요 조건**: jemalloc + `JEMALLOC_FRAG_HINT` → `HAVE_DEFRAG`
- 동작: 단편화된 블록을 새 주소에 재할당하여 jemalloc이 빈 슬랩을 OS에 반환할 수 있도록 함
- **비파괴적**: 데이터 손실 없이 포인터 갱신

### 5.2 핵심 재할당 API

```c
// defrag.c

/* 재할당 + 이전 포인터 해제 (가장 일반적) */
void *activeDefragAlloc(void *ptr);

/* 재할당만 (포인터 갱신은 호출자가 수행) */
void *activeDefragAllocWithoutFree(void *ptr);

/* 이전 포인터 해제 */
void activeDefragFree(void *ptr);

/* 순수 새 할당 (조각 모음 컨텍스트) */
void *activeDefragAllocRaw(size_t size);
void activeDefragFreeRaw(void *ptr);
```

### 5.3 타입별 조각 모음 함수

```c
/* SDS 문자열 */
sds activeDefragSds(sds sdsptr);

/* listpack Entry */
Entry *activeDefragEntry(Entry *entry);

/* 문자열 robj (EMBSTR, RAW) */
robj *activeDefragStringOb(robj *ob);
robj *activeDefragStringObEx(robj *ob, int expected_refcount); /* refcount 확인 포함 */

/* Lua 스크립트 */
luaScript *activeDefragLuaScript(luaScript *script);

/* Skiplist 노드 (ZSet) */
void activeDefragZsetNode(zset *zs, dictEntry *de, dictEntryLink plink);

/* Dict 전체 조각 모음 */
void activeDefragSdsDict(dict *d, int val_type);
void activeDefragHfieldDict(dict *d);   /* Hash field 전용 */
```

`activeDefragSdsDict()` val_type:

| val_type | 값 타입 |
|----------|--------|
| `DEFRAG_SDS_DICT_VAL_IS_SDS` | SDS 직접 |
| `DEFRAG_SDS_DICT_VAL_IS_STROB` | string robj |
| `DEFRAG_SDS_DICT_VAL_VOID_PTR` | void * |
| `DEFRAG_SDS_DICT_VAL_LUA_SCRIPT` | luaScript |

### 5.4 조각 모음 트리거 조건

```
단편화율(frag_ratio) = active / allocated
낭비 바이트(frag_bytes) = active - allocated

시작 조건: frag_ratio >= active-defrag-threshold-lower(10%)
           AND frag_bytes >= active-defrag-ignore-bytes(100MB)

CPU 사용량:
    frag_ratio가 낮을수록 → active-defrag-cycle-min(1%)
    frag_ratio가 높을수록 → active-defrag-cycle-max(25%)

중단 조건: frag_ratio < active-defrag-threshold-lower
```

---

## 6. 공유 객체와 robj 메모리 최적화

### 6.1 정수 공유 풀

```c
// server.h
#define OBJ_SHARED_INTEGERS 10000  /* 0~9999 정수 미리 할당 */
```

- 서버 초기화 시 `createSharedObjects()`: 0~9999에 해당하는 robj를 생성 후 `shared.integers[]`에 보관
- `GET`, `INCR` 응답에서 이 범위의 정수는 새 객체 생성 없이 공유
- LFU/LRU 정책에서는 `lru` 필드를 갱신해야 하므로 공유 불가 → `MAXMEMORY_FLAG_NO_SHARED_INTEGERS`

### 6.2 robj 구조와 LRU/LFU 필드

```c
typedef struct redisObject {
    unsigned type:4;         /* 객체 타입 */
    unsigned encoding:4;     /* 인코딩 방식 */
    unsigned lru:LRU_BITS;   /* 24비트: LRU 또는 LFU 정보 */
    int refcount;            /* 참조 카운터 */
    void *ptr;               /* 실제 데이터 포인터 */
} robj;
```

LRU 모드 (`lru` 24비트):
```
[ 24비트: 초 단위 타임스탬프 (LRU_CLOCK_RESOLUTION=1000ms 기준) ]
최대 표현 범위: ~194일 (오버플로우 처리 포함)
```

LFU 모드 (`lru` 24비트):
```
[ 16비트: 분 단위 마지막 접근 시각 | 8비트: 카운터 (0~255) ]
초기값: LFU_INIT_VAL = 5
```

### 6.3 EMBSTR 최적화

```c
/* sds.h */
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44

/* robj + sdshdr8 + null byte가 64바이트 jemalloc 슬롯에 딱 맞음 */
/* EMBSTR: robj와 SDS를 단일 연속 블록으로 할당 → 캐시 친화적 */
```

---

## 7. 메모리 프리패치 (memory_prefetch.c)

```c
// memory_prefetch.h
/* dict lookup 전에 다음 버킷 주소를 CPU 캐시에 미리 로드 */
/* __builtin_prefetch(addr, rw, locality) 사용 */
/* 메모리 접근 지연(레이턴시) 은폐 → 처리량 향상 */
```

---

## 8. 메모리 관련 설정 파라미터 요약

### 기본 메모리

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `maxmemory` | 0 (무제한) | 최대 메모리 한계 |
| `maxmemory-policy` | noeviction | 한계 초과 시 정책 |
| `maxmemory-samples` | 5 | LRU/LFU 샘플링 키 수 |
| `maxmemory-eviction-tenacity` | 10 | 축출 시간 제한 비율 |

### LFU 설정

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `lfu-log-factor` | 10 | 카운터 증가 속도 (높을수록 느림) |
| `lfu-decay-time` | 1 | 카운터 감쇄 주기 (분) |

### LazyFree 설정

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `lazyfree-lazy-eviction` | no | 축출 비동기 해제 |
| `lazyfree-lazy-expire` | no | 만료 비동기 해제 |
| `lazyfree-lazy-server-del` | no | DEL 비동기 해제 |
| `lazyfree-lazy-user-del` | no | DEL을 UNLINK처럼 동작 |
| `lazyfree-lazy-user-flush` | no | FLUSHDB/FLUSHALL 비동기 |

### Active Expire 설정

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `active-expire-enabled` | 1 | 활성 만료 활성화 |
| `active-expire-effort` | 1 | 만료 노력 수준 (1~10) |

### Active Defrag 설정

| 설정 | 기본값 | 의미 |
|------|--------|------|
| `activedefrag` | no | 활성 조각 모음 활성화 |
| `active-defrag-ignore-bytes` | 100mb | 낭비 바이트 최소 임계값 |
| `active-defrag-threshold-lower` | 10 | 시작 단편화율 % |
| `active-defrag-threshold-upper` | 100 | 최대 노력 단편화율 % |
| `active-defrag-cycle-min` | 1 | 최소 CPU 사용 % |
| `active-defrag-cycle-max` | 25 | 최대 CPU 사용 % |
| `active-defrag-max-scan-fields` | 1000 | 집합/해시 스캔 최대 필드 수 |

---

## 9. 메모리 관리 전체 데이터 흐름

```
메모리 할당 경로
    zmalloc(size)
        │
        ├── jemalloc: je_malloc(size)
        ├── tcmalloc: tc_malloc(size)
        └── libc:     malloc(size) + PREFIX_SIZE 헤더

    used_memory[my_thread_index].used_memory += actual_size
    100KB마다 전역 zmalloc_peak CAS 업데이트

메모리 회수 경로
    ┌─── Passive Expire: 키 접근 시 expireIfNeeded() → 즉시 삭제
    ├─── Active Expire: activeExpireCycle() → 주기적 스캔
    │       FAST(1000μs, beforeSleep) / SLOW(25% CPU, serverCron)
    ├─── Eviction: performEvictions()
    │       maxmemory 초과 → LRU/LFU/LRM/TTL/RANDOM 정책으로 키 선택
    │       근사 LRU: EvictionPool[16] + maxmemory-samples(5) 랜덤 샘플
    └─── LazyFree: BIO 스레드로 오프로드
            effort > 64 → bioCreateLazyFreeJob(lazyfreeFreeObject)
            effort <= 64 → decrRefCount() 동기 해제

단편화 해소
    activeDefragCycle() (DEFRAG_CYCLE_US=500μs)
        → activeDefragAlloc(): 단편화된 블록 새 주소 재할당
        → 구 포인터 해제 → jemalloc 슬랩 반환 → RSS 감소
```

---

## 10. 메모리 모니터링 지표 (INFO memory)

| 필드 | 소스 함수 | 의미 |
|------|----------|------|
| `used_memory` | `zmalloc_used_memory()` | Redis 할당 메모리 합계 |
| `used_memory_rss` | `zmalloc_get_rss()` | OS가 보는 RSS |
| `mem_fragmentation_ratio` | rss / used_memory | 단편화율 |
| `mem_allocator` | `ZMALLOC_LIB` | 사용 중인 할당자 |
| `used_memory_peak` | `zmalloc_get_peak_memory()` | 최대 사용량 |
| `lazyfree_pending_objects` | `lazyfree_objects` | 비동기 해제 대기 객체 수 |
| `active_defrag_running` | defrag 상태 | 조각 모음 실행 중 여부 |
| `active_defrag_hits` | defrag 통계 | 재할당 성공 횟수 |
| `active_defrag_misses` | defrag 통계 | 재할당 불필요 횟수 |
