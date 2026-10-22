# Redis 핵심 알고리즘 & 자료구조 상세 분석

## 개요

Redis의 고성능은 용도에 따라 최적화된 자료구조들을 조합하여 구현된다. 이 문서는 Redis 소스코드에서 핵심적으로 사용되는 알고리즘과 자료구조를 분석한다.

---

## 1. SDS (Simple Dynamic Strings) — `src/sds.h`, `src/sds.c`

### 설계 목표

C 문자열(`char *`)의 한계(길이 계산 O(n), 바이너리 안전 불가, 버퍼 오버플로우 위험)를 극복하면서 C 문자열과 호환되는 동적 문자열 구현.

### 5가지 헤더 타입 (packed 구조체)

```c
struct __attribute__ ((__packed__)) sdshdr5  { unsigned char flags; char buf[]; };
struct __attribute__ ((__packed__)) sdshdr8  { uint8_t  len; uint8_t  alloc; unsigned char flags; char buf[]; };
struct __attribute__ ((__packed__)) sdshdr16 { uint16_t len; uint16_t alloc; unsigned char flags; char buf[]; };
struct __attribute__ ((__packed__)) sdshdr32 { uint32_t len; uint32_t alloc; unsigned char flags; char buf[]; };
struct __attribute__ ((__packed__)) sdshdr64 { uint64_t len; uint64_t alloc; unsigned char flags; char buf[]; };
```

| 타입 | 최대 길이 | len 필드 | 사용 사례 |
|------|---------|---------|---------|
| sdshdr5 | 31 bytes | flags 상위 5비트에 내장 | 매우 짧은 문자열, 읽기 전용 |
| sdshdr8 | 255 bytes | uint8_t | 일반 짧은 문자열 |
| sdshdr16 | 65,535 bytes | uint16_t | 중간 크기 문자열 |
| sdshdr32 | 4GB | uint32_t | 대용량 값 |
| sdshdr64 | 이론상 무제한 | uint64_t | 극대형 데이터 |

### 핵심 포인터 트릭

`sds` 타입은 `char *`로 `buf[]`를 직접 가리킨다. 헤더 접근은 포인터를 역방향으로 이동:

```c
#define SDS_HDR(T,s) ((struct sdshdr##T *)((s)-(sizeof(struct sdshdr##T))))
#define sdslen(s)    (SDS_HDR_VAR(T,s)->len)  /* O(1) 길이 조회 */
```

`flags[-1]`로 타입 판별 → 타입에 맞는 헤더 캐스팅 → O(1) 길이/용량 접근.

### 메모리 최적화

- `__attribute__((packed))`: 헤더 크기 최소화, 패딩 없음
- `SDS_TYPE_5`: `alloc` 필드 없음 (변경 불가 용도)
- jemalloc 타입 업그레이드: 문자열 확장 시 새 메모리 할당 + 타입 승격 (hdrs8 → hdrs16 등)
- `sdsRemoveFreeSpace()`: 잉여 공간 반환으로 메모리 절약

### C 문자열 호환성

`buf[]` 끝에 항상 `\0` 추가 → 기존 C 라이브러리 함수 직접 사용 가능.

---

## 2. Hash Table (dict) — `src/dict.c`, `src/dict.h`

### 구조체

```c
struct dictEntry {
    struct dictEntry *next;   /* Must be first: 인라인 체이닝 */
    void *key;                /* Must be second: 최적화 접근 */
    union {
        void *val;
        uint64_t u64;
        int64_t  s64;
        double   d;
    } v;
};

struct dict {
    dictType *type;
    dictEntry **ht_table[2];  /* 두 해시 테이블: [0]=현재, [1]=rehash 대상 */
    long      ht_used[2];     /* 각 테이블의 사용 엔트리 수 */
    long      rehashidx;      /* -1 = rehash 비활성, >=0 = 진행 중인 버킷 인덱스 */
    ...
};
```

### 증분 재해싱 (Incremental Rehashing)

블로킹을 방지하기 위해 재해싱을 여러 단계로 나눠서 실행:

```c
// dictRehash(dict *d, int n): n개 버킷을 한 번에 처리
// 빈 버킷 최대 방문 횟수 = n * 10 (공연 공간 건너뛰기)
// 매 읽기/쓰기 명령마다 1 step 수행 (100 μs 단위로 서버루프에서도 수행)
```

**재해싱 트리거 조건:**
- `load factor > 1` (일반): `ht_used / ht_size > 1`
- `load factor > dict_force_resize_ratio (= 4)`: fork 중에도 강제 resize
- `DICT_RESIZE_AVOID` 플래그: fork(COW) 시 가능하면 resize 회피

### 해시 함수: SipHash-1-2

```c
// DoS 저항을 위한 랜덤 시드(서버 시작 시 초기화)
// SipHash-1-2: 1 compression round + 2 finalization rounds
// 64-bit 출력, 암호학적으로 안전한 PRF
uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
```

### 충돌 해결

체이닝(Separate Chaining): `dictEntry.next` 포인터로 연결 리스트 구성.  
새 항목은 버킷 헤드에 삽입 (O(1) 삽입).

---

## 3. Skip List — `src/t_zset.c`

### 설계 파라미터

```c
#define ZSKIPLIST_MAXLEVEL 32   /* 최대 레벨: 약 4^32 = 10^19 원소 지원 */
#define ZSKIPLIST_P       0.25  /* 레벨 승격 확률: 각 레벨은 이전의 1/4 크기 */
```

### 랜덤 레벨 생성

```c
static int zslRandomLevel(void) {
    static const int threshold = ZSKIPLIST_P * RAND_MAX;  /* = RAND_MAX/4 */
    int level = 1;
    while (random() < threshold) level += 1;
    return (level < ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}
// 기댓값: 1/(1-p) ≈ 1.33 레벨
// 레벨 k 노드 비율: p^(k-1) = (0.25)^(k-1)
```

### 노드 구조 (단일 메모리 할당)

```c
typedef struct zskiplistNode {
    sds ele;              /* 멤버 (embedded SDS) */
    double score;         /* 정렬 기준 점수 */
    struct zskiplistNode *backward;  /* 역방향 포인터 (level[0]만) */
    struct zskiplistLevel {
        struct zskiplistNode *forward;  /* 다음 노드 포인터 */
        unsigned long span;             /* 이 레벨에서 건너뛴 노드 수 */
    } level[];            /* 가변 길이 배열: 노드+level[]을 한 번에 malloc */
} zskiplistNode;
```

**`span` 필드**: 각 레벨의 포인터가 몇 개의 노드를 건너뛰는지 기록 → `ZRANK` 명령을 O(log N)에 구현.

### 복잡도

| 연산 | 평균 | 최악 |
|------|------|------|
| 삽입/삭제 | O(log N) | O(N) |
| 범위 검색 | O(log N + M) | O(N) |
| 랭크 조회 | O(log N) | O(N) |

---

## 4. Listpack — `src/listpack.c`

### 메모리 레이아웃

```
[total_bytes:4B][num_elements:2B][entry...][end:1B=0xFF]
```

헤더 `LP_HDR_SIZE = 6` bytes. 끝 마커 `0xFF` 1 byte.

### 7가지 엔트리 인코딩

| 인코딩 | 형식 | 값 범위 | 크기 |
|--------|------|---------|------|
| 7-bit uint | `0xxxxxxx` | 0-127 | 2B (data+backlen) |
| 13-bit int | `110xxxxx xxxxxxxx` | -4096~4095 | 3B |
| 16-bit int | `11110001 [2B]` | ±32768 | 4B |
| 24-bit int | `11110010 [3B]` | ±8M | 5B |
| 32-bit int | `11110011 [4B]` | ±2G | 6B |
| 64-bit int | `11110100 [8B]` | int64 전체 | 10B |
| 6-bit str | `10xxxxxx [N]` | N≤63 byte | 2+N B |
| 12-bit str | `1110xxxx xxxxxxxx [N]` | N≤4095 | 3+N B |
| 32-bit str | `11110000 [4B] [N]` | N≤4GB | 5+N B |

### 양방향 순회 (backlen)

각 엔트리 끝에 **backlen 필드** (1~5 bytes)가 자신의 인코딩+데이터 크기를 저장:
- 1~127 bytes → 1 byte backlen
- 128 bytes 이상 → 가변 인코딩 (최대 5 bytes)

이를 통해 역방향 포인터 없이 O(1) 이전 엔트리 이동 가능.

### 적용 범위

hash, zset의 원소가 적을 때 (`hash-max-listpack-entries`, `zset-max-listpack-entries`) 사용.

---

## 5. Quicklist — `src/quicklist.c`

### 구조

Listpack 노드들을 doubly-linked list로 연결한 이중 구조:

```
[quicklist head] ↔ [node:listpack] ↔ [node:listpack] ↔ ... ↔ [quicklist tail]
```

```c
typedef struct quicklistNode {
    struct quicklistNode *prev, *next;
    unsigned char *entry;    /* listpack 또는 LZF 압축 데이터 */
    size_t sz;               /* entry 크기 (bytes) */
    unsigned int count:16;   /* listpack 내 원소 수 */
    unsigned int encoding:2; /* QUICKLIST_NODE_ENCODING_RAW or LZF */
    unsigned int container:2;
    unsigned int recompress:1;
    ...
} quicklistNode;
```

### 노드 크기 결정 (list-max-listpack-size)

```c
static const size_t optimization_level[] = {4096, 8192, 16384, 32768, 65536};
// list-max-listpack-size: -1~-5 → 각각 4KB~64KB per node
// list-max-listpack-size: 양수 → 엔트리 개수 제한
#define SIZE_SAFETY_LIMIT 8192  /* 안전 상한: 기본 8KB */
```

### LZF 압축 (list-compress-depth)

```c
#define MIN_COMPRESS_BYTES 48   /* 48바이트 미만은 압축하지 않음 */
// list-compress-depth=N: 양 끝 N개 노드는 압축하지 않음 (빠른 접근)
// 중간 노드는 LZF 알고리즘으로 압축 → 메모리 절약
```

**압축 전략**: push/pop이 빈번한 양 끝은 raw, 거의 접근하지 않는 중간은 압축.

---

## 6. Intset — `src/intset.c`

### 구조

정수만을 위한 정렬된 압축 배열:

```c
typedef struct intset {
    uint32_t encoding;  /* INTSET_ENC_INT16/32/64 */
    uint32_t length;    /* 원소 수 */
    int8_t   contents[];/* 실제 데이터 (encoding에 따라 2/4/8 bytes per item) */
} intset;
```

### 3가지 인코딩

```c
#define INTSET_ENC_INT16 (sizeof(int16_t))  /* = 2: -32768 ~ 32767 */
#define INTSET_ENC_INT32 (sizeof(int32_t))  /* = 4: -2^31 ~ 2^31-1 */
#define INTSET_ENC_INT64 (sizeof(int64_t))  /* = 8: int64 전체 범위 */
```

### 이진 탐색 + 업그레이드

- **탐색**: `intsetSearch()` — 정렬된 배열 이진 탐색 O(log N)
- **삽입**: 이진 탐색으로 위치 찾기 → `intsetMoveTail()` (memmove) → 삽입
- **인코딩 업그레이드**: 범위 초과 값 삽입 시 전체 배열을 더 큰 타입으로 변환
  ```c
  // intsetUpgradeAndAdd(): 뒤에서 앞으로 복사 (덮어쓰기 방지)
  // 새 값은 음수면 맨 앞, 양수면 맨 뒤에 위치 (정렬 보장)
  ```
- **다운그레이드 없음**: 업그레이드 후 원소 삭제해도 인코딩 유지 (구현 단순화)

### 빅 엔디안 지원

모든 읽기/쓰기에서 `memrev16/32/64ifbe()` 매크로로 바이트 순서 변환.

---

## 7. Doubly Linked List (adlist) — `src/adlist.c`

### 구조

```c
typedef struct listNode {
    struct listNode *prev, *next;
    void *value;
} listNode;

typedef struct list {
    listNode *head, *tail;
    void *(*dup)(void *ptr);    /* 복제 함수 포인터 */
    void  (*free)(void *ptr);   /* 해제 함수 포인터 */
    int   (*match)(void *ptr, void *key); /* 비교 함수 포인터 */
    unsigned long len;           /* O(1) 길이 */
} list;
```

### 특징

- **함수 포인터**: 타입에 무관하게 범용적으로 사용 가능 (Generic)
- **양방향 반복자**: `AL_START_HEAD` / `AL_START_TAIL`
- `listLinkNodeHead/Tail()`: 이미 할당된 노드를 연결 (분리된 alloc/link)
- `listJoin()`: 두 리스트를 O(1)로 합병
- `listRotateTailToHead()` / `listRotateHeadToTail()`: LMOVE 등에 사용

**사용처**: 클라이언트 리스트, 슬레이브 리스트, 슬로우로그, 서버 내부 관리용.

---

## 8. Radix Tree (rax) — `src/rax.c`, `src/rax.h`

### 설계 목표

prefix를 공유하는 키들을 압축하여 메모리를 절약하는 Adaptive Radix Tree. Streams의 소비자 그룹, 슬롯별 키 추적 등에 사용.

### 노드 구조

```c
typedef struct raxNode {
    uint32_t iskey:1;    /* 이 노드가 키를 담고 있는가 */
    uint32_t isnull:1;   /* 값이 NULL인가 (저장 생략) */
    uint32_t iscompr:1;  /* 압축 노드인가 */
    uint32_t size:29;    /* 비압축: 자식 수, 압축: 저장된 문자열 길이 */
    unsigned char data[];/* 가변 크기 데이터 영역 */
} raxNode;
```

### 두 가지 노드 형태

**비압축 노드** (`iscompr=0`): 여러 자식 분기
```
[header iscompr=0][abc][a-ptr][b-ptr][c-ptr](value-ptr?)
                   ^^^  각 문자 1개씩, 각 포인터는 해당 자식
```

**압축 노드** (`iscompr=1`): 단일 체인 경로 압축
```
[header iscompr=1][xyz][z-ptr](value-ptr?)
                   ^^^  연속 단일 경로 "xyz", 마지막 문자의 자식 1개
```

### raxStack: 부모 노드 추적

```c
#define RAX_STACK_STATIC_ITEMS 32   /* 스택 정적 배열 크기 */
// 스택이 32개 초과 시 힙으로 전환 (Small Buffer Optimization)
```

---

## 9. 메모리 할당자 래퍼 (zmalloc) — `src/zmalloc.c`

### 할당자 추상화

컴파일 타임에 선택:
```c
#if defined(USE_JEMALLOC)
    #define malloc(size)  je_malloc(size)
    #define free(ptr)     je_free(ptr)
    // + je_mallocx, je_rallocx, je_dallocx (flags 버전)
#elif defined(USE_TCMALLOC)
    #define malloc(size)  tc_malloc(size)
#else
    /* libc malloc: PREFIX_SIZE(8 bytes) 헤더에 크기 저장 */
#endif
```

### PREFIX_SIZE 전략

```c
#ifdef HAVE_MALLOC_SIZE  /* jemalloc/tcmalloc: 할당자가 크기 알고 있음 */
    #define PREFIX_SIZE (0)  /* 헤더 불필요 */
#else                    /* libc: 앞에 size_t 크기 저장 */
    #define PREFIX_SIZE 8
#endif
```

### 멀티스레드 메모리 추적

```c
#define MAX_THREADS 16   /* 2의 제곱: & 연산으로 인덱싱 */
typedef struct used_memory_entry {
    redisAtomic long long used_memory;
    redisAtomic long long last_peak_check;
    char padding[CACHE_LINE_SIZE - ...];  /* 거짓 공유(False Sharing) 방지 */
} used_memory_entry;

static __attribute__((aligned(CACHE_LINE_SIZE))) used_memory_entry used_memory[MAX_THREADS];
static __thread long my_thread_index = -1;  /* 스레드-로컬 인덱스 */
```

**피크 메모리 추적**: 스레드당 100KB(`PEAK_CHECK_THRESHOLD`) 할당마다 전역 피크 확인 → `zmalloc_peak` CAS로 원자적 업데이트.

### API 계층

| 함수 | 동작 |
|------|------|
| `zmalloc(size)` | 실패 시 `abort()` |
| `ztrymalloc(size)` | 실패 시 NULL 반환 |
| `zmalloc_usable(size, *usable)` | 실제 할당된 크기도 반환 |
| `zrealloc(ptr, size)` | 재할당 + 통계 업데이트 |
| `zfree(ptr)` | 해제 + 통계 업데이트 |
| `zmalloc_used_memory()` | 현재 사용 메모리 합산 |

---

## 10. 이벤트 루프 (ae) — `src/ae.c`

### 플랫폼별 멀티플렉서 선택

```c
/* 성능 순서: evport > epoll > kqueue > select */
#ifdef HAVE_EVPORT
    #include "ae_evport.c"   /* Solaris */
#elif HAVE_EPOLL
    #include "ae_epoll.c"    /* Linux */
#elif HAVE_KQUEUE
    #include "ae_kqueue.c"   /* macOS, FreeBSD */
#else
    #include "ae_select.c"   /* 범용 fallback */
#endif
```

### 이벤트 루프 구조

```c
typedef struct aeEventLoop {
    int setsize;             /* 최대 fd 수 */
    int nevents;             /* 현재 할당된 이벤트 배열 크기 */
    aeFileEvent  *events;    /* fd → 이벤트 핸들러 배열 */
    aeFiredEvent *fired;     /* poll 결과 임시 배열 */
    aeTimeEvent  *timeEventHead; /* 타임 이벤트 연결 리스트 */
    int maxfd;               /* 등록된 최대 fd */
    aeBeforeSleepProc *beforesleep;
    aeBeforeSleepProc *aftersleep;
    int flags;               /* AE_DONT_WAIT 등 */
    ...
} aeEventLoop;
```

### 동적 배열 확장

```c
#define INITIAL_EVENT 1024
// fd가 nevents 초과 시 2배씩 확장 (최대 setsize까지)
// 새 슬롯은 AE_NONE으로 초기화
```

### AE_BARRIER 플래그

읽기/쓰기 실행 순서 역전:
- 기본: readable → writable (쿼리 처리 후 즉시 응답)
- `AE_BARRIER` 설정 시: writable → readable (fsync 완료 후 응답)

### 타임 이벤트

단순 연결 리스트 (O(N) 탐색):
- `AE_DELETED_EVENT_ID`로 삭제 표시 → 다음 루프에서 실제 제거
- `refcount`로 재귀 호출 중 삭제 방지
- 반환값 `AE_NOMORE` → 이벤트 삭제, 양수 → ms 후 재실행

### 메인 루프

```c
void aeMain(aeEventLoop *eventLoop) {
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop,
            AE_ALL_EVENTS | AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
    }
}
```

---

## 11. HyperLogLog — `src/hyperloglog.c`

### 알고리즘 개요

```
입력 원소 → 64비트 해시 → 상위 14비트(P=14)로 레지스터 선택 → 
하위 50비트에서 선행 0 개수(ρ) 계산 → 레지스터[j] = max(레지스터[j], ρ+1)
카디널리티 추정: E = α_∞ × m² / Σ(2^(-M[j]))
```

### 핵심 상수

```c
#define HLL_P       14               /* 레지스터 선택 비트 수 */
#define HLL_REGISTERS (1 << HLL_P)  /* = 16384개 레지스터 */
#define HLL_BITS     6               /* 레지스터당 6비트 (0-63 선행 0 표현) */
#define HLL_DENSE_SIZE (HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS + 7) / 8)
                     /* = 16 + 12288 = 12,304 bytes ≈ 12KB */
#define HLL_ALPHA_INF 0.721347520444481703680  /* 0.5/ln(2) */
```

오차율: `1.04 / √(16384) ≈ 0.81%`

### 두 가지 표현 형식

**Dense 표현** (기본):
```
6비트 레지스터 16384개 → 12KB 고정 크기
+--------+--------+--------+------...--+
|11000000|22221111|33333322|55444444   |
+--------+--------+--------+------...--+
```

**Sparse 표현** (초기 상태, 메모리 효율):
- `ZERO opcode (00xxxxxx)`: 연속 1-64개 레지스터가 0
- `XZERO opcode (01xxxxxx yyyyyyyy)`: 연속 1-16384개 레지스터가 0 (2 bytes)
- `VAL opcode (1vvvvvxx)`: 값 vvvvv, 연속 xx+1개 레지스터

**전환 조건**: `hll-sparse-max-bytes` (기본 3000) 초과 시 Sparse → Dense 자동 변환.

### 16바이트 헤더

```
+------+---+-----+----------+
| HYLL | E | N/U | Cardin.  |  (16 bytes)
+------+---+-----+----------+
  매직   인코딩  예약   캐시된 카디널리티 (MSB=dirty 플래그)
```

### SIMD 최적화

```c
#ifdef HAVE_AVX2    /* x86: 256비트 AVX2 명령어 */
#ifdef HAVE_AARCH64_NEON  /* ARM: NEON 명령어 */
// 32/16개 레지스터를 병렬 처리 (hllDenseSum 함수)
```

---

## 자료구조 선택 기준 (Redis 인코딩 전략)

| Redis 타입 | 적은 데이터 | 많은 데이터 |
|------------|-----------|-----------|
| String | embstr / SDS | SDS |
| List | Quicklist (listpack 노드) | Quicklist (더 많은 노드) |
| Hash | Listpack | Hash Table (dict) |
| Set (정수) | Intset | Hash Table |
| Set (문자열) | Listpack | Hash Table |
| Sorted Set | Listpack | Skiplist + Hash Table |
| HyperLogLog | Sparse | Dense |
| Stream | Listpack (entries) | Listpack + Rax (index) |

---

## 상호 의존 관계

```
zmalloc ← 모든 자료구조의 기반
   ↓
SDS ← dict, skiplist, listpack의 키/값
   ↓
adlist ← 서버 내부 관리 (클라이언트 목록 등)
dict   ← 전역 키 공간, Hash 타입 내부
   ↓
listpack → quicklist → List 타입
intset               → Set 타입 (정수)
skiplist + dict      → Sorted Set 타입
rax                  → Stream 내부 인덱스
ae                   → 전체 서버 이벤트 루프
hyperloglog          → HLL 명령어 전용
```
