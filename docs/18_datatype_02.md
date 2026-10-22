# Redis 핵심 자료구조 소스 분석

> 분석 대상: `redis/src/` 디렉터리의 핵심 `.c` / `.h` 파일
> 총 분석 라인: ~29,762줄

---

## 목차

1. [SDS (Simple Dynamic String)](#1-sds-simple-dynamic-string)
2. [adlist (이중 연결 리스트)](#2-adlist-이중-연결-리스트)
3. [dict (해시 테이블)](#3-dict-해시-테이블)
4. [listpack (컴팩트 리스트)](#4-listpack-컴팩트-리스트)
5. [quicklist (List 타입 인코딩)](#5-quicklist-list-타입-인코딩)
6. [intset (정수 집합)](#6-intset-정수-집합)
7. [ziplist (레거시 컴팩트 리스트)](#7-ziplist-레거시)
8. [skiplist (정렬 집합)](#8-skiplist-정렬-집합)
9. [rax (기수 트리)](#9-rax-기수-트리)
10. [HyperLogLog](#10-hyperloglog)
11. [인코딩 전환 임계값 요약](#11-인코딩-전환-임계값-요약)

---

## 1. SDS (Simple Dynamic String)

**파일**: `sds.h`, `sds.c` (1,580줄)

### 구조체 정의

Redis의 모든 문자열은 C의 `char*` 대신 SDS를 사용한다.
`sds` 타입은 단순히 `char*`의 별칭이지만, 포인터 **앞**에 헤더가 붙는다.

```c
// sds.h:24
typedef char *sds;

// 크기별로 5가지 헤더 타입 존재 (모두 __packed__ 적용)
struct __attribute__((__packed__)) sdshdr8 {
    uint8_t  len;    // 현재 사용 길이
    uint8_t  alloc;  // 할당 크기 (헤더·null 제외)
    unsigned char flags; // 하위 3비트 = 타입, 상위 5비트 = 보조 비트
    char buf[];      // 실제 문자열 데이터
};
```

### 5가지 헤더 타입

| 타입 | 최대 길이 | len/alloc 타입 | 헤더 크기 |
|------|-----------|---------------|-----------|
| `sdshdr5` | 31 bytes | flags에 인코딩 | 1 byte |
| `sdshdr8` | ~255 bytes | uint8_t | 3 bytes |
| `sdshdr16` | ~65KB | uint16_t | 5 bytes |
| `sdshdr32` | ~4GB | uint32_t | 9 bytes |
| `sdshdr64` | ~18EB | uint64_t | 17 bytes |

```c
// sds.c:33 - 길이에 따라 최소 타입 자동 선택
char sdsReqType(size_t string_size) {
    if (string_size < 1 << 5)  return SDS_TYPE_5;
    if (string_size <= (1<<8)  - sizeof(struct sdshdr8)  - 1) return SDS_TYPE_8;
    if (string_size <= (1<<16) - sizeof(struct sdshdr16) - 1) return SDS_TYPE_16;
    if (string_size <= (1ll<<32)- sizeof(struct sdshdr32)- 1) return SDS_TYPE_32;
    return SDS_TYPE_64;
}
```

### 핵심 설계 포인트

| 항목 | 설명 |
|------|------|
| **O(1) 길이 조회** | `len` 필드를 읽으면 되므로 strlen() 불필요 |
| **바이너리 세이프** | `\0` 포함 데이터도 저장 가능 (길이는 `len`으로 관리) |
| **레이지 프리얼록** | 성장 시 `SDS_MAX_PREALLOC`(1MB) 이하면 2배, 이상이면 +1MB 추가 할당 |
| **메모리 절약** | `__packed__` 로 구조체 패딩 제거 |
| **보조 비트** | `flags`의 상위 5비트를 특수 목적으로 사용 가능 |

### EMBSTR 최적화 (object.c)

```c
// object.c:337
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44

// ≤44바이트: robj + sdshdr8 + 데이터를 한 번의 malloc으로 할당
// → 64바이트 jemalloc 아레나에 딱 맞음 (캐시 미스 감소)
// >44바이트: RAW 인코딩 (robj와 SDS를 별도 할당)
```

---

## 2. adlist (이중 연결 리스트)

**파일**: `adlist.h`, `adlist.c` (395줄)

### 구조체 정의

```c
// adlist.h:16
typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;          // 제네릭 포인터 - 어떤 타입도 저장 가능
} listNode;

typedef struct list {
    listNode *head;
    listNode *tail;
    void *(*dup)(void *ptr);    // 값 복사 콜백
    void (*free)(void *ptr);    // 값 해제 콜백
    int  (*match)(void *ptr, void *key); // 값 비교 콜백
    unsigned long len;
} list;
```

### 특징

- Redis **내부** 범용 리스트 (클라이언트 List 타입과 다름)
- 함수 포인터로 제네릭 구현: 타입별 dup/free/match 주입
- 양방향 순회 이터레이터 지원 (`AL_START_HEAD` / `AL_START_TAIL`)
- `listLinkNodeHead` / `listLinkNodeTail`: 이미 할당된 노드를 연결 (zero-copy)

```c
// adlist.c:90 - head 삽입 O(1)
void listLinkNodeHead(list* list, listNode *node) {
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
}
```

> **활용처**: 슬로우 로그, 클라이언트 리스트, 구독 목록 등 내부 관리용

---

## 3. dict (해시 테이블)

**파일**: `dict.h`, `dict.c` (2,340줄)

### 구조체 정의

```c
// dict.h:143
struct dict {
    dictType *type;            // 타입별 콜백 함수 모음
    dictEntry **ht_table[2];   // 두 개의 해시 테이블 (리해싱용)
    unsigned long ht_used[2];  // 각 테이블의 사용 중 엔트리 수
    long rehashidx;            // -1이면 리해싱 미진행
    unsigned pauserehash;      // >0이면 리해싱 일시 정지
    signed char ht_size_exp[2];// 테이블 크기 지수 (size = 1<<exp)
    int16_t pauseAutoResize;
    void *metadata[];          // 확장 메타데이터
};

// dict.c:48 - 엔트리 구조 (체이닝용 연결 리스트)
struct dictEntry {
    struct dictEntry *next;    // 충돌 체이닝
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t  s64;
        double   d;
    } v;                       // 값을 union으로 타입 효율화
};
```

### 해시 함수

```c
// dict.c:115 - SipHash 사용 (보안성 + 성능 균형)
uint64_t dictGenHashFunction(const void *key, size_t len) {
    return siphash(key, len, dict_hash_function_seed);
}
// 해시 시드는 서버 시작 시 랜덤 생성 → Hash Flooding 공격 방어
```

### 점진적 리해싱 (Incremental Rehashing)

```
ht_table[0] (old) ──────────┐
                              │← rehashidx 버킷씩 이동
ht_table[1] (new, 2x) ──────┘

- 읽기: ht_table[0] 먼저, 없으면 ht_table[1]
- 쓰기: 항상 ht_table[1]에만
- 매 dict 연산마다 1 스텝씩 진행 (블로킹 없음)
- dict_force_resize_ratio = 4 (강제 리사이즈 비율)
```

| 이벤트 | 동작 |
|--------|------|
| `used/buckets >= 1` (기본) | Expand 시작 |
| `used/buckets >= 4` | 강제 Expand (RESIZE_AVOID 중에도) |
| `used/buckets <= 1/32` | Shrink 시작 |

### 포인터 태깅 최적화 (Set 모드)

```c
// dict.h:121 - no_value=1 시 dictEntry 할당 생략
// 하위 3비트를 메타데이터로 활용 (8-byte alignment 보장)
// ENTRY_PTR_NORMAL=000, ENTRY_PTR_IS_ODD_KEY=XX1, ENTRY_PTR_IS_EVEN_KEY=010
unsigned int no_value:1;
unsigned int keys_are_odd:1;
```

---

## 4. listpack (컴팩트 리스트)

**파일**: `listpack.h`, `listpack.c` (3,340줄)

### 메모리 레이아웃

```
+──────────────────+──────────+──────────── ... ──+──────+
│ total-bytes (4B) │ num-elems│  entries ...      │ 0xFF │
│ (uint32 LE)      │ (uint16) │                   │ EOF  │
+──────────────────+──────────+──────────── ... ──+──────+
         ← LP_HDR_SIZE = 6 bytes →
```

### 엔트리 인코딩 (listpack.c:34~83)

**정수 인코딩:**

| 인코딩 | 첫 바이트 패턴 | 범위 | 총 크기 |
|--------|--------------|------|---------|
| 7-bit uint | `0xxxxxxx` | 0~127 | 2B (data+backlen) |
| 13-bit int | `110xxxxx xxxxxxxx` | -4096~4095 | 3B |
| 16-bit int | `0xF1` + 2B | int16 | 4B |
| 24-bit int | `0xF2` + 3B | int24 | 5B |
| 32-bit int | `0xF3` + 4B | int32 | 6B |
| 64-bit int | `0xF4` + 8B | int64 | 10B |

**문자열 인코딩:**

| 인코딩 | 첫 바이트 패턴 | 최대 길이 |
|--------|--------------|----------|
| 6-bit len  | `10xxxxxx` | 63 bytes |
| 12-bit len | `1110xxxx xxxxxxxx` | 4,095 bytes |
| 32-bit len | `0xF0` + 4B | ~4GB |

### 역방향 탐색을 위한 backlen 필드

```
각 엔트리 끝에 자신의 길이를 저장 (1~5바이트 가변)
→ lpPrev() 호출 시 backlen으로 이전 엔트리 시작 위치를 O(1)에 계산
```

> **ziplist 대비 개선점**: cascade update 문제 해소
> (ziplist는 앞 엔트리 삭제 시 prevrawlensize 변경이 연쇄 발생)

---

## 5. quicklist (List 타입 인코딩)

**파일**: `quicklist.h`, `quicklist.c` (3,658줄)

### 구조체 정의

```c
// quicklist.h:47 - 노드 (32 bytes)
typedef struct quicklistNode {
    struct quicklistNode *prev;
    struct quicklistNode *next;
    unsigned char *entry;          // listpack 데이터 포인터
    size_t sz;                     // 바이트 크기
    unsigned int count     : 16;   // listpack 내 항목 수 (최대 65536)
    unsigned int encoding  : 2;    // RAW=1, LZF=2
    unsigned int container : 2;    // PLAIN=1, PACKED(listpack)=2
    unsigned int recompress: 1;    // 임시 압축 해제 여부
    unsigned int attempted_compress: 1;
    unsigned int dont_compress : 1;
    unsigned int extra     : 9;
} quicklistNode;

// quicklist.h:107 - 리스트 헤더 (40 bytes on 64-bit)
typedef struct quicklist {
    quicklistNode *head;
    quicklistNode *tail;
    unsigned long count;           // 전체 엔트리 수
    unsigned long len;             // 노드 수
    size_t alloc_size;
    signed int fill     : QL_FILL_BITS;    // 노드당 최대 크기/개수
    unsigned int compress: QL_COMP_BITS;   // 양 끝 미압축 노드 수
    unsigned int bookmark_count: QL_BM_BITS;
    quicklistBookmark bookmarks[];
} quicklist;
```

### fill 설정값 (list-max-listpack-size, 기본값: -2)

```c
// quicklist.c:49 - 음수 = 바이트 제한, 양수 = 개수 제한
static const size_t optimization_level[] = {4096, 8192, 16384, 32768, 65536};
// -1: 4KB,  -2: 8KB(기본),  -3: 16KB,  -4: 32KB,  -5: 64KB
```

### LZF 압축 구조

```c
typedef struct quicklistLZF {
    size_t sz;          // 압축 후 크기
    char compressed[];  // LZF 압축 데이터
} quicklistLZF;

// 압축 조건: MIN_COMPRESS_BYTES(48B) 이상 & 압축 후 MIN_COMPRESS_IMPROVE(8B) 이상 절감
// compress=N: 양 끝 N개 노드는 압축 안 함 (자주 접근하는 HEAD/TAIL 최적화)
```

### 아키텍처

```
quicklist
  ├── [node1: listpack, RAW]  ← HEAD (미압축)
  ├── [node2: listpack, LZF]  ← 중간 (LZF 압축)
  ├── [node3: listpack, LZF]  ← 중간 (LZF 압축)
  └── [node4: listpack, RAW]  ← TAIL (미압축)
```

---

## 6. intset (정수 집합)

**파일**: `intset.h`, `intset.c` (566줄)

### 구조체 정의

```c
// intset.h:35
typedef struct intset {
    uint32_t encoding;  // INTSET_ENC_INT16/32/64
    uint32_t length;    // 요소 수
    int8_t   contents[];// 가변 배열 (정렬된 정수 배열)
} intset;
```

### 3가지 인코딩

```c
// intset.c:41
#define INTSET_ENC_INT16 (sizeof(int16_t))  // = 2
#define INTSET_ENC_INT32 (sizeof(int32_t))  // = 4
#define INTSET_ENC_INT64 (sizeof(int64_t))  // = 8

// intset.c:46 - 값 범위에 따라 자동 선택
static uint8_t _intsetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX) return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX) return INTSET_ENC_INT32;
    else return INTSET_ENC_INT16;
}
```

### 인코딩 업그레이드

```
INT16 → INT32 → INT64 (단방향, 다운그레이드 없음)
업그레이드 시: 전체 배열을 새 인코딩으로 재할당 + 기존값 변환
```

### 검색 및 삽입

- **이진 탐색** O(log N): 배열이 정렬 상태 유지
- **삽입 시**: 위치 찾아 memmove로 뒤 요소 이동 후 삽입
- **엔디안 처리**: Big-endian 시스템에서 바이트 반전 (`memrev16/32/64ifbe`)
- **기본 최대 크기**: `set-max-intset-entries = 512`

---

## 7. ziplist (레거시)

**파일**: `ziplist.h`, `ziplist.c` (2,665줄)

```
+─────────+──────────+──────+──────────── ... ──+──────+
│ zlbytes │ zltail   │ zlen │  entries ...      │ 0xFF │
│ (uint32)│ (uint32) │(u16) │                   │ EOF  │
+─────────+──────────+──────+──────────── ... ──+──────+
```

> **현재 상태**: listpack으로 대체되어 레거시 지원 목적으로만 유지됨.
> `prevrawlen` 필드 방식으로 cascade update 문제가 있어 listpack으로 교체됨.

---

## 8. skiplist (정렬 집합)

**파일**: `t_zset.c` (5,020줄), 구조체는 `server.h`에 정의

### 구조체 정의

```c
// server.h:1712 - 노드 메타데이터 (level[0].span에 내장)
typedef struct zskiplistNodeInfo {
    uint16_t sdsoffset; // 노드 시작~sds 데이터 오프셋
    uint8_t  levels;    // 이 노드의 레벨 수 (1~32)
    uint8_t  reserved;
} zskiplistNodeInfo;

// server.h:1718
typedef struct zskiplistNode {
    double score;
    struct zskiplistNode *backward;  // level 1 역방향 포인터
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned long span;  // level>0: 다음 노드까지 거리, level=0: NodeInfo 저장
    } level[];                       // 가변 길이 레벨 배열
    // sds ele는 level[] 뒤에 임베딩됨 (sdsoffset으로 접근)
} zskiplistNode;

// server.h:1730
typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;           // 현재 최대 레벨
    size_t alloc_size;
} zskiplist;

// server.h:1737 - zset = dict + skiplist 이중 구조
typedef struct zset {
    dict     *dict;  // member → node (O(1) score 조회)
    zskiplist *zsl;  // score 순서 유지 (범위 쿼리 O(log N))
} zset;
```

### 스킵리스트 파라미터

```c
// server.h:633
#define ZSKIPLIST_MAXLEVEL 32   // 2^64 엔트리까지 충분
#define ZSKIPLIST_P        0.25 // 각 레벨 승격 확률 1/4
```

### 랜덤 레벨 생성

```c
// t_zset.c:254
static int zslRandomLevel(void) {
    static const int threshold = ZSKIPLIST_P * RAND_MAX; // 25%
    int level = 1;
    while (random() < threshold)
        level += 1;
    return (level < ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}
// 기댓값: 레벨 L에 있을 확률 = (1/4)^(L-1)
// 평균 레벨 수: 1/(1-P) = 1.33
```

### zset 이중 구조의 역할

```
ZADD 시:
  1. dict에 member → node 포인터 저장  (O(1) score 조회)
  2. skiplist에 (score, member) 삽입   (O(log N) 정렬 유지)

SDS 문자열은 skiplist 노드에 임베딩되고,
dict는 해당 노드 포인터만 저장 → 메모리 공유 (중복 없음)
```

### 인코딩 전환

| 조건 | 인코딩 |
|------|--------|
| 요소 수 ≤ 128 AND 최대 값 길이 ≤ 64B | **listpack** |
| 초과 시 | **skiplist + dict** |

---

## 9. rax (기수 트리)

**파일**: `rax.h`, `rax.c` (2,117줄)

### 구조체 정의

```c
// rax.h:78
typedef struct raxNode {
    uint32_t iskey   : 1;  // 이 노드가 키를 나타내는가
    uint32_t isnull  : 1;  // 연결 값이 NULL인가
    uint32_t iscompr : 1;  // 압축 노드인가 (단일 자식 체인 압축)
    uint32_t size    : 29; // 자식 수 OR 압축 문자열 길이
    unsigned char data[];  // 문자 배열 + 자식 포인터 + 값 포인터
} raxNode;

// rax.h:113
typedef struct rax {
    raxNode *head;
    uint64_t  numele;   // 키 수
    uint64_t  numnodes; // 노드 수
    size_t   *alloc_size;
    void     *metadata[];
} rax;
```

### 압축 최적화

```
비압축 노드 (iscompr=0):
  [header][abc][a-ptr][b-ptr][c-ptr](value-ptr?)
  → 3개 자식 'a','b','c' 각각 포인터

압축 노드 (iscompr=1):
  [header][xyz][z-ptr](value-ptr?)
  → 단일 자식 체인 "xyz"를 한 노드에 압축

삽입 시: 노드 분할(split) 발생
삭제 시: 단일 자식 체인 발생 시 다시 압축(merge)
```

### 활용처

- **Stream 타입**: 메시지 ID를 키로 하는 rax 트리로 구현
- `stream_node_max_bytes = 4096` 기본값으로 각 rax 노드의 listpack 크기 제한

---

## 10. HyperLogLog

**파일**: `hyperloglog.c` (2,098줄)

### 알고리즘 구조

```
64-bit 해시값 적용:
  - 상위 14비트 → 레지스터 인덱스 (0~16383, 총 2^14 = 16384개)
  - 나머지 50비트 → 앞에서 처음 나오는 1의 위치(run length) → 레지스터 값 갱신

카디널리티 추정:
  E = alpha_m * m^2 * (Σ 2^(-M[j]))^(-1)
  (m = 16384, alpha_m ≒ 0.7213)
```

### 두 가지 표현 방식

**Dense 표현** (12KB):

```
+--------+--------+--------+──── ... ────+
|11000000|22221111|33333322|             |
+--------+--------+--------+──── ... ────+
 6비트 레지스터를 LSB부터 패킹 (바이트 경계 무시)
```

**Sparse 표현** (메모리 효율 우선):

```c
// 3가지 opcode로 런-렝스 인코딩
// ZERO  (00xxxxxx)          : N개(1~64) 연속 0 레지스터
// XZERO (01xxxxxx yyyyyyyy) : N개(1~16384) 연속 0 레지스터
// VAL   (1vvvvvxx)          : 값 vvvvv, N개(1~4) 연속 레지스터
```

### 16바이트 헤더

```
+──────+───+─────+────────────────────+
│ HYLL │ E │ N/U │ Cardinality (8B)   │
+──────+───+─────+────────────────────+
  (4B)  (1B) (3B)   (int64 LE, 캐시)

E: HLL_DENSE=0 or HLL_SPARSE=1
MSB=1이면 캐시 무효 → 재계산 필요
```

### SIMD 최적화

```c
#ifdef HAVE_AVX2
// AVX2 256비트 벡터 연산으로 dense 레지스터 집계 가속
#include <immintrin.h>
#endif

#ifdef HAVE_AARCH64_NEON
// ARM NEON 128비트 벡터 연산 지원
#include <arm_neon.h>
#endif
```

---

## 11. 인코딩 전환 임계값 요약

Redis 타입은 데이터 크기에 따라 메모리 효율적인 인코딩을 자동 선택한다.

### Redis 타입별 내부 인코딩

| Redis 타입 | 인코딩 1 (소규모) | 인코딩 2 (대규모) |
|-----------|-----------------|-----------------|
| String | INT (정수 fit) / EMBSTR (≤44B) | RAW (>44B) |
| List | LISTPACK | QUICKLIST |
| Set | INTSET (모두 정수) / LISTPACK | HT (dict) |
| Sorted Set | LISTPACK | SKIPLIST + dict |
| Hash | LISTPACK | HT (dict) |
| Stream | - | RAX (rax tree) |

### 인코딩 전환 기본값 (config.c)

| 설정 키 | 기본값 | 의미 |
|---------|--------|------|
| `hash-max-listpack-entries` | **512** | Hash: listpack → HT 전환 항목 수 |
| `hash-max-listpack-value` | **64 bytes** | Hash: listpack → HT 전환 값 크기 |
| `zset-max-listpack-entries` | **128** | ZSet: listpack → skiplist 전환 항목 수 |
| `zset-max-listpack-value` | **64 bytes** | ZSet: listpack → skiplist 전환 값 크기 |
| `set-max-intset-entries` | **512** | Set: intset → HT 전환 항목 수 |
| `set-max-listpack-entries` | **128** | Set: listpack → HT 전환 항목 수 |
| `set-max-listpack-value` | **64 bytes** | Set: listpack → HT 전환 값 크기 |
| `list-max-listpack-size` | **-2** (8KB) | List: 노드당 최대 listpack 크기 |
| `list-compress-depth` | **0** | List: 압축 안 함 (N 이면 양 끝 N개 제외 압축) |

### 자료구조 상속 관계

```
Redis 사용자 타입
│
├─ String ─────────── SDS
│                      └─ EMBSTR (단일 청크 할당, ≤44B)
│
├─ List ──────────── quicklist
│                      └─ listpack nodes (LZF 압축 가능)
│
├─ Set ─────────────┬─ intset      (정수 전용, 정렬 배열)
│                   ├─ listpack    (소규모 범용)
│                   └─ dict        (대규모)
│
├─ Sorted Set ──────┬─ listpack    (소규모)
│                   └─ zset        (dict + skiplist)
│
├─ Hash ────────────┬─ listpack    (소규모)
│                   └─ dict        (대규모)
│
├─ Stream ──────────── rax         (radix tree of listpacks)
│
└─ HyperLogLog ─────── SDS         (dense/sparse 표현)
```

---

## 핵심 설계 철학 요약

| 원칙 | 적용 예 |
|------|---------|
| **메모리 효율 우선** | SDS 헤더 5종, listpack 가변 인코딩, intset |
| **점진적 처리** | dict 인크리멘탈 리해싱 (블로킹 없음) |
| **이중 구조** | zset = skiplist + dict (읽기 패턴별 최적화) |
| **인코딩 자동 전환** | 소규모엔 압축, 대규모엔 속도 우선 |
| **캐시 지역성** | EMBSTR, listpack (연속 메모리), quicklist 압축 |
| **SIMD 활용** | HyperLogLog AVX2/NEON 가속 |

---

*분석 기준: Redis 소스 (`redis/src/`), 2024-present 버전 기준*
