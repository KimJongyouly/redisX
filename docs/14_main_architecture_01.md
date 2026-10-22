# Redis 주요 설계 결정 (Main Architecture & Design Decisions) 소스 분석

## 개요

Redis의 아키텍처는 수십 년간 축적된 다수의 핵심 설계 결정들로 이루어져 있다. 각 결정은 성능, 단순성, 정확성 사이의 트레이드오프를 명확하게 반영한다.

---

## 1. 단일 스레드 커맨드 실행 모델

### 설계 결정

모든 커맨드 실행은 **메인 스레드 하나**에서 순차적으로 처리된다.

```
클라이언트 요청
    │
    ▼
IO 스레드 (네트워크 읽기/파싱)  ← 멀티스레드
    │
    ▼
메인 스레드 (커맨드 실행)        ← 싱글스레드 ★
    │
    ▼
IO 스레드 (네트워크 쓰기)        ← 멀티스레드
```

### 이유

1. **락(Lock) 없는 자료구조**: 모든 데이터 구조(dict, skiplist 등)에 뮤텍스 불필요
2. **원자성 보장**: `MULTI/EXEC` 트랜잭션, Lua 스크립트의 원자 실행이 자연스럽게 보장
3. **디버깅 단순화**: 레이스 컨디션, 데드락 없음
4. **실제 병목은 메모리**: CPU가 아니라 메모리 접근 대역폭이 병목

### 보완 구조

| 스레드 종류 | 역할 | 개수 제한 |
|------------|------|---------|
| 메인 스레드 | 커맨드 실행, 이벤트 루프 | 1개 |
| IO 스레드 | 네트워크 read/write, 커맨드 파싱 | 최대 128개 (`IO_THREADS_MAX_NUM`) |
| BIO 스레드 | fsync, 파일 닫기, lazy free | 3개 고정 (bio_aof, bio_close_file, bio_lazy_free) |
| 모듈 스레드 | `RedisModule_BlockClient()` 블로킹 작업 | 모듈별 자유 |

---

## 2. `redisObject` (robj) 구조체 설계

### 핵심 구조 (`src/object.h:99`)

```c
struct redisObject {
    unsigned type     : 4;   // OBJ_TYPE_* (STRING/LIST/SET/ZSET/HASH/STREAM/MODULE)
    unsigned encoding : 4;   // OBJ_ENCODING_* (13종)
    unsigned refcount : 23;  // 참조 카운트
    unsigned iskvobj  : 1;   // kvobj 기본 구조체 여부 (Redis 8+)
    unsigned metabits : 8;   // 메타데이터 비트맵 (kvobj용)
    unsigned lru      : 24;  // LRU 타임스탬프 또는 LFU 데이터
    void *ptr;               // 실제 데이터 포인터
};
```

**총 크기**: 16바이트 (8바이트 비트필드 + 8바이트 ptr)

### type + encoding 조합 (적응형 인코딩)

| 타입 | 초기 인코딩 | 업그레이드 조건 | 최종 인코딩 |
|------|------------|----------------|------------|
| `STRING` | `INT` | 정수가 아닌 경우 | `EMBSTR`(≤44) or `RAW` |
| `LIST` | `LISTPACK` | 원소수 > 128 또는 원소 > 64바이트 | `QUICKLIST` |
| `HASH` | `LISTPACK` | 원소수 > 128 또는 원소 > 64바이트 | `HT` |
| `SET` | `LISTPACK` | intset 변환 가능 시 `INTSET`, 이후 크기 초과 | `HT` |
| `ZSET` | `LISTPACK` | 원소수 > 128 또는 원소 > 64바이트 | `SKIPLIST`+`HT` |
| `STREAM` | `STREAM` | — | `STREAM` (radix tree + listpack) |

**구식 인코딩** (소스에 상수는 남아있으나 더 이상 사용 안 함):
- `ZIPMAP(3)`, `LINKEDLIST(4)`, `ZIPLIST(5)` → listpack으로 대체

### LRU/LFU 24비트 공유

```c
#define LRU_BITS 24
#define LRU_CLOCK_MAX        ((1<<24)-1)  // 16,777,215 ≈ 194일
#define LRU_CLOCK_RESOLUTION 1000         // 1초 단위
```

**LRU 모드**: 상위 24비트 = 마지막 접근 시각 (초 단위, 전역 `lru_clock` 기준)

**LFU 모드**: 하위 8비트 = 접근 빈도(Morris counter), 상위 16비트 = 마지막 감소 시각

### refcount 특수값

```c
#define OBJ_SHARED_REFCOUNT  ((1<<23)-1)  // 8,388,607: 공유 객체 (절대 해제 안 함)
#define OBJ_STATIC_REFCOUNT  ((1<<23)-2)  // 스택에 할당된 객체
```

### EMBSTR 최적화 (`object.c:337`)

```c
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44
```

**단일 할당**으로 `robj` + `sdshdr8` + 문자열 데이터를 연속 배치:
```
[robj 16B][sdshdr8 3B][data 44B]['\0' 1B] = 64B = CPU 캐시 라인 1개
```

44바이트 초과 시 `RAW` (robj와 SDS가 별도 할당, 포인터로 연결).

---

## 3. kvobj: 키-값 통합 구조 (Redis 8+ 신규)

### 설계 동기

기존 Redis는 키(SDS)와 값(robj)을 각각 별도로 할당 → 캐시 미스, 메모리 오버헤드

### 메모리 레이아웃 (`src/object.h:38`)

```
기본 kvobj (키만):
┌──────────────┬──────────────┬──────────────────────┐
│ serverObject │ key-hdr-size │  sdshdr5 "mykey" \0  │
│   16 bytes   │   1 byte     │  1 + 5 + 1 = 7 bytes │
└──────────────┴──────────────┴──────────────────────┘

만료시간 포함 kvobj (metabits=0b00000001):
┌──────────────┬──────────────┬──────────────┬──────────────────────┐
│  Expiry Time │ serverObject │ key-hdr-size │  sdshdr5 "mykey" \0  │
│   8 bytes    │   16 bytes   │   1 byte     │  1 + 5 + 1 = 7 bytes │
└──────────────┴──────────────┴──────────────┴──────────────────────┘
                ▲
                kvobjCreate() 반환 포인터
```

**메타데이터 클래스**: 최대 8개 (metabits 비트맵), 클래스 0 = 만료 시각. 실제 메타데이터 블록은 kvobj 앞에 역순으로 배치.

```c
// 메타데이터 접근
uint64_t *kvobjMetaRef(kvobj *kv, int metaId) {
    uint32_t numMetaBytes = __builtin_popcount(kv->metabits) * sizeof(uint64_t);
    // kv 앞 메모리에서 metaId에 해당하는 블록 반환
}
```

---

## 4. 메모리 관리 설계

### zmalloc 추상화 (`src/zmalloc.c`)

```c
// 할당자 우선순위
#ifdef USE_JEMALLOC   → jemalloc  (기본, 단편화 최소화)
#elif USE_TCMALLOC    → tcmalloc
#elif USE_HOARD       → Hoard
#else                 → libc malloc
```

모든 할당에 앞서 `PREFIX_SIZE` (8바이트)에 크기 기록 → `zmalloc_used_memory()` O(1).

### 공유 정수 (`OBJ_SHARED_INTEGERS = 10000`)

정수 0~9999는 서버 시작 시 미리 생성, refcount = `OBJ_SHARED_REFCOUNT`.
INCR/INCRBY 등의 결과로 자주 나오는 값의 메모리 할당 제로.

### EMBSTR 불변성

EMBSTR은 **읽기 전용**: 수정 시 자동으로 RAW로 변환.
이유: robj와 SDS가 하나의 연속 블록 → SDS만 재할당 불가.

### Lazy Free (비동기 해제)

```c
// 설정 옵션
lazyfree_lazy_eviction    // eviction 시 비동기
lazyfree_lazy_expire      // 만료 키 비동기 삭제
lazyfree_lazy_server_del  // DEL 커맨드 비동기
lazyfree_lazy_user_del    // 사용자 DEL 비동기
lazyfree_lazy_user_flush  // FLUSHDB/FLUSHALL 비동기
```

큰 자료구조(수백만 원소 해시) 삭제 시 메인 스레드 블로킹 방지 → `BIO_LAZY_FREE` 작업으로 위임.

### 액티브 디프래그

`activeDefragCycle()`: 이벤트 루프 유휴 시 조각난 메모리 블록 재배치.
`jemalloc_frag_hint` API로 단편화율 확인 후 `activeDefragAlloc()`으로 객체 이동.

---

## 5. SDS (Simple Dynamic Strings) 설계

### 5가지 헤더 타입

```c
struct sdshdr5  { unsigned char flags; char buf[]; }                     // ≤31 bytes
struct sdshdr8  { uint8_t  len, alloc; unsigned char flags; char buf[]; } // ≤255 bytes
struct sdshdr16 { uint16_t len, alloc; unsigned char flags; char buf[]; } // ≤65535 bytes
struct sdshdr32 { uint32_t len, alloc; unsigned char flags; char buf[]; }
struct sdshdr64 { uint64_t len, alloc; unsigned char flags; char buf[]; }
```

**설계 원칙**:
- `buf[-1]` = flags (헤더 타입 식별)
- `sds` 타입은 `char *`로 `buf`를 직접 가리킴 → C 문자열 호환
- `len` 분리: `strlen()` O(1), 이진 안전(null 포함 가능)
- `alloc - len` = 여유 공간 → append 시 재할당 최소화

---

## 6. dict 설계 (해시 테이블)

### 점진적 리해시 (Incremental Rehash)

```
ht[0]: 기존 해시 테이블 (N 버킷)
ht[1]: 새 해시 테이블 (2N 버킷)
rehashidx: 현재 리해시 중인 버킷 인덱스

커맨드 실행마다 ht[0][rehashidx] 버킷의 항목들 → ht[1] 이동
조회 시 ht[0]과 ht[1] 모두 검색
삽입은 항상 ht[1]에만
rehashidx = -1 이면 리해시 완료
```

**이유**: 대규모 dict 리해시 시 수백 밀리초 블로킹 방지. 1 버킷/연산으로 분산.

### SipHash-1-3

해시 함수로 SipHash-1-3 사용 (2013년 Jean-Philippe Aumasson 설계).
**이유**: Hash DoS 공격 방어. 공격자가 같은 버킷으로 해시되는 키를 대량 삽입해 O(N) 성능 저하 유발 가능 → 랜덤 시드로 예측 불가.

---

## 7. 키 만료(Expiry) 전략

### 이중 전략

**1. 수동 만료 (Lazy Expiry)**: 키 접근 시 만료 여부 확인
```c
robj *lookupKeyRead(redisDb *db, robj *key) {
    expireIfNeeded(db, key, ...);  // 접근 전 만료 체크
    return lookupKey(db, key, flags);
}
```

**2. 능동 만료 (Active Expiry)**: `serverCron()`에서 주기적 샘플링
```c
void activeExpireCycle(int type) {
    // SLOW 모드: 25% CPU 시간 사용
    // FAST 모드: 1ms 이내 빠른 실행
    // 각 DB에서 20개 랜덤 샘플링 → 만료된 것 삭제
    // 만료율 > 25% 이면 반복
}
```

**kvobj 통합**: 만료 시각이 kvobj의 metabits 클래스 0에 내장되어 별도 `expires` 딕셔너리 불필요 (Redis 8+ 최적화).

---

## 8. 메모리 축출(Eviction) 전략 (10종)

```c
MAXMEMORY_VOLATILE_LRU    // TTL 있는 키만 LRU
MAXMEMORY_VOLATILE_LFU    // TTL 있는 키만 LFU
MAXMEMORY_VOLATILE_TTL    // TTL 가장 짧은 키
MAXMEMORY_VOLATILE_RANDOM // TTL 있는 키 랜덤
MAXMEMORY_VOLATILE_LRM    // TTL 있는 키 LRM
MAXMEMORY_ALLKEYS_LRU     // 전체 키 LRU
MAXMEMORY_ALLKEYS_LFU     // 전체 키 LFU
MAXMEMORY_ALLKEYS_RANDOM  // 전체 키 랜덤
MAXMEMORY_ALLKEYS_LRM     // 전체 키 LRM
MAXMEMORY_NO_EVICTION     // 축출 안 함 (에러 반환)
```

### 근사 LRU/LFU

정확한 LRU는 전체 키 정렬 필요 → O(N). Redis는 **샘플링 근사**:
- `MAXMEMORY_SAMPLES` (기본 5) 개의 키를 랜덤 샘플링
- Eviction Pool (16개)에 후보 축적
- LRU 시각이 가장 오래된 키 축출

**LFU Morris Counter**:
- 8비트로 수백만까지 표현 가능한 확률적 카운터
- 접근마다 `1/(현재값 * lfu_log_factor + 1)` 확률로 증가
- `LFU_INIT_VAL = 5` (새 키의 초기값, 즉시 축출 방지)
- 시간 경과에 따른 감소: `lfu_decay_time` 분마다 카운터 -1

---

## 9. Copy-On-Write (COW) 기반 퍼시스턴스

### 설계

```
fork()
  ├─ 부모 프로세스: 계속 클라이언트 요청 처리
  │    쓰기 발생 시 OS가 해당 페이지를 복사
  │    (COW: Copy-On-Write)
  │
  └─ 자식 프로세스: 스냅샷 시점의 메모리 뷰로 RDB/AOF 생성
       완료 후 exitFromChild()
       sendChildCowInfo() → 부모에게 COW 크기 보고
```

**이유**: 별도 스냅샷 메모리 불필요. `fork()` 직후 두 프로세스가 물리 메모리 공유, 변경된 페이지만 복사. 데이터 크기와 관계없이 fork 자체는 O(1).

**주의**: 쓰기가 많으면 COW 증폭 → 메모리 사용량 2배까지 가능.

---

## 10. 이벤트 루프 (ae) 설계

### 4개 플랫폼 백엔드

```
Linux   → epoll  (레벨 트리거 + EPOLLERR/HUP)
macOS   → kqueue (2-pass merge: 읽기+쓰기 이벤트 통합)
Solaris → evport
기타    → select (FD_SETSIZE 제한, O(maxfd) 스캔, 폴백)
```

### `AE_BARRIER` 플래그

```c
#define AE_READABLE 1
#define AE_WRITABLE 2
#define AE_BARRIER  4   // 같은 이벤트루프 반복에서 read 전에 write 실행
```

**이유**: 클라이언트가 커맨드를 보내고 그 응답을 받기 전에 먼저 파일에 데이터를 fsync해야 하는 경우(`AOF_FSYNC_ALWAYS`)를 위한 순서 보장.

### beforeSleep / afterSleep 생명주기

`beforeSleep()`: `aeApiPoll()` 직전 14가지 작업 수행 (AOF flush, lazy expire, replication 등)
`afterSleep()`: `aeApiPoll()` 직후 GIL 획득, 시간 캐시 갱신

---

## 11. RESP 프로토콜 설계

### RESP2 vs RESP3

| 항목 | RESP2 | RESP3 |
|------|-------|-------|
| 타입 | 5종 | 13종+ |
| Map | 배열 쌍으로 표현 | 네이티브 Map 타입 |
| Double | 문자열로 전달 | Double 타입 |
| 클라이언트 감지 | 불필요 | 필요 (HELLO 3) |

**설계 원칙**: 텍스트 기반으로 디버깅 용이, `*N\r\n`으로 배열 길이 선언하여 단일 read() 호출로 완전한 응답 파싱 가능.

---

## 12. 클러스터 해시 슬롯 설계

### 16384 슬롯 선택 이유

- 2^14 = 16384: 비트맵으로 2KB (`CLUSTER_SLOTS/8`)에 표현
- 각 클러스터 메시지(PING/PONG)에 2KB 슬롯 비트맵 포함 가능
- 1000개 노드 클러스터에서도 평균 16 슬롯/노드 → 균등 분산
- 너무 많으면(2^16=65536) 메시지가 너무 커짐

### 해시 태그 (`cluster.h:60`)

```c
// {tag} 패턴: tag 부분만 CRC16 해싱
// → 여러 키를 같은 슬롯에 강제 배치 (multi-key 연산 지원)
return crc16(key+s+1, e-s-1) & 0x3FFF;
```

### Config Epoch (단조 증가 논리 시계)

```c
uint64_t currentEpoch;   // 클러스터 전역 epoch
uint64_t configEpoch;    // 각 노드의 설정 버전
```

슬롯 소유권 충돌 시 **더 높은 configEpoch** 가 승리 → 네트워크 분할 후 재합류 시 일관성 보장.

---

## 13. Sentinel Raft-like 리더 선출

### RAFT 변형

Redis Sentinel은 완전한 Raft가 아닌 단순화된 버전:
- 리더를 선출할 때만 투표 (로그 복제 없음)
- 당선 조건: `votes > voters/2 + 1` AND `votes >= quorum`
- 각 epoch당 1표만 행사

**이유**: 완전한 Raft는 복잡도가 높음. Redis Sentinel의 목적은 마스터 장애 시 리더 센티널을 선출하여 페일오버를 조율하는 것이므로 단순화된 선출로 충분.

---

## 14. 적응형 인코딩 업그레이드 전략

### 단방향 업그레이드

```
LISTPACK → HT       (단방향, 다운그레이드 없음)
LISTPACK → QUICKLIST
LISTPACK → INTSET → HT
```

**이유**: 다운그레이드는 안전하지 않음. 예를 들어 HT → LISTPACK 전환 시 모든 원소가 listpack 크기 제한을 만족하는지 확인 필요, 이를 위한 추가 스캔 비용 대비 실익 없음.

### listpack이 ziplist를 대체한 이유

- `ziplist`: 앞 원소의 크기 변경 시 **연쇄 재인코딩(cascade update)** 발생 가능 → O(N²) 최악
- `listpack`: 각 항목이 **자신의 길이만 저장** (이전 항목 크기 없음) → cascade update 없음

---

## 15. 복제 백로그 (Replication Backlog) 설계

### Ring Buffer 기반

```
replication_buffer (링 버퍼, 기본 1MB)
  └─ refcount 기반 공유 (슬레이브 수에 무관하게 1개만 유지)
```

각 슬레이브는 `repl_offset`을 추적. 슬레이브가 잠시 끊겼다 재연결 시:
- 오프셋이 백로그 범위 내 → 부분 재동기화 (PSYNC)
- 범위 초과 → 전체 재동기화 (FULLRESYNC, RDB 전송)

**백로그 공유**: 여러 슬레이브가 같은 `replication_buffer` 노드를 `refcount`로 공유. IO 스레드가 슬레이브별로 독립적 전송.

---

## 16. 주요 설계 원칙 요약

| 원칙 | 구현 | 트레이드오프 |
|------|------|------------|
| **단순성 우선** | 싱글스레드 커맨드 실행 | 코어 1개만 사용 (IO 스레드로 보완) |
| **메모리 효율** | 적응형 인코딩 (listpack→HT) | 소규모엔 느린 인코딩, 대규모엔 빠른 인코딩 |
| **캐시 친화성** | EMBSTR 64바이트 캐시라인, kvobj 인라인 키 | 크기 제한 (44바이트) |
| **점진적 작업** | 증분 rehash, 점진적 expire | 단일 연산이 약간 느려짐 |
| **COW 활용** | fork 기반 스냅샷 | 쓰기 많으면 메모리 2배 |
| **근사로 충분** | 샘플링 LRU/LFU | 완벽하지 않지만 충분히 좋음 |
| **비동기로 분산** | BIO fsync/lazyfree, IO 스레드 | 복잡도 증가, 순서 보장 필요 |
| **이중 안전망** | lazy + active expire | CPU 낭비 최소화 + 메모리 누수 방지 |

---

## 17. Redis 8+ 주요 아키텍처 변화

| 변화 | 이전 | 이후 |
|------|------|------|
| **kvobj** | 키/값 별도 할당 | 키 인라인 임베딩 |
| **만료 저장** | 별도 `expires` 딕셔너리 | kvobj metabits 클래스 0 |
| **Vector Sets** | 외부 .so 모듈 | 바이너리 내장 (`INCLUDE_VEC_SETS`) |
| **IO 스레드** | 선택적 | 더욱 통합된 IO 스레드 모델 |
| **listpack_ex** | `LISTPACK` 인코딩 | `LISTPACK_EX` (확장 메타데이터) |

---

**참조 파일**:
- [src/object.h](redis/src/object.h) — robj/kvobj 구조체 정의
- [src/object.c](redis/src/object.c) — 객체 생성/변환 구현
- [src/server.h](redis/src/server.h) — 전역 서버 상태, 설정 상수
- [src/ae.h](redis/src/ae.h) / [src/ae.c](redis/src/ae.c) — 이벤트 루프
- [src/dict.c](redis/src/dict.c) — 해시 테이블 (SipHash, 점진적 리해시)
- [src/sds.c](redis/src/sds.c) — SDS 문자열
- [src/zmalloc.c](redis/src/zmalloc.c) — 메모리 관리 추상화
- [src/expire.c](redis/src/expire.c) / [src/lazyfree.c](redis/src/lazyfree.c) — 만료/지연 해제
- [src/evict.c](redis/src/evict.c) — LRU/LFU 축출 정책
- [src/cluster_legacy.c](redis/src/cluster_legacy.c) — 클러스터 구현
- [src/sentinel.c](redis/src/sentinel.c) — Sentinel 구현
