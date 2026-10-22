# Redis 프로젝트 전체 분석 요약

> 분석 일자: 2026-04-13  
> 대상 경로: `/Users/apt/Documents/OpenSource/redis/`

---

## 1. 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 이름 | Redis (Remote Dictionary Server) |
| 분류 | 인메모리 데이터 구조 서버 / 데이터베이스 |
| 주 언어 | C (코어), TCL (테스트), Lua (스크립팅) |
| 라이선스 | RSALv2 / SSPLv1 / AGPLv3 (삼중 라이선스) |
| 코드 규모 | `.c` + `.h` 파일 208개, 약 348,191 라인 |
| 주요 용도 | 캐싱, 세션 관리, 실시간 분석, 메시지 브로커, 벡터 검색 |

Redis는 모든 데이터를 RAM에 보관하여 서브밀리초 수준의 레이턴시를 제공하는 고성능 인메모리 데이터 구조 서버다.

---

## 2. 디렉터리 구조

```
redis/
├── src/            # 핵심 소스 코드 (211개 .c/.h 파일)
├── deps/           # 외부 의존성 (Jemalloc, Lua, Hiredis 등)
├── modules/        # 내장 옵셔널 모듈 (RediSearch, RedisJSON 등)
├── tests/          # 테스트 스위트 (TCL 기반)
│   ├── unit/       # 단위 테스트
│   ├── integration/# 통합 테스트
│   ├── cluster/    # 클러스터 테스트
│   ├── sentinel/   # 센티널 테스트
│   ├── modules/    # 모듈 API 테스트
│   ├── vectorset/  # 벡터 셋 테스트
│   └── support/    # 테스트 지원 유틸리티
├── utils/          # 유틸리티 스크립트
├── redis.conf      # 기본 설정 파일 (118KB)
├── redis-full.conf # 확장 설정 파일
├── sentinel.conf   # 센티널 설정
└── Makefile        # 빌드 시스템
```

---

## 3. 소스 코드 아키텍처

### 3-1. 핵심 서버

| 파일 | 라인 수 | 역할 |
|------|---------|------|
| `server.c` | 8,196 | 메인 서버 로직, 초기화, 커맨드 디스패치 |
| `server.h` | 4,534 | 전역 데이터 구조 및 타입 정의 |

### 3-2. 데이터 타입 구현

| 파일 | 크기 | 담당 자료형 |
|------|------|------------|
| `t_string.c` | 46KB | String (GET/SET) |
| `t_list.c` | 53KB | List (LPUSH/RPOP/LRANGE) |
| `t_set.c` | 71KB | Set (SADD/SREM/SINTER) |
| `t_hash.c` | 154KB | Hash (HSET/HGET/HINCRBY) |
| `t_zset.c` | 250KB | Sorted Set (스킵 리스트 + 해시 테이블) |
| `t_stream.c` | 250KB | Stream (추가 전용 로그) |

### 3-3. 핵심 자료구조

| 파일 | 라인 수 | 역할 |
|------|---------|------|
| `dict.c/h` | 2,340 | 해시 테이블 (포인터 태깅 최적화 포함) |
| `quicklist.c/h` | 3,658 | 연결 리스트 + 리스트팩 혼합 구조 |
| `listpack.c/h` | 3,340 | ziplist의 현대적 대체제 |
| `ziplist.c/h` | 2,665 | 소형 리스트/해시용 압축 배열 (레거시) |
| `rax.c/h` | 2,117 | 라딕스 트리 (스트림 ID, 프리픽스 매칭) |
| `intset.c/h` | - | 정수 전용 컴팩트 셋 인코딩 |
| `hyperloglog.c` | 2,098 | 확률적 카디널리티 추정 |
| `ebuckets.c/h` | 2,725 | 키 만료를 위한 계층형 버킷 구조 |

### 3-4. 네트워킹 & 이벤트 루프

| 파일 | 역할 |
|------|------|
| `ae.c/h` | 이벤트 드리븐 라이브러리 (코어 이벤트 루프) |
| `ae_epoll.c` | Linux epoll 백엔드 |
| `ae_kqueue.c` | BSD/macOS kqueue 백엔드 |
| `ae_select.c` | POSIX select 폴백 |
| `ae_evport.c` | Solaris 이벤트 포트 백엔드 |
| `networking.c` | 클라이언트 처리, RESP 파싱, 응답 버퍼링 (5,783줄) |
| `anet.c/h` | 네트워크 소켓 추상화 계층 |
| `connection.c/h` | TLS/평문 연결 추상화 |
| `tls.c` | TLS/SSL 지원 (43KB) |

### 3-5. 퍼시스턴스

| 파일 | 라인 수 | 역할 |
|------|---------|------|
| `rdb.c/h` | 4,611 | RDB 스냅샷 (바이너리 포맷) |
| `aof.c` | 3,035 | AOF 커맨드 로그 |
| `rio.c/h` | - | I/O 추상화 계층 |

### 3-6. 복제 & 클러스터

| 파일 | 라인 수 | 역할 |
|------|---------|------|
| `replication.c` | 5,409 | 마스터-레플리카 복제, SYNC 프로토콜 |
| `cluster.c` | 2,348 | 클러스터 코디네이션 |
| `cluster.h` | 18KB | 클러스터 자료구조 |
| `cluster_legacy.c` | 6,581 | 레거시 클러스터 프로토콜 |

### 3-7. 스크립팅 & Lua

| 파일 | 역할 |
|------|------|
| `eval.c` (64KB) | EVAL/EVALSHA 커맨드 |
| `script.c/h` | 스크립트 관리 |
| `script_lua.c/h` | Lua 통합 |
| `function_lua.c` | Redis Functions (Lua) |

### 3-8. 메모리 관리

| 파일 | 역할 |
|------|------|
| `zmalloc.c/h` (53KB) | 메모리 할당 + 통계 추적 |
| `defrag.c` (1,976줄) | 능동적 메모리 조각화 해소 |
| `lazyfree.c` | 백그라운드 메모리 해제 |
| `evict.c` | 메모리 축출 정책 |
| `expire.c` (38KB) | 키 만료 처리 |

### 3-9. 인프라 & 유틸리티

| 파일 | 크기 | 역할 |
|------|------|------|
| `sds.c/h` | 53KB | Simple Dynamic String (Redis의 문자열 타입) |
| `acl.c` | 3,313줄 | Access Control List |
| `bio.c/h` | 16KB | 백그라운드 I/O (RDB 저장, AOF 재작성) |
| `iothread.c` | - | I/O 스레딩 |
| `config.c/h` | 3,776줄 | 런타임 설정 관리 |
| `commands.def` | 484KB | 자동 생성된 커맨드 메타데이터 |
| `sentinel.c` | 5,474줄 | 고가용성 모니터링 |
| `debug.c` | 2,866줄 | 디버깅 유틸리티 |

---

## 4. 빌드 시스템

- **빌드 도구**: GNU Make (`Makefile` → `src/Makefile` 위임)
- **빌드 타겟**: `redis-server`, `redis-cli`, `redis-benchmark`, `redis-check-aof`, `redis-check-rdb`, `redis-sentinel`
- **컴파일 플래그**: `-O3`, LTO(링크 타임 최적화), `-fno-omit-frame-pointer`
- **플랫폼**: Linux, macOS, BSD, Solaris 지원
- **할당자 옵션**: Jemalloc (Linux 기본), libc, tcmalloc
- **표준**: C11 원자 연산 지원, 미지원 시 C99 폴백

### 의존성 (deps/)

| 라이브러리 | 크기 | 역할 |
|-----------|------|------|
| jemalloc | 4.8MB | 메모리 할당자 (단편화 감소) |
| lua | 1.1MB | Lua 스크립팅 엔진 |
| hiredis | 724KB | Redis C 클라이언트 |
| xxhash | 328KB | 고속 해시 |
| fast_float | 152KB | 문자열→부동소수점 변환 |
| hdr_histogram | 92KB | 히스토그램 |
| linenoise | 80KB | 라인 편집 (CLI용) |
| fpconv | 36KB | 부동소수점→문자열 변환 |

---

## 5. 설정 (redis.conf)

주요 섹션별 핵심 옵션:

### 네트워크
- `bind`: 바인딩 인터페이스
- `port`: TCP 포트 (기본 6379)
- `protected-mode`: 보호 모드
- `tcp-keepalive`: TCP Keep-Alive 간격

### 퍼시스턴스
- **RDB**: `save`, `rdbcompression`, `dbfilename`
- **AOF**: `appendonly`, `appendfsync` (always/everysec/no)

### 복제
- `replicaof`: 마스터 주소
- `min-replicas-to-write`: 쓰기 쿼럼
- `repl-backlog-size`: 복제 버퍼 크기

### 메모리
- `maxmemory`: 메모리 한도
- `maxmemory-policy`: 축출 정책 (lru/lfu/random/ttl/noeviction 등)
- `lazyfree-lazy-eviction`: 백그라운드 축출

### 서버 동작
- `hz`: 서버 틱 주파수 (기본 10)
- `maxclients`: 최대 동시 연결 수

---

## 6. 테스트 인프라

- **테스트 프레임워크**: TCL 기반 (`test_helper.tcl`)
- **테스트 실행**: `make test`, `runtest`, `runtest-cluster`, `runtest-sentinel`

| 디렉터리 | 역할 |
|---------|------|
| `tests/unit/` | 개별 데이터 구조 및 커맨드 단위 테스트 |
| `tests/integration/` | 멀티 서버 시나리오 통합 테스트 |
| `tests/cluster/` | 클러스터 토폴로지, 페일오버, 마이그레이션 |
| `tests/sentinel/` | HA 모니터링 테스트 |
| `tests/modules/` | 모듈 API 검증 |
| `tests/vectorset/` | 벡터 검색 연산 테스트 |

---

## 7. 핵심 알고리즘 & 자료구조 상세

### 해시 테이블 (dict)
- 2의 거듭제곱 크기로 조정 가능한 해시 테이블
- 충돌 처리: 체이닝
- 포인터 태깅 최적화 (셋 전용 dict)
- **점진적 리해싱**: 블로킹 없이 증분적으로 크기 조정

### 스킵 리스트 (t_zset.c)
- Sorted Set의 핵심 자료구조
- O(log n) 삽입/삭제/검색
- 순방향/역방향 탐색 지원
- 스코어 및 위성 데이터 기반 비교

### 퀵리스트
- 연결 리스트 + 리스트팩 하이브리드
- 중간 노드에 LZF 압축 적용
- 노드 크기 및 압축 깊이 설정 가능
- 대형 리스트의 메모리 효율 극대화

### 인코딩 적응형 전략 (메모리 최적화)
| 자료형 | 작을 때 | 클 때 |
|--------|---------|-------|
| List | Listpack | Quicklist |
| Hash | Ziplist/Listpack | Hash Table |
| Set | Intset | Hash Table |
| Sorted Set | Listpack | Hash Table + Skip List |
| String | Embedded/Int | Raw SDS |

### 키 만료 (ebuckets)
- 계층형 버킷 구조로 만료 관리
- **능동적 만료**: 백그라운드 주기적 검사
- **지연 만료**: 커맨드 실행 시 체크
- TTL 분포에 따른 적응형 빈도 조정

---

## 8. 커맨드 시스템

### 커맨드 등록 & 디스패치 흐름

```
클라이언트 → RESP 프로토콜 전송
→ networking.c: RESP 파싱
→ server.c: 커맨드 테이블 조회 & 디스패치
→ 해당 커맨드 함수 실행 (트랜잭션/스크립트 컨텍스트 포함)
→ 응답 직렬화 → 클라이언트 전송
```

### 특수 커맨드 카테고리
- **트랜잭션**: `MULTI`/`EXEC`/`WATCH`
- **블로킹**: `BLPOP`, `BRPOP`, `BZPOPMIN` (데이터 대기)
- **Pub/Sub**: `PUBLISH`, `SUBSCRIBE` (패턴 기반 메시징)
- **Lua**: `EVAL`, `EVALSHA` (원자적 스크립트 실행)
- **클러스터**: `CLUSTER NODES`, `CLUSTER SLOTS`

### commands.def (484KB)
자동 생성된 커맨드 메타데이터:
- 커맨드 정의, 인수 스펙, 키 패턴
- 버전별 히스토리 및 사용 예시
- ACL 카테고리, 클러스터 리다이렉트용 키 스펙

---

## 9. 스레딩 모델

### 싱글 스레드 코어
- 메인 이벤트 루프 (`ae.c`)에서 모든 클라이언트 커맨드 처리
- 커맨드 실행은 순차적이며 원자적 (락 불필요)
- 단순성 + 고성능의 핵심 설계 철학

### 멀티스레드 컴포넌트

| 구성요소 | 파일 | 역할 |
|---------|------|------|
| I/O 스레드 | `iothread.c` | 병렬 소켓 I/O (읽기/쓰기 멀티플렉싱) |
| 백그라운드 I/O | `bio.c` | RDB 저장, AOF 재작성, Lazy Free |
| 지연 해제 | `lazyfree.c` | 대형 객체 백그라운드 메모리 해제 |
| 원자 변수 | `atomicvar.h` | C11 원자 연산으로 락 없는 동기화 |

### Copy-on-Write (fork 기반)
- `BGSAVE` 시 `fork()`로 자식 프로세스 생성
- 부모는 클라이언트 요청 계속 처리
- 자식이 스냅샷 디스크 저장

---

## 10. 이벤트 드리븐 아키텍처

### 이벤트 루프 동작

```
서버 실행 중:
  1. 파일 이벤트 폴링 (epoll/kqueue/select, 타임아웃: 1ms)
  2. 발생한 파일 이벤트 처리 (readable/writable)
  3. 보류 중인 타임 이벤트 실행
  4. before_sleep 훅 호출
  5. 다음 이벤트까지 대기
  6. after_sleep 훅 호출
```

### 이벤트 소스
- **클라이언트 연결**: 읽기 가능 이벤트 → 커맨드 처리
- **서버 크론 (100ms 주기)**:
  - 클라이언트 타임아웃 검사
  - RDB/AOF 백그라운드 작업
  - 복제 동기화
  - 메모리 축출
  - 능동적 만료 검사
  - 클러스터 하트비트

---

## 11. 퍼시스턴스 메커니즘

### RDB (스냅샷)
- 전체 데이터셋의 특정 시점 스냅샷
- 바이너리 포맷 (AOF 대비 더 컴팩트)
- 재시작 시 빠른 로딩
- 트리거: 시간 간격, 변경 횟수, 명시적 커맨드 (`SAVE`, `BGSAVE`)

### AOF (Append-Only File)
- 실행된 커맨드를 순차적으로 로그
- 텍스트 기반 (사람이 읽기 가능)
- Fsync 모드: `always` (안전) / `everysec` (균형) / `no` (빠름)
- 주기적 AOF 재작성으로 로그 압축

### 하이브리드 퍼시스턴스 (Redis 4.0+)
- RDB (대량 상태) + AOF (최근 커맨드) 결합
- 빠른 로딩 + 내구성 보장

---

## 12. 클러스터 & 센티널

### 클러스터
- 16,384개의 해시 슬롯으로 분산 저장
- 중앙 코디네이터 없음 (가십 프로토콜)
- 슬롯 마이그레이션 (원자적 연산)
- 자동 페일오버

### 센티널 (`sentinel.c` - 5,474줄)
- 마스터 인스턴스 모니터링
- 자동 페일오버 감지 및 레플리카 승격
- 설정 전파

---

## 13. 옵셔널 모듈 (modules/)

| 모듈 | 역할 |
|------|------|
| RedisBloom | 확률적 자료구조 (블룸 필터, 쿠쿠 필터) |
| RediSearch | 전문 검색 및 인덱싱 |
| RedisJSON | JSON 문서 지원 |
| RedisTimeSeries | 시계열 데이터 타입 |
| Vector-Sets | AI/ML 벡터 유사도 검색 |

**모듈 API** (`redismodule.h` - 112KB):
- 커스텀 데이터 타입 및 커맨드 등록
- 키 접근 (만료 포함), 클러스터 지원
- 블로킹 연산, ACL 통합, 키 메타데이터 클래스

---

## 14. 주요 설계 결정

| 결정 | 이유 |
|------|------|
| 인메모리 기본 저장소 | 서브밀리초 레이턴시 달성 |
| 싱글 스레드 코어 + 멀티 I/O | 락 없는 단순한 커맨드 로직 + 네트워크 병렬성 |
| 적응형 인코딩 | 메모리와 속도 사이의 자동 트레이드오프 |
| Fork 기반 CoW 스냅샷 | 무중단 백그라운드 저장 |
| 비동기 마스터-레플리카 복제 | 성능 우선, 오프셋 기반 부분 재동기화 |
| 슬롯 기반 클러스터 (16,384) | 중앙화 없는 분산 스토리지 |
| RESP 프로토콜 | 단순하고 파이프라이닝 가능한 텍스트 프로토콜 |
| 모듈 시스템 | 내부 API 직접 접근 가능한 동적 확장성 |
| 능동/지연 만료 이중 전략 | TTL 분포에 따른 적응형 메모리 관리 |
| Jemalloc + 능동적 조각화 해소 | 장기 실행 서버의 메모리 단편화 최소화 |

---

## 15. 성능 특성

### 복잡도 요약

| 자료형 | 주요 연산 | 복잡도 |
|--------|---------|--------|
| String | GET/SET | O(1) |
| List | LPUSH/RPOP | O(1), LRANGE O(n) |
| Set | SADD/SREM | O(1), SINTER O(n) |
| Hash | HSET/HGET | O(1), HSCAN O(n) |
| Sorted Set | ZADD/ZRANK | O(log n) |
| HyperLogLog | PFADD/PFCOUNT | O(1) |
| Stream | XADD | O(1), XRANGE O(log n) |

### 처리량
- 단일 인스턴스: 100K+ ops/sec
- I/O 스레딩 활성화 시 수배 향상

---

## 16. 최근 개발 동향 (unstable 브랜치)

- **I/O 스레딩 강화**: 프라이머리/레플리카 클라이언트 처리 개선
- **메모리 최적화**: 임계값 기반 피크 메모리 통계
- **스트림**: `XNACK` 커맨드 (컨슈머 그룹 개선)
- **Keymeta 인프라**: 커스텀 키 메타데이터 지원
- **벡터 연산**: Vector Set 기능 개선
- **클러스터**: 슬롯 통계 및 마이그레이션 개선

---

*이 요약은 `/Users/apt/Documents/OpenSource/redis/` 디렉터리의 전체 소스 코드 정적 분석을 기반으로 작성되었습니다.*
