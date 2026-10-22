# Redis 데이터 타입 정리

## 개요

Redis는 **Re**mote **Di**ctionary **S**erver의 약자로, 인메모리 기반의 키-값(Key-Value) 저장소입니다.
단순한 문자열 외에도 다양한 데이터 구조를 지원하며, 각 타입은 고유한 명령어와 활용 목적을 가집니다.

---

## 1. String (문자열)

가장 기본적인 데이터 타입. 바이너리 세이프(binary-safe)하며 최대 **512MB**까지 저장 가능.

### 특징
- 텍스트, 숫자, 직렬화된 JSON, 바이너리 등 모든 값을 저장 가능
- 숫자 값은 원자적(atomic) 증감 연산 지원

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `SET` | 값 저장 | `SET key "hello"` |
| `GET` | 값 조회 | `GET key` |
| `DEL` | 키 삭제 | `DEL key` |
| `INCR` | 정수 값 1 증가 | `INCR counter` |
| `INCRBY` | 정수 값 N 증가 | `INCRBY counter 10` |
| `DECR` | 정수 값 1 감소 | `DECR counter` |
| `APPEND` | 문자열 뒤에 추가 | `APPEND key " world"` |
| `STRLEN` | 문자열 길이 반환 | `STRLEN key` |
| `MSET` | 여러 키-값 동시 저장 | `MSET k1 v1 k2 v2` |
| `MGET` | 여러 키 동시 조회 | `MGET k1 k2` |
| `SETEX` | TTL과 함께 값 저장 | `SETEX key 60 "value"` |
| `SETNX` | 키가 없을 때만 저장 | `SETNX key "value"` |

### 활용 사례
- 세션 토큰, 캐싱, 카운터(조회수, 좋아요), 분산 락

---

## 2. List (리스트)

Linked List 기반의 **순서 있는 문자열 컬렉션**. 인덱스로 접근 가능하며 양 끝에서의 삽입/삭제가 O(1).

### 특징
- 중복 값 허용
- 최대 약 **42억 개** 요소 저장 가능
- 스택(Stack)과 큐(Queue) 구현에 적합

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `LPUSH` | 왼쪽(앞)에 삽입 | `LPUSH mylist "a"` |
| `RPUSH` | 오른쪽(뒤)에 삽입 | `RPUSH mylist "b"` |
| `LPOP` | 왼쪽에서 꺼내기 | `LPOP mylist` |
| `RPOP` | 오른쪽에서 꺼내기 | `RPOP mylist` |
| `LRANGE` | 범위 조회 | `LRANGE mylist 0 -1` |
| `LLEN` | 리스트 길이 | `LLEN mylist` |
| `LINDEX` | 인덱스로 조회 | `LINDEX mylist 0` |
| `LSET` | 인덱스 위치 값 변경 | `LSET mylist 0 "x"` |
| `LINSERT` | 특정 값 앞/뒤에 삽입 | `LINSERT mylist BEFORE "b" "ab"` |
| `LREM` | 특정 값 삭제 | `LREM mylist 1 "a"` |
| `BLPOP` | 블로킹 방식으로 왼쪽 꺼내기 | `BLPOP mylist 5` |
| `BRPOP` | 블로킹 방식으로 오른쪽 꺼내기 | `BRPOP mylist 5` |

### 활용 사례
- 메시지 큐, 최근 활동 로그, 타임라인, 작업 대기열

---

## 3. Set (집합)

**순서 없는 고유한 문자열의 집합**. 중복을 자동으로 제거.

### 특징
- 순서 보장 없음
- 집합 연산(합집합, 교집합, 차집합) 지원
- O(1) 삽입/삭제/검색

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `SADD` | 요소 추가 | `SADD myset "a" "b"` |
| `SREM` | 요소 삭제 | `SREM myset "a"` |
| `SMEMBERS` | 전체 조회 | `SMEMBERS myset` |
| `SISMEMBER` | 요소 포함 여부 확인 | `SISMEMBER myset "a"` |
| `SCARD` | 요소 개수 | `SCARD myset` |
| `SPOP` | 랜덤 요소 꺼내기 | `SPOP myset` |
| `SRANDMEMBER` | 랜덤 요소 조회(삭제 없음) | `SRANDMEMBER myset 2` |
| `SUNION` | 합집합 | `SUNION set1 set2` |
| `SINTER` | 교집합 | `SINTER set1 set2` |
| `SDIFF` | 차집합 | `SDIFF set1 set2` |
| `SUNIONSTORE` | 합집합 결과를 새 키에 저장 | `SUNIONSTORE dest set1 set2` |
| `SINTERSTORE` | 교집합 결과를 새 키에 저장 | `SINTERSTORE dest set1 set2` |

### 활용 사례
- 태그 관리, 팔로워/팔로잉 관계, 중복 방지, 공통 친구 추출

---

## 4. Sorted Set / ZSet (정렬된 집합)

Set에 **score(점수)** 가 추가된 형태. score를 기준으로 자동 정렬.

### 특징
- 각 요소는 고유(중복 불가)하지만 score는 중복 가능
- score는 `double` 타입 부동소수점
- score가 같으면 사전(lexicographic) 순서로 정렬
- 범위 검색에 최적화

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `ZADD` | 요소 추가 (score 포함) | `ZADD rank 100 "user1"` |
| `ZREM` | 요소 삭제 | `ZREM rank "user1"` |
| `ZSCORE` | 요소의 score 조회 | `ZSCORE rank "user1"` |
| `ZRANK` | 오름차순 순위 조회 (0부터) | `ZRANK rank "user1"` |
| `ZREVRANK` | 내림차순 순위 조회 | `ZREVRANK rank "user1"` |
| `ZRANGE` | score 오름차순 범위 조회 | `ZRANGE rank 0 -1` |
| `ZREVRANGE` | score 내림차순 범위 조회 | `ZREVRANGE rank 0 -1` |
| `ZRANGEBYSCORE` | score 범위로 조회 | `ZRANGEBYSCORE rank 50 100` |
| `ZCARD` | 요소 개수 | `ZCARD rank` |
| `ZCOUNT` | 특정 score 범위 요소 수 | `ZCOUNT rank 50 100` |
| `ZINCRBY` | score 증가 | `ZINCRBY rank 10 "user1"` |
| `ZUNIONSTORE` | 합집합 결과 저장 | `ZUNIONSTORE dest 2 z1 z2` |
| `ZINTERSTORE` | 교집합 결과 저장 | `ZINTERSTORE dest 2 z1 z2` |

### 활용 사례
- 리더보드(랭킹), 우선순위 큐, 시간 기반 이벤트 스케줄링, 자동완성

---

## 5. Hash (해시)

**필드-값(field-value) 쌍의 컬렉션**. 하나의 키 아래 여러 속성을 저장하는 구조.

### 특징
- 객체(Object)를 저장하기에 적합
- 필드 개수가 적을 때 메모리 효율이 높음 (ziplist 인코딩)
- 필드 단위로 개별 읽기/쓰기 가능

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `HSET` | 필드 값 설정 | `HSET user:1 name "Alice"` |
| `HGET` | 필드 값 조회 | `HGET user:1 name` |
| `HMSET` | 여러 필드 동시 설정 | `HMSET user:1 name "Alice" age 30` |
| `HMGET` | 여러 필드 동시 조회 | `HMGET user:1 name age` |
| `HGETALL` | 모든 필드-값 조회 | `HGETALL user:1` |
| `HDEL` | 필드 삭제 | `HDEL user:1 age` |
| `HEXISTS` | 필드 존재 여부 | `HEXISTS user:1 name` |
| `HKEYS` | 모든 필드명 조회 | `HKEYS user:1` |
| `HVALS` | 모든 값 조회 | `HVALS user:1` |
| `HLEN` | 필드 개수 | `HLEN user:1` |
| `HINCRBY` | 필드 정수 값 증가 | `HINCRBY user:1 age 1` |
| `HINCRBYFLOAT` | 필드 부동소수 값 증가 | `HINCRBYFLOAT user:1 score 1.5` |

### 활용 사례
- 사용자 프로필, 상품 정보, 설정 데이터 저장

---

## 6. Bitmap (비트맵)

String 타입을 비트 단위로 조작하는 **비트 배열**. 메모리 효율이 매우 높음.

### 특징
- 최대 2³² 비트 (약 42억 비트 = 512MB) 지원
- 비트 하나당 0 또는 1 저장
- 대규모 불리언 데이터 처리에 최적

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `SETBIT` | 특정 오프셋 비트 설정 | `SETBIT login:2024 42 1` |
| `GETBIT` | 특정 오프셋 비트 조회 | `GETBIT login:2024 42` |
| `BITCOUNT` | 1인 비트 수 카운트 | `BITCOUNT login:2024` |
| `BITOP` | 비트 연산 (AND/OR/XOR/NOT) | `BITOP AND dest k1 k2` |
| `BITPOS` | 처음 0 또는 1이 나오는 위치 | `BITPOS login:2024 1` |

### 활용 사례
- 일별 사용자 출석 체크, 기능 플래그(Feature Flag), 대규모 사용자 활성 여부 추적

---

## 7. HyperLogLog

**확률적 카디널리티(cardinality) 추정** 자료구조. 중복을 제거한 고유 요소 수를 근사적으로 계산.

### 특징
- 최대 **12KB** 메모리로 수십억 개의 고유 항목 추정 가능
- 오차율 약 **0.81%**
- 실제 데이터를 저장하지 않고 통계적 계산만 수행

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `PFADD` | 요소 추가 | `PFADD visitors "user1" "user2"` |
| `PFCOUNT` | 고유 요소 수 추정 | `PFCOUNT visitors` |
| `PFMERGE` | 여러 HyperLogLog 병합 | `PFMERGE total hll1 hll2` |

### 활용 사례
- 페이지 UV(고유 방문자 수), 검색어 고유 수, 대규모 집합의 카디널리티 추정

---

## 8. Stream (스트림)

Redis 5.0에서 추가된 **로그 형태의 시계열 데이터 구조**. Kafka와 유사한 메시지 스트리밍 지원.

### 특징
- 각 항목은 고유한 ID(`timestamp-sequence`)를 가짐
- 소비자 그룹(Consumer Group)을 통한 분산 처리 지원
- 메시지 영속성 보장 (List와 달리 소비 후 삭제되지 않음)

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `XADD` | 메시지 추가 | `XADD stream * field1 val1` |
| `XREAD` | 메시지 읽기 | `XREAD COUNT 10 STREAMS stream 0` |
| `XRANGE` | 범위로 메시지 조회 | `XRANGE stream - +` |
| `XLEN` | 스트림 길이 | `XLEN stream` |
| `XGROUP CREATE` | 소비자 그룹 생성 | `XGROUP CREATE stream grp $ MKSTREAM` |
| `XREADGROUP` | 그룹 소비자로 읽기 | `XREADGROUP GROUP grp consumer1 COUNT 1 STREAMS stream >` |
| `XACK` | 메시지 처리 완료 확인 | `XACK stream grp <id>` |
| `XTRIM` | 스트림 크기 제한 | `XTRIM stream MAXLEN 1000` |

### 활용 사례
- 이벤트 소싱, 실시간 로그 수집, IoT 데이터 처리, 마이크로서비스 간 메시지 전달

---

## 9. Geospatial (지리공간)

**위도/경도 좌표**를 저장하고 위치 기반 쿼리를 수행하는 타입. 내부적으로 Sorted Set을 활용.

### 특징
- 위도: -85.05 ~ 85.05, 경도: -180 ~ 180
- 거리 단위: `m`, `km`, `mi`, `ft`
- 정밀도 오차: 최대 약 0.6%

### 주요 명령어

| 명령어 | 설명 | 예시 |
|--------|------|------|
| `GEOADD` | 위치 추가 | `GEOADD places 127.0 37.5 "Seoul"` |
| `GEOPOS` | 좌표 조회 | `GEOPOS places "Seoul"` |
| `GEODIST` | 두 위치 간 거리 | `GEODIST places "Seoul" "Busan" km` |
| `GEORADIUS` | 반경 내 위치 검색 | `GEORADIUS places 127.0 37.5 100 km` |
| `GEOSEARCH` | 반경/박스 내 검색 (Redis 6.2+) | `GEOSEARCH places FROMMEMBER "Seoul" BYRADIUS 100 km ASC` |
| `GEOHASH` | 위치를 Geohash 문자열로 변환 | `GEOHASH places "Seoul"` |

### 활용 사례
- 주변 매장 찾기, 배달 거리 계산, 위치 기반 서비스

---

## 데이터 타입 비교 요약

| 타입 | 중복 허용 | 순서 보장 | 주요 활용 |
|------|-----------|-----------|-----------|
| String | - | - | 캐싱, 카운터, 세션 |
| List | O | O (삽입 순) | 큐, 스택, 로그 |
| Set | X | X | 태그, 관계, 집합 연산 |
| Sorted Set | X (값), O (score) | O (score 기준) | 랭킹, 우선순위 큐 |
| Hash | O | X | 객체 저장 |
| Bitmap | - | O (비트 위치) | 출석, 플래그 |
| HyperLogLog | - | - | 대규모 카디널리티 추정 |
| Stream | - | O (시간 기반) | 이벤트, 로그 스트리밍 |
| Geospatial | X | O (score 기반) | 위치 기반 검색 |

---

## TTL (Time To Live) - 만료 시간 설정

모든 데이터 타입에 공통으로 적용 가능.

```bash
EXPIRE key 60          # 60초 후 만료
PEXPIRE key 60000      # 60000밀리초 후 만료
TTL key                # 남은 TTL 조회 (초)
PTTL key               # 남은 TTL 조회 (밀리초)
PERSIST key            # TTL 제거 (영구 보관)
EXPIREAT key 1700000000  # 유닉스 타임스탬프 시점에 만료
```

---

*참고: Redis 공식 문서 — https://redis.io/docs/data-types/*
