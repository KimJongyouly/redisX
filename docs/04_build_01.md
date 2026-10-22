# Redis 빌드 시스템 분석

## 1. 빌드 시스템 전체 구조

Redis는 **GNU Make** 기반의 계층형 빌드 시스템을 사용한다. 빌드 진입점은 최상위 `Makefile`이며, 실제 컴파일 로직은 `src/Makefile`에 집중되어 있다.

```
redis/
├── Makefile                  ← 최상위 진입점 (src/, modules/ 위임)
├── src/
│   ├── Makefile              ← 핵심 빌드 로직 (컴파일·링크·설치)
│   ├── mkreleasehdr.sh       ← 릴리스 헤더 자동 생성
│   └── commands/             ← 커맨드 메타데이터 JSON (코드 생성 원본)
├── deps/
│   └── Makefile              ← 서드파티 의존성 빌드
├── modules/
│   └── Makefile              ← 선택적 모듈 빌드 (Rust 포함)
└── utils/
    └── generate-command-code.py  ← commands.c 코드 생성기
```

---

## 2. 최상위 Makefile (`redis/Makefile`)

```makefile
SUBDIRS = src
ifeq ($(BUILD_WITH_MODULES), yes)
    SUBDIRS += modules
endif

.DEFAULT:
    for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@; done
```

역할은 단순하다. 모든 Make 타깃을 `src/`(기본)와 `modules/`(옵션)로 위임한다.  
`BUILD_WITH_MODULES=yes` 환경 변수를 지정하면 모듈도 함께 빌드된다 (32비트 빌드에서는 불가).

---

## 3. 핵심 빌드 파일: `src/Makefile`

### 3-1. 빌드 결과물

| 바이너리 | 설명 |
|---|---|
| `redis-server` | Redis 서버 본체 |
| `redis-sentinel` | redis-server의 심볼릭 복사본 |
| `redis-cli` | 커맨드라인 클라이언트 |
| `redis-benchmark` | 성능 벤치마크 도구 |
| `redis-check-rdb` | redis-server의 심볼릭 복사본 |
| `redis-check-aof` | redis-server의 심볼릭 복사본 |
| `redis-tls.so` | TLS 모듈 (BUILD_TLS=module 시) |

> sentinel, check-rdb, check-aof는 실제로 `redis-server` 바이너리를 복사/심볼릭 링크하여 생성된다.  
> 실행 시 `argv[0]` 이름으로 동작 모드를 결정하는 단일 바이너리 구조다.

### 3-2. 컴파일 플래그 구성

최종 컴파일 플래그는 여러 변수가 합산되어 결정된다.

```
FINAL_CFLAGS = STD + WARN + OPT + DEBUG + CFLAGS + REDIS_CFLAGS
FINAL_LDFLAGS = LDFLAGS + OPT + REDIS_LDFLAGS + DEBUG
```

| 변수 | 기본값 | 의미 |
|---|---|---|
| `STD` | `-pedantic -DREDIS_STATIC=''` | C 표준 준수 (gnu11 또는 c99 자동 감지) |
| `WARN` | `-Wall -W -Wno-missing-field-initializers ...` | 경고 수준 |
| `OPT` | `-O3 -flto` | 최적화 (clang은 `-flto`, GCC는 `-flto=auto`) |
| `DEBUG` | `-g -ggdb` | 디버그 심볼 항상 포함 |
| `CFLAGS` | (외부 주입) | 사용자 정의 컴파일 플래그 |
| `REDIS_CFLAGS` | (외부 주입) | Redis 전용 플래그 (deps에는 전달 안 됨) |

**C 표준 자동 감지:**

```makefile
C11_ATOMIC := $(shell ... $(CC) -std=gnu11 -c foo.c ...)
ifeq ($(C11_ATOMIC),yes)
    STD += -std=gnu11      # C11 _Atomic 지원 시 → Vector Sets 활성화
else
    SKIP_VEC_SETS=yes
    STD += -std=c99        # 미지원 시 c99 폴백 → Vector Sets 비활성화
endif
```

### 3-3. 메모리 할당자 선택

```makefile
MALLOC=libc                     # 기본값
ifeq ($(uname_S),Linux)
    MALLOC=jemalloc             # Linux에서는 jemalloc이 기본
endif
```

| 옵션 | 방법 |
|---|---|
| jemalloc | `MALLOC=jemalloc` 또는 Linux 기본, `-DUSE_JEMALLOC` 정의 |
| tcmalloc | `USE_TCMALLOC=yes`, `-DUSE_TCMALLOC` 정의 |
| libc | `MALLOC=libc` 또는 `USE_JEMALLOC=no` |

jemalloc 선택 시 `deps/jemalloc/lib/libjemalloc.a`를 정적 링크한다.

### 3-4. 플랫폼별 처리

`uname -s` 결과에 따라 컴파일 플래그와 링크 라이브러리를 자동 조정한다.

| OS | 주요 처리 |
|---|---|
| Linux | `-rdynamic -ldl -pthread -lrt`, jemalloc 기본 |
| Darwin (macOS) | `-ldl`, Homebrew OpenSSL 경로 자동 감지 (arm64: `/opt/homebrew`, x86: `/usr/local`) |
| SunOS | `-m64 -D__EXTENSIONS__ -D_XPG6 -ldl -lnsl -lsocket ...` |
| AIX | `-Wl,-bexpall -ldl -pthread -lcrypt -lbsd` |
| FreeBSD/DragonFly | `-lpthread -lexecinfo` |
| OpenBSD/NetBSD | `-lpthread` (옵션: `USE_BACKTRACE=yes`로 스택 트레이스 활성화) |
| Haiku | `-DBSD_SOURCE -lbsd -lnetwork -lpthread` |
| ARM32 (`armv*`) | `-funwind-tables -latomic` (스택 트레이스 + 원자 연산) |

### 3-5. TLS 지원

```makefile
# BUILD_TLS=yes → 정적 링크
FINAL_CFLAGS += -DUSE_OPENSSL=1 $(OPENSSL_CFLAGS) -DBUILD_TLS_MODULE=0
FINAL_LIBS   += ../deps/hiredis/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)

# BUILD_TLS=module → 공유 라이브러리로 분리
# → redis-tls.so 생성, 런타임에 loadmodule로 로드
```

OpenSSL은 `pkg-config`로 자동 감지하며, 없으면 `-lssl -lcrypto`로 폴백한다.

### 3-6. 새니타이저(Sanitizer) 지원

| `SANITIZER=` | 추가 플래그 | 비고 |
|---|---|---|
| `address` | `-fsanitize=address` | jemalloc 비활성화 |
| `undefined` | `-fsanitize=undefined` | jemalloc 비활성화 |
| `thread` | `-fsanitize=thread` | — |
| `memory` | `-fsanitize=memory -fsanitize-memory-track-origins=2` | **clang 전용**, jemalloc 비활성화 |

### 3-7. 빌드 설정 캐싱 (`.make-settings`)

빌드 플래그가 바뀌면 자동으로 전체 클린 후 재빌드한다:

```makefile
-include .make-settings    # 이전 설정 로드

ifneq ($(strip $(PREV_FINAL_CFLAGS)), $(strip $(FINAL_CFLAGS)))
.make-prerequisites: persist-settings   # 변경 감지 → distclean + 재빌드
endif
```

`persist-settings` 타깃이 `.make-settings`에 현재 플래그를 기록하고 `deps` 빌드도 트리거한다.

### 3-8. 의존성 추적 (`.d` 파일)

```makefile
%.o: %.c .make-prerequisites
    $(REDIS_CC) -MMD -o $@ -c $<   # -MMD로 .d 파일 자동 생성

-include $(DEP)    # 헤더 변경도 자동 감지
```

GCC/clang의 `-MMD` 플래그로 각 `.c` 파일의 헤더 의존성을 `.d` 파일에 기록하고 자동 포함한다.

---

## 4. 의존성 빌드: `deps/Makefile`

Redis는 빌드 시 필요한 서드파티 라이브러리를 소스째 번들링한다.

### 기본 의존성 (`DEPENDENCY_TARGETS`)

| 라이브러리 | 역할 | 링크 방식 |
|---|---|---|
| **hiredis** | Redis 프로토콜 파서 / 클라이언트 | 정적 (`libhiredis.a`) |
| **linenoise** | readline 대체 (CLI 히스토리·자동완성) | 오브젝트 (`linenoise.o`) |
| **lua** | Lua 스크립팅 엔진 | 정적 (`liblua.a`) |
| **hdr_histogram** | 레이턴시 히스토그램 | 정적 (`libhdrhistogram.a`) |
| **fpconv** | 부동소수점 → 문자열 변환 | 정적 (`libfpconv.a`) |
| **fast_float** | 문자열 → 부동소수점 파싱 | 정적 (`libfast_float.a`) |
| **xxhash** | 고속 해시 함수 | 정적 (`libxxhash.a`) |

### 조건부 의존성

| 라이브러리 | 조건 |
|---|---|
| **jemalloc** | `MALLOC=jemalloc` 시 (`-DUSE_JEMALLOC`, configure → make) |
| **hiredis_ssl** | `BUILD_TLS=yes` 또는 `module` 시 |

### 의존성 변경 감지

```makefile
ifneq ($(shell cat .make-cflags || echo none), $(CFLAGS))
.make-cflags: distclean   # CFLAGS 변경 시 모든 dep 재빌드
```

CFLAGS나 LDFLAGS가 바뀌면 deps 전체를 distclean 후 재빌드한다.

---

## 5. 코드 생성 파이프라인

### 5-1. 릴리스 헤더 생성 (`mkreleasehdr.sh`)

`src/Makefile`의 첫 줄에서 실행된다:

```makefile
release_hdr := $(shell sh -c './mkreleasehdr.sh')
```

`release.h`를 생성하며 다음 정보를 포함한다:

```c
#define REDIS_GIT_SHA1   "a1b2c3d4"   // git HEAD SHA (8자리)
#define REDIS_GIT_DIRTY  "0"           // 미커밋 변경 줄 수
#define REDIS_BUILD_ID   "hostname-timestamp"
#define REDIS_BUILD_ID_RAW REDIS_VERSION REDIS_BUILD_ID REDIS_GIT_DIRTY REDIS_GIT_SHA1
```

SHA1이 바뀌면 `release.c`를 touch하여 강제 재컴파일한다.  
`SOURCE_DATE_EPOCH` 환경 변수로 재현 가능한 빌드(reproducible build)를 지원한다.

### 5-2. 커맨드 코드 생성 (`generate-command-code.py`)

Redis 커맨드 메타데이터는 `src/commands/*.json` (~26,000줄)으로 관리된다.

```makefile
$(COMMANDS_DEF_FILENAME).def: commands/*.json ../utils/generate-command-code.py
    $(PYTHON) ../utils/generate-command-code.py $(GEN_COMMANDS_FLAGS)

commands.c: $(COMMANDS_DEF_FILENAME).def
```

Python 스크립트가 JSON → `commands.def` → `commands.c`를 생성한다.  
`LOG_REQ_RES` 플래그 설정 시 응답 스키마가 포함된 `commands_with_reply_schema.def`로 변경된다.

---

## 6. 선택적 모듈: `modules/Makefile`

`BUILD_WITH_MODULES=yes`로 빌드 시 네 가지 모듈을 추가 컴파일한다:

| 모듈 | 언어 |
|---|---|
| RedisJSON | C (+ Rust) |
| RedisTimeSeries | C (+ Rust) |
| RedisBloom | C |
| RediSearch | C (+ Rust) |

### Rust 툴체인 관리

Rust 의존 모듈들을 위해 빌드 시 Rust를 자동 설치/제거할 수 있다:

```makefile
ifeq ($(INSTALL_RUST_TOOLCHAIN),yes)
    # Rust 1.94.0 다운로드 + SHA256 검증 + 설치
    # x86_64/aarch64 × gnu/musl 4가지 조합 지원
```

`DISABLE_WERRORS=yes`로 모듈 빌드 시 `-Werror` 플래그를 일괄 제거할 수 있다.

---

## 7. 주요 Make 타깃 정리

| 타깃 | 설명 |
|---|---|
| `make` / `make all` | 전체 빌드 (server, cli, benchmark, check-* 포함) |
| `make install` | `/usr/local/bin`에 설치 (PREFIX 변경 가능) |
| `make clean` | 빌드 산출물 제거 (deps 제외) |
| `make distclean` | 전체 클린 (deps 포함, `.make-*` 설정 파일 포함) |
| `make test` | 전체 테스트 실행 (`runtest`) |
| `make test-modules` | 모듈 API 테스트 |
| `make test-sentinel` | Sentinel 테스트 |
| `make test-cluster` | 클러스터 테스트 |
| `make 32bit` | 32비트 빌드 (`-m32`) |
| `make noopt` | 최적화 없이 빌드 (`-O0`) |
| `make valgrind` | valgrind용 빌드 (`-O0 + libc`) |
| `make gcov` | 코드 커버리지 빌드 |
| `make lcov` | lcov HTML 리포트 생성 |
| `make bench` | 벤치마크 실행 |
| `make dep` | `Makefile.dep` 재생성 |

---

## 8. 빌드 커스터마이징 변수 요약

```bash
# 메모리 할당자
make MALLOC=jemalloc        # 기본(Linux)
make MALLOC=libc
make USE_JEMALLOC=no

# TLS
make BUILD_TLS=yes          # 정적 링크
make BUILD_TLS=module       # 공유 라이브러리

# 새니타이저
make SANITIZER=address
make SANITIZER=undefined
make SANITIZER=thread
make CC=clang SANITIZER=memory

# 최적화
make OPTIMIZATION=-O2
make OPT="-O0 -ggdb3"       # 최적화 완전 제어

# 모듈
make BUILD_WITH_MODULES=yes

# systemd 알림
make USE_SYSTEMD=yes

# 설치 경로
make install PREFIX=/opt/redis

# 상세 출력
make V=1

# 디버그 defrag
make DEBUG_DEFRAG=force
```

---

## 9. 빌드 흐름 다이어그램

```
make
 └─ src/Makefile
     ├─ [1] mkreleasehdr.sh          → release.h 생성
     ├─ [2] .make-prerequisites      → 플래그 변경 감지
     │       └─ persist-settings
     │           └─ deps/Makefile    → 의존성 라이브러리 빌드
     │               ├─ hiredis       → libhiredis.a
     │               ├─ lua           → liblua.a
     │               ├─ jemalloc      → libjemalloc.a (Linux 기본)
     │               ├─ hdr_histogram → libhdrhistogram.a
     │               ├─ fpconv        → libfpconv.a
     │               ├─ fast_float    → libfast_float.a
     │               └─ xxhash        → libxxhash.a
     ├─ [3] generate-command-code.py → commands.def → commands.c
     ├─ [4] *.c → *.o  (-MMD 의존성 추적)
     └─ [5] 링크
             ├─ redis-server  (모든 OBJ + 정적 라이브러리)
             ├─ redis-cli     (CLI OBJ + hiredis + linenoise)
             ├─ redis-benchmark
             └─ redis-sentinel / check-rdb / check-aof (서버 복사)
```

---

## 10. 특징 및 설계 의도

1. **단일 바이너리 전략**: sentinel, check-rdb, check-aof가 모두 redis-server 바이너리를 재사용. `argv[0]`으로 모드 분기하여 유지보수 비용 절감.

2. **의존성 완전 번들링**: 외부 패키지 관리자 없이 소스 레벨 빌드. 일관된 빌드 재현성 보장.

3. **점진적 재빌드**: `.make-settings`로 플래그 변경을 감지하여 불필요한 전체 재빌드를 방지하면서도 정확성을 보장.

4. **코드 생성 기반 커맨드 관리**: JSON → Python 코드 생성으로 커맨드 메타데이터(인자 타입, 복잡도, 도움말)와 구현을 분리. 새 커맨드 추가 시 JSON만 편집하면 된다.

5. **새니타이저 통합**: ASAN/UBSAN/TSan/MSan을 Make 플래그 하나로 전환 가능. MSan은 deps도 함께 재컴파일하여 false positive 방지.
