# redisX

**README** · [Architecture](architecture/README.md) · [Scripts](scripts/README.md)

[Redis](https://redis.io)를 포크한 C 코드베이스(`redis_src`)에 SLOWLOG/AOF/Python 스크립팅 같은 커스텀 기능을
직접 patch로 얹어 쓰다가, 그 변경들을 C로 계속 유지보수하는 게 번거로워져서 시작한 프로젝트다. 같은 기능을
메모리 안전성과 유지보수성이 더 나은 형태로 가져가기 위해, 처음부터 새 클론을 만드는 것이 아니라 커맨드·
서브시스템 단위로 하나씩 Rust로 전면 재구현하기로 결정했다. 포팅 과정에서는 DataFrame 같은 데이터를 불러와
가볍게 분석하고 그 결과를 공유할 수 있는 수단으로 DataFrame 쿼리 기능도 함께 만들었다.

이 README는 "어떻게 빌드하고 실행하는가"를 다루는 실용적인 요약이고, 각 단계의 판단 근거는
[`./redis_document/25_redisX.md`](./redis_document/25_redisX.md) 로드맵 문서에 Phase별로 정리되어 있다.

## 구성

네 개 크레이트로 이뤄진 Cargo 워크스페이스다:

| 크레이트 | 역할 |
|---|---|
| `redisx-core` | 외부 의존성 없는 자료구조/유틸리티: `sds`, `crc16`/`crc64`, `intset`, `dict`, `listpack`, `kvstore`, `lzf`(압축 해제), `sha1`. |
| `redisx-net` | RESP2/RESP3 와이어 프로토콜(요청 파싱, 응답 인코딩 **및** 디코딩)과 `mio` 기반 단일 스레드 이벤트 루프. |
| `redisx-server` | 서버 본체: 커맨드 디스패치, 모든 데이터 타입, RDB/AOF 영속화, 복제, 스크립팅(Lua + 선택적 Python), 그 위에 쌓인 모든 커맨드. |
| `redisx-cli` | `redis-cli`와 같은 취지의 대화형 REPL + 단발성 커맨드 실행기. |

바이너리: `redisx-server`(서버), `redisx-cli`(클라이언트), `redisx-benchmark` (`redis-benchmark`와 같은 취지의 소형 처리량 벤치마크 도구).

## 빌드

```bash
cargo build --workspace            # 전체 빌드, Lua 스크립팅만 포함
cargo build -p redisx-server --features python   # + Python FUNCTION/FCALL 엔진
cargo build -p redisx-server --features polars   # + 네이티브 DataFrame 타입(DF.* 커맨드)
cargo build -p redisx-server --features python,polars   # + Python FUNCTION/FCALL 엔진,  네이티브 DataFrame 타입(DF.* 커맨드)
```

기본 빌드는 Python이나 `polars` 의존성이 전혀 없다 — 둘 다 옵트인이고 서로 독립적인 feature라 `--features python,polars`로 같이 켤 수도 있다.
`--features python`은 시스템에 이미 설치된 Python 3 인터프리터에 (`pyo3`의 `auto-initialize`를 통해) 바로 링크하므로 별도 설정이 필요 없고, 빌드 시점에 올바른 `rpath`를 바이너리에 심어 넣어(자세한 내용은 `redisx-server/build.rs` 참고) 런타임에 `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`를 수동으로 export하지 않아도 해당 인터프리터의 공유 라이브러리를 찾는다. `--features polars`는 순수 Rust라 설치할 시스템 의존성은 없고, 컴파일만 더 오래 걸린다. polars에 대한 처리를 위해 Rust로 포팅을 선택한 부분도 있다. 

## 독립 실행 파일 설치하기 (실행에 `cargo`가 필요 없음)

```bash
./install.sh                                  # 릴리스 바이너리를 빌드해 /usr/local/bin/redisX에 설치
INSTALL_DIR="$HOME/.local/redisX" ./install.sh # 다른 위치에 설치 (sudo 불필요)
./install.sh --force                          # $INSTALL_DIR를 지우고 깨끗이 재설치 (복구용, 아래 참고)
./install.sh --clean                          # --force에 더해 먼저 `cargo clean` 실행 (완전 처음부터 재빌드)
```

항상 `--features redisx-server/python,redisx-server/polars`로 빌드하므로,
설치된 `redisx-server`는 별도 설정 없이도 `FUNCTION LOAD`/`FCALL` Python 라이브러리와 
네이티브 `DF.*` DataFrame 커맨드를 바로 쓸 수 있다(`python`은 빌드 머신에 Python 3 개발 환경이 필요하다 
— `cargo test --features python`과 동일한 요구사항이고, `polars`는 추가 시스템 요구사항이 없다).

이 스크립트는 빌드와 파일 복사만 한다 — `/usr/local/bin`은 보통(최초 디렉터리 생성 시) 쓰기에 `sudo`가 필요하므로, `$INSTALL_DIR`가 아직 쓰기
가능한 상태가 아닐 때만 `sudo`를 요청한다. 디렉터리가 만들어지면 그 소유권을 다시 사용자에게 넘겨서, 이후 `redisx-server`가 매 실행마다 `sudo` 없이도
자신의 `redisx.conf`를 그 자리에 쓸 수 있게 한다. 기본값이 `/usr/bin`이 아니라 `/usr/local/bin`인 점에 주의 
— 최신 macOS에서는 `/usr/bin`이 SIP로 보호된 시스템 볼륨에 있어서, root로도 그곳에 쓰면 "Operation not permitted" 에러가 난다. 
`--force`는 이 스크립트의 이전 실행이 남겨둔 root 소유 디렉터리를 복구하기 위한 옵션으로(먼저 `sudo` 접근을 확인한 뒤 
`$INSTALL_DIR`를 `rm -rf`하고 재설치), 그냥 다시 실행하면 세 바이너리는 이미
제자리에서 덮어써지므로, `redisx-server`가 시작할 때 권한 에러로 실패할 때만 `--force`가 필요하다. `--clean`은 재빌드 전에 추가로 `cargo clean`을
실행한다 — `polars`/`pyo3`/`mlua`를 포함한 모든 의존성을 처음부터 다시 컴파일하므로 훨씬 느리니, (예: feature 플래그를 바꿨는데 반영 안 되는 것
같은) 빌드 캐시가 오래됐다고 의심될 때만 쓰고 일상적인 재설치용으로는 쓰지 말 것. 서버를 직접 시작하거나 셸 시작 파일을 건드리지는 않는다 — 마지막에
생성된 설정 파일이 어디 있는지, `redisx-server`/`redisx-cli`를 어디서든 실행하고 싶다면 
`~/.zshrc`/`~/.shrc`에 추가할 `export PATH=...` 줄은 무엇인지, 두 가지 후속 조치를 직접 하도록 안내만 출력한다.

## 실행

```bash
cargo run -p redisx-server --bin redisx-server               # 바이너리 옆 설정 파일, 처음 실행 시 기본값으로 생성됨
cargo run -p redisx-server --bin redisx-server -- /path/to/redisx.conf  # 대신 명시적인 설정 파일 지정
cargo run -p redisx-server --bin redisx-server -- 2417 16    # 레거시 방식: 포트, DB 개수, 설정 파일 없이
cargo run -p redisx-cli --bin redisx-cli -- -p 2417
```

인자 없이 실행하거나 (아직 존재하지 않는) 설정 파일 경로를 주면, `redisx-server`는 `port 2417`, 
`databases 16`을 비롯해 `CONFIG GET`으로 보이는 모든 기본값을 담은 초기 설정을 실행 중인 바이너리 바로 옆의
`redisx.conf`에 써넣고(그래서 `/usr/local/bin/redisX/redisx-server`로
설치된 사본은 어느 디렉터리에서 실행하든 기본적으로 `/usr/local/bin/redisX/redisx.conf`를 쓴다  
시작할 때 그 위치를 알려준다. 설정을 바꾸려면 그 파일을 수정하고 서버를 재시작해야 한다 — 실행 중에 적용되는 `CONFIG REWRITE`/리로드는 없다.

`redisx-cli`는 실제 `redis-cli`처럼 동작한다 — 터미널에서 실행하면 줄  편집과 영구 히스토리(`~/.redisx-cli_history`)를 
지원하는 대화형 REPL이 되고, 그렇지 않으면 단발성 커맨드 실행기(`redisx-cli -p 2417 GET foo`)가 된다. 
실제 `redis-cli`와 똑같은 방식으로 raw/포맷 출력을 자동 판별한다 —  stdout이 터미널이면 포맷, 파이프로 연결되면 raw — `--raw`/`--no-raw`로
강제할 수도 있다.

`SLOWLOG`는 기본적으로 꺼져 있다 — redisX가 포팅한 원본 C 포크의 설계를 그대로 이어받아(저장소 루트 `CHANGE.md` 1절 참고) 메모리가 아니라
**파일** 기반으로 동작하고, `slowlog-file-path`가 빈 문자열인 기본값 상태에서는 `SLOWLOG GET`/`LEN`이 항상 빈 결과만 낸다. 실제로 느린
커맨드를 기록하게 하려면 `redisx.conf`에 다음을 설정하고 재시작하거나 (또는 런타임에 `CONFIG SET`으로 즉시 적용):

```
slowlog-file-path /var/log/redisx-slow.log   # 기본값: "" (비활성)
slowlog-log-slower-than 10000                 # 기본값: 10000(마이크로초, 10ms) — 0으로 두면 사실상 모든 커맨드가 기록됨
```

```bash
redisx-cli CONFIG SET slowlog-file-path /var/log/redisx-slow.log
redisx-cli CONFIG SET slowlog-log-slower-than 0   # 데모/디버깅용: 전부 기록
redisx-cli SLOWLOG GET
redisx-cli SLOWLOG LEN
redisx-cli SLOWLOG RESET   # 파일을 0바이트로 비움
```

`SLOWLOG GET`/`LEN`/`RESET`은 전부 그 파일을 직접 읽고 쓰는 방식으로 동작한다 — 메모리에는 아무것도 보관하지 않으므로 `slowlog-max-len`은
더 이상 아무 효과가 없다. 실제로 켜고 읽어보는 예제는 `example/python/slowlog.py`를 비롯해 `example/<언어>/`마다 있다.
평문 리스너와 함께 암호화된 리스너도 열려면, `redisx.conf`에 다음 세 키를 설정하고 재시작하면 된다:

```
tls-port 6380
tls-cert-file /path/to/cert.pem
tls-key-file /path/to/key.pem
```

두 포트 모두 같은 데이터셋을 서빙한다. 실제 `redis-cli --tls`는 평소처럼  `tls-port`에 접속하면 된다(자체 서명 인증서라면 `--insecure` 추가). 
이 인스턴스 자신이 내보내는 `REPLICAOF` 연결도 마스터에 TLS로 붙게 하려면 `tls-replication yes`를 추가하면 된다
(`tls-ca-cert-file`은 없음 — 이쪽은 마스터의 인증서 체인을 전혀 검증하지 않는다).

Redis Cluster를 구성하려면 각 노드 설정에서 `cluster-enabled yes`를  설정하고
(`cluster-port`를 `0`으로 두면 기본값은 `port + 10000`) 노드들을 띄운 뒤, 
아무 노드에서나 나머지 노드들에 `CLUSTER MEET <ip> <port>`를 실행하면 된다
(가십이 거기서부터 전이적으로 퍼지므로 모든 쌍끼리 `MEET`할 필요는 없다). 
그리고 `CLUSTER ADDSLOTS`/`ADDSLOTSRANGE`로 16384개 슬롯 전체를 나눈다. 
그러면 클러스터 인식 클라이언트(`redis-cli -c`, `redis-py`의 `RedisCluster` 등)가 
실제 `MOVED`/`CROSSSLOT`/`CLUSTERDOWN` 리다이렉트를 받게 된다. 

단일/복제/클러스터 세 배치 토폴로지를 실제로 띄우고 검증하는 설정 파일·
스크립트·테스트는 [`./architecture/`](architecture/README.md)에
따로 정리돼 있다 — 복제(2대/3대 구성)와 클러스터(3대) 각각의 `*.conf`,
`setup_cluster.sh` 같은 기동 스크립트, `redis-py`로 동작을 검증하는
`test_*.py`, 그리고 직접 띄워서 확인한 redisX 고유의 동작 차이(`REPLICAOF
NO ONE`이 마스터 쪽 연결을 끊지 않는 것 등)까지 문서화돼 있다.


```bash
$ cargo run -p redisx-cli --bin redisx-benchmark -- -p 2417 -c 50 -n 100000        # 기본 PING/SET/GET/INCR 세트
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.05s
     Running `target/debug/redisx-benchmark -p 2417 -c 50 -n 100000`
====== PING ======
  100000 requests completed in 0.460 seconds
  50 parallel clients
  217287.50 requests per second

====== SET ======
  100000 requests completed in 2.415 seconds
  50 parallel clients
  41409.46 requests per second

====== GET ======
  100000 requests completed in 0.454 seconds
  50 parallel clients
  220392.11 requests per second

====== INCR ======
  100000 requests completed in 2.652 seconds
  50 parallel clients
  37703.91 requests per second

$ cargo run -p redisx-cli --bin redisx-benchmark -- -p 2417 -c 10 -n 5000 LPUSH mylist val   # 특정 커맨드 하나만
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.17s
     Running `target/debug/redisx-benchmark -p 2417 -c 10 -n 5000 LPUSH mylist val`
====== LPUSH mylist val ======
  5000 requests completed in 0.161 seconds
  10 parallel clients
  31018.48 requests per second
```


`--features polars`로 빌드한 서버에서는 redisX에 네이티브 DataFrame 값 타입이 생긴다 
— 데이터 소스에서 불러오고, SQL을 돌리고, 다시 데이터 소스로 내보낼 수 있다:

| | CSV | Parquet |
|---|---|---|
| **불러오기** | `DF.FROMCSV key <csv-text>` | `DF.FROMPARQUET key <parquet-bytes>` |
| **내보내기** | `DF.TOCSV key` | `DF.TOPARQUET key` |

```bash
redisx-cli DF.FROMCSV sales $'region,amount\nEU,100\nUS,250\nEU,80\n'
redisx-cli DF.SHAPE sales      # [3, 2]
redisx-cli DF.COLUMNS sales    # region, amount
redisx-cli DF.TOCSV sales      # 다시 CSV 텍스트로

# Parquet은 바이너리라 인라인으로 붙여넣을 수 없다 — 파일에서 불러온다:
redisx-cli -x DF.FROMPARQUET sales < sales.parquet
```

`DF.GET`/`DF.TOPARQUET`의 바이너리 응답은 그냥 `redisx-cli ... > file`처럼
셸 리다이렉트하면 온전히 보존되지 않는다: `--raw` 모드(stdout이 터미널이 아니면 기본값, 즉 리다이렉트하는 순간 항상 적용됨)는 bulk 응답 뒤에
줄바꿈을 하나 덧붙이는데, 텍스트(`DF.TOCSV`의 CSV 출력, `GET` 등)에는 무해하지만 Arrow IPC나 Parquet처럼 정확한 끝 바이트에 민감한 바이너리
포맷은 깨뜨린다(실제 `redis-cli`도 같은 함정이 있다). 이 둘은 셸 리다이렉트 대신 프로그램으로 직접 뽑아내야 한다 — 이 프로젝트의 테스트 스위트 자체가
그렇듯, raw 응답 바이트를 직접 읽는 RESP 클라이언트라면(셸 리다이렉트 경로를 거치지 않으므로) 정확히 왕복한다.

불러온 DataFrame을 쿼리하는 방법은 두 가지이고, 둘 다 완전히 같은 SQL을 받는다 — 차이는 응답 형태뿐이다:

- **`DF.QUERY key "<sql>"`** 
다른 커맨드 응답과 같은 형태의 평범한 RESP
행 배열(각 행은 셀의 배열)이다. 결과를 프로그램으로 파싱하는 클라이언트/스크립트용.

- **`DF.PRETTY key ["<sql>"]`** 
- 사람이 터미널에서 보기 좋은 박스 드로잉
테이블(컬럼명, dtype, `shape: (rows, cols)` 줄 — polars 자체의 `Display`출력)이다. 
SQL 인자는 선택사항이며, 생략하면 프레임 전체를 보기 좋게출력한다.

```bash
redisx-cli DF.QUERY sales "SELECT region, SUM(amount) AS total FROM self GROUP BY region ORDER BY region"
redisx-cli DF.PRETTY sales "SELECT region, SUM(amount) AS total FROM self GROUP BY region ORDER BY region"
redisx-cli DF.PRETTY sales   # SQL 없이: 프레임 전체
```

이 SQL은 진짜 SQL이고 `polars::sql::SQLContext`를 통해 프로세스 내부에서 실행된다 
— 필터, 집계, `GROUP BY`, `ORDER BY`, 윈도우 함수 등 그 엔진이 지원하는 건 뭐든 가능하지만, 고정된 규칙이 하나 있다: 
DataFrame은 항상 테이블 **`self`**(키의 이름이 아니라)로 등록되고, 키 간 `JOIN`은 없다(여기서 `SQLContext`는 항상 그 테이블 하나만 등록되어 있다). 
`DF.PRETTY`의 출력은 `redisx-cli`에서 특별 취급된다 — 이미 포맷된 테이블을 또 "포맷"할 방법이 없으므로,  
`--raw`/`--no-raw`와 무관하게 항상 따옴표/이스케이프 없이 그대로 출력된다.

`DF.GET`/`DF.SET`은 값을 raw Arrow IPC 바이트로 주고받는다(바이너리 안전, `SAVE`/재시작 시 영속화하는 것과 동일한 포맷) 
전체 스코프(SQL 전용 쿼리 표면, 제자리 행 변경 없음, 키 간 조인 없음)는 `dataframe.rs`의 모듈 문서 참고.

### Python 클라이언트에서 쓰기 (예: `redis-py`)

redisX 전용 Python SDK는 없다 — `DF.*`도 그냥 RESP 커맨드일 뿐이라 평범한 Redis 클라이언트면 다 된다. 
두 경로는 무엇을 돌려받고 거기서 뭘 더 해야 하는지가 다르다:

**`DF.GET`으로 프레임 전체 받기** — 응답이 raw Arrow IPC 바이트이므로 그대로 `polars`/`pyarrow`에 넘기면 된다:

```python
import redis, io
import polars as pl

r = redis.Redis(port=2417)
raw = r.execute_command('DF.GET', 'sales')   # bytes
df = pl.read_ipc(io.BytesIO(raw))            # polars DataFrame
print(df)
```

`raw`가 `None`으로 오면 키가 없는 것이다(`DF.GET`은 키가 없을 때 `GET`과
마찬가지로 RESP nil을 응답한다). 커맨드가 `unknown command`로 에러나면
서버가 `--features polars` 없이 빌드된 것이다.

**`DF.QUERY`로 일부만 받기**(예: `LIMIT 5`) — 응답이 이미 평범한 RESP 행
배열이라 Arrow IPC 디코딩이 전혀 필요 없다. DataFrame은
`pl.DataFrame(rows, schema=cols, orient='row')`을 그냥 한 줄로 호출하지
말고 아래 `rows_to_dataframe` 헬퍼로 만들 것 — 그 한 줄짜리 호출은
`DF.QUERY`가 많은 행에 대해 `SELECT *`를 할 때 흔히 돌려주는 바로 그런
데이터에서 깨진다:

```python
def rows_to_dataframe(rows, cols):
    """DF.QUERY의 행-배열 응답으로부터 polars DataFrame을 만든다. 한 컬럼의
    값들이 타입을 공유하지 않을 때(DF.QUERY는 컬럼별로 고정된 dtype이 없다 —
    `dataframe.rs`의 any_value_to_reply는 Null/bool/int/float/string만
    특별 취급하고, 그 외(날짜/범주형/중첩 컬럼 등)는 polars의 Display
    포맷팅으로 평범한 문자열로 폴백한다) polars 자체 에러 대신 읽기 쉬운
    에러를 낸다."""
    try:
        # orient='row'는 각 컬럼의 dtype을 처음 `infer_schema_length`개
        # 행(기본값 100)만 보고 추론한다 — 만약 그 구간이 전부 null이고
        # 실제 값은 그 뒤에야 나온다면(처음엔 `NULL`인 날짜 컬럼이 나중에
        # 실제 타임스탬프 행을 만나는 경우, 바로 이게 원래 이 문제를
        # 일으킨 상황이다) polars가 미리 잘못된 dtype을 확정해버리고
        # 실제 값에서 깨진다. `None`은 모든 행을 먼저 스캔해서 이 문제를
        # 해결하는 동시에, 진짜로 타입이 섞인 컬럼(한 컬럼에 int와 str이
        # 섞인 경우)도 에러 없이 조용히 문자열로 넓혀버린다.
        return pl.DataFrame(rows, schema=cols, orient='row', infer_schema_length=None)
    except pl.exceptions.PolarsError as e:
        # `infer_schema_length=None`으로도 못 덮는 경우가 여전히 있을 수
        # 있다(예: GROUP BY의 중첩 리스트 컬럼이 다른 행의 스칼라 값과
        # 섞이는 경우 — 이건 `ComputeError`가 아니라 "공통 상위 타입 없음"
        # 이라는 `SchemaError`라서, 둘의 공통 베이스 타입을 잡는다) —
        # polars 자체의 불친절한 메시지 하나 대신, 정확히 어느 컬럼의
        # 몇 번째 행인지 알려준다.
        conflicts = {}
        for col_idx, col_name in enumerate(cols):
            seen = {}
            for row_idx, row in enumerate(rows):
                value = row[col_idx]
                if value is not None:
                    seen.setdefault(type(value).__name__, (row_idx, value))
            if len(seen) > 1:
                conflicts[col_name] = seen
        if not conflicts:
            raise
        detail = "\n".join(
            f"  '{col}': " + ", ".join(f"{t} (e.g. row {i}: {v!r})" for t, (i, v) in types.items())
            for col, types in conflicts.items()
        )
        raise ValueError(f"DF.QUERY result has columns with inconsistent per-row types:\n{detail}") from e
```

```python
import redis
import polars as pl

r = redis.Redis(port=2417)

rows = r.execute_command('DF.QUERY', 'sales', 'SELECT * FROM self LIMIT 5')
cols = [c.decode() for c in r.execute_command('DF.COLUMNS', 'sales')]

# redis-py는 RESP bulk string을 `bytes`로 돌려준다(기본값
# decode_responses=False) — polars에 넘기기 전에 텍스트 셀을 디코딩하지
# 않으면 `str`이 아니라 `binary` dtype으로 들어간다(int/null은 변환이
# 필요 없다 — RESP가 이미 Integer/nil로 타입을 매기기 때문):
def decode_cell(v):
    return v.decode('utf-8') if isinstance(v, bytes) else v
rows = [[decode_cell(v) for v in row] for row in rows]

df = rows_to_dataframe(rows, cols)
print(df)
```

`DF.QUERY`의 행에는 컬럼명이 들어있지 않고(`DF.COLUMNS`는 별도 호출이다),
SQL의 테이블명은 항상 `self`이지 키의 이름이 아니다. 같은 커넥션에서
`DF.GET`/`DF.TOPARQUET`도 호출한다면 그 커넥션을 `decode_responses=True`로
바꾸지 말 것 — 그 설정은 모든 bulk 응답을 UTF-8로 디코딩해버려서 Arrow
IPC나 Parquet 같은 진짜 바이너리 데이터를 깨뜨린다. 스크립트에 둘 다
필요하면 커넥션을 하나 더 쓰거나(위처럼) 수동으로 디코딩할 것.

**더 다양한 예제** — 타입별로 실제로 돌려볼 수 있는 실행 가능한 스크립트가
`example/python/`에 있다(순수 클라이언트 측 Python이고 redisX 전용 SDK는
아니다 — `pip install redis` 외엔 필요 없고, DataFrame 예제만 추가로
`polars`가 필요하다). 전부 자기 완결적이다 — 자신이 쓸 데모 키를 먼저
지우고 시작해서 실행 전 상태를 신경 쓸 필요가 없고, 끝에서 그 키들을
다시 지운다. `redisx-server`가 기본 포트(`2417`)로 떠 있기만 하면 순서
상관없이 하나씩 실행해볼 수 있다:

```bash
cd example/python
python redis_string.py        # SET/GET/APPEND/GETRANGE/SETRANGE/MSET/INCR/EXPIRE/TTL
python list.py          # LPUSH/RPUSH/LINSERT/LREM/LTRIM/LPOS/LMOVE — 작업 큐 예제
python hash.py          # HSET/HGETALL/HINCRBY/HRANDFIELD — 사용자 프로필 예제
python set.py           # SADD/SINTER/SUNION/SDIFF/SMOVE/SPOP — 태그 집합 연산 예제
python zset.py          # ZADD/ZRANGEBYSCORE/ZINCRBY/ZUNIONSTORE — 리더보드 예제
python hyperloglog.py   # PFADD/PFCOUNT/PFMERGE — 근사 카디널리티를 실제 Set과 비교
python slowlog.py       # redisX의 파일 기반 SLOWLOG를 켜고 직접 읽어보기
```

`hyperloglog.py`는 `HyperLogLog`가 진짜 "근사치"라는 걸 느낄 수
있게, 같은 데이터를 `Set`에도 똑같이 넣어서 정확한 개수와 나란히 비교한다.
`slowlog.py`는 redisX의 `SLOWLOG`가 (원본 C 포크부터 그런)
파일 기반이라 기본값으로는 꺼져 있다는 점(`slowlog-file-path`가 빈
문자열)을 `CONFIG SET`으로 직접 켜보고, 로그 파일 내용과 `SLOWLOG GET`
응답을 나란히 확인한다 — 자세한 설계는 루트의 `CHANGE.md` 1절 참고.

DataFrame(`--features polars`) 쪽은 좀 더 작은 클라이언트 클래스 하나로
감싸놓았다:

- `redis_df.py`는 `redis_df(key)` / `my_df.query(sql)` 뒤에 DataFrame 키
  하나를 감싼다 — 위의 세 단계(bulk-string 셀 디코딩, `DF.QUERY` 실행,
  `polars.DataFrame` 생성)를 처리하고 한 가지가 더 있다: `DF.QUERY`의
  응답에는 컬럼명 메타데이터가 전혀 없으므로, `query()`가 SQL 자체의
  `SELECT` 목록을 파싱해서 출력 컬럼명을 복원한다 — `*`는 `DF.COLUMNS`로
  펼치고, `expr AS alias`는 그 별칭을 쓰고, 단순 컬럼 참조는 그 이름 자체를
  쓴다. 별칭이 **없는** 집계/표현식(`SELECT COUNT(*) FROM self`)은 이
  방식으로 이름을 알 수 없다(그 이름은 polars의 SQL 엔진이 내부적으로
  정한다 — 예를 들어 별칭 없는 `COUNT(*)`는 `len`으로, 별칭 없는
  `SUM(amount)`는 `COUNT(*)`/`SUM(amount)`가 아니라 `amount`로 돌아오는데,
  이 매핑은 `DF.QUERY` 응답에 드러나지 않는다), 그래서 틀리게 추측하는
  대신 일반적인 `col_0`, `col_1`, ... 이름으로 폴백한다 — 실제 이름이
  필요하면 명시적으로 별칭을 달 것. `query(sql, profile=True)`는 단계별
  소요 시간(왕복 / 디코딩 / `DF.COLUMNS` / 컬럼 추론 / `DataFrame` 생성)도
  출력한다.
- `dataframe.py`는 이미 `DF.FROMPARQUET`/`DF.FROMCSV`로 불러온
  키를 대상으로 하는 실행 가능한 예제다(다른 예제들과 달리 미리 데이터를
  불러와야 한다) — `COUNT(*)`, 컬럼 일부, 평범한 `SELECT * ... LIMIT`을
  각각 `profile=True`로 실행한다:
  ```bash
  cd example/python
  python dataframe.py   # DataFrame 키가 이미 로드되어 있어야 함, 파일 자체 주석 참고
  ```
- `dataframe_csv.py`는 반대로 **자기 완결적인** 버전이다 — 작은
  재고 CSV를 코드 안에 직접 담아 그 자리에서 `DF.FROMCSV`로 적재하므로
  사전 준비 없이 바로 돌려볼 수 있다. 이 시나리오(5행짜리 재고 CSV, 카테고리별
  `SUM(price * qty)` 집계, `DF.PRETTY`, `DF.TOCSV` 왕복 확인)가 아래 "다른
  언어 예제"의 `dataframe` 예제 전부가 공유하는 기준판이다.

## DataGrip 등 SQL GUI 클라이언트에서 DataFrame을 보는 것에 대해

`DF.QUERY`의 RESP 응답을 아무리 다르게 포장해도(`r<행>c<열>`로 주소를 매긴
RESP 맵 등 여러 모양을 실제로 만들어 시도해봤다), DataGrip 같은 범용 Redis GUI
클라이언트는 자기가 미리 아는 몇 가지 커맨드(`HGETALL` 등) 말고는 어떤 응답도
그냥 평평한 한 컬럼짜리 목록으로만 보여준다 — **그 클라이언트의 Redis
커넥션으로는 어떤 RESP 모양을 보내도 진짜 여러 컬럼 그리드가 될 수 없다.**
한때 이를 우회하기 위해 최소한의 PostgreSQL 와이어 프로토콜 리스너를
`--features postgres`로 추가했었지만(DataGrip이 Postgres 커넥션으로는 실제
그리드를 그려준다는 점을 이용), 이후 제거됐다 — 지금은 알려진, 받아들인 한계로
남겨둔다: `DF.QUERY` 결과를 진짜 그리드로 보려면 RESP 테이블 모양을 이해하는
클라이언트(`redisx-cli`의 `DF.PRETTY`)를 쓸 것.

## 다른 언어 예제

같은 9개 시나리오(String/List/Hash/Set/ZSet/HyperLogLog/SLOWLOG/DataFrame/Pub-Sub)를
Python 말고 Go/Rust/C/C++/Lua/Dart/PHP/Java/Kotlin/Scala로도 옮겨뒀다 —
전부 그 언어에서 가장 널리 쓰이는 평범한 Redis 클라이언트를 쓰고(redisX
전용 SDK는 없다), 같은 데모 키·흐름·주석을 쓰므로 언어 사이를 비교해 보기도
좋다. 각 언어의 설치 방법·실행 명령·소스 구조, 그리고 각 언어가 실제로
부딪힌 redisX 고유 동작(`SLOWLOG`의 파일 기반 설계, `DF.QUERY`의 RESP2
숫자 응답 등)까지 정리한 전체 가이드는 [`example/README.md`](example/README.md)
참고.

## 백그라운드 서비스로 실행하기

`redisx-server`는 스스로 데몬화하지 않는다(`--daemonize` 플래그도, PID
파일도 없음) — `redis-server --daemonize no`와 마찬가지로 그냥 포그라운드에서
실행되며 stderr에 로그를 남긴다. 운영 방식에 맞는 걸 고르면 된다:

**간단하게(아무 유닉스에서나), 로그아웃은 버티지만 재부팅은 못 버팀:**

```bash
nohup ./target/release/redisx-server 2417 16 > /var/log/redisx-server.log 2>&1 &
disown
```

**systemd(Linux), 재부팅에도 살아남음:**

```ini
# /etc/systemd/system/redisx-server.service
[Unit]
Description=redisX server
After=network.target

[Service]
ExecStart=/path/to/redisX/target/release/redisx-server 2417 16
Restart=on-failure
# redisx-server는 `dir`(CONFIG SET dir, 기본값은 ".") 기준 상대 경로로
# RDB/AOF 파일을 읽고 쓰므로, 쓰기 권한이 있는 작업 디렉터리를 지정할 것:
WorkingDirectory=/var/lib/redisx

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now redisx-server
journalctl -u redisx-server -f   # 로그 따라가며 보기
```

**launchd(macOS), 재부팅/로그아웃에도 살아남음:**

```xml
<!-- ~/Library/LaunchAgents/com.redisx.server.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.redisx.server</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/redisX/target/release/redisx-server</string>
    <string>2417</string>
    <string>16</string>
  </array>
  <key>WorkingDirectory</key><string>/path/to/a/writable/dir</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/tmp/redisx-server.log</string>
  <key>StandardErrorPath</key><string>/tmp/redisx-server.log</string>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.redisx.server.plist
```

위 둘 중 하나를 연결하기 전에 먼저 릴리스 바이너리를 빌드할 것
(`cargo build --release --workspace`) — 위 경로들은 `cargo run`이 만드는,
훨씬 느린 최적화 안 된 `target/debug/` 빌드가 아니라 `target/release/`를
가정한다.

## 테스트

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
```

이 포팅의 모든 단계는 두 가지 방식으로 검증됐다 — 각 서브시스템에 대한
유닛 테스트, 그리고 실제 `redis-server`/`redis-cli`와의 수동 교차검증
(바이너리 RDB 왕복, 실제 Redis와 redisX 사이의 양방향 실시간 복제, 바이트
단위로 확인한 RESP3 push 프레임 등등). 유닛 테스트 스위트만으로는 전체
이야기가 안 된다 — 각 Phase의 실제 바이너리 검증이 무엇을 다뤘는지는
`redis_document/25_redisX.md`를 참고.

### 장기(long-term) 안정성 테스트

`example/python/`의 string/hash/list/set/zset/hyperloglog/pub-sub 예제가 쓰는
커맨드들을 섞어 초당 10~20개 요청을 무작위로 계속 보내는 부하 스크립트로
원격 인스턴스를 약 6시간 연속으로 돌려봤다. 클라이언트가 장시간 유휴
상태였다가 재연결하는 구간의 비정상 샘플은 제외한, 정상 구간 기준
커맨드별 레이턴시:

| 커맨드 | 샘플 수 | 평균 | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| SET/GET | 40,117 | 22.9ms | 17.9ms | 34.1ms | 105.2ms | 399.6ms |
| RPUSH | 27,249 | 34.1ms | 27.4ms | 51.3ms | 129.4ms | 1174.5ms |
| ZINCRBY | 25,634 | 12.4ms | 8.6ms | 18.7ms | 84.3ms | 382.4ms |
| SADD | 24,369 | 33.6ms | 27.4ms | 49.9ms | 126.3ms | 438.0ms |
| HINCRBY/HGETALL | 18,848 | 22.9ms | 17.9ms | 34.2ms | 104.4ms | 441.3ms |
| PFADD | 13,731 | 12.7ms | 8.7ms | 19.9ms | 85.1ms | 380.6ms |
| INCRBY | 13,557 | 12.1ms | 8.6ms | 18.4ms | 81.5ms | 364.1ms |
| PUBLISH | 13,422 | 12.4ms | 8.6ms | 18.6ms | 87.2ms | 328.2ms |
| HSET | 7,990 | 12.4ms | 8.5ms | 18.3ms | 84.7ms | 557.3ms |
| SADD/SINTER | 2,632 | 44.0ms | 36.9ms | 64.7ms | 143.1ms | 890.5ms |
| **전체** | **187,549** | **22.1ms** | **17.5ms** | **38.1ms** | **108.8ms** | 1174.5ms |

`SADD/SINTER`(두 Set 간 교집합)와 `RPUSH`/`SADD`가 가장 느린 축인데,
각각 O(n) 비교 비용과 리스트/셋 트리밍 비용이 반영된 것으로 전체 구간
동안 특별한 성능 열화 없이 일관됐다.

## 구현된 기능

대략 만들어진 순서대로 나열하면(Phase별 전체 서술과 모든 스코프 결정은
로드맵 문서 참고):

- 커맨드 디스패치, `CONFIG`, `MULTI`/`EXEC`, `RESET`, RESP2/RESP3(`HELLO`).
- 범용 키 커맨드: `KEYS`/`SCAN`(`redisx-core`의 `KvStore`와 같은 역방향
  이진 커서를 재사용하는 진짜 증분 스캔)/`RANDOMKEY`/`TOUCH`/`UNLINK`/
  `RENAME`/`RENAMENX`/`COPY`/`MOVE`/`OBJECT ENCODING`.
- 데이터 타입: `String`/`List`/`Hash`/`Set`/`ZSet`/`Stream`에 더해 그 위에
  얹힌 비트 연산, `SORT`, `HyperLogLog`(`PF*`), `Geo` 커맨드 — 각 타입이
  처음 시작했던 최소 집합이 아니라 표준 CRUD 표면 전체를 커버한다(예:
  `String`의 `APPEND`/`GETRANGE`/`SETRANGE`/`MSET`/`GETEX`, `List`의
  `LINSERT`/`LREM`/`LTRIM`/`LPOS`/`LMOVE`, `Hash`의 `HINCRBY`/`HRANDFIELD`,
  `Set`의 `SPOP`/`SMOVE`/`SINTER`/`SUNION`/`SDIFF`(+ `STORE` 변형), `ZSet`의
  `ZRANGEBYSCORE`/`ZINCRBY`/`ZPOPMIN`/`ZPOPMAX`/`ZUNIONSTORE`/
  `ZINTERSTORE`/`ZDIFFSTORE`). `HSCAN`/`SSCAN`/`ZSCAN`은 의도적인
  단순화다 — 한 번의 호출로 매칭되는 전부를 돌려주고 커서는 항상 `0`
  (프로토콜상 유효하지만 증분은 아님 — 로드맵 문서 Phase 10 섹션 참고).
  `ZRANGEBYLEX`/`ZREVRANGEBYLEX`/`ZLEXCOUNT`가 `ZSet`의 범위 쿼리 표면을
  마저 채운다.
- 블로킹 커맨드: `BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BZPOPMIN`/
  `BZPOPMAX`에 더해 같은 인프라 위의 `XREAD BLOCK`/`XREADGROUP ... BLOCK`
  — 여러 대기자 사이의 진짜 FIFO 공정성과 파이프라이닝 안에서의 올바른
  동작까지 갖췄다. 실제 Redis의 규칙과 동일하게 `MULTI`/`EXEC`나 스크립트
  안에서는 절대 실제로 블로킹하지 않는다.
- 영속화: RDB 저장/로드(`Stream`을 제외한 위의 모든 타입에 대해 실제
  Redis와 바이트 호환 — `Stream`은 이 포크 자체의 더 단순한 포맷 사용)와
  AOF 스타일의 쓰기 전용 커맨드 로그. RDB 로딩은 7.0 이전 real Redis가
  디스크에 남겼을 법한 레거시 ziplist/zipmap 인코딩 list/hash/zset
  타입도 이해하고, 그보다 더 오래된 ziplist 이전의 plain list/zset
  포맷까지도 이해한다(디코드 전용 — 이제 아무도 그걸 만들어내지 않으므로
  이 포크도 쓰지는 않는다). `FUNCTION`으로 불러온 라이브러리는
  `SAVE`/재시작을 거쳐도 왕복하며, `SLOT_INFO`/`MODULE_AUX`/`KEY_META`
  RDB 옵코드(클러스터 슬롯 정보, 모듈 데이터, 모듈 키 메타데이터)는 이
  포크에 적용할 클러스터나 모듈 시스템이 없으므로 전체 로드를 중단하는
  대신 인식만 하고 건너뛴다.
- 복제: `REPLICAOF` 마스터/레플리카 동기화 — 레플리카가 아직 유효한
  오프셋으로 재접속할 때는(용량 제한된 복제 백로그가 뒷받침하는 `PSYNC`의
  `+CONTINUE` 경로를 통한) 부분 재동기화를 하고, 그렇지 않으면 전체
  재동기화로 폴백한다. `WAIT`와 `FAILOVER`는 존재하지만 의도적으로
  최소한만 구현했다 — 정확히 무엇을 보장하고 무엇을 보장하지 않는지는
  로드맵 문서 참고.
- 스크립팅: Lua와 Python 엔진을 모두 갖춘 `FUNCTION`/`FCALL`, 그리고
  레거시 `EVAL`/`EVALSHA`/`SCRIPT`. 두 언어 모두 바로 불러올 수 있는
  `FUNCTION LOAD` 예제는 `scripts/` 참고.
- 운영/기타: `SLOWLOG`(파일 기반), `EXPIRE`/TTL(`NX`/`XX`/`GT`/`LT`
  포함), `PUBSUB`, 키스페이스 알림, `CLIENT TRACKING`(`BCAST`/`REDIRECT`/
  `NOLOOP` 포함), `AUTH`와 전체 ACL 사용자 테이블(아래 참고), 근사치인
  `MEMORY USAGE`(메모리 관리 전반의 real Redis 대비 차이는 아래 전용 절
  참고), `LOLWUT`, `INFO`(서버/클라이언트/메모리/영속화/통계/
  복제/키스페이스 섹션 — 이 포크가 실제로 추적하는 것만, 지어낸 값
  없음), `TIME`/`LASTSAVE`/`BGSAVE`/`SHUTDOWN`, `COMMAND`(`COUNT`/`LIST`/
  `INFO`/`DOCS`/`GETKEYS`), `WATCH`/`UNWATCH`(`MULTI`를 위한 낙관적
  잠금), `DUMP`/`RESTORE`.
- `ZUNIONSTORE`/`ZINTERSTORE`의 `WEIGHTS`/`AGGREGATE` 옵션과 `STORE`가
  없는 형제 커맨드 `ZUNION`/`ZINTER`/`ZDIFF`/`ZINTERCARD`. `GEOSEARCH`의
  `FROMMEMBER`와 `GEOSEARCHSTORE` 커맨드. Stream을 위한 `XSETID`와
  `XINFO STREAM`/`XINFO GROUPS`/`XINFO CONSUMERS`.
- Stream 컨슈머 그룹: `XGROUP`(`CREATE`/`SETID`/`DESTROY`/
  `CREATECONSUMER`/`DELCONSUMER`), `XREADGROUP`, `XACK`, `XPENDING`
  (요약형/확장형), `XCLAIM`, `XAUTOCLAIM` — 이 포크의 RDB 포맷에 있는 다른
  모든 것과 마찬가지로 `SAVE`/재시작을 거쳐도 왕복하는 그룹별 PEL
  (pending entries list)이 뒷받침한다.
- `rustls` 기반 TLS, 양방향 지원: 설정에서 `tls-port`, `tls-cert-file`,
  `tls-key-file`을 설정하면 평문 리스너와 나란히 두 번째 암호화 리스너가
  열리고(둘 다 같은 데이터셋을 서빙), `tls-replication yes`를 설정하면 이
  포크 자신이 내보내는 `REPLICAOF` 핸드셰이크도 마스터에 TLS로 붙는다.
  클라이언트 인증서 검증, SNI, 암호/프로토콜 튜닝 옵션은 없고, 복제 쪽은
  CA 신뢰 저장소 자체가 없다 — 마스터의 인증서 체인을 전혀 검증하지
  않는다.
- 전체 ACL: 다중 사용자 테이블(`ACL SETUSER`/`GETUSER`/`DELUSER`/`LIST`/
  `USERS`/`CAT`/`WHOAMI`/`DRYRUN`)에 사용자별 커맨드 권한(`+cmd`/`-cmd`/
  `+@category`/`-@category`, 엄선된 10개 카테고리 집합), 키 패턴
  (`~pattern`/`allkeys`), 채널 패턴(`&pattern`/`allchannels`) 권한이
  모든 커맨드에 적용된다. `default` 사용자 자신의 인증은 여전히
  예전처럼 `requirepass`를 거친다 — 그 이유는 로드맵 문서 Phase 22
  섹션 참고. `ACL SAVE`/`LOAD`는 `aclfile`이 설정되지 않았을 때의 실제
  Redis 동작과 똑같이 에러를 낸다.
- Redis Cluster: 해시 슬롯(`CRC16` + `{hashtag}`), 실제 `MOVED`/
  `CROSSSLOT`/`CLUSTERDOWN` 리다이렉트, `CLUSTER` 커맨드 표면 전체
  (`MEET`/`NODES`/`INFO`/`SLOTS`/`SHARDS`/`KEYSLOT`/`(COUNT|GET)KEYSINSLOT`/
  `ADDSLOTS(RANGE)`/`DELSLOTS(RANGE)`/`SETSLOT ... NODE`/`BUMPEPOCH`/
  `SET-CONFIG-EPOCH`/`FORGET`/`RESET`/`REPLICATE`), `READONLY`/`READWRITE`
  (레플리카가 자기 마스터 슬롯에 대해 stale 읽기를 서빙할 수 있음). 가십은
  연속적이 아니라 틱 기반으로 동작하는 커스텀(바이트 비호환) 텍스트
  프로토콜 위에서 돈다 — 무엇이 축소됐는지(리샤딩 댄스/`ASK`/`MIGRATE`
  없음, 자동 장애조치 없음, `nodes.conf` 영속화 없음, 장애 감지 쿼럼
  없음) 전체 목록은 로드맵 문서 Phase 23 섹션과 `cluster.rs`의 모듈 문서
  참고.
- 네이티브 DataFrame 타입(`--features polars`, 옵트인): `DF.FROMCSV`/
  `DF.TOCSV`(CSV 가져오기/내보내기), `DF.FROMPARQUET`/`DF.TOPARQUET`
  (Parquet 가져오기/내보내기, pandas와 상호운용 테스트 완료), `DF.SET`/
  `DF.GET`(raw Arrow IPC 바이트, RDB 영속화에 쓰는 것과 같은 포맷),
  `DF.SHAPE`/`DF.COLUMNS`(내부 구조 조회), `DF.QUERY`(`polars::sql::SQLContext`를
  통한 진짜 SQL, 테이블 `self`로 등록된 dataframe에 대해 프로세스 내부에서
  실행, RESP 배열 응답), `DF.PRETTY`(같은 SQL이지만 polars 자체의 박스
  드로잉 테이블 응답) — 전체 스코프(의도적으로 뺀 것들: `DF.FILTER`류의
  연산별 커맨드, 제자리 행 변경, 키 간 조인, `CLUSTER` 리다이렉션 인식)는
  `dataframe.rs`의 모듈 문서 참고.
- `maxmemory` 축출(8개 정책)/`UNLINK`+`lazyfree-lazy-*`(전용 백그라운드
  스레드로 실제 비동기 해제)/`MEMORY USAGE`+`STATS`+`DOCTOR`+`PURGE`+
  `MALLOC-STATS`/`activedefrag`(컨테이너 레벨 용량 회수)/`DEBUG OBJECT`+
  `SET-ACTIVE-EXPIRE` — 전부 real Redis엔 원래 있었지만 이 fork에선 한동안
  "설정만 있고 동작은 없음"으로 범위 밖이었던 것을 단계적으로 실제
  구현했다. 무엇이 진짜 동일하게 동작하는지와 무엇이 jemalloc이 없어서
  정말 불가능한지의 전체 비교는 아래 "메모리 관리" 절 참고.
- CLI와 벤치마크 도구 자체.

**명시적으로 범위 밖에 둔 것들**(빠뜨린 게 아니라 로드맵 문서에 각각
이유가 적혀 있는 의도적 결정): Redis Cluster의 리샤딩 댄스(`ASK`/
`MIGRATE`/`IMPORTING`/`MIGRATING`)와 자동/투표 기반 장애조치(둘 다 자체
컨트롤 플레인 설계가 필요해서, 클라이언트 측 관리 도구나 수동 승격에
의도적으로 맡겼다 — Phase 23 참고), `LATENCY`류 커맨드와 jemalloc 아레나
통계에 묶인 `DEBUG`/`MEMORY` 서브커맨드 일부(`DEBUG OBJECT`/
`SET-ACTIVE-EXPIRE`와 `MEMORY USAGE`/`STATS`/`DOCTOR`/`PURGE`/
`MALLOC-STATS`는 실제로 구현돼 있다 — 아래 "메모리 관리" 절에 전체
비교가 있다), ACL selector와 서브커맨드 단위 규칙(`+config|get`),
TLS 클라이언트 인증서 검증/SNI, TLS 전용 모드(`tls-port`만 설정하고
`port 0`) — 평문 리스너는 TLS가 설정돼도 항상 함께 바인딩된다.

## 메모리 관리: real Redis vs redisX

`maxmemory`/축출/`UNLINK`-lazyfree/메모리 진단/active defrag/`DEBUG`는
모두 **실제로 동작한다** — jemalloc 아레나 통계 그 자체(`allocator.*`
같은 필드, 진짜 아레나 압축)만 여전히 범위 밖이다. 이 절은 원래 "설정만
있고 동작은 없다"는 정반대 상태를 기록했던 절인데, redisX에 실제 구현이
단계적으로 들어가면서(Phase 26) 아래 내용 전체가 갱신됐다 — 무엇이 real
Redis와 **진짜 동일하게 동작**하는지, 무엇이 **의도적으로 더 단순화**됐는지,
무엇이 **jemalloc이 없어서 정말 불가능한지**를 구분해서 적는다. 근거는
[`docs/19_memory_01.md`](docs/19_memory_01.md)(real Redis `zmalloc.c`/
`evict.c`/`lazyfree.c`/`defrag.c`/`expire.c` 분석), [`docs/25_redisX.md`]
(docs/25_redisX.md)의 §4-2 결정 및 Phase 26 전체, 그리고 아래 각 항목이
직접 가리키는 `server.rs`/`config.rs` 소스 라인이다.

### 할당자: jemalloc vs Rust 기본 allocator

Real Redis는 컴파일 타임에 jemalloc(기본)/tcmalloc/libc 중 하나를
고르고, jemalloc을 쓸 때는 스레드별 원자적 사용량 카운터·아레나 분리
(Lua 전용 아레나 포함)·`MALLOCX_ARENA`/`MALLOCX_TCACHE` 플래그까지
`zmalloc.c`가 직접 다룬다. redisX는 **Rust 기본 글로벌 allocator만
쓴다** — jemalloc을 아예 도입하지 않기로 한 의도적 결정이다(`tikv-
jemallocator`로 맞출 수도 있었지만, "C 전용 최적화 트릭을 Rust에 억지로
들여오지 않는다"는 Phase 0부터의 기조와 이 포크의 실제 규모(단일 프로세스
개발/테스트 서버)에서 아레나 분석의 실익이 크지 않다는 판단 — 로드맵
문서 §4-2). 이 한 가지 결정이 아래 항목 대부분의 공통 원인이다.

### TTL 만료는 실제로 동작한다 — "축출(eviction)"과는 다른 이야기

Real Redis는 수동 만료(키 접근 시 `expireIfNeeded`)에 더해 능동 만료를
`activeExpireCycle`로 돌린다 — FAST(최대 1000μs, 매 이벤트 루프 반복마다)와
SLOW(CPU의 최대 25%, 10Hz cron) 두 모드로 나뉘고, 매번 20개씩 랜덤
샘플링한다. redisX는 `server.rs`의 `sweep_expired` 함수 doc 그대로
"`activeExpireCycle`을 **매 커맨드 진입 시점마다 대상 db 전체를 동기
스윕**하는 것으로 단순화"했다 — 샘플링도, FAST/SLOW 구분도 없지만, 빈
`expires` 맵은 스캔 비용이 사실상 0이라 일반적인 경우엔 손해가 없고,
오히려 real Redis보다 더 즉각적으로 만료를 반영한다. **TTL 만료 자체는
정확하게 동작한다** — 아래에서 다루는 "메모리 압박에 의한 축출"과 절대
혼동하지 말 것.

### `maxmemory` 축출: 8개 정책 전부 실제로 동작한다

Real Redis는 `maxmemory-policy`로 8가지 정책(volatile/allkeys ×
LRU/LFU/RANDOM, `volatile-ttl`, `noeviction`)을 지원하고,
`performEvictions()`가 `maxmemory-samples`(기본 5)개씩 랜덤 샘플링해
16칸짜리 `EvictionPool`에 근사 LRU/LFU 후보를 채워가며 실제로 키를
지운다. redisX는 이 중 "랜덤 샘플링"만 **의도적으로 다르게** 구현했고,
나머지는 그대로 동작한다:

- `run_command`가 메모리를 늘릴 수 있는 쓰기 커맨드(`SET`/`APPEND`/
  `HSET`/`LPUSH`/`SADD`/`ZADD`/`INCR` 등 — `DENY_OOM` 허용 목록, real
  Redis의 `CMD_DENYOOM` 플래그에 대응) 실행 **직전에** `enforce_
  maxmemory`를 호출한다. `used_memory() <= maxmemory`면 그냥 통과,
  초과했는데 정책이 `noeviction`이면 real Redis와 똑같은 에러 문자열
  `"OOM command not allowed when used memory > 'maxmemory'."`을 그대로
  반환하고 **핸들러 자체를 실행하지 않는다**.
- `DEL`/`UNLINK`/`EXPIRE`류/`FLUSHALL`/`LPOP`/`SREM`처럼 메모리를
  *줄이는* 쓰기는 `DENY_OOM`에서 제외했다 — `noeviction`으로 한도를
  넘긴 서버가 자기 자신을 비울 방법마저 막히는 걸 피하기 위해서다(real
  Redis도 동일한 설계).
- `noeviction`이 아니면 `collect_eviction_order`가 한도 아래로 내려갈
  때까지 후보를 지운다 — `allkeys-random`/`volatile-random`은 무작위,
  `allkeys-lru`/`volatile-lru`는 `Database::access` side table(아래
  `OBJECT` 절 참고)의 `idle_since_ms`가 가장 오래된 키, `allkeys-lfu`/
  `volatile-lfu`는 `freq`가 가장 낮은 키, `volatile-ttl`은 만료 시각이
  가장 임박한 키를 고른다. `maxmemory-samples`는 `CONFIG SET`/설정
  파일 호환을 위해 키 자체는 받아주지만 실제로 읽지는 않는다 —
  **샘플링이 아니라 전체 스캔 기반 정렬**이라 샘플링보다 항상 더 정확한
  후보를 고르기 때문에(같은 트레이드오프를 active-expire 스윕에서도
  이미 했다), 받아주고 무시하는 게 샘플링을 흉내 내는 것보다 정직하다.
- 지워진 키는 real Redis와 동일하게 키스페이스 알림 `e`(evicted) 클래스로
  통지되고, 마스터라면 합성 `DEL`을 복제본/AOF로 전파한다.
- `CONFIG SET maxmemory-policy <값>`은 8개 정책 이름 중 하나가 아니면
  바로 에러를 낸다(`"ERR Invalid maxmemory policy '...'"`) — 이 fork가
  타입 검증을 하는 몇 안 되는 설정 키 중 하나로, real Redis 자신도 이
  키 하나만큼은 enum으로 검증하는 것과 같은 이유다.

즉 `maxmemory`를 작게 설정하고 채우면, 정책에 따라 실제로 키가
지워지거나(`noeviction`이면) 진짜 `OOM` 에러가 난다 — 더 이상 장식이
아니다.

**키가 많을 때의 지연 시간**: 처음 구현은 축출 후보 하나를 고를 때마다
db 전체를 다시 스캔해서, K개를 축출하는 데 `O(N×K)`가 들었다 — 20,000키
짜리 db에서 거의 다 비우는 축출 한 번에 약 430ms가 걸릴 정도로 실측
확인됐다. 세 가지를 고쳤다:

- **`collect_eviction_order`**: db당 **딱 한 번**만 스캔해서 정렬된 축출
  순서 전체를 만들고, 그 뒤로는 재스캔 없이 순서대로 지운다 — `O(N×K)`가
  `O(N log N)` 한 번으로 줄었다.
- **`used_memory_cached`**: `maxmemory`가 한도에 가깝지 않은 "일반적인"
  경우를 위한 50ms 캐시 — 이 fork엔 jemalloc 같은 실시간 카운터가 없어
  `used_memory()` 자체가 키스페이스 전체를 순회하는 함수라, 캐시 없이는
  **메모리를 늘리는 쓰기 커맨드마다** 매번 전체 스캔 비용을 냈다. 캐시가
  "한도 안에 있다"고 판단하면 그대로 통과하고, "한도를 넘었을 수도
  있다"고 판단하는 순간부터는 항상 정확히 재계산한 뒤 축출하므로, 지연은
  최대 캐시 수명(50ms)만큼만 늦게 감지될 뿐 축출 자체가 틀린 적은 없다.
- **복제 백로그 압축 버그**: 위 두 가지를 고치고 나서도 20,000키 기준
  약 430ms가 여전히 남아 있었는데, 원인은 축출과 무관한 기존 코드
  — `feed_backlog`가 1MiB 캡을 넘을 때마다 `Vec::drain`으로 앞부분을
  잘라내는데, 이 `drain`은 `O(남은 바이트 수)`(~1MiB)다. 축출마다 `DEL`
  하나씩 전파되므로, 캡이 꽉 찬 상태에서 축출 수천 번이면 매번 ~1MiB
  shift가 반복돼 실질적으로 이게 병목이었다. 캡을 넘자마자 바로
  compact하는 대신 25% 여유(`REPL_BACKLOG_SLACK`)를 두고 그 여유분까지
  찼을 때만 한 번에 compact하도록 바꿔, `O(1MiB)` 비용을 매번이 아니라
  "그만큼 더 써질 때마다 한 번"으로 분산시켰다. `try_partial_resync`가
  보장하는 백로그 윈도우는 이제 "정확히 1MiB"가 아니라 "1~1.25MiB
  사이"가 됐다 — 더 널널해질 뿐, 부분 재동기화가 더 잘 성공하는 방향의
  변화라 안전하다.
- **결과**: 20,000키 기준 약 430ms → 약 60ms(약 7배), 100,000키는
  약 340ms로 거의 선형 스케일링(20k→100k는 5배인데 시간도 약 5.5배) —
  더 이상 `O(N×K)`가 아니라는 걸 실측으로 확인했다.

### `MEMORY` 커맨드: `USAGE`/`STATS`/`DOCTOR`/`PURGE`/`MALLOC-STATS` 구현

Real Redis의 `MEMORY` 서브커맨드군 중 `STATS`의 약 30개 필드 대부분
(`allocator.*`/`allocator-fragmentation.*` 등)은 jemalloc 아레나 통계
위에 지어져 있어 여전히 범위 밖이다 — 하지만 jemalloc 없이도 **정직하게
계산/실행 가능한** 부분은 전부 들어갔다(`PURGE`/`MALLOC-STATS` 포함,
아래 active defrag 절 참고).

- **`MEMORY USAGE key`**: 변경 없음 — 값의 내용을 직접 순회해 바이트 수를
  더한 근사치(문자열 길이 합, 리스트/해시/셋/zset 원소 바이트 합,
  DataFrame은 Arrow IPC 인코딩 크기 등) + 키 이름 길이 + 고정 오버헤드
  16바이트. **샘플링 없음**(`SAMPLES` 인자는 받되 무시).
- **`MEMORY STATS`**: real Redis의 ~30개 필드를 전부 흉내 내는 대신,
  이 fork가 실제로 뒷받침할 수 있는 필드만 정직하게 돌려준다 —
  `used_memory`(키스페이스 콘텐츠 순회 추정치), `keys.count`(모든 db의
  키 수 합), 그리고 OS에서 실제로 읽어온 `used_memory_rss`/
  `mem_fragmentation_ratio`(아래 참고). 둘 다 OS에서 값을 못 가져오면
  (지원 안 되는 플랫폼이거나 샌드박스 제약) 필드 자체를 생략한다 —
  `0`이나 가짜 값으로 채우지 않는다.
- **`MEMORY DOCTOR`**: `mem_fragmentation_ratio`를 기준으로 실제 진단을
  내린다. 1.5배 이상이면 "단편화가 높다"고 알리고, `activedefrag`가
  이미 켜져 있으면 `MEMORY PURGE`로 즉시 한 번 더 돌려보라고, 꺼져
  있으면 `CONFIG SET activedefrag yes`나 `MEMORY PURGE`를 먼저
  권한다 — 그래도 안 내려가면 그 나머지는 jemalloc 아레나 압축 없이는
  못 줄이는 진짜 할당자 단편화이므로 **프로세스 재시작**(RDB/AOF가
  영속화하므로 안전)이 마지막 수단이라고 명시한다. RSS를 못 가져온
  경우엔 진단할 수 없다고 그대로 알린다.
- **`MEMORY PURGE`**: real Redis는 jemalloc에게 빈 아레나 페이지를 OS에
  반환하라고 요청하지만, 이 fork는 그 대신 **즉시, 무제한으로** defrag를
  한 바퀴 돈다(아래 active defrag 절의 `force_full_defrag`) — `activedefrag`
  설정과 무관하게 항상 동작하는, 사용자가 명시적으로 요청한 1회성
  작업이다.
- **`MEMORY MALLOC-STATS`**: real Redis는 jemalloc의 `malloc_stats_print`로
  아레나 리포트를 stdout에 찍는다. 이 fork는 찍을 아레나가 없으므로,
  대신 실제로 가진 값(`used_memory`/`used_memory_rss`/
  `mem_fragmentation_ratio`/`activedefrag` 설정)을 stderr에 찍고
  `+OK`를 반환한다 — 이 커맨드를 무조건 호출하는 운영 스크립트가 "이
  fork는 이야기해 줄 게 더 적다"는 이유만으로 깨지지 않도록.

### `used_memory_rss`/`mem_fragmentation_ratio`: 진짜 OS RSS 기반

`INFO memory`에도 real Redis와 **완전히 같은 필드 이름**으로
`used_memory_rss`/`mem_fragmentation_ratio`가 추가됐다 — 다만 값을 얻는
경로는 다르다. real Redis는 jemalloc의 `allocator_active`/
`allocator_resident` 통계를 쓰지만, redisX의 `rss_bytes()`는 이 프로세스의
실제 RSS를 OS에서 직접 읽는다(Linux는 `/proc/self/status`의 `VmRSS`,
그 외 플랫폼은 `ps -o rss=`). `mem_fragmentation_ratio`는 단순히
`used_memory_rss / used_memory`다 — `used_memory`는 키스페이스 콘텐츠만
세고 Lua VM/polars/이 프로세스의 다른 모든 할당은 세지 않으므로, 이
비율은 키스페이스가 작을수록 더 커 보일 수 있다(빈 서버에 키 하나만
있으면 비율이 수만 배로 나올 수 있다 — 이건 버그가 아니라 "실제 RSS 대비
키스페이스 비중"이라는 정의 그대로의 결과다). jemalloc 아레나 분석이
아니라 "진짜 OS 측정값 대 진짜 추정값"이라는 점에서 여전히 정직한
숫자다.

### `UNLINK`/lazyfree: 진짜 백그라운드 스레드로 비동기 해제한다

Real Redis는 객체를 지울 때 비용을 추정해서(리스트 노드 수, 셋/해시
원소 수, 스킵리스트 길이 등 — `lazyfreeGetFreeEffort`) 64를 넘으면 BIO
스레드로 떠넘기고(`lazyfree-lazy-eviction`/`-expire`/`-server-del`/
`-user-del`/`-user-flush` 5개 설정으로 어떤 작업을 그렇게 할지 고른다).
redisX는 "비용 추정" 단계만 생략하고(이 fork의 값 크기에서 추정 자체가
real Redis만큼 중요하지 않다는 판단) 나머지는 그대로 동작한다:

- **`UNLINK`는 키를 키스페이스에서 즉시 지우되, 실제 값은 설정과 무관하게
  항상 백그라운드 스레드로 넘겨서 드롭한다** — real Redis의 "`UNLINK`는
  항상 명시적 비동기"라는 규칙 그대로. `Server::lazyfree_unconditional`이
  `mpsc::channel`로 값을 보내고, 단 하나의 백그라운드 스레드가 그걸 받아
  `drop`한다(Rust는 할당 해제가 바로 `Drop`이므로, 드롭을 다른 스레드로
  옮기는 것 자체가 곧 "느린 해제 작업을 호출자 스레드에서 떼어낸다"는 뜻과
  같다).
- **`lazyfree-lazy-*` 5개 설정 키가 전부 존재하고 실제로 동작한다**:
  `-user-del`은 `DEL`을 `UNLINK`처럼 만들고, `-user-flush`는 `FLUSHALL`/
  `FLUSHDB`가 지우는 *db 전체*를 통째로 백그라운드 스레드에 넘긴다(키
  하나씩이 아니라 — 그러면 비싼 작업을 호출자 스레드에서 이미 다 해버린
  뒤라 의미가 없다), `-eviction`/`-expire`는 각각 `maxmemory` 축출/active-
  expire 스윕이 지운 값을 비동기로 돌리고, `-server-del`은 `SET`의 덮어쓰기,
  `RENAME`의 목적지 덮어쓰기, `COPY`/`RESTORE ... REPLACE`처럼 커맨드가
  "부수 효과로" 기존 값을 지우는 주요 지점에 연결돼 있다(real Redis는
  `dbOverwrite`라는 단일 지점을 거치지만 이 fork엔 그런 공통 지점이 없어,
  가장 흔한 덮어쓰기 커맨드들에만 연결했다 — 모든 타입 내부의 모든 덮어쓰기
  까지 완전히 망라하진 않는다). 전부 기본값은 `no`(real Redis와 동일).
- **백그라운드 스레드는 실제로 쓰일 때만 뜬다** — 5개 설정이 전부 기본값
  `no`인 서버(테스트의 절대다수 포함)는 `UNLINK`를 한 번도 안 부르면
  스레드를 아예 띄우지 않는다(`Server::lazyfree_sender`는 `Option`, 첫
  실제 사용 시점에 지연 생성).

### active defrag: jemalloc 아레나 압축은 불가능하지만, 실제로 동작한다

Real Redis의 `activedefrag`는 jemalloc 아레나의 압축 API로 살아있는
객체를 애플리케이션 몰래 재배치해 빈 슬랩을 OS에 반환한다 — 이 API
자체는 Rust 기본 global allocator에 대응물이 없어 **여전히 불가능**하다.
다만 "단편화"의 실제 정체를 들여다보면, 이 fork 안에서 jemalloc 없이도
고칠 수 있는 부분이 분명히 있었다: `ListPack`/`IntSet`이 내부적으로
쓰는 `Vec<u8>`는 `remove()`가 `Vec::drain`으로 원소를 지우는데, 이게
**할당된 용량(capacity)은 전혀 줄이지 않는다** — 한 번 크게 자랐다가
대부분 비워진 리스트/셋은 그 용량을 계속 붙들고 있다. `ZSetObj::Sorted`
(정렬 `Vec`)도 `Vec::remove`라 같은 문제가 있다. (반면 해시테이블 기반
타입(`HashObj::Dict`/`SetObj::Dict`, db의 메인 dict 자체)은 `RDict`가
이미 `dictShrinkIfNeeded`와 같은 정책으로 자동으로 테이블을 줄이므로
손댈 게 없었다.)

- **`RObj::defrag_in_place`**: 값 종류에 맞는 `shrink_to_fit()`을
  호출한다 — `String`/`List`/listpack 단계의 `Hash`·`ZSet`/`IntSet`
  단계의 `Set`은 전부 실제로 용량이 줄어든다(`ListPack::shrink_to_fit`/
  `IntSet::shrink_to_fit`를 redisx-core에 새로 추가, 유닛테스트로 "용량이
  실제로 줄어드는지"를 직접 확인). 해시테이블 기반 값은 이미 자동으로
  처리되므로 no-op.
- **`defrag_step`**: `SCAN`이 쓰는 것과 똑같은 역방향-이진 커서로 db를
  한 번에 `DEFRAG_STEP_BUDGET`(200)개씩, 중단된 지점부터 재개 가능하게
  훑으며 `defrag_in_place`를 적용한다 — real Redis의 `activeDefragCycle`이
  `dictScan`을 재사용하는 것과 동일한 이유(리사이즈 중에도 안전하게
  재개 가능).
- **`activedefrag yes`** + **`active-defrag-threshold`**(기본 `1.5`,
  real Redis의 lower/upper 두 값을 이 fork의 단일 `mem_fragmentation_
  ratio`에 맞춰 하나로 단순화): 켜져 있고 비율이 임계값을 넘으면,
  커맨드 처리 중(`run_command`)에 최대 1초에 한 번만(`rss_bytes()`가
  비 Linux에서 `ps` 서브프로세스를 띄우므로, 매 커맨드마다 확인하면
  그 자체가 새 성능 문제가 된다) 체크하고, 넘었으면 db당 한 번씩
  `defrag_step`을 돌린다 — 매 호출마다 전체를 다 돌지 않고 점진적으로.
- **`MEMORY PURGE`**: `activedefrag` 설정과 무관하게 즉시, 전체 db를
  끝까지(한 번의 호출로 커서가 한 바퀴 돌 때까지) defrag하고, `expires`/
  `access` side map도 `shrink_to_fit`한다 — "지금 당장 다 하라"는
  명시적 요청이라 점진적으로 하지 않는다.

이건 jemalloc 아레나 압축이 아니라 **이 fork 자신의 컨테이너 레벨
용량 낭비를 되찾는 것**이다 — 범위는 더 좁지만, `LPOP`/`SREM`/`LTRIM`
등으로 커졌다 줄어든 값이 많은 워크로드에서는 진짜로 효과가 있다.
`DEBUG` 커맨드(아래 절)는 이 fork에 전혀 없었다가 이번에 처음 생겼다.

### `OBJECT`: `ENCODING`/`IDLETIME`/`FREQ`가 전부 진짜, `REFCOUNT`만 고정값

Real Redis의 `robj.lru` 24비트 필드는 LRU 모드에선 초 단위 타임스탬프,
LFU 모드에선 "분 단위 마지막 접근 시각(16비트) + 0~255 빈도 카운터(8비트,
Morris 카운터로 로그 스케일 증가)"로 재해석된다. redisX는 이 필드를
한 비트에 욱여넣는 대신 `Database`에 `access: HashMap<Vec<u8>, AccessInfo>`
라는 별도 side table(`expires`와 같은 "같은 키, 다른 수명" 패턴)을 둬서
`idle_since_ms`/`freq` 둘 다 항상 평범하게 들고 있다 — 어떤 정책이
선택됐든 상관없이. `run_command`가 커맨드가 성공할 때마다(읽기/쓰기
구분 없이, real Redis의 `lookupKeyRead`/`lookupKeyWrite`가 공유하는
`updateLFU` 호출과 동일) `touch_access`로 이걸 갱신한다:

- **`IDLETIME`**: `(지금 - idle_since_ms)`를 초 단위로 반환 — 실제 마지막
  접근 이후 경과 시간이다.
- **`FREQ`**: `maxmemory-policy`에 `lfu`가 포함돼 있을 때만, `evict.c`의
  `LFULogIncr`/`LFUDecrAndReturn` 공식을 그대로 포팅한 `lfu_log_incr`/
  `lfu_decay`로 관리되는 실제 0~255 카운터를 반환한다(새 키는 real
  Redis와 같은 `LFU_INIT_VAL = 5`에서 시작). LFU 정책이 아니면 real
  Redis와 동일한 에러 문자열을 그대로 낸다.
- **`ENCODING`**: 변경 없음 — 타입을 정확히 반영.
- **`REFCOUNT`**: 여전히 항상 `1` — 이 fork엔 객체 공유(shared integer
  캐시 등)가 없어 추적할 실제 참조 카운트 자체가 없다.

### `DEBUG`: 메모리/만료 진단용 두 서브커맨드가 새로 생겼다

이 fork엔 `DEBUG`가 아예 없었다(fork 전체 포팅 범위에서 C의 fork 기반
프로세스 모델·jemalloc 내부 구조에 결합된 기능으로 명시적으로 제외돼
있었다). real Redis의 수십 개 서브커맨드(`JMAP`/`SEGFAULT`/
`STRUCTSIZE`/`CHANGE-REPL-ID`/... 대부분은 safe Rust 프로세스엔 아예
해당하지 않거나 C 메모리 레이아웃 전용이다) 전체를 포팅하는 대신,
메모리/만료 진단 목적의 딱 두 개만 구현했다:

- **`DEBUG OBJECT key`**: real Redis에서 실제로 가장 많이 쓰이는
  `DEBUG` 서브커맨드 — 값 하나의 메모리/인코딩을 한 줄로 보여준다.
  `serializedlength`는 `DUMP`/`RESTORE`가 쓰는 것과 같은 `rdb.rs`의
  `dump_value` 바이트 길이, `lru_seconds_idle`은 `OBJECT IDLETIME`과
  같은 `AccessInfo` side table을 읽는다. 리스트일 때만 `ql_nodes:1
  ql_avg_node:<길이>`가 붙는다 — `list.rs`가 항상 listpack 노드 하나뿐이라는
  그 모듈 자신의 단순화를 숨기지 않고 그대로 보여주는 것.
- **`DEBUG SET-ACTIVE-EXPIRE 0|1`**: 진단/테스트 세션 동안 active-expire
  스윕을 멈춰서, 메모리/TTL 상태를 들여다보는 동안 밑에서 조용히 키가
  사라지지 않게 한다 — real Redis와 같은 용도. 다만 **real Redis보다
  효과가 더 강하다**: 이 fork는 `sweep_expired`(커맨드마다 도는 능동
  스윕)가 TTL을 처리하는 **유일한** 경로라서(개별 커맨드가 자기 나름의
  passive expire 체크를 따로 하지 않는다), 이 플래그를 끄면 TTL 집행
  자체가 완전히 멈춘다 — real Redis는 능동 스윕만 멈추고 수동(접근 시)
  만료는 계속 동작하는 것과 다른 점.

### 한눈에 보는 요약

| 영역 | real Redis | redisX |
|---|---|---|
| 할당자 | jemalloc(기본)/tcmalloc/libc, 아레나 통계 | Rust 기본 global allocator만(의도적 결정, §4-2) |
| TTL 만료 | passive + active(FAST/SLOW, 샘플링 cron) | passive + active(커맨드마다 전체 스윕) — **실제로 동작함** |
| `maxmemory` 축출 | LRU/LFU/RANDOM/TTL 정책, 근사 샘플링 풀 | **8개 정책 전부 동작** — 샘플링 대신 전체 스캔 한 번(더 정확, `O(N log N)` 한 번), 실제 `OOM` 에러 |
| `MEMORY` 커맨드 | `USAGE`/`STATS`/`DOCTOR`/`MALLOC-STATS`/`PURGE` | **전부 구현** — `allocator.*` 등 jemalloc 전용 `STATS` 필드만 제외 |
| `used_memory_rss`/`mem_fragmentation_ratio` | jemalloc 아레나 통계 기반 | OS에서 직접 읽은 실제 RSS 기반(`/proc` 또는 `ps`) — **실제로 동작함** |
| `UNLINK` | 비동기(BIO 스레드로 오프로드) | **실제 비동기** — 전용 백그라운드 스레드로 값을 넘겨 드롭 |
| `lazyfree-lazy-*` | 설정 5개, 비용 추정 후 BIO 오프로드 | **5개 전부 동작**(비용 추정 단계만 생략) |
| active defrag | jemalloc 아레나 재배치/페이지 반환 | jemalloc 압축 자체는 불가능 — 대신 `ListPack`/`IntSet`/정렬 `Vec`의 **용량 낭비를 실제로 회수**(`activedefrag yes` 점진적, `MEMORY PURGE` 즉시 전체) |
| `OBJECT REFCOUNT`/`IDLETIME`/`FREQ` | 실제 참조 카운트/LRU idle/LFU 빈도 | `REFCOUNT`만 고정 `1`, `IDLETIME`/`FREQ`는 **실제 side table 기반** |
| `DEBUG`(메모리 관련 서브커맨드) | jemalloc stats, defrag 등 다수 | **`OBJECT`/`SET-ACTIVE-EXPIRE` 신규 구현**(이전엔 `DEBUG` 자체가 없었음), 나머지는 범위 밖 |

## 라이선스

[`LICENSE.md`](./LICENSE.md) 참고. 이 프로젝트는 Redis의 파생 포팅이며,
포팅 대상이 된 Redis 소스와 동일한 라이선스 조건을 그대로 이어받는다.
