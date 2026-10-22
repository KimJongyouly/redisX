# single — 단일 인스턴스

복제도 클러스터도 없는 redisX 한 대. 다른 두 토폴로지(`replication/`,
`cluster/`)의 기준점이자, 설정 토글(특히 **SLOWLOG on/off**)을 가장 단순한
환경에서 확인하는 용도다.

## 실행

```bash
cd redisX/architecture/single
redisx-server ./redisx-single.conf
```

다른 터미널에서 확인:

```bash
redisx-cli -p 2417 PING          # PONG
redisx-cli -p 2417 INFO server | grep -E "redis_version|tcp_port"
```

종료는 `redisx-cli -p 2417 SHUTDOWN NOSAVE` 또는 서버 프로세스에 Ctrl-C.

## SLOWLOG: on/off 토글

redisX의 SLOWLOG는 실제 Redis(메모리 링 버퍼)와 다르게 **파일 기반**이다
(`CHANGE.md` 1절). 그래서 "켜고 끄는" 기준이 메모리 설정이 아니라 **파일
경로가 비어있는지 여부**다:

| 상태 | 조건 |
|---|---|
| **꺼짐 (기본값)** | `slowlog-file-path`가 빈 문자열(`""`) — `SLOWLOG GET`/`LEN`이 항상 빈 결과 |
| **켜짐** | `slowlog-file-path`가 실제 경로 — 그 파일에 기록되고 `SLOWLOG GET`/`LEN`이 그 파일을 읽어서 답함 |

`slowlog-max-len`은 효과가 없다(메모리에 아무것도 들고 있지 않으므로) —
대신 얼마나 "느려야" 기록할지는 `slowlog-log-slower-than`(마이크로초)이
결정한다.

### 설정 파일로 켜기 (재시작 필요)

`redisx-single.conf`에서 주석 두 줄을 해제:

```
slowlog-file-path ./redisx-slow.log
slowlog-log-slower-than 10000
```

### 런타임에 켜기/끄기 (재시작 불필요 — `CONFIG SET`이 즉시 반영됨)

```bash
# 켜기
redisx-cli -p 2417 CONFIG SET slowlog-file-path ./redisx-slow.log
redisx-cli -p 2417 CONFIG SET slowlog-log-slower-than 0   # 0 = 사실상 전부 기록 (데모/디버깅용)

# 확인
redisx-cli -p 2417 SET foo bar
redisx-cli -p 2417 SLOWLOG LEN
redisx-cli -p 2417 SLOWLOG GET 10
redisx-cli -p 2417 SLOWLOG RESET          # 파일을 0바이트로 비움

# 끄기 — 경로를 다시 빈 문자열로
redisx-cli -p 2417 CONFIG SET slowlog-file-path ""
redisx-cli -p 2417 SLOWLOG LEN            # -> 0 (비활성 상태로 복귀)
```

운영값으로 되돌리려면 `slowlog-log-slower-than`을 `10000`(10ms, 실제 Redis
기본값과 동일) 근처로 올려서 "진짜 느린 커맨드"만 잡히게 한다 — `0`은 데모용.

`test_single.py`가 이 on/off 왕복을 그대로 자동화해서 검증한다.

## 그 밖의 주요 토글

| 설정 | 기본값 | 켜는 법 |
|---|---|---|
| **AOF** (`appendonly`) | `no` | `CONFIG SET appendonly yes` 또는 conf에 `appendonly yes` + 재시작. `INFO persistence`의 `aof_enabled:1`로 확인 |
| **maxmemory-policy** | `noeviction` | `CONFIG SET maxmemory-policy allkeys-lru` 등. `maxmemory 0`인 동안은 정책과 무관하게 축출이 일어나지 않음 — 같이 설정해야 의미가 있다 |
| **requirepass (AUTH)** | 없음(빈 문자열) | conf에 `requirepass <secret>` + 재시작, 또는 `CONFIG SET requirepass <secret>`(즉시 적용). 끌 때는 `CONFIG SET requirepass ""` |
| **TLS 리스너 추가** | 꺼짐 (`tls-port 0`) | conf에 `tls-port 6380` + `tls-cert-file`/`tls-key-file` 설정 후 재시작 — 평문 `port`와 TLS `tls-port`가 같은 데이터셋을 동시에 서빙 (`redisx-cli --tls -p 6380 --insecure`로 접속) |

TLS/AOF/maxmemory-policy는 이 폴더의 범위를 벗어나므로(토폴로지와 무관한
일반 설정) 표로만 남겨둔다 — 자세한 내용은 `../../README.md`와
`../../docs/19_memory_01.md`/`11_persistant_01.md` 참고.

## 테스트

```bash
pip install redis   # 처음 한 번
cd redisX/architecture/single
redisx-server ./redisx-single.conf &     # 백그라운드로 띄우거나 다른 터미널에서
python3 test_single.py
```

`test_single.py`가 검증하는 것:
1. 기본 연결 + `PING`/`SET`/`GET`
2. SLOWLOG가 기본적으로 꺼져 있음을 확인
3. `CONFIG SET`으로 켜고, 느린 임계값을 0으로 낮춰 전부 기록되게 한 뒤 실제 기록 확인
4. 다시 끄고 `SLOWLOG LEN`이 0으로 돌아오는 것까지 확인
5. `appendonly` on/off 토글과 `INFO persistence`의 `aof_enabled` 반영 확인
