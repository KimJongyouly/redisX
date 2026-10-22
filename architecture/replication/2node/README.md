# replication/2node — 마스터 1대 + 슬레이브 1대

가장 단순한 복제 구성. `REPLICAOF` 핸드셰이크 → 초기 RDB 전체 동기화 →
이후 쓰기 스트리밍, 세 단계를 직접 눈으로 확인한다.

## 포트

| 노드 | 포트 | 설정 파일 |
|---|---|---|
| master | 7001 | `master/redisx-master.conf` |
| slave  | 7002 | `slave/redisx-slave.conf` |

## 실행 순서

**1) 마스터 먼저 띄운다**

```bash
cd redisX/architecture/replication/2node/master
redisx-server ./redisx-master.conf
```

**2) 슬레이브를 띄운다 (다른 터미널)**

```bash
cd redisX/architecture/replication/2node/slave
redisx-server ./redisx-slave.conf
```

이 시점까지는 둘 다 그냥 독립된 마스터다 — `INFO replication`을 찍어보면
둘 다 `role:master`.

**3) 슬레이브에서 수동으로 REPLICAOF 실행**

```bash
redisx-cli -p 7002 REPLICAOF 127.0.0.1 7001
```

`+OK`가 바로 돌아오지만, 이건 "핸드셰이크를 시작했다"는 뜻이지 "동기화가
끝났다"는 뜻이 아니다(실제 Redis와 동일) — 슬레이브 서버 프로세스 쪽
stderr에 다음과 같은 로그가 순서대로 찍히는 걸 볼 수 있다:

```
REPLICAOF 127.0.0.1 7001: starting handshake (blocking)...
REPLICAOF 127.0.0.1 7001: loaded initial N byte RDB, streaming now
```

**4) 상태 확인**

```bash
redisx-cli -p 7001 INFO replication    # role:master, connected_slaves:1
redisx-cli -p 7002 INFO replication    # role:slave, master_link_status:up
```

**5) 복제 확인**

```bash
redisx-cli -p 7001 SET greeting hello
redisx-cli -p 7002 GET greeting        # -> "hello" (스트리밍으로 전파됨)
```

**6) (선택) 되돌리기 / 승격**

```bash
redisx-cli -p 7002 REPLICAOF NO ONE    # 슬레이브를 독립 마스터로 승격
```

## 알아둘 것

- 마스터에 `requirepass`를 걸면 이 핸드셰이크에 인증 단계가 없어서 복제가
  그냥 실패한다 — 이 conf들은 그래서 `requirepass`를 설정하지 않는다.
- 슬레이브도 직접 쓰기를 받는다(`replica-read-only` 미구현) — `redisx-cli -p
  7002 SET x y`가 에러 없이 성공한다. `test_replication_2node.py`에서 이
  차이를 그대로 확인한다.
- `WAIT`는 연결된 슬레이브 수만 셀 뿐 "그 쓰기가 복제됐는지"는 보장하지
  않는다 — 테스트는 슬레이브 쪽 값을 직접 폴링해서 전파를 확인한다.
- **`REPLICAOF NO ONE`은 마스터 쪽 연결을 실제로 끊지 않는다** (이 저장소를
  직접 띄워서 확인한 동작 — `src/bin/redisx-server.rs`의 `handle_replicaof`가
  "NO ONE" 분기에서 이 노드의 `role` 필드만 `Master`로 바꾸고, 마스터로부터
  스트림을 읽어 적용하는 백그라운드 핸들러(`service_master_link`)는 그대로
  살아있다). 그 결과: (1) 마스터의 `connected_slaves`가 승격 후에도 줄지
  않고, (2) 승격된 노드는 `role:master`라고 답하면서도 마스터가 그 뒤에 쓰는
  값을 계속 조용히 받아 적용한다("유령 적용"). `CLIENT KILL`도 없어서 마스터
  쪽에서 그 연결을 끊을 방법도 없다 — 확실히 끊는 유일한 방법은 **승격된
  노드의 프로세스를 재시작**하는 것뿐이다. 이 스크립트를 재실행하기 전에
  두 서버를 모두 재시작해야 하는 이유이기도 하다.

## 테스트

```bash
pip install redis   # 처음 한 번
# 1), 2)까지 두 서버를 띄운 상태에서
python3 test_replication_2node.py
```

스크립트가 하는 일: 두 서버에 연결 → 아직 복제 전인지 확인 →
`REPLICAOF`를 직접 실행 → `master_link_status:up`이 될 때까지 폴링 →
마스터에 쓰고 슬레이브에서 읽혀지는지 확인 → 슬레이브 직접 쓰기가 거부되지
않음을 확인 → `REPLICAOF NO ONE`으로 승격.

**재실행하려면 두 서버를 모두 재시작할 것** — 위에서 설명한 "연결이 끊기지
않는" 한계 때문에, 같은 서버 두 대에 대고 또 실행하면 유령 연결이 쌓여
`connected_slaves` 체크가 어긋난다. 스크립트가 시작할 때 이 상태를 감지해서
분명한 에러로 안내한다.
