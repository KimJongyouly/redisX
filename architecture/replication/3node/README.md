# replication/3node — 마스터 1대 + 슬레이브 2대 (팬아웃)

마스터 한 대의 쓰기를 슬레이브 두 대가 동시에 받는 구성. `2node/`와 순서는
같고, 슬레이브를 하나 더 반복할 뿐이다 — 한 마스터가 여러 복제 연결을
동시에 유지/스트리밍할 수 있는지, `connected_slaves`가 정확히 2로
올라가는지가 포인트.

## 포트

| 노드 | 포트 | 설정 파일 |
|---|---|---|
| master  | 9001 | `master/redisx-master.conf` |
| slave1  | 9002 | `slave1/redisx-slave1.conf` |
| slave2  | 9003 | `slave2/redisx-slave2.conf` |

## 실행 순서

**1) 마스터**

```bash
cd redisX/architecture/replication/3node/master
redisx-server ./redisx-master.conf
```

**2) 슬레이브 1, 슬레이브 2 (각각 다른 터미널)**

```bash
cd redisX/architecture/replication/3node/slave1 && redisx-server ./redisx-slave1.conf
cd redisX/architecture/replication/3node/slave2 && redisx-server ./redisx-slave2.conf
```

**3) 두 슬레이브 모두에서 REPLICAOF 실행 (마스터는 같은 호스트/포트)**

```bash
redisx-cli -p 9002 REPLICAOF 127.0.0.1 9001
redisx-cli -p 9003 REPLICAOF 127.0.0.1 9001
```

**4) 확인**

```bash
redisx-cli -p 9001 INFO replication    # role:master, connected_slaves:2
redisx-cli -p 9002 INFO replication    # role:slave, master_link_status:up
redisx-cli -p 9003 INFO replication    # role:slave, master_link_status:up
```

**5) 팬아웃 전파 확인**

```bash
redisx-cli -p 9001 SET greeting hello
redisx-cli -p 9002 GET greeting        # -> "hello"
redisx-cli -p 9003 GET greeting        # -> "hello"
```

## 알아둘 것

- `2node/README.md`의 "알아둘 것"(REPLICAOF는 런타임 전용, 인증 핸드셰이크
  없음, 슬레이브도 쓰기를 거부하지 않음, `WAIT`의 한계)이 여기도 그대로
  적용된다.
- 슬레이브끼리는 서로 연결되지 않는다 — 둘 다 **같은 마스터**를 보고 있을
  뿐 체인 복제(슬레이브의 슬레이브)는 이 구성에서 다루지 않는다(redisX가
  막는 건 아니고, 그냥 이 샘플의 범위 밖).
- 마스터가 내려가면 두 슬레이브 모두 그 사실을 능동적으로 감지하지
  않는다(`master_link_status`가 곧바로 `down`으로 바뀌지 않을 수 있음) —
  수동 승격은 살아남을 슬레이브 하나를 골라 `REPLICAOF NO ONE`, 나머지
  하나는 그 슬레이브로 다시 `REPLICAOF`를 가리키게 하면 된다.
- **`REPLICAOF NO ONE`은 마스터 쪽 연결을 실제로 끊지 않는다** — 직접
  띄워서 확인한 동작이다. 슬레이브 1을 승격시켜도 예전 마스터의
  `connected_slaves`는 줄지 않고, 승격된 노드는 `role:master`라고 답하면서도
  예전 마스터가 그 뒤에 쓰는 값을 계속 조용히 받아 적용한다("유령 적용" —
  `test_replication_3node.py`가 이 현상을 그대로 재현해서 보여준다).
  `CLIENT KILL`도 없어서 마스터 쪽에서 그 연결을 강제로 끊을 방법이 없다 —
  확실히 끊는 유일한 방법은 **승격된 노드의 프로세스를 재시작**하는
  것뿐이다. 그래서 이 README의 수동 승격 절차도, 그 뒤 "다시 깨끗하게
  REPLICAOF를 건다"는 보장이 아니라 "역할만 바뀐다"는 뜻으로 읽어야 한다.

## 테스트

```bash
pip install redis   # 처음 한 번
# 1), 2)까지 세 서버를 띄운 상태에서
python3 test_replication_3node.py
```

스크립트가 하는 일: 세 서버에 연결 → 슬레이브 둘 다 `REPLICAOF` 실행 →
둘 다 `master_link_status:up`이 될 때까지 폴링 → 마스터에 쓰고 **양쪽
슬레이브 모두**에서 읽히는지 확인 → `connected_slaves == 2` 확인 → 슬레이브
1을 승격(`REPLICAOF NO ONE`)해도 슬레이브 2는 원래 마스터에 그대로
붙어있는지 확인 → 위에서 설명한 "연결이 끊기지 않는" 한계를 실제로
재현해서 보여줌(실패로 처리하지 않음).

**재실행하려면 세 서버를 모두 재시작할 것** — 이 스크립트는 일부러 그
한계를 드러내며 끝나기 때문에 slave1을 깨끗한 상태로 되돌리지 못한다.
시작할 때 유령 연결 유무를 감지해서 분명한 에러로 안내한다.
