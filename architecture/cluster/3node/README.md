# cluster/3node — 마스터 3대, 16384 슬롯 3등분

슬롯 리더 3대로만 구성한 가장 단순한 Redis Cluster. 복제본(슬레이브) 없이
마스터 3대가 각각 슬롯 범위를 나눠 갖는다 — `CLUSTER MEET`로 묶고
`CLUSTER ADDSLOTSRANGE`로 슬롯을 할당하는 절차와, `MOVED`/`CROSSSLOT`
리다이렉트가 실제로 동작하는지가 포인트.

## 포트

| 노드 | 클라이언트 포트 | 클러스터 버스 포트 (자동: port+10000) | 슬롯 범위 | 설정 파일 |
|---|---|---|---|---|
| node1 | 7301 | 17301 | 0 – 5460 | `node1/redisx-node1.conf` |
| node2 | 7302 | 17302 | 5461 – 10922 | `node2/redisx-node2.conf` |
| node3 | 7303 | 17303 | 10923 – 16383 | `node3/redisx-node3.conf` |

각 conf에 `cluster-enabled yes`, `cluster-port 0`(= `port + 10000`으로 자동
파생)이 이미 들어있다.

## 자동 구성: `setup_cluster.sh`

```bash
cd redisX/architecture/cluster/3node
./setup_cluster.sh
```

이 스크립트가 하는 일 (전부 `redisx-cli`로 하는 것과 동일, 아래 "수동으로"
절에 그 커맨드들이 그대로 있다):
1. 세 노드를 백그라운드로 기동 (`node*/server.log`, `node*/server.pid`)
2. 전부 `PING`에 응답할 때까지 대기
3. node1 기준으로 `CLUSTER MEET`을 두 번 실행해 node2, node3를 소개
   (가십이 전이적으로 퍼지므로 모든 쌍끼리 `MEET`할 필요는 없다)
4. 가십이 퍼질 시간 대기
5. `CLUSTER ADDSLOTSRANGE`로 16384 슬롯을 위 표대로 3등분해서 각 노드에 할당
6. 할당이 가십으로 다시 퍼질 시간 대기
7. node1에서 본 `CLUSTER NODES` 출력

`redisx-server`/`redisx-cli`가 `PATH`에 없다면:
```bash
REDISX_SERVER_BIN=/path/to/redisx-server REDISX_CLI_BIN=/path/to/redisx-cli ./setup_cluster.sh
```

멈추기: `./stop_cluster.sh` (각 노드 폴더의 `server.pid`를 보고 종료한다)

## 수동으로 (스크립트가 하는 일을 그대로 풀어서)

```bash
# 1) 세 노드를 각자 폴더에서 기동 (터미널 3개)
cd redisX/architecture/cluster/3node/node1 && redisx-server ./redisx-node1.conf
cd redisX/architecture/cluster/3node/node2 && redisx-server ./redisx-node2.conf
cd redisX/architecture/cluster/3node/node3 && redisx-server ./redisx-node3.conf

# 2) node1 기준으로 나머지를 소개 (cluster-port는 기본 파생값 사용)
redisx-cli -p 7301 CLUSTER MEET 127.0.0.1 7302 17302
redisx-cli -p 7301 CLUSTER MEET 127.0.0.1 7303 17303

# 3) 슬롯 3등분 할당 (각 노드에서 자기 몫만)
redisx-cli -p 7301 CLUSTER ADDSLOTSRANGE 0 5460
redisx-cli -p 7302 CLUSTER ADDSLOTSRANGE 5461 10922
redisx-cli -p 7303 CLUSTER ADDSLOTSRANGE 10923 16383

# 4) 확인
redisx-cli -p 7301 CLUSTER INFO     # cluster_state:ok, cluster_slots_assigned:16384
redisx-cli -p 7301 CLUSTER NODES    # 세 노드와 각자의 슬롯 범위
```

## 클라이언트에서 써보기

**클러스터 인식 클라이언트**(`redis-cli -c`, `redis-py`의 `RedisCluster`)는
어느 노드에 붙어도 알아서 올바른 노드로 재접속한다:

```bash
redisx-cli -c -p 7301 SET foo bar
redisx-cli -c -p 7301 GET foo
```

```python
from redis.cluster import RedisCluster
rc = RedisCluster(host="127.0.0.1", port=7301)
rc.set("foo", "bar")
print(rc.get("foo"))
```

**클러스터를 모르는 평범한 클라이언트**로 틀린 노드에 직접 쓰면 `MOVED`를
그대로 받는다(리다이렉트를 자동으로 따라가지 않는다):

```bash
redisx-cli -p 7301 SET foo bar      # foo는 slot 12182 -> node3 소유라 OK 대신
                                     # "MOVED 12182 127.0.0.1:7303"를 받는다
```

서로 다른 슬롯의 키를 한 커맨드에 섞으면 `CROSSSLOT`:

```bash
redisx-cli -p 7301 MGET foo k5      # CROSSSLOT Keys in request don't hash to the same slot
```

같은 슬롯으로 묶고 싶으면 `{tag}` 해시태그를 쓴다 — `{user1}:profile`과
`{user1}:orders`는 태그(`user1`)만으로 해시되므로 항상 같은 슬롯/노드에
간다.

## 알아둘 것 (redisX 클러스터가 실제 Redis Cluster와 다른 지점)

- **자동 장애조치가 없다.** `CLUSTER FAILOVER`는 항상 에러. 마스터 하나가
  죽으면 그 슬롯은 `CLUSTERDOWN`(기본값 `cluster-require-full-coverage
  yes`이므로 전체 클러스터가 내려간 것으로 취급됨) — 복구하려면 살아있는
  다른 노드에서 그 슬롯 범위를 수동으로 `CLUSTER ADDSLOTSRANGE`(또는 복제본이
  있었다면 그 복제본에서 `CLUSTER RESET` 후 재할당)해야 한다.
- **재분산(resharding)이 없다.** `CLUSTER SETSLOT`은 `NODE` variant만
  지원 — 즉시, 강제로 소유자만 바꿀 뿐 그 슬롯에 이미 있는 키를 옮겨주지
  않는다(`IMPORTING`/`MIGRATING`/`ASK`/`MIGRATE` 없음). 키를 실제로 옮기려면
  `DUMP`/`RESTORE`를 양쪽에서 수동으로 해야 한다.
- **토폴로지가 재시작 시 사라진다.** `cluster-config-file`에 저장/복원하지
  않는다(`CLUSTER SAVECONFIG`는 수락되지만 no-op) — 노드를 재시작하면 슬롯
  없는 빈 마스터로 돌아온다. 재시작한 노드는 `CLUSTER MEET`/`ADDSLOTS`를
  다시 실행해야 한다.
- **가십은 cron 틱 기반**이라 전파에 지연이 있다 — `MEET`/`ADDSLOTSRANGE`
  직후 바로 `CLUSTER NODES`를 찍으면 아직 안 퍼져 있을 수 있다(`setup_cluster.sh`가
  각 단계 뒤에 넣은 `sleep`이 그래서 있다). `test_cluster.py`는 폴링으로
  기다린다.
- **복제본(슬레이브)은 이 샘플에 없다** — 마스터 3대뿐이다. 마스터에
  복제본을 붙이려면 빈 노드를 하나 더 띄워서 `CLUSTER REPLICATE
  <master-node-id>`(내부적으로 평범한 `REPLICAOF`를 그대로 재사용한다)를
  실행하면 된다 — 단, `replication/` 토폴로지 문서에 적은 "`REPLICAOF NO
  ONE`이 연결을 끊지 않는다"는 한계가 여기서도 동일하게 적용된다.

## 테스트

```bash
pip install redis   # 처음 한 번
./setup_cluster.sh
python3 test_cluster.py
./stop_cluster.sh
```

스크립트가 하는 일: `CLUSTER INFO`로 `cluster_state:ok`/`cluster_slots_
assigned:16384` 확인 → `redis.cluster.RedisCluster`로 여러 키를 쓰고 읽어서
정상적으로 알맞은 노드로 라우팅되는지 확인 → 평범한(클러스터 모르는)
클라이언트로 틀린 노드를 직접 찔러서 `MOVED` 에러를 실제로 받는지 확인 →
서로 다른 슬롯의 키를 묶은 `MGET`이 `CROSSSLOT` 에러를 내는지 확인 →
`{tag}` 해시태그로 묶은 키들이 항상 같은 슬롯으로 가는지 확인.
