# replication — 복제 토폴로지

redisX의 복제(`REPLICAOF`)로 구성한 두 가지 규모:

- **`2node/`** — 마스터 1대 + 슬레이브 1대. 복제의 가장 단순한 형태, 핸드셰이크/전체
  동기화/스트리밍 확인용.
- **`3node/`** — 마스터 1대 + 슬레이브 2대(팬아웃). 한 마스터가 여러 슬레이브에
  동시에 쓰기를 전파하는지, `INFO replication`의 `connected_slaves`가 정확히
  올라가는지 확인용.

두 구성 모두 공통으로 적용되는 제약은 상위 `../README.md`의 "토폴로지별 핵심
제약" 절을 먼저 읽을 것 — 특히:

- `REPLICAOF`는 **런타임 커맨드로만** 동작한다(conf 파일의 `replicaof` 줄은
  무시됨) — 그래서 아래 모든 순서는 "마스터 먼저 띄우기 → 슬레이브 띄우기 →
  슬레이브에서 수동으로 `REPLICAOF` 실행"이다.
- 슬레이브도 직접 쓰기를 받아준다(`replica-read-only` 강제가 없음) — 실제
  Redis와 달리 슬레이브에 `SET`을 날려도 에러가 나지 않는다는 점을
  `test_replication_*.py`가 직접 확인한다.
- `WAIT`는 "복제가 몇 개나 붙어 있는가"(`connected_slaves` 그대로 반환)만 알려줄
  뿐, "그 쓰기가 실제로 복제됐는가"는 보장하지 않는다 — 테스트에서는 전파를
  확인할 때 `WAIT` 대신 슬레이브 쪽에서 직접 값을 폴링한다.

## 공통 확인 커맨드

아무 복제 구성에서나 역할/연결 상태는 `INFO replication` 또는 `ROLE`로 본다:

```bash
redisx-cli -p <master-port> INFO replication
# role:master
# connected_slaves:N
# master_repl_offset:...

redisx-cli -p <slave-port> INFO replication
# role:slave
# master_host:127.0.0.1
# master_port:<master-port>
# master_link_status:up
```

승격(마스터 승격)은 항상 수동이다:

```bash
redisx-cli -p <slave-port> REPLICAOF NO ONE   # 이제부터 독립 마스터
```

각 하위 폴더(`2node/`, `3node/`)의 `README.md`에 포트/실행 순서/테스트
실행법이 있다.
