# architecture

[README](../README.md) · **Architecture** · [Scripts](../scripts/README.md)

상위 폴더인 `redisX`(`..`)를 세 가지 배치 토폴로지로 직접 띄워보고, 그
위에서 동작을 검증하는 설정 파일과 테스트 스크립트 모음이다. `redis_document/`가
"내부가 어떻게 포팅됐는가"를 다룬다면, 여기는 "실제로 몇 대를 띄워서 어떻게
동작하는가"를 다룬다 — 전부 실행 가능하다.

```
redisX/architecture/
├── single/            # 단일 인스턴스 — 복제도 클러스터도 없는 기본형
├── replication/
│   ├── 2node/         # 마스터 1대 + 슬레이브 1대
│   └── 3node/         # 마스터 1대 + 슬레이브 2대
└── cluster/
    └── 3node/         # 마스터 3대로 16384 슬롯을 나눈 Redis Cluster
```

각 폴더에는:
- `*.conf` — 그 노드용 `redisx.conf` (포트, `dir`, 토폴로지별 옵션이 이미 채워져 있다)
- `README.md` — 그 토폴로지에 한정된 상세 설명과 커맨드 샘플
- `test_*.py` — `redis-py`로 실제 동작을 확인하는 테스트 스크립트

## 공통 준비물

1. `redisX` 빌드 (워크스페이스 루트에서):
   ```bash
   cd ..
   cargo build --workspace --release
   ```
   또는 `./install.sh`로 `redisx-server`/`redisx-cli`를 `PATH`에 설치해둔다
   (아래 모든 커맨드 샘플은 `redisx-server`/`redisx-cli`가 `PATH`에 있다고
   가정한다 — 설치하지 않았다면 `cargo run -p redisx-server --bin
   redisx-server --`, `cargo run -p redisx-cli --bin redisx-cli --`로 바꿔
   쓰면 된다).
2. 테스트 스크립트는 `redis-py`가 필요하다: `pip install redis`.
3. **`dir`이 각 노드의 설정 파일과 같은 폴더를 가리키도록 전부 상대 경로
   (`dir .`)로 돼 있다** — redisX는 설정 파일 경로 기준이 아니라 **프로세스의
   현재 작업 디렉터리(cwd) 기준**으로 `dir`을 해석하므로, 서버를 띄울 때는
   반드시 그 노드의 폴더로 `cd`한 뒤 실행해야 한다(그렇지 않으면 여러 노드가
   같은 `dump.rdb`/`appendonly.aof`를 두고 충돌한다). 각 README의 실행
   커맨드가 이미 그렇게 돼 있다.
4. 여기 설정들은 전부 테스트/학습용 — `requirepass`, TLS 등은 꺼져 있다
   (TLS는 `single/README.md`에서 옵션으로만 다룬다).

## 토폴로지별 핵심 제약 (redisX가 실제 Redis와 다른 지점)

세 토폴로지 문서 모두에서 반복해서 등장하므로 여기 한 번에 정리한다:

- **복제는 설정 파일로 선언할 수 없다.** 실제 Redis는 `replicaof <host>
  <port>`를 `redis.conf`에 써두면 시작할 때 자동으로 붙지만, redisX는
  `REPLICAOF`를 **런타임 커맨드로만** 지원한다 — 설정 파일에 `replicaof`
  줄을 넣어도 조용히 무시된다. 그래서 `replication/*/README.md`는 전부
  "슬레이브를 띄운 뒤 `redisx-cli -p <port> REPLICAOF <host> <port>`를
  수동으로 실행"하는 순서다.
- **`masterauth`/`requirepass`로 보호된 복제가 없다.** `REPLICAOF`
  핸드셰이크에 인증 단계가 없으므로, 마스터에 `requirepass`가 걸려 있으면
  복제 자체가 실패한다 — 이 폴더의 샘플 설정은 그래서 전부 `requirepass`가
  비어 있다.
- **`REPLICAOF NO ONE`이 마스터 쪽 연결을 실제로 끊지 않는다.** 이 노드의
  `role`만 `Master`로 바뀔 뿐, 마스터로부터 스트림을 읽어 적용하는
  백그라운드 핸들러는 그대로 살아있다(직접 띄워서 확인한 동작, `src/bin/
  redisx-server.rs`의 `handle_replicaof`). 그래서 승격 후에도 마스터의
  `connected_slaves`는 줄지 않고, 승격된 노드는 `role:master`라고 답하면서도
  마스터의 새 쓰기를 계속 조용히 받아 적용한다. `CLIENT KILL`도 없어
  마스터 쪽에서 끊을 수도 없다 — 유일한 해법은 승격된 노드의 **프로세스
  재시작**. `replication/2node`와 `replication/3node`의 테스트 스크립트가
  이 현상을 직접 재현해서 보여준다.
- **슬레이브가 쓰기를 막지 않는다.** 실제 Redis는 `replica-read-only yes`
  (기본값)로 슬레이브에 대한 직접 쓰기를 거부하지만, redisX는 그 체크 자체가
  없다 — 슬레이브에 직접 `SET`을 날리면 그냥 받아준다(그리고 다음 번
  마스터→슬레이브 스트림이나 전체 재동기화가 그 값을 덮어쓸 수 있다). 복제
  테스트 스크립트에서 "슬레이브는 읽기 전용이어야 한다"를 기대하는 assert는
  넣지 않았다 — 대신 이 차이를 테스트 안에서 직접 확인한다.
- **클러스터에 자동 장애조치가 없다.** `CLUSTER FAILOVER`는 항상 에러를
  반환한다 — 마스터가 죽으면 살아있는 슬레이브에서 수동으로 `CLUSTER RESET`
  + `CLUSTER ADDSLOTS`/`SETSLOT`을 실행해 승격시켜야 한다
  (`cluster/3node/README.md`에 그 수동 절차가 있다).
- **클러스터 토폴로지는 재시작 시 사라진다.** `cluster-config-file`에
  저장/복원하지 않으므로, 노드를 재시작하면 슬롯 없는 빈 마스터로
  돌아온다 — `CLUSTER MEET`/`ADDSLOTS`를 다시 실행해야 한다.

각 항목의 근거(소스 라인)는 `../crates/redisx-server/src/cluster.rs`와
`../crates/redisx-server/src/bin/redisx-server.rs`의 모듈 문서에 있다.
