"""replication/2node 검증 — REPLICAOF 핸드셰이크, 동기화, 전파, 승격.

사전 준비 (README 참고):
    터미널 1: cd redisX/architecture/replication/2node/master && redisx-server ./redisx-master.conf
    터미널 2: cd redisX/architecture/replication/2node/slave  && redisx-server ./redisx-slave.conf

실행:
    python3 test_replication_2node.py

이 스크립트가 REPLICAOF 자체를 실행하므로, 서버 두 대만 띄워두면 된다
(README의 3번 단계를 미리 수동으로 할 필요는 없다).

주의 — 재실행하려면 서버를 재시작할 것: redisX의 `REPLICAOF NO ONE`은
마스터 쪽 복제 연결을 실제로 끊지 않는다(README의 "알아둘 것" 참고). 이
스크립트는 끝에서 `REPLICAOF NO ONE`으로 슬레이브를 승격시키는데, 그 뒤에도
예전 연결이 마스터에 남아 조용히 커맨드를 계속 적용받는다. 그래서 같은
서버 두 대에 대고 이 스크립트를 또 실행하면 `connected_slaves`가 쌓여서
값이 맞지 않는다 — 재실행 전에 반드시 마스터/슬레이브 프로세스를 둘 다
재시작할 것. 이 스크립트도 시작하자마자 그 상태를 감지해서 분명한 에러로
안내한다.
"""

import time

import redis

MASTER_PORT = 7001
SLAVE_PORT = 7002

master = redis.Redis(host="127.0.0.1", port=MASTER_PORT)
slave = redis.Redis(host="127.0.0.1", port=SLAVE_PORT)


def section(title):
    print(f"\n--- {title} ---")


def expect(label, actual, expected):
    ok = actual == expected
    mark = "OK" if ok else "FAIL"
    print(f"[{mark}] {label}: {actual!r} (expected {expected!r})")
    if not ok:
        raise SystemExit(f"{label} 검증 실패")


def role_of(conn):
    info = conn.info("replication")
    return info.get("role")


def wait_link_up(conn, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = conn.info("replication")
        if info.get("master_link_status") == "up":
            return info
        time.sleep(0.2)
    raise SystemExit("master_link_status가 timeout 안에 up이 되지 않음")


section("시작 전 상태: 아직 둘 다 독립 마스터, 유령 연결 없음")
expect("master role (연결 전)", role_of(master), "master")
expect("slave role (연결 전)", role_of(slave), "master")
master_connected = int(master.info("replication").get("connected_slaves"))
if master_connected != 0:
    raise SystemExit(
        f"master connected_slaves가 0이 아니라 {master_connected} — 이전 실행이 남긴 유령 연결로 보인다. "
        "마스터/슬레이브 프로세스를 둘 다 재시작한 뒤 다시 실행할 것 (이 파일 상단 주석 참고)."
    )

section("REPLICAOF 127.0.0.1 7001 실행")
reply = slave.execute_command("REPLICAOF", "127.0.0.1", str(MASTER_PORT))
print("REPLICAOF 응답 ->", reply)
if reply not in (True, b"OK", "OK"):
    raise SystemExit(f"REPLICAOF가 OK를 반환하지 않음: {reply!r}")

info = wait_link_up(slave)
expect("slave role (연결 후)", info.get("role"), "slave")
expect("master_host", info.get("master_host"), "127.0.0.1")
expect("master_port", int(info.get("master_port")), MASTER_PORT)
print("[OK] master_link_status: up")

section("전파 확인: 마스터에 쓰고 슬레이브에서 읽기")
master.set("repl2:greeting", "hello-from-master")


def wait_value(conn, key, expected, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if conn.get(key) == expected:
            return
        time.sleep(0.1)
    raise SystemExit(f"{key}가 timeout 안에 슬레이브로 전파되지 않음")


wait_value(slave, "repl2:greeting", b"hello-from-master")
print("[OK] 슬레이브가 마스터의 SET을 전달받음")

master_info = master.info("replication")
expect("master connected_slaves", int(master_info.get("connected_slaves")), 1)

section("redisX의 알려진 차이: 슬레이브도 직접 쓰기를 거부하지 않는다")
# 실제 Redis의 기본값(replica-read-only yes)과 달리, redisX는 이 체크가 없다.
# 여기서 에러 없이 성공하는 것 자체가 기대하는 동작이다 (상위 README 참고).
slave.set("repl2:direct-write-on-slave", "should-not-error")
expect(
    "슬레이브 직접 쓰기 (redisX는 거부하지 않음)",
    slave.get("repl2:direct-write-on-slave"),
    b"should-not-error",
)

section("원상 복구: REPLICAOF NO ONE")
slave.execute_command("REPLICAOF", "NO", "ONE")
time.sleep(0.3)
expect("slave role (복구 후)", role_of(slave), "master")

section("정리")
master.delete("repl2:greeting")
slave.delete("repl2:direct-write-on-slave")
print("모든 검증 통과")
