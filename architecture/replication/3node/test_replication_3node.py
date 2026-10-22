"""replication/3node 검증 — 마스터 1대 + 슬레이브 2대 팬아웃.

사전 준비 (README 참고):
    터미널 1: cd redisX/architecture/replication/3node/master && redisx-server ./redisx-master.conf
    터미널 2: cd redisX/architecture/replication/3node/slave1 && redisx-server ./redisx-slave1.conf
    터미널 3: cd redisX/architecture/replication/3node/slave2 && redisx-server ./redisx-slave2.conf

실행:
    python3 test_replication_3node.py

이 스크립트가 REPLICAOF를 직접 실행하므로 세 서버만 띄워두면 된다.

주의 — 재실행하려면 세 서버를 모두 재시작할 것: 이 스크립트는 일부러
redisX의 알려진 한계(`REPLICAOF NO ONE`이 마스터 쪽 연결을 끊지 않음)를
드러내며 끝나고, 그래서 slave1을 깨끗한 상태로 되돌리지 못한다. 같은
서버들에 대고 또 실행하면 유령 연결이 쌓여 `connected_slaves` 체크가
어긋난다 — 재실행 전에 master/slave1/slave2 프로세스를 전부 재시작할 것.
시작하자마자 그 상태를 감지해서 분명한 에러로 안내한다.
"""

import time

import redis

MASTER_PORT = 9001
SLAVE1_PORT = 9002
SLAVE2_PORT = 9003

master = redis.Redis(host="127.0.0.1", port=MASTER_PORT)
slave1 = redis.Redis(host="127.0.0.1", port=SLAVE1_PORT)
slave2 = redis.Redis(host="127.0.0.1", port=SLAVE2_PORT)


def section(title):
    print(f"\n--- {title} ---")


def expect(label, actual, expected):
    ok = actual == expected
    mark = "OK" if ok else "FAIL"
    print(f"[{mark}] {label}: {actual!r} (expected {expected!r})")
    if not ok:
        raise SystemExit(f"{label} 검증 실패")


def role_of(conn):
    return conn.info("replication").get("role")


def wait_link_up(conn, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = conn.info("replication")
        if info.get("master_link_status") == "up":
            return info
        time.sleep(0.2)
    raise SystemExit("master_link_status가 timeout 안에 up이 되지 않음")


def wait_value(conn, key, expected, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if conn.get(key) == expected:
            return
        time.sleep(0.1)
    raise SystemExit(f"{key}가 timeout 안에 전파되지 않음")


section("시작 전 상태 확인: 유령 연결 없는 깨끗한 상태여야 함")
expect("master role", role_of(master), "master")
expect("slave1 role (연결 전)", role_of(slave1), "master")
expect("slave2 role (연결 전)", role_of(slave2), "master")
master_connected = int(master.info("replication").get("connected_slaves"))
if master_connected != 0:
    raise SystemExit(
        f"master connected_slaves가 0이 아니라 {master_connected} — 이전 실행이 남긴 유령 연결로 보인다. "
        "master/slave1/slave2 프로세스를 전부 재시작한 뒤 다시 실행할 것 (이 파일 상단 주석 참고)."
    )

section("두 슬레이브 모두 마스터에 REPLICAOF")
for conn, port in ((slave1, SLAVE1_PORT), (slave2, SLAVE2_PORT)):
    reply = conn.execute_command("REPLICAOF", "127.0.0.1", str(MASTER_PORT))
    print(f"slave on {port}: REPLICAOF ->", reply)

wait_link_up(slave1)
wait_link_up(slave2)
expect("slave1 role", role_of(slave1), "slave")
expect("slave2 role", role_of(slave2), "slave")
print("[OK] 두 슬레이브 모두 master_link_status: up")

section("팬아웃 전파 확인")
master.set("repl3:greeting", "hello-everyone")
wait_value(slave1, "repl3:greeting", b"hello-everyone")
wait_value(slave2, "repl3:greeting", b"hello-everyone")
print("[OK] 양쪽 슬레이브 모두 전파받음")

master_info = master.info("replication")
expect("master connected_slaves", int(master_info.get("connected_slaves")), 2)

section("슬레이브 1만 독립 마스터로 승격 — 슬레이브 2는 영향 없어야 함")
slave1.execute_command("REPLICAOF", "NO", "ONE")
time.sleep(0.3)
expect("slave1 role (승격 후)", role_of(slave1), "master")
expect("slave2 role (영향 없음)", role_of(slave2), "slave")

master.set("repl3:after-promotion", "still-replicating")
wait_value(slave2, "repl3:after-promotion", b"still-replicating")
print("[OK] 슬레이브 2는 승격과 무관하게 계속 복제됨")

section("알려진 한계: REPLICAOF NO ONE은 마스터 쪽 연결을 끊지 않는다")
# `handle_replicaof`의 "NO ONE" 분기는 이 노드의 `role` 필드만 Master로 바꿀 뿐,
# 마스터로부터 스트림을 읽어 적용하는 백그라운드 이벤트 루프 핸들러
# (`service_master_link`)는 그대로 등록된 채로 남는다 — 그 핸들러는 소켓이
# 끊기기 전까지는 `role`을 보지 않고 계속 커맨드를 적용한다. 그래서:
#   1) 마스터의 connected_slaves는 승격 후에도 줄어들지 않고,
#   2) 승격된 노드는 role:master라고 답하면서도 마스터의 새 쓰기를
#      "유령 적용(ghost-apply)"으로 계속 조용히 반영한다.
# CLIENT KILL이 없어 마스터 쪽에서 그 연결을 강제로 끊을 방법도 없다 — 이
# 링크를 확실히 끊는 유일한 방법은 승격된 노드의 **프로세스를 재시작**하는
# 것뿐이다(상위 README의 "알아둘 것" 참고). 여기서는 그 간극을 실패로 처리하지
# 않고 있는 그대로 보여준다.
master_info = master.info("replication")
still_two = int(master_info.get("connected_slaves")) == 2
print(f"master connected_slaves (승격 후) -> {master_info.get('connected_slaves')} "
      f"({'예상대로 여전히 2 — 알려진 한계' if still_two else '2가 아님 — 동작이 바뀌었을 수 있음, README 갱신 필요'})")

master.set("repl3:ghost-probe", "written-after-promotion")
time.sleep(0.3)
ghost_value = slave1.get("repl3:ghost-probe")
if ghost_value == b"written-after-promotion":
    print("[관찰됨] 승격된 slave1이 role:master이면서도 마스터의 새 쓰기를 여전히 유령 적용함 (알려진 한계)")
else:
    print("[관찰됨] slave1이 더 이상 마스터의 쓰기를 받지 않음 — 이 fork의 동작이 바뀌었다면 README도 갱신할 것")

section("정리")
# 주의: 위에서 드러난 한계 때문에 slave1은 이 스크립트만으로는 깨끗하게
# "원상 복구"할 수 없다 — 다시 처음부터 실행하려면 slave1 프로세스 자체를
# 재시작해야 한다 (README 참고).
master.delete("repl3:greeting", "repl3:after-promotion", "repl3:ghost-probe")
print("모든 검증 통과 (알려진 한계 1건은 README에 문서화됨)")
