"""cluster/3node 검증 — MEET/ADDSLOTSRANGE로 구성한 3마스터 클러스터.

사전 준비:
    cd redisX/architecture/cluster/3node
    ./setup_cluster.sh

실행:
    python3 test_cluster.py

끝낼 때:
    ./stop_cluster.sh
"""

import time

import redis
from redis.cluster import RedisCluster
from redis.crc import key_slot

NODE_PORTS = {"node1": 7301, "node2": 7302, "node3": 7303}
SLOT_RANGES = {"node1": (0, 5460), "node2": (5461, 10922), "node3": (10923, 16383)}


def section(title):
    print(f"\n--- {title} ---")


def expect(label, actual, expected):
    ok = actual == expected
    mark = "OK" if ok else "FAIL"
    print(f"[{mark}] {label}: {actual!r} (expected {expected!r})")
    if not ok:
        raise SystemExit(f"{label} 검증 실패")


def owner_of(slot):
    for node, (lo, hi) in SLOT_RANGES.items():
        if lo <= slot <= hi:
            return node
    raise AssertionError(f"slot {slot} not covered")


# 1. 클러스터 상태 확인 ---------------------------------------------------
section("CLUSTER INFO: 세 노드 모두 cluster_state:ok, 슬롯 16384개 전부 할당")


def wait_cluster_ok(port, timeout=15):
    deadline = time.monotonic() + timeout
    conn = redis.Redis(host="127.0.0.1", port=port)
    last = None
    while time.monotonic() < deadline:
        info_text = conn.execute_command("CLUSTER", "INFO")
        if isinstance(info_text, bytes):
            info_text = info_text.decode()
        last = info_text
        fields = dict(line.split(":", 1) for line in info_text.strip().splitlines() if ":" in line)
        if fields.get("cluster_state") == "ok" and fields.get("cluster_slots_assigned") == "16384":
            return fields
        time.sleep(0.3)
    raise SystemExit(f"포트 {port}: timeout 안에 cluster_state:ok가 되지 않음. 마지막 CLUSTER INFO:\n{last}")


for node, port in NODE_PORTS.items():
    fields = wait_cluster_ok(port)
    expect(f"{node} cluster_state", fields.get("cluster_state"), "ok")
    expect(f"{node} cluster_slots_assigned", fields.get("cluster_slots_assigned"), "16384")
    expect(f"{node} cluster_known_nodes", fields.get("cluster_known_nodes"), "3")

# 2. 클러스터 인식 클라이언트로 쓰기/읽기 ----------------------------------
section("RedisCluster 클라이언트: 여러 키를 쓰고 읽기 (알맞은 노드로 자동 라우팅)")
rc = RedisCluster(host="127.0.0.1", port=NODE_PORTS["node1"])
test_keys = {f"cluster:k{i}": str(i) for i in range(30)}
for k, v in test_keys.items():
    rc.set(k, v)
for k, v in test_keys.items():
    expect(f"GET {k}", rc.get(k), v.encode())
print(f"[OK] {len(test_keys)}개 키가 여러 슬롯에 걸쳐 정상적으로 쓰고 읽힘")

# 각 키가 실제로 그 슬롯을 소유한 노드에 저장됐는지 직접 확인
section("각 키가 실제로 올바른 노드(슬롯 소유자)에 저장됐는지 직접 확인")
sample_key, sample_val = next(iter(test_keys.items()))
slot = key_slot(sample_key.encode())
owner_node = owner_of(slot)
owner_port = NODE_PORTS[owner_node]
direct = redis.Redis(host="127.0.0.1", port=owner_port)
expect(f"{sample_key} (slot {slot}) -> {owner_node}에 직접 GET", direct.get(sample_key), sample_val.encode())

# 3. 클러스터를 모르는 평범한 클라이언트: MOVED -----------------------------
section("평범한 클라이언트로 틀린 노드를 직접 찌르면 MOVED 에러")
wrong_port = next(p for n, p in NODE_PORTS.items() if n != owner_node)
plain = redis.Redis(host="127.0.0.1", port=wrong_port)
try:
    plain.get(sample_key)
    raise SystemExit("MOVED 에러가 발생해야 하는데 조용히 성공함")
except redis.exceptions.MovedError as e:
    # redis-py는 MOVED를 일반 ResponseError 대신 전용 MovedError로 파싱해서
    # slot_id/host/port를 구조화된 속성으로 내준다 (메시지 자체에는 "MOVED"
    # 접두어가 남지 않는다 — AskError.__init__ 참고).
    print(f"받은 에러 -> slot_id={e.slot_id} host={e.host} port={e.port}")
    expect("MovedError.slot_id", e.slot_id, slot)
    expect("MovedError.port (슬롯 소유자 포트)", e.port, owner_port)
    print(f"[OK] MOVED {slot} 127.0.0.1:{owner_port} (슬롯 소유자로 정확히 안내)")

# 4. CROSSSLOT -------------------------------------------------------------
section("서로 다른 슬롯의 키를 한 커맨드에 섞으면 CROSSSLOT 에러")
key_a, key_b = "foo", "bar"
slot_a, slot_b = key_slot(key_a.encode()), key_slot(key_b.encode())
if slot_a == slot_b:
    raise SystemExit("테스트에 고른 두 키가 우연히 같은 슬롯이다 — 스크립트의 키를 바꿀 것")
any_port = next(iter(NODE_PORTS.values()))
probe = redis.Redis(host="127.0.0.1", port=any_port)
try:
    probe.mget(key_a, key_b)
    raise SystemExit("CROSSSLOT 에러가 발생해야 하는데 조용히 성공함")
except redis.exceptions.ClusterCrossSlotError as e:
    print("받은 에러 ->", e)
    print(f"[OK] {key_a}(slot {slot_a})와 {key_b}(slot {slot_b})를 묶으면 CROSSSLOT")

# 5. 해시태그로 같은 슬롯에 묶기 ---------------------------------------------
section("{tag} 해시태그로 묶은 키들은 항상 같은 슬롯")
tagged_a, tagged_b = "{user1}:profile", "{user1}:orders"
slot_tagged_a, slot_tagged_b = key_slot(tagged_a.encode()), key_slot(tagged_b.encode())
expect("같은 태그 키들의 슬롯이 동일함", slot_tagged_a, slot_tagged_b)
# 같은 슬롯이므로 MGET이 CROSSSLOT 없이 바로 성공해야 함 (RedisCluster로 먼저 값 채우기)
rc.set(tagged_a, "alice")
rc.set(tagged_b, "order-1")
tagged_owner_port = NODE_PORTS[owner_of(slot_tagged_a)]
tagged_probe = redis.Redis(host="127.0.0.1", port=tagged_owner_port)
result = tagged_probe.mget(tagged_a, tagged_b)
expect("해시태그로 묶은 MGET 결과", result, [b"alice", b"order-1"])

section("정리")
for k in list(test_keys) + [tagged_a, tagged_b]:
    rc.delete(k)
print("모든 검증 통과")
