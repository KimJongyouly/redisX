"""single 토폴로지 검증 — 기본 동작 + SLOWLOG on/off 토글 + AOF on/off 토글.

사전 준비:
    cd redisX/architecture/single
    redisx-server ./redisx-single.conf &   # 또는 다른 터미널에서 포그라운드 실행

실행:
    python3 test_single.py
"""

import tempfile
from pathlib import Path

import redis

PORT = 2417
r = redis.Redis(host="127.0.0.1", port=PORT)


def section(title):
    print(f"\n--- {title} ---")


def expect(label, actual, expected):
    ok = actual == expected
    mark = "OK" if ok else "FAIL"
    print(f"[{mark}] {label}: {actual!r} (expected {expected!r})")
    if not ok:
        raise SystemExit(f"{label} 검증 실패")


# 1. 기본 연결 확인 -----------------------------------------------------
section("기본 연결: PING / SET / GET")
expect("PING", r.ping(), True)
r.set("single:hello", "world")
expect("GET single:hello", r.get("single:hello"), b"world")

# 2. SLOWLOG: 기본 꺼짐 확인 ---------------------------------------------
section("SLOWLOG 기본 상태 확인")
path_cfg = r.config_get("slowlog-file-path")
expect("slowlog-file-path (기본값)", path_cfg.get("slowlog-file-path"), "")
expect("SLOWLOG LEN (기본값, 비활성)", r.execute_command("SLOWLOG", "LEN"), 0)

# 3. SLOWLOG 켜기 --------------------------------------------------------
section("SLOWLOG 켜기: CONFIG SET + 임계값 0 (전부 기록)")
log_path = Path(tempfile.gettempdir()) / "redisx-single-slowlog-test.log"
log_path.unlink(missing_ok=True)
r.config_set("slowlog-file-path", str(log_path))
r.config_set("slowlog-log-slower-than", "0")

r.set("single:k", "v")
r.get("single:k")
r.lpush("single:list", "a", "b", "c")

length = r.execute_command("SLOWLOG", "LEN")
print("SLOWLOG LEN ->", length)
if length < 3:
    raise SystemExit("SLOWLOG가 켜진 뒤 기록이 전혀 없다 — 켜는 절차 확인 필요")
print("[OK] SLOWLOG가 켜진 뒤 커맨드가 실제로 기록됨")

entries = r.execute_command("SLOWLOG", "GET", "5")
for entry_id, timestamp, duration_us, argv, peer, _client_name in entries[:3]:
    cmd = b" ".join(argv).decode("utf-8", "replace")
    print(f"  #{entry_id}  {duration_us}us  {peer.decode()}  {cmd}")

# 4. SLOWLOG 다시 끄기 ----------------------------------------------------
section("SLOWLOG 끄기: 경로를 다시 빈 문자열로")
r.execute_command("SLOWLOG", "RESET")
r.config_set("slowlog-file-path", "")
r.config_set("slowlog-log-slower-than", "10000")  # 운영 기본값으로 복귀
expect("SLOWLOG LEN (끈 뒤)", r.execute_command("SLOWLOG", "LEN"), 0)

# 5. AOF on/off 토글 ------------------------------------------------------
section("AOF on/off 토글")


def aof_enabled():
    info = r.info("persistence")
    return info.get("aof_enabled")


expect("appendonly 기본값", aof_enabled(), 0)
r.config_set("appendonly", "yes")
expect("appendonly 켠 뒤", aof_enabled(), 1)
r.config_set("appendonly", "no")
expect("appendonly 끈 뒤", aof_enabled(), 0)

section("정리")
r.delete("single:hello", "single:k", "single:list")
print("모든 검증 통과")
