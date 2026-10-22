"""SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.

실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 `CHANGE.md` 1절 참고).
그 결과 redisX에서 SLOWLOG를 쓰려면:

1. `slowlog-file-path`를 빈 문자열이 아닌 실제 경로로 설정해야 하고
   (기본값은 ""로 비활성화 상태),
2. `SLOWLOG GET`/`LEN`은 매번 그 파일을 읽어서 답한다 — 메모리에 아무것도
   들고 있지 않다.

이 스크립트는 그 두 가지를 직접 켜보고 확인한다.

실행:
    python 07_slowlog.py
"""

import tempfile
from pathlib import Path

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


section("현재 설정 확인 (기본값은 비활성화 상태)")
print("slowlog-file-path ->", r.config_get("slowlog-file-path"))
print("slowlog-log-slower-than (마이크로초) ->", r.config_get("slowlog-log-slower-than"))

section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기")
log_path = Path(tempfile.gettempdir()) / "redisx-slowlog-demo.log"
log_path.unlink(missing_ok=True)
r.config_set("slowlog-file-path", str(log_path))
r.config_set("slowlog-log-slower-than", "0")  # 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

section("몇 가지 커맨드를 실행해서 로그에 쌓기")
r.set("k", "v")
r.get("k")
r.lpush("mylist", "a", "b", "c")
r.delete("mylist", "k")

section("SLOWLOG LEN / GET으로 읽기")
print("SLOWLOG LEN ->", r.execute_command("SLOWLOG", "LEN"))
entries = r.execute_command("SLOWLOG", "GET", "5")
for entry_id, timestamp, duration_us, argv, peer, _client_name in entries:
    cmd = b" ".join(argv).decode("utf-8", "replace")
    print(f"  #{entry_id}  {duration_us}us  {peer.decode()}  {cmd}")

section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)")
print(log_path.read_text(encoding="utf-8", errors="replace").rstrip())

section("SLOWLOG RESET으로 파일 비우기")
r.execute_command("SLOWLOG", "RESET")
# 임계값이 여전히 0이라 RESET/LEN 자신도 "느린 커맨드"로 기록된다 — 그래서
# 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
print("RESET 이후 SLOWLOG LEN ->", r.execute_command("SLOWLOG", "LEN"))

section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기")
r.config_set("slowlog-log-slower-than", "10000")  # redisX 기본값
r.config_set("slowlog-file-path", "")  # 기본값(비활성화)으로 복원
log_path.unlink(missing_ok=True)
print("완료")
