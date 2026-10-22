"""DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.

`dataframe.py`/`redis_df.py`는 이미 DF.FROMPARQUET 등으로 불러온
실데이터 키를 전제로 하지만, 이 스크립트는 작은 CSV를 코드 안에 직접 담아
그 자리에서 DF.FROMCSV로 적재한다 — 서버가 `--features polars`로 빌드됐기만
하면 사전 준비 없이 바로 돌려볼 수 있다. 다른 언어의 같은 이름(dataframe)
예제들과 똑같은 시나리오(작은 재고 CSV)를 쓴다.

실행:
    python dataframe_csv.py
"""

import redis

r = redis.Redis(host="127.0.0.1", port=2417)


def section(title):
    print(f"\n--- {title} ---")


key = "inventory"
r.delete(key)

csv_text = (
    "name,category,price,qty\n"
    "widget,tools,9.99,100\n"
    "gadget,electronics,29.99,50\n"
    "gizmo,electronics,19.99,75\n"
    "bolt,tools,0.99,500\n"
    "screw,tools,0.49,1000\n"
)

section("DF.FROMCSV로 인라인 CSV 적재")
try:
    r.execute_command("DF.FROMCSV", key, csv_text)
except redis.exceptions.ResponseError as e:
    if "unknown command" in str(e).lower():
        raise SystemExit(
            "DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.\n"
            "cargo build --release -p redisx-server --features polars 로 다시 빌드할 것."
        ) from e
    raise

section("DF.SHAPE / DF.COLUMNS로 구조 확인")
print("DF.SHAPE inventory ->", r.execute_command("DF.SHAPE", key))
print("DF.COLUMNS inventory ->", [c.decode() for c in r.execute_command("DF.COLUMNS", key)])

section("DF.QUERY로 카테고리별 재고 가치 집계")
rows = r.execute_command(
    "DF.QUERY", key, "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category"
)
for category, total_value in rows:
    # RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string(bytes)으로
    # 온다 — redisx-py의 decode_responses=False 기본값에서는 항상 그렇다.
    print(f"  {category.decode():<12} total_value={total_value.decode()}")

section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)")
# redis-py는 DF.PRETTY를 평범한 bulk string으로 받으므로 decode만 하면 된다 —
# redisx-cli가 이 커맨드만 특별 취급해서 raw로 찍어주는 것과 달리, 어떤
# 클라이언트로 받든 항상 하나의 문자열 응답일 뿐이다.
pretty = r.execute_command("DF.PRETTY", key)
print(pretty.decode())

section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교")
exported = r.execute_command("DF.TOCSV", key).decode()
print(exported, end="")
print("원본과 동일? ->", exported == csv_text)

print("\n정리:", r.delete(key), "개 키 삭제")
