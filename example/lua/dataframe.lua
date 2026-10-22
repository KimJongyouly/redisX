-- DataFrame(DF.*) 커맨드 둘러보기 -- 자기 완결적인 버전.
--
-- 다른 예제들과 달리 미리 로드된 키를 전제하지 않고, 작은 재고 CSV를
-- 스크립트 안에 직접 담아 그 자리에서 DF.FROMCSV로 적재한다 -- 서버가
-- `--features polars`로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
-- 있다. 다른 언어의 같은 이름(dataframe) 예제들과 똑같은 시나리오(작은
-- 재고 CSV)를 쓴다 -- example/python/dataframe_csv.py,
-- example/golang/dataframe/main.go 참고.
--
-- DF.*는 그냥 RESP 커맨드라 redis-lua에 전용 지원이 없어도 문제없다 --
-- redisx_demo.lua에서 define_command로 등록해 둔 것을 그대로 쓴다.
--
-- 실행: lua dataframe.lua
local demo = require "redisx_demo"
local client = demo.connect()
for _, name in ipairs({ "df_fromcsv", "df_shape", "df_columns", "df_query", "df_pretty", "df_tocsv" }) do
	-- `string.gsub` returns two values (string, count) -- parenthesize to
	-- truncate to just the replaced name, or `count` ends up passed as
	-- `define_command`'s `opts` argument and crashes when it tries to index it.
	client:define_command((name:gsub("_", ".")))
end

local key = "inventory"
client:del(key)

local csv_text = "name,category,price,qty\n"
	.. "widget,tools,9.99,100\n"
	.. "gadget,electronics,29.99,50\n"
	.. "gizmo,electronics,19.99,75\n"
	.. "bolt,tools,0.99,500\n"
	.. "screw,tools,0.49,1000\n"

demo.section("DF.FROMCSV로 인라인 CSV 적재")
local ok, err = pcall(function()
	client["df.fromcsv"](client, key, csv_text)
end)
if not ok then
	if tostring(err):lower():find("unknown command") then
		print("DF.FROMCSV를 모르는 커맨드라고 함 -- 서버가 '--features polars' 없이 빌드됐을 가능성이 높음.")
		print("cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.")
		os.exit(0)
	end
	error(err)
end

demo.section("DF.SHAPE / DF.COLUMNS로 구조 확인")
print("DF.SHAPE inventory ->", table.concat(client["df.shape"](client, key), ", "))
print("DF.COLUMNS inventory ->", table.concat(client["df.columns"](client, key), ", "))

demo.section("DF.QUERY로 카테고리별 재고 가치 집계")
local rows = client["df.query"](client, key, "SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category")
for _, row in ipairs(rows) do
	-- RESP2에는 전용 Double 타입이 없어서 숫자 응답도 문자열로 온다.
	print(string.format("  %-12s total_value=%s", row[1], row[2]))
end

demo.section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)")
print(client["df.pretty"](client, key))

demo.section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교")
local exported = client["df.tocsv"](client, key)
io.write(exported)
print("원본과 동일? ->", exported == csv_text)

print("\n정리:", client:del(key), "개 키 삭제")
