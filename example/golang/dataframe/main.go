// DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
//
// 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
// 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
// 있다. Go에는 polars 같은 네이티브 DataFrame 라이브러리가 없으므로, 여기서는
// DF.QUERY의 RESP 행 배열을 직접 다루고 DF.PRETTY로 보기 좋은 표를 받는다 —
// 다른 언어의 같은 이름(dataframe) 예제들과 동일한 시나리오.
//
// 실행:
//
//	go run ./dataframe
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func section(title string) {
	fmt.Printf("\n--- %s ---\n", title)
}

func addr() string {
	if a := os.Getenv("REDISX_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:2417"
}

const csvText = "name,category,price,qty\n" +
	"widget,tools,9.99,100\n" +
	"gadget,electronics,29.99,50\n" +
	"gizmo,electronics,19.99,75\n" +
	"bolt,tools,0.99,500\n" +
	"screw,tools,0.49,1000\n"

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()

	key := "inventory"
	rdb.Del(ctx, key)

	section("DF.FROMCSV로 인라인 CSV 적재")
	if err := rdb.Do(ctx, "DF.FROMCSV", key, csvText).Err(); err != nil {
		fmt.Fprintln(os.Stderr, "DF.FROMCSV 실패:", err)
		fmt.Fprintln(os.Stderr, "unknown command라면 서버가 --features polars 없이 빌드된 것 — "+
			"cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.")
		os.Exit(1)
	}

	section("DF.SHAPE / DF.COLUMNS로 구조 확인")
	shape, _ := rdb.Do(ctx, "DF.SHAPE", key).Result()
	fmt.Println("DF.SHAPE inventory ->", shape)
	cols, _ := rdb.Do(ctx, "DF.COLUMNS", key).Result()
	fmt.Println("DF.COLUMNS inventory ->", cols)

	section("DF.QUERY로 카테고리별 재고 가치 집계")
	rowsReply, err := rdb.Do(ctx, "DF.QUERY", key,
		"SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category").Result()
	if err != nil {
		panic(err)
	}
	// go-redis는 범용 RESP 배열을 []interface{}로 돌려준다 — 각 행도
	// 마찬가지로 []interface{}이고, 숫자 셀은 RESP2에 전용 Double 타입이
	// 없어서 string으로 온다(이 포크의 Reply::Double 인코딩 규칙).
	rows := rowsReply.([]interface{})
	for _, row := range rows {
		cells := row.([]interface{})
		fmt.Printf("  %-12v total_value=%v\n", cells[0], cells[1])
	}

	section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)")
	pretty, _ := rdb.Do(ctx, "DF.PRETTY", key).Result()
	fmt.Println(pretty)

	section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교")
	exported, _ := rdb.Do(ctx, "DF.TOCSV", key).Result()
	exportedStr := exported.(string)
	fmt.Print(exportedStr)
	fmt.Println("원본과 동일? ->", exportedStr == csvText)

	deleted, _ := rdb.Del(ctx, key).Result()
	fmt.Println("\n정리:", deleted, "개 키 삭제")
}
