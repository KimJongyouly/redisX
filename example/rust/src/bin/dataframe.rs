//! DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
//!
//! 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
//! 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
//! 있다. 여기 쓰는 `redis` 클라이언트 크레이트는 redisX가 서버 내부에서 쓰는
//! `polars` 크레이트와는 아무 관계 없는 별개 라이브러리이므로(이름이
//! 비슷해 보이지만 하나는 클라이언트, 하나는 서버의 DataFrame 엔진이다),
//! DF.QUERY의 RESP 행 배열은 그냥 `redis::Value`로 직접 다루고 DF.PRETTY로
//! 보기 좋은 표를 받는다 — 다른 언어의 같은 이름(dataframe) 예제들과 동일한
//! 시나리오.
//!
//! 실행: `cargo run --bin dataframe`

use redis::Value;

fn redis_url() -> String {
    let addr = std::env::var("REDISX_ADDR").unwrap_or_else(|_| "127.0.0.1:2417".to_string());
    format!("redis://{addr}/")
}

fn section(title: &str) {
    println!("\n--- {title} ---");
}

const CSV_TEXT: &str = "name,category,price,qty\n\
widget,tools,9.99,100\n\
gadget,electronics,29.99,50\n\
gizmo,electronics,19.99,75\n\
bolt,tools,0.99,500\n\
screw,tools,0.49,1000\n";

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open(redis_url())?;
    let mut con = client.get_connection()?;

    let key = "inventory";
    let _: () = redis::cmd("DEL").arg(key).query(&mut con)?;

    section("DF.FROMCSV로 인라인 CSV 적재");
    if let Err(e) = redis::cmd("DF.FROMCSV")
        .arg(key)
        .arg(CSV_TEXT)
        .query::<()>(&mut con)
    {
        eprintln!("DF.FROMCSV 실패: {e}");
        eprintln!(
            "unknown command라면 서버가 --features polars 없이 빌드된 것 — \
             cargo build --release -p redisx-server --features polars 로 다시 빌드할 것."
        );
        std::process::exit(1);
    }

    section("DF.SHAPE / DF.COLUMNS로 구조 확인");
    let shape: Vec<i64> = redis::cmd("DF.SHAPE").arg(key).query(&mut con)?;
    println!("DF.SHAPE inventory -> {shape:?}");
    let cols: Vec<String> = redis::cmd("DF.COLUMNS").arg(key).query(&mut con)?;
    println!("DF.COLUMNS inventory -> {cols:?}");

    section("DF.QUERY로 카테고리별 재고 가치 집계");
    // RESP2에는 전용 Double 타입이 없어서 숫자 셀도 string으로 온다(이
    // 포크의 Reply::Double 인코딩 규칙) — Value::Bulk 그대로 문자열로 꺼낸다.
    let rows: Value = redis::cmd("DF.QUERY")
        .arg(key)
        .arg("SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category")
        .query(&mut con)?;
    if let Value::Array(rows) = rows {
        for row in rows {
            if let Value::Array(cells) = row {
                let category = cell_to_string(&cells[0]);
                let total_value = cell_to_string(&cells[1]);
                println!("  {category:<12} total_value={total_value}");
            }
        }
    }

    section("DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)");
    let pretty: String = redis::cmd("DF.PRETTY").arg(key).query(&mut con)?;
    println!("{pretty}");

    section("DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교");
    let exported: String = redis::cmd("DF.TOCSV").arg(key).query(&mut con)?;
    print!("{exported}");
    println!("원본과 동일? -> {}", exported == CSV_TEXT);

    let deleted: i64 = redis::cmd("DEL").arg(key).query(&mut con)?;
    println!("\n정리: {deleted} 개 키 삭제");

    Ok(())
}

fn cell_to_string(v: &Value) -> String {
    match v {
        Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        Value::SimpleString(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Double(d) => d.to_string(),
        other => format!("{other:?}"),
    }
}
