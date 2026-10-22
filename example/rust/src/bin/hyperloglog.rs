//! HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
//!
//! 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
//! HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
//! 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
//! 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
//! example/python, example/golang과 같은 시나리오.
//!
//! `redis` crate에 MEMORY USAGE 타입 헬퍼가 없어서 그 부분만 `redis::cmd`로
//! 직접 구성한다.
//!
//! 실행: `cargo run --bin hyperloglog`

use rand::Rng;
use redis::Commands;

fn redis_url() -> String {
    let addr = std::env::var("REDISX_ADDR").unwrap_or_else(|_| "127.0.0.1:2417".to_string());
    format!("redis://{addr}/")
}

fn section(title: &str) {
    println!("\n--- {title} ---");
}

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open(redis_url())?;
    let mut con = client.get_connection()?;

    let (hll_mon, hll_tue, hll_week) =
        ("visitors:mon:hll", "visitors:tue:hll", "visitors:week:hll");
    let exact_set = "visitors:mon:exact";
    let _: () = con.del(&[hll_mon, hll_tue, hll_week, exact_set])?;

    section("PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기");
    let mut rng = rand::thread_rng();
    // 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
    // 3,000명 정도의 유니크 방문자만 나오게 만든다.
    for _ in 0..10_000 {
        let visitor_id = format!("user-{}", rng.gen_range(1..=3000));
        let _: i64 = con.pfadd(hll_mon, &visitor_id)?;
        let _: i64 = con.sadd(exact_set, &visitor_id)?;
    }

    let exact_count: i64 = con.scard(exact_set)?;
    let approx_count: i64 = con.pfcount(hll_mon)?;
    let error_pct = (approx_count - exact_count).unsigned_abs() as f64 / exact_count as f64 * 100.0;
    println!("정확한 유니크 방문자 수 (SCARD)   -> {exact_count}");
    println!("HyperLogLog 근사치 (PFCOUNT)      -> {approx_count}");
    println!("오차율                             -> {error_pct:.2}% (표준 오차는 약 0.81%)");

    section("메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)");
    let set_bytes: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg(exact_set)
        .query(&mut con)?;
    let hll_bytes: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg(hll_mon)
        .query(&mut con)?;
    println!("Set 메모리 사용량 근사치 (MEMORY USAGE) -> {set_bytes} bytes");
    println!("HLL 메모리 사용량 근사치 (MEMORY USAGE) -> {hll_bytes} bytes");

    section("PFMERGE로 이틀치 방문자 로그 합치기");
    for _ in 0..5_000 {
        let visitor_id = format!("user-{}", rng.gen_range(2000..6000)); // 월요일과 일부 겹침
        let _: i64 = con.pfadd(hll_tue, &visitor_id)?;
    }
    let _: () = con.pfmerge(hll_week, &[hll_mon, hll_tue])?;
    let mon_count: i64 = con.pfcount(hll_mon)?;
    let tue_count: i64 = con.pfcount(hll_tue)?;
    let week_count: i64 = con.pfcount(hll_week)?;
    println!("월요일 근사 방문자 수 -> {mon_count}");
    println!("화요일 근사 방문자 수 -> {tue_count}");
    println!(
        "합산(PFMERGE) 근사 방문자 수 -> {week_count} (단순 합보다 적은 건 겹치는 방문자가 있어서)"
    );

    let deleted: i64 = con.del(&[hll_mon, hll_tue, hll_week, exact_set])?;
    println!("\n정리: {deleted} 개 키 삭제");
    Ok(())
}
