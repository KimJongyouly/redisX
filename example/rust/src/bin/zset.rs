//! ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
//!
//! ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
//! 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
//! 합산하는 것까지 — example/python, example/golang과 같은 시나리오.
//!
//! 실행: `cargo run --bin zset`

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

    let (board, weekly, combined) = ("leaderboard", "leaderboard:weekly", "leaderboard:combined");
    let _: () = con.del(&[board, weekly, combined])?;

    section("ZADD로 점수 매기기");
    let _: i64 = con.zadd_multiple(
        board,
        &[(120, "alice"), (98, "bob"), (150, "carol"), (110, "dave")],
    )?;
    let ranking: Vec<(String, f64)> = con.zrevrange_withscores(board, 0, -1)?;
    println!("전체 랭킹 (ZREVRANGE, 점수 포함) -> {ranking:?}");

    section("ZSCORE / ZREVRANK로 한 명만 조회");
    let score: f64 = con.zscore(board, "carol")?;
    println!("carol 점수 (ZSCORE) -> {score}");
    let rank: i64 = con.zrevrank(board, "carol")?;
    println!("carol 등수, 0부터 (ZREVRANK) -> {rank}");

    section("ZINCRBY로 점수 올리기 (보너스 획득)");
    let _: f64 = con.zincr(board, "bob", 30)?;
    let ranking: Vec<(String, f64)> = con.zrevrange_withscores(board, 0, -1)?;
    println!("bob 보너스 이후 전체 랭킹 -> {ranking:?}");

    section("ZRANGEBYSCORE로 특정 점수 구간만 뽑기");
    let in_range: Vec<(String, f64)> = con.zrangebyscore_withscores(board, 100, 120)?;
    println!("100점 이상 120점 이하 -> {in_range:?}");

    section("ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거");
    let top: Vec<(String, f64)> = con.zpopmax(board, 1)?;
    println!("ZPOPMAX -> {top:?}");
    let bottom: Vec<(String, f64)> = con.zpopmin(board, 1)?;
    println!("ZPOPMIN -> {bottom:?}");
    let ranking: Vec<(String, f64)> = con.zrevrange_withscores(board, 0, -1)?;
    println!("남은 랭킹 -> {ranking:?}");

    section("ZUNIONSTORE로 이번 주 보너스 점수와 합산");
    // 방금 뽑아서 지워진 것 복원
    let _: i64 = con.zadd_multiple(board, &[(120, "alice"), (128, "bob"), (110, "dave")])?;
    let _: i64 = con.zadd_multiple(weekly, &[(20, "alice"), (5, "bob"), (40, "eve")])?;
    let _: i64 = con.zunionstore(combined, &[board, weekly])?;
    let ranking: Vec<(String, f64)> = con.zrevrange_withscores(combined, 0, -1)?;
    println!("합산 랭킹 (ZUNIONSTORE) -> {ranking:?}");

    let deleted: i64 = con.del(&[board, weekly, combined])?;
    println!("\n정리: {deleted} 개 키 삭제");
    Ok(())
}
