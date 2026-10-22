//! Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
//!
//! 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
//! Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
//! 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
//! example/python, example/golang과 같은 시나리오.
//!
//! `redis` crate는 HRANDFIELD에 대한 타입 지정 헬퍼가 없어서, 그 부분만
//! `redis::cmd`로 직접 커맨드를 구성한다.
//!
//! 실행: `cargo run --bin hash`

use redis::Commands;
use std::collections::HashMap;

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

    let user_key = "user:42";
    let _: () = con.del(user_key)?;

    section("HSET으로 여러 필드를 한 번에 채우기");
    // `hset_multiple()`은 레거시 `HMSET`을 보내는데, redisX는 그 별칭을
    // 포팅하지 않고 다중 필드를 받는 `HSET` 하나로만 지원한다 — 그래서
    // 직접 커맨드를 구성한다.
    let _: i64 = redis::cmd("HSET")
        .arg(user_key)
        .arg(&[
            ("name", "jiyeon"),
            ("level", "5"),
            ("coins", "100"),
            ("logins", "0"),
        ])
        .query(&mut con)?;
    let fields: HashMap<String, String> = con.hgetall(user_key)?;
    println!("HGETALL user:42 -> {fields:?}");

    section("HGET / HMGET으로 필드 일부만 읽기");
    let name: String = con.hget(user_key, "name")?;
    println!("HGET user:42 name -> {name}");
    let pair: Vec<String> = con.hmget(user_key, &["name", "level"])?;
    println!("HMGET user:42 name level -> {pair:?}");

    section("HINCRBY로 코인/로그인 횟수를 원자적으로 증가");
    let _: i64 = con.hincr(user_key, "coins", 50)?;
    let _: i64 = con.hincr(user_key, "logins", 1)?;
    let fields: HashMap<String, String> = con.hgetall(user_key)?;
    println!("HGETALL user:42 -> {fields:?}");

    section("HEXISTS / HDEL");
    let exists: bool = con.hexists(user_key, "level")?;
    println!("HEXISTS user:42 level -> {exists}");
    let _: i64 = con.hdel(user_key, "logins")?;
    let fields: HashMap<String, String> = con.hgetall(user_key)?;
    println!("HDEL 이후 HGETALL -> {fields:?}");

    section("HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기");
    let raffle = "raffle:entries";
    let _: () = con.del(raffle)?;
    let _: i64 = redis::cmd("HSET")
        .arg(raffle)
        .arg(&[("alice", "1"), ("bob", "1"), ("carol", "1"), ("dave", "1")])
        .query(&mut con)?;
    let winner: String = redis::cmd("HRANDFIELD").arg(raffle).query(&mut con)?;
    println!("당첨자 한 명 -> {winner}");
    let picks: Vec<String> = redis::cmd("HRANDFIELD")
        .arg(raffle)
        .arg(-3)
        .query(&mut con)?;
    println!("HRANDFIELD -3 (중복 허용 3명 뽑기) -> {picks:?}");

    let deleted: i64 = con.del(&[user_key, raffle])?;
    println!("\n정리: {deleted} 개 키 삭제");
    Ok(())
}
