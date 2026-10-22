//! Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
//!
//! 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
//! 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
//! 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
//! example/python, example/golang과 같은 시나리오.
//!
//! 실행: `cargo run --bin set`

use redis::Commands;
use std::collections::HashSet;

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

    let (post_a, post_b, archive) = ("post:1:tags", "post:2:tags", "tags:archive");
    let _: () = con.del(&[post_a, post_b, archive])?;

    section("SADD로 글마다 태그 달기");
    let _: i64 = con.sadd(post_a, &["rust", "redis", "database"])?;
    let _: i64 = con.sadd(post_b, &["rust", "web", "async"])?;
    let v: HashSet<String> = con.smembers(post_a)?;
    println!("post:1:tags -> {v:?}");
    let v: HashSet<String> = con.smembers(post_b)?;
    println!("post:2:tags -> {v:?}");

    section("SINTER / SUNION / SDIFF로 집합 연산");
    let v: HashSet<String> = con.sinter(&[post_a, post_b])?;
    println!("둘 다 쓴 태그 (SINTER) -> {v:?}");
    let v: HashSet<String> = con.sunion(&[post_a, post_b])?;
    println!("둘 중 하나라도 쓴 태그 (SUNION) -> {v:?}");
    let v: HashSet<String> = con.sdiff(&[post_a, post_b])?;
    println!("post:1에만 있는 태그 (SDIFF) -> {v:?}");

    section("SINTERSTORE로 교집합을 새 키에 저장");
    let common_key = "tags:common";
    let _: () = con.del(common_key)?;
    let _: i64 = con.sinterstore(common_key, &[post_a, post_b])?;
    let v: HashSet<String> = con.smembers(common_key)?;
    println!("tags:common -> {v:?}");

    section("SISMEMBER / SCARD");
    let is_member: bool = con.sismember(post_a, "redis")?;
    println!("post:1에 'redis' 태그가 있나? -> {is_member}");
    let card: i64 = con.scard(post_a)?;
    println!("post:1 태그 개수 (SCARD) -> {card}");

    section("SMOVE로 태그 하나를 보관함으로 옮기기");
    let _: bool = con.smove(post_a, archive, "database")?;
    let v: HashSet<String> = con.smembers(post_a)?;
    println!("post:1:tags -> {v:?}");
    let v: HashSet<String> = con.smembers(archive)?;
    println!("tags:archive -> {v:?}");

    section("SPOP으로 무작위로 하나 뽑아서 제거");
    let popped: String = con.spop(post_b)?;
    let remaining: HashSet<String> = con.smembers(post_b)?;
    println!("뽑힌 태그 -> {popped} / 남은 post:2:tags -> {remaining:?}");

    let deleted: i64 = con.del(&[post_a, post_b, archive, common_key])?;
    println!("\n정리: {deleted} 개 키 삭제");
    Ok(())
}
