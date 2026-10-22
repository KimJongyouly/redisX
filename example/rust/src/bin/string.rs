//! String(키/값) 커맨드 둘러보기.
//!
//! 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
//! 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
//! 만료(TTL) 관련 커맨드까지 — example/python, example/golang의 같은
//! 시나리오를 Rust(`redis` crate)로 옮긴 것이다.
//!
//! 실행: `cargo run --bin string`

use redis::Commands;
use std::thread::sleep;
use std::time::Duration;

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

    let demo_keys = ["greeting", "page:views", "user:1:session", "k1", "k2", "k3"];
    let _: () = con.del(&demo_keys[..])?;

    section("SET / GET");
    let _: () = con.set("greeting", "hello world")?;
    let v: String = con.get("greeting")?;
    println!("GET greeting -> {v}");

    section("APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)");
    let _: i64 = con.append("greeting", "!")?;
    let v: String = con.get("greeting")?;
    println!("APPEND 이후: {v}");
    let v: String = con.getrange("greeting", 0, 4)?;
    println!("GETRANGE greeting 0 4 -> {v}");
    let _: i64 = con.setrange("greeting", 6, "redisX")?; // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
    let v: String = con.get("greeting")?;
    println!("SETRANGE 이후: {v}");

    section("MSET / MGET (여러 키 한 번에)");
    let _: () = con.mset(&[("k1", "v1"), ("k2", "v2"), ("k3", "v3")])?;
    let vs: Vec<String> = con.mget(&["k1", "k2", "k3"])?;
    println!("MGET k1 k2 k3 -> {vs:?}");

    section("INCR / INCRBY (카운터)");
    let _: () = con.del("page:views")?;
    for _ in 0..3 {
        let _: i64 = con.incr("page:views", 1)?;
    }
    let _: i64 = con.incr("page:views", 10)?;
    let v: i64 = con.get("page:views")?;
    println!("page:views -> {v}");

    section("EXPIRE / TTL / PERSIST");
    let _: () = con.set("user:1:session", "token-abc")?;
    let _: bool = con.expire("user:1:session", 2)?;
    let ttl: i64 = con.ttl("user:1:session")?;
    println!("설정 직후 TTL -> {ttl}초");
    sleep(Duration::from_millis(2200));
    let exists: bool = con.exists("user:1:session")?;
    println!("2.2초 후 EXISTS -> {exists} (TTL이 지나 자동 삭제됨)");

    // redisX implements `SET key value EX seconds`, but not the separate
    // legacy `SETEX` command the `redis` crate's `set_ex()` helper issues —
    // so build it as a plain `SET ... EX ...` instead.
    let _: () = redis::cmd("SET")
        .arg("user:1:session")
        .arg("token-def")
        .arg("EX")
        .arg(60)
        .query(&mut con)?;
    let _: bool = con.persist("user:1:session")?;
    let ttl: i64 = con.ttl("user:1:session")?;
    println!("PERSIST 이후 TTL -> {ttl} (-1 = 만료 없음)");

    let deleted: i64 = con.del(&demo_keys[..])?;
    println!("\n정리: {deleted} 개 키 삭제");
    Ok(())
}
