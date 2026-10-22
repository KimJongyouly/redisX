//! List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
//!
//! LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
//! 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
//! 옮기는 흐름 — example/python, example/golang과 같은 시나리오.
//!
//! 실행: `cargo run --bin list`

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

    let (queue, done) = ("tasks:todo", "tasks:done");
    let _: () = con.del(&[queue, done])?;

    section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)");
    let _: i64 = con.rpush(queue, &["reboot-db", "send-email", "resize-image"])?;
    let v: Vec<String> = con.lrange(queue, 0, -1)?;
    println!("LRANGE tasks:todo 0 -1 -> {v:?}");

    section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기");
    let _: i64 = con.lpush(queue, "URGENT:rotate-logs")?;
    let v: Vec<String> = con.lrange(queue, 0, -1)?;
    println!("LRANGE tasks:todo 0 -1 -> {v:?}");

    section("LINSERT로 특정 작업 바로 뒤에 끼워넣기");
    let _: i64 = con.linsert_after(queue, "send-email", "send-email-retry")?;
    let v: Vec<String> = con.lrange(queue, 0, -1)?;
    println!("LRANGE tasks:todo 0 -1 -> {v:?}");

    section("LPOS로 작업 위치 찾기, LREM으로 제거");
    let pos: Option<i64> = con.lpos(queue, "send-email-retry", redis::LposOptions::default())?;
    println!("send-email-retry의 위치 -> {pos:?}");
    let _: i64 = con.lrem(queue, 1, "send-email-retry")?; // 결국 재시도는 취소하기로 함
    let v: Vec<String> = con.lrange(queue, 0, -1)?;
    println!("LREM 이후 LRANGE -> {v:?}");

    section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기");
    let task: String = con.lpop(queue, None)?;
    println!("처리할 작업 -> {task}");
    let _: i64 = con.rpush(done, &task)?;
    let _: String = con.lmove(queue, done, redis::Direction::Left, redis::Direction::Right)?; // 다음 작업도 바로 완료 큐로 옮겨본다
    let v: Vec<String> = con.lrange(queue, 0, -1)?;
    println!("tasks:todo -> {v:?}");
    let v: Vec<String> = con.lrange(done, 0, -1)?;
    println!("tasks:done -> {v:?}");

    section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)");
    let _: i64 = con.rpush(done, &["old-task-1", "old-task-2", "old-task-3"])?;
    let _: () = con.ltrim(done, -2, -1)?;
    let v: Vec<String> = con.lrange(done, 0, -1)?;
    println!("LTRIM 이후 tasks:done -> {v:?}");

    let deleted: i64 = con.del(&[queue, done])?;
    println!("\n정리: {deleted} 개 키 삭제");
    Ok(())
}
