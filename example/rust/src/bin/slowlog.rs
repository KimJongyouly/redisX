//! SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.
//!
//! 실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
//! 포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
//! 남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 CHANGE.md 1절 참고).
//! 그래서 redisX에서 SLOWLOG를 쓰려면 slowlog-file-path를 실제 경로로
//! 설정해야 하고(기본값은 ""로 비활성화), SLOWLOG GET/LEN은 매번 그 파일을
//! 읽어서 답한다 — 메모리에 아무것도 들고 있지 않다.
//!
//! `redis` crate에는 CONFIG GET/SET, SLOWLOG용 타입 헬퍼가 없어서 전부
//! `redis::cmd`로 직접 구성한다 — SLOWLOG GET의 각 항목은 (id, timestamp,
//! duration_us, argv, client-addr, client-name) 6개짜리 배열이라, 그대로
//! 튜플로 역직렬화한다.
//!
//! 실행: `cargo run --bin slowlog`

use redis::Commands;
use std::collections::HashMap;
use std::env;

fn redis_url() -> String {
    let addr = env::var("REDISX_ADDR").unwrap_or_else(|_| "127.0.0.1:2417".to_string());
    format!("redis://{addr}/")
}

fn section(title: &str) {
    println!("\n--- {title} ---");
}

type SlowlogEntry = (i64, i64, i64, Vec<String>, String, String);

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open(redis_url())?;
    let mut con = client.get_connection()?;

    section("현재 설정 확인 (기본값은 비활성화 상태)");
    let path_cfg: HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("slowlog-file-path")
        .query(&mut con)?;
    println!("slowlog-file-path -> {path_cfg:?}");
    let threshold_cfg: HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("slowlog-log-slower-than")
        .query(&mut con)?;
    println!("slowlog-log-slower-than (마이크로초) -> {threshold_cfg:?}");

    section("CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기");
    let log_path = env::temp_dir().join("redisx-slowlog-demo-rust.log");
    let _ = std::fs::remove_file(&log_path);
    let log_path_str = log_path.to_string_lossy().to_string();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("slowlog-file-path")
        .arg(&log_path_str)
        .query(&mut con)?;
    // 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("slowlog-log-slower-than")
        .arg("0")
        .query(&mut con)?;

    section("몇 가지 커맨드를 실행해서 로그에 쌓기");
    let _: () = con.set("k", "v")?;
    let _: String = con.get("k")?;
    let _: i64 = con.lpush("mylist", &["a", "b", "c"])?;
    let _: i64 = con.del(&["mylist", "k"])?;

    section("SLOWLOG LEN / GET으로 읽기");
    let len: i64 = redis::cmd("SLOWLOG").arg("LEN").query(&mut con)?;
    println!("SLOWLOG LEN -> {len}");
    let entries: Vec<SlowlogEntry> = redis::cmd("SLOWLOG").arg("GET").arg(5).query(&mut con)?;
    for (id, _ts, duration_us, argv, peer, _client_name) in entries {
        println!("  #{id}  {duration_us}us  {peer}  {}", argv.join(" "));
    }

    section("실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)");
    let content = std::fs::read_to_string(&log_path).unwrap_or_default();
    print!("{content}");

    section("SLOWLOG RESET으로 파일 비우기");
    let _: () = redis::cmd("SLOWLOG").arg("RESET").query(&mut con)?;
    // 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
    // 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
    let len: i64 = redis::cmd("SLOWLOG").arg("LEN").query(&mut con)?;
    println!("RESET 이후 SLOWLOG LEN -> {len}");

    section("정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기");
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("slowlog-log-slower-than")
        .arg("10000")
        .query(&mut con)?; // redisX 기본값
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("slowlog-file-path")
        .arg("")
        .query(&mut con)?; // 기본값(비활성화)으로 복원
    let _ = std::fs::remove_file(&log_path);
    println!("완료");
    Ok(())
}
