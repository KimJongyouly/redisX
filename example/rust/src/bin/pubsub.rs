//! Pub/Sub(발행/구독) 둘러보기.
//!
//! SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
//! 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
//! 지우고 시작할 "키"가 없다. `redis` crate는 전용 `Connection`을
//! `as_pubsub()`으로 감싸서 구독 모드로 바꾸므로, 발행자는 별도 연결을
//! 하나 더 연다.
//!
//! **이 crate만의 특징**: `subscribe()`/`psubscribe()`가 서버의 구독 확인
//! 응답을 내부적으로 알아서 소비한다(redis-py/go-redis처럼 그 확인 메시지를
//! 직접 받아서 출력할 필요가 없다) — `cache_messages_until_received_response`가
//! "message"/"pmessage"가 아닌 응답(바로 그 확인 응답)을 받을 때까지 읽다가
//! 반환하기 때문.
//!
//! 실행: `cargo run --bin pubsub`

use redis::Commands;

const CHANNEL: &str = "notifications";
const PATTERN: &str = "notif.*";
const PATTERN_CHANNEL: &str = "notif.sports";

fn redis_url() -> String {
    let addr = std::env::var("REDISX_ADDR").unwrap_or_else(|_| "127.0.0.1:2417".to_string());
    format!("redis://{addr}/")
}

fn section(title: &str) {
    println!("\n--- {title} ---");
}

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open(redis_url())?;

    section("SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)");
    let mut sub_conn = client.get_connection()?;
    let mut sub = sub_conn.as_pubsub();
    sub.subscribe(CHANNEL)?;
    sub.psubscribe(PATTERN)?;
    println!("구독 완료 (확인 응답은 subscribe()/psubscribe() 안에서 이미 소비됨)");

    section("PUBLISH (발행자는 별도 연결)");
    let mut pub_conn = client.get_connection()?;
    let n: i64 = pub_conn.publish(CHANNEL, "서버 점검이 10분 후 시작됩니다")?;
    println!("PUBLISH {CHANNEL} -> 받은 구독자 수: {n}");
    let msg = sub.get_message()?;
    let payload: String = msg.get_payload()?;
    println!("구독자가 받은 메시지 -> {} : {payload}", msg.get_channel_name());

    section("패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)");
    let n: i64 = pub_conn.publish(PATTERN_CHANNEL, "손흥민 결승골!")?;
    println!("PUBLISH {PATTERN_CHANNEL} -> 받은 구독자 수: {n}");
    let msg = sub.get_message()?;
    let payload: String = msg.get_payload()?;
    let pattern: String = msg.get_pattern()?;
    println!("패턴으로 받은 메시지 -> pattern={pattern} channel={} : {payload}", msg.get_channel_name());

    // `redis` crate 1.x엔 PUBSUB CHANNELS/NUMSUB/NUMPAT에 대한 타입드
    // 메서드가 없다(DF.*/SLOWLOG와 같은 사정 — 다른 언어 예제 README 참고) —
    // 그냥 raw 커맨드로 보낸다.
    section("PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)");
    let channels: Vec<String> = redis::cmd("PUBSUB").arg("CHANNELS").arg("*").query(&mut pub_conn)?;
    println!("PUBSUB CHANNELS -> {channels:?}");
    let numsub: Vec<redis::Value> = redis::cmd("PUBSUB").arg("NUMSUB").arg(CHANNEL).query(&mut pub_conn)?;
    println!("PUBSUB NUMSUB -> {numsub:?}");
    let numpat: i64 = redis::cmd("PUBSUB").arg("NUMPAT").query(&mut pub_conn)?;
    println!("PUBSUB NUMPAT -> {numpat}");

    section("UNSUBSCRIBE / PUNSUBSCRIBE");
    sub.unsubscribe(CHANNEL)?;
    sub.punsubscribe(PATTERN)?;
    std::thread::sleep(std::time::Duration::from_millis(200)); // 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
    let n: i64 = pub_conn.publish(CHANNEL, "이제 아무도 안 듣는다")?;
    println!("구독 해제 후 PUBLISH -> 받은 구독자 수: {n} (0이어야 정상)");

    println!("\n정리 완료");
    Ok(())
}
