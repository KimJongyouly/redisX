import redis.clients.jedis.Jedis
import redis.clients.jedis.args.ListDirection
import redis.clients.jedis.args.ListPosition
import redis.clients.jedis.params.LPosParams

/**
 * List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
 *
 * LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
 * 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
 * 옮기는 흐름 — example/python/list.py와 같은 시나리오.
 */

fun addr(): String = System.getenv("REDISX_ADDR") ?: "127.0.0.1:2417"

fun section(title: String) = println("\n--- $title ---")

fun main() {
    val (host, port) = addr().split(":")
    Jedis(host, port.toInt()).use { jedis ->
        val queue = "tasks:todo"
        val done = "tasks:done"
        jedis.del(queue, done)

        section("RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)")
        jedis.rpush(queue, "reboot-db", "send-email", "resize-image")
        println("LRANGE tasks:todo 0 -1 -> ${jedis.lrange(queue, 0, -1)}")

        section("LPUSH로 긴급 작업을 맨 앞에 끼워넣기")
        jedis.lpush(queue, "URGENT:rotate-logs")
        println("LRANGE tasks:todo 0 -1 -> ${jedis.lrange(queue, 0, -1)}")

        section("LINSERT로 특정 작업 바로 뒤에 끼워넣기")
        jedis.linsert(queue, ListPosition.AFTER, "send-email", "send-email-retry")
        println("LRANGE tasks:todo 0 -1 -> ${jedis.lrange(queue, 0, -1)}")

        section("LPOS로 작업 위치 찾기, LREM으로 제거")
        val pos = jedis.lpos(queue, "send-email-retry", LPosParams())
        println("send-email-retry의 위치 -> $pos")
        jedis.lrem(queue, 1, "send-email-retry") // 결국 재시도는 취소하기로 함
        println("LREM 이후 LRANGE -> ${jedis.lrange(queue, 0, -1)}")

        section("LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기")
        val task = jedis.lpop(queue)
        println("처리할 작업 -> $task")
        jedis.rpush(done, task)
        jedis.lmove(queue, done, ListDirection.LEFT, ListDirection.RIGHT) // 다음 작업도 바로 완료 큐로 옮겨본다
        println("tasks:todo -> ${jedis.lrange(queue, 0, -1)}")
        println("tasks:done -> ${jedis.lrange(done, 0, -1)}")

        section("LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)")
        jedis.rpush(done, "old-task-1", "old-task-2", "old-task-3")
        jedis.ltrim(done, -2, -1)
        println("LTRIM 이후 tasks:done -> ${jedis.lrange(done, 0, -1)}")

        val deleted = jedis.del(queue, done)
        println("\n정리: $deleted 개 키 삭제")
    }
}
