<?php

/**
 * List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
 *
 * LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
 * 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
 * 옮기는 흐름 — 작업 큐에서 흔히 쓰는 패턴을 그대로 밟아본다.
 *
 * 실행:
 *     php list.php
 */

function section(string $title): void
{
    echo "\n--- $title ---\n";
}

function redisxAddr(): array
{
    $addr = getenv('REDISX_ADDR') ?: '127.0.0.1:2417';
    [$host, $port] = explode(':', $addr, 2);
    return [$host, (int) $port];
}

[$host, $port] = redisxAddr();
$r = new Redis();
$r->connect($host, $port);

$queue = 'tasks:todo';
$done = 'tasks:done';
$r->del([$queue, $done]);

section('RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)');
$r->rPush($queue, 'reboot-db', 'send-email', 'resize-image');
echo 'LRANGE tasks:todo 0 -1 -> ' . implode(', ', $r->lRange($queue, 0, -1)) . "\n";

section('LPUSH로 긴급 작업을 맨 앞에 끼워넣기');
$r->lPush($queue, 'URGENT:rotate-logs');
echo 'LRANGE tasks:todo 0 -1 -> ' . implode(', ', $r->lRange($queue, 0, -1)) . "\n";

section('LINSERT로 특정 작업 바로 뒤에 끼워넣기');
$r->lInsert($queue, Redis::AFTER, 'send-email', 'send-email-retry');
echo 'LRANGE tasks:todo 0 -1 -> ' . implode(', ', $r->lRange($queue, 0, -1)) . "\n";

section('LPOS로 작업 위치 찾기, LREM으로 제거');
$pos = $r->lPos($queue, 'send-email-retry');
echo 'send-email-retry의 위치 -> ' . $pos . "\n";
$r->lRem($queue, 'send-email-retry', 1); // 결국 재시도는 취소하기로 함
echo 'LREM 이후 LRANGE -> ' . implode(', ', $r->lRange($queue, 0, -1)) . "\n";

section('LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기');
$task = $r->lPop($queue);
echo '처리할 작업 -> ' . $task . "\n";
$r->rPush($done, $task);
$r->lMove($queue, $done, Redis::LEFT, Redis::RIGHT); // 다음 작업도 바로 완료 큐로 옮겨본다
echo 'tasks:todo -> ' . implode(', ', $r->lRange($queue, 0, -1)) . "\n";
echo 'tasks:done -> ' . implode(', ', $r->lRange($done, 0, -1)) . "\n";

section('LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)');
$r->rPush($done, 'old-task-1', 'old-task-2', 'old-task-3');
$r->lTrim($done, -2, -1);
echo 'LTRIM 이후 tasks:done -> ' . implode(', ', $r->lRange($done, 0, -1)) . "\n";

echo "\n정리: " . $r->del([$queue, $done]) . "개 키 삭제\n";
