<?php

/**
 * Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
 *
 * 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
 * Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
 * 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다.
 *
 * 실행:
 *     php hash.php
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

function formatMap(array $m): string
{
    $parts = [];
    foreach ($m as $k => $v) {
        $parts[] = "$k: $v";
    }
    return '{' . implode(', ', $parts) . '}';
}

[$host, $port] = redisxAddr();
$r = new Redis();
$r->connect($host, $port);

$userKey = 'user:42';
$r->del($userKey);

section('HSET으로 여러 필드를 한 번에 채우기');
// phpredis의 hMSet()은 레거시 HMSET 커맨드를 그대로 전송하는데, redisX는
// (실제 최신 Redis처럼 HSET 자체가 여러 필드-값 쌍을 받을 수 있게 된 뒤로)
// HMSET을 아예 등록하지 않았다 — hMSet()을 쓰면 "unknown command 'HMSET'"
// 에러가 난다. rawCommand로 HSET에 여러 쌍을 직접 넘긴다.
$r->rawCommand('HSET', $userKey, 'name', 'jiyeon', 'level', '5', 'coins', '100', 'logins', '0');
echo 'HGETALL user:42 -> ' . formatMap($r->hGetAll($userKey)) . "\n";

section('HGET / HMGET으로 필드 일부만 읽기');
echo 'HGET user:42 name -> ' . $r->hGet($userKey, 'name') . "\n";
echo 'HMGET user:42 name level -> ' . implode(', ', $r->hMGet($userKey, ['name', 'level'])) . "\n";

section('HINCRBY로 코인/로그인 횟수를 원자적으로 증가');
$r->hIncrBy($userKey, 'coins', 50);
$r->hIncrBy($userKey, 'logins', 1);
echo 'HGETALL user:42 -> ' . formatMap($r->hGetAll($userKey)) . "\n";

section('HEXISTS / HDEL');
echo 'HEXISTS user:42 level -> ' . ($r->hExists($userKey, 'level') ? 'true' : 'false') . "\n";
$r->hDel($userKey, 'logins');
echo 'HDEL 이후 HGETALL -> ' . formatMap($r->hGetAll($userKey)) . "\n";

section('HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기');
$raffle = 'raffle:entries';
$r->del($raffle);
$r->rawCommand('HSET', $raffle, 'alice', '1', 'bob', '1', 'carol', '1', 'dave', '1');
$winner = $r->hRandField($raffle);
echo '당첨자 한 명 -> ' . $winner . "\n";
echo 'HRANDFIELD -3 (중복 허용 3명 뽑기) -> ' . implode(', ', $r->hRandField($raffle, ['count' => -3])) . "\n";

echo "\n정리: " . $r->del([$userKey, $raffle]) . "개 키 삭제\n";
