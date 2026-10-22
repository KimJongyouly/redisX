<?php

/**
 * String(키/값) 커맨드 둘러보기.
 *
 * 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
 * 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
 * 만료(TTL) 관련 커맨드까지 — Redis를 처음 접할 때 가장 먼저 보는 타입이다.
 *
 * 실행:
 *     php string.php
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

// 데모가 매번 같은 상태에서 시작하도록 사용하는 키를 미리 지운다.
$demoKeys = ['greeting', 'page:views', 'user:1:session', 'k1', 'k2', 'k3'];
$r->del($demoKeys);

section('SET / GET');
$r->set('greeting', 'hello world');
echo 'GET greeting -> ' . $r->get('greeting') . "\n";

section('APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)');
$r->append('greeting', '!');
echo 'APPEND 이후: ' . $r->get('greeting') . "\n";
echo 'GETRANGE greeting 0 4 -> ' . $r->getRange('greeting', 0, 4) . "\n";
$r->setRange('greeting', 6, 'redisX'); // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
echo 'SETRANGE 이후: ' . $r->get('greeting') . "\n";

section('MSET / MGET (여러 키 한 번에)');
$r->mset(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3']);
echo 'MGET k1 k2 k3 -> ' . implode(', ', $r->mget(['k1', 'k2', 'k3'])) . "\n";

section('INCR / INCRBY (카운터)');
$r->del('page:views');
for ($i = 0; $i < 3; $i++) {
    $r->incr('page:views');
}
$r->incrBy('page:views', 10);
echo 'page:views -> ' . $r->get('page:views') . "\n";

section('EXPIRE / TTL / PERSIST');
$r->set('user:1:session', 'token-abc');
$r->expire('user:1:session', 2);
echo '설정 직후 TTL -> ' . $r->ttl('user:1:session') . "초\n";
usleep(2_200_000);
echo '2.2초 후 EXISTS -> ' . ($r->exists('user:1:session') ? 'true' : 'false') . " (TTL이 지나 자동 삭제됨)\n";

$r->set('user:1:session', 'token-def', ['ex' => 60]);
$r->persist('user:1:session');
echo 'PERSIST 이후 TTL -> ' . $r->ttl('user:1:session') . " (-1 = 만료 없음)\n";

echo "\n정리: " . $r->del($demoKeys) . "개 키 삭제\n";
