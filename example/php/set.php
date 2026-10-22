<?php

/**
 * Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
 *
 * 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
 * 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
 * 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다.
 *
 * 실행:
 *     php set.php
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

function formatSet(array $s): string
{
    return '{' . implode(', ', $s) . '}';
}

[$host, $port] = redisxAddr();
$r = new Redis();
$r->connect($host, $port);

$postA = 'post:1:tags';
$postB = 'post:2:tags';
$archive = 'tags:archive';
$r->del([$postA, $postB, $archive]);

section('SADD로 글마다 태그 달기');
$r->sAdd($postA, 'rust', 'redis', 'database');
$r->sAdd($postB, 'rust', 'web', 'async');
echo 'post:1:tags -> ' . formatSet($r->sMembers($postA)) . "\n";
echo 'post:2:tags -> ' . formatSet($r->sMembers($postB)) . "\n";

section('SINTER / SUNION / SDIFF로 집합 연산');
echo '둘 다 쓴 태그 (SINTER) -> ' . formatSet($r->sInter([$postA, $postB])) . "\n";
echo '둘 중 하나라도 쓴 태그 (SUNION) -> ' . formatSet($r->sUnion([$postA, $postB])) . "\n";
echo 'post:1에만 있는 태그 (SDIFF) -> ' . formatSet($r->sDiff([$postA, $postB])) . "\n";

section('SINTERSTORE로 교집합을 새 키에 저장');
$commonKey = 'tags:common';
$r->del($commonKey);
$r->sInterStore($commonKey, $postA, $postB);
echo 'tags:common -> ' . formatSet($r->sMembers($commonKey)) . "\n";

section('SISMEMBER / SCARD');
echo "post:1에 'redis' 태그가 있나? -> " . ($r->sIsMember($postA, 'redis') ? 'true' : 'false') . "\n";
echo 'post:1 태그 개수 (SCARD) -> ' . $r->sCard($postA) . "\n";

section('SMOVE로 태그 하나를 보관함으로 옮기기');
$r->sMove($postA, $archive, 'database');
echo 'post:1:tags -> ' . formatSet($r->sMembers($postA)) . "\n";
echo 'tags:archive -> ' . formatSet($r->sMembers($archive)) . "\n";

section('SPOP으로 무작위로 하나 뽑아서 제거');
$popped = $r->sPop($postB);
echo '뽑힌 태그 -> ' . $popped . ' / 남은 post:2:tags -> ' . formatSet($r->sMembers($postB)) . "\n";

echo "\n정리: " . $r->del([$postA, $postB, $archive, $commonKey]) . "개 키 삭제\n";
