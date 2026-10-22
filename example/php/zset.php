<?php

/**
 * ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
 *
 * ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
 * 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
 * 합산하는 것까지 — 게임 리더보드에서 흔히 하는 작업들이다.
 *
 * 실행:
 *     php zset.php
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

// phpredis는 withscores 결과를 ["member" => score, ...] 연관 배열로 준다.
function formatScored(array $m): string
{
    $parts = [];
    foreach ($m as $member => $score) {
        $parts[] = "($member, $score)";
    }
    return '[' . implode(', ', $parts) . ']';
}

[$host, $port] = redisxAddr();
$r = new Redis();
$r->connect($host, $port);

$board = 'leaderboard';
$weekly = 'leaderboard:weekly';
$combined = 'leaderboard:combined';
$r->del([$board, $weekly, $combined]);

section('ZADD로 점수 매기기');
$r->zAdd($board, 120, 'alice', 98, 'bob', 150, 'carol', 110, 'dave');
echo '전체 랭킹 (ZREVRANGE, 점수 포함) -> ' . formatScored($r->zRevRange($board, 0, -1, true)) . "\n";

section('ZSCORE / ZREVRANK로 한 명만 조회');
echo 'carol 점수 (ZSCORE) -> ' . $r->zScore($board, 'carol') . "\n";
echo 'carol 등수, 0부터 (ZREVRANK) -> ' . $r->zRevRank($board, 'carol') . "\n";

section('ZINCRBY로 점수 올리기 (보너스 획득)');
$r->zIncrBy($board, 30, 'bob');
echo 'bob 보너스 이후 전체 랭킹 -> ' . formatScored($r->zRevRange($board, 0, -1, true)) . "\n";

section('ZRANGEBYSCORE로 특정 점수 구간만 뽑기');
echo '100점 이상 120점 이하 -> ' . formatScored($r->zRangeByScore($board, 100, 120, ['withscores' => true])) . "\n";

section('ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거');
echo 'ZPOPMAX -> ' . formatScored($r->zPopMax($board)) . "\n";
echo 'ZPOPMIN -> ' . formatScored($r->zPopMin($board)) . "\n";
echo '남은 랭킹 -> ' . formatScored($r->zRevRange($board, 0, -1, true)) . "\n";

section('ZUNIONSTORE로 이번 주 보너스 점수와 합산');
$r->zAdd($board, 120, 'alice', 128, 'bob', 110, 'dave'); // 방금 뽑아서 지워진 것 복원
$r->zAdd($weekly, 20, 'alice', 5, 'bob', 40, 'eve');
$r->zUnionStore($combined, [$board, $weekly]);
echo '합산 랭킹 (ZUNIONSTORE) -> ' . formatScored($r->zRevRange($combined, 0, -1, true)) . "\n";

echo "\n정리: " . $r->del([$board, $weekly, $combined]) . "개 키 삭제\n";
