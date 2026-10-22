<?php

/**
 * HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
 *
 * 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
 * HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
 * 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
 * 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
 *
 * 실행:
 *     php hyperloglog.php
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

$hllMon = 'visitors:mon:hll';
$hllTue = 'visitors:tue:hll';
$hllWeek = 'visitors:week:hll';
$exactSet = 'visitors:mon:exact';
$r->del([$hllMon, $hllTue, $hllWeek, $exactSet]);

section('PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기');
mt_srand(0);
// 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
// 3,000명 정도의 유니크 방문자만 나오게 만든다.
for ($i = 0; $i < 10_000; $i++) {
    $visitorId = 'user-' . mt_rand(1, 3000);
    $r->pfAdd($hllMon, [$visitorId]);
    $r->sAdd($exactSet, $visitorId);
}

$exactCount = $r->sCard($exactSet);
$approxCount = $r->pfCount($hllMon);
$errorPct = abs($approxCount - $exactCount) / $exactCount * 100;
printf("정확한 유니크 방문자 수 (SCARD)   -> %d\n", $exactCount);
printf("HyperLogLog 근사치 (PFCOUNT)      -> %d\n", $approxCount);
printf("오차율                             -> %.2f%% (표준 오차는 약 0.81%%)\n", $errorPct);

section('메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)');
echo 'Set 메모리 사용량 근사치 (MEMORY USAGE) -> ' . $r->rawCommand('MEMORY', 'USAGE', $exactSet) . " bytes\n";
echo 'HLL 메모리 사용량 근사치 (MEMORY USAGE) -> ' . $r->rawCommand('MEMORY', 'USAGE', $hllMon) . " bytes\n";

section('PFMERGE로 이틀치 방문자 로그 합치기');
for ($i = 0; $i < 5_000; $i++) {
    $r->pfAdd($hllTue, ['user-' . mt_rand(2000, 6000)]); // 월요일과 일부 겹침
}
$r->pfMerge($hllWeek, [$hllMon, $hllTue]);
echo '월요일 근사 방문자 수 -> ' . $r->pfCount($hllMon) . "\n";
echo '화요일 근사 방문자 수 -> ' . $r->pfCount($hllTue) . "\n";
echo '합산(PFMERGE) 근사 방문자 수 -> ' . $r->pfCount($hllWeek) . " (단순 합보다 적은 건 겹치는 방문자가 있어서)\n";

echo "\n정리: " . $r->del([$hllMon, $hllTue, $hllWeek, $exactSet]) . "개 키 삭제\n";
