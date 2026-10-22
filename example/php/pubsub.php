<?php

/**
 * Pub/Sub(발행/구독) 둘러보기.
 *
 * SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
 * 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
 * 지우고 시작할 "키"가 없다.
 *
 * **이 언어만의 사정**: phpredis의 `subscribe()`/`psubscribe()`는 콜백을
 * 등록하고 그 자리에서 영원히(콜백 안에서 `unsubscribe()`를 부를 때까지)
 * 블로킹한다 — 같은 PHP 프로세스 안에서 그 호출 다음 줄로 넘어가 PUBLISH를
 * 보낼 수가 없다. 다른 언어들처럼 "읽을 메시지 하나만 꺼내오기"가 안 되는
 * 구조라서, 여기서는 `pcntl_fork()`로 구독자를 별도 프로세스로 떼어내고
 * (phpredis 연결은 각자 fork 이후에 새로 맺는다 — fork 이전에 연결해둔
 * 소켓을 부모/자식이 공유하면 안 된다), 부모 프로세스가 발행자 역할을 한다.
 * exact 채널과 패턴 채널을 각각 별도의 fork로 나눈 것도 같은 이유 —
 * `subscribe()`를 호출한 연결은 그 안에서 영원히 못 빠져나오므로, 한
 * 프로세스에서 SUBSCRIBE와 PSUBSCRIBE를 동시에 유지하며 번갈아 받는 건
 * phpredis의 블로킹 콜백 API로는 자연스럽게 안 된다.
 *
 * 실행:
 *     php pubsub.php
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

// `$key`로 지정한 채널(또는 패턴)의 구독자 수가 `$expected` 이상이 될 때까지
// 최대 `$timeoutSec`초 동안 짧게 폴링한다 — fork로 떼어낸 자식이 실제로
// SUBSCRIBE/PSUBSCRIBE를 보낼 때까지는 시간이 걸리므로, 그 전에 PUBLISH를
// 보내면 아무도 못 받는 경쟁 상태를 피하기 위함이다.
function waitForCount(Redis $r, string $pubsubKind, string $key, int $expected, float $timeoutSec = 2.0): int
{
    $deadline = microtime(true) + $timeoutSec;
    while (microtime(true) < $deadline) {
        $count = $pubsubKind === 'numpat'
            ? $r->pubsub('numpat')
            : ($r->pubsub('numsub', [$key])[$key] ?? 0);
        if ($count >= $expected) {
            return $count;
        }
        usleep(20_000);
    }
    fwrite(STDERR, "timeout waiting for $pubsubKind on $key\n");
    exit(1);
}

[$host, $port] = redisxAddr();
$channel = 'notifications';
$pattern = 'notif.*';
$patternChannel = 'notif.sports';

$pub = new Redis();
$pub->connect($host, $port);

section('SUBSCRIBE (구독자는 자식 프로세스)');
$childPid = pcntl_fork();
if ($childPid === -1) {
    fwrite(STDERR, "fork 실패\n");
    exit(1);
}
if ($childPid === 0) {
    $sub = new Redis();
    $sub->connect($host, $port);
    $sub->subscribe([$channel], function (Redis $redis, string $chan, string $msg) use ($channel) {
        echo "구독자가 받은 메시지 -> $chan : $msg\n";
        $redis->unsubscribe([$channel]);
    });
    exit(0);
}
$numsub = waitForCount($pub, 'numsub', $channel, 1);
echo "PUBSUB NUMSUB (발행 전) -> $channel: $numsub\n";
echo 'PUBSUB CHANNELS -> ' . json_encode($pub->pubsub('channels', '*')) . "\n";

section('PUBLISH (발행자는 부모 프로세스)');
$n = $pub->publish($channel, '서버 점검이 10분 후 시작됩니다');
echo "PUBLISH $channel -> 받은 구독자 수: $n\n";
pcntl_waitpid($childPid, $status); // 자식이 메시지를 받아 출력하고 unsubscribe한 뒤 끝날 때까지 대기

section('PSUBSCRIBE (패턴 구독자도 별도 자식 프로세스)');
$childPid2 = pcntl_fork();
if ($childPid2 === -1) {
    fwrite(STDERR, "fork 실패\n");
    exit(1);
}
if ($childPid2 === 0) {
    $sub = new Redis();
    $sub->connect($host, $port);
    $sub->psubscribe([$pattern], function (Redis $redis, string $pat, string $chan, string $msg) use ($pattern) {
        echo "패턴으로 받은 메시지 -> pattern=$pat channel=$chan : $msg\n";
        $redis->punsubscribe([$pattern]);
    });
    exit(0);
}
$numpat = waitForCount($pub, 'numpat', $pattern, 1);
echo "PUBSUB NUMPAT (발행 전) -> $numpat\n";

echo "\n--- 패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음) ---\n";
$n = $pub->publish($patternChannel, '손흥민 결승골!');
echo "PUBLISH $patternChannel -> 받은 구독자 수: $n\n";
pcntl_waitpid($childPid2, $status2);

section('UNSUBSCRIBE / PUNSUBSCRIBE 이후 확인');
usleep(200_000); // 두 자식이 끝나며 보낸 UNSUBSCRIBE/PUNSUBSCRIBE를 서버가 전부 처리할 시간
echo 'PUBSUB CHANNELS -> ' . json_encode($pub->pubsub('channels', '*')) . "\n";
echo 'PUBSUB NUMPAT -> ' . $pub->pubsub('numpat') . "\n";
$n = $pub->publish($channel, '이제 아무도 안 듣는다');
echo "구독 해제 후 PUBLISH -> 받은 구독자 수: $n (0이어야 정상)\n";

echo "\n정리 완료\n";
