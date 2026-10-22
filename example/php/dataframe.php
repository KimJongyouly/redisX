<?php

/**
 * DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
 *
 * 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
 * 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
 * 있다. PHP에는 polars 같은 네이티브 DataFrame 라이브러리가 없고 DF.*도
 * phpredis에 내장 메서드가 없으므로, rawCommand로 직접 호출하고 DF.QUERY의
 * RESP 행 배열을 그대로 다룬다 — 다른 언어의 같은 이름(dataframe) 예제들과
 * 동일한 시나리오.
 *
 * 실행:
 *     php dataframe.php
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

$key = 'inventory';
$r->del($key);

$csvText = "name,category,price,qty\n"
    . "widget,tools,9.99,100\n"
    . "gadget,electronics,29.99,50\n"
    . "gizmo,electronics,19.99,75\n"
    . "bolt,tools,0.99,500\n"
    . "screw,tools,0.49,1000\n";

section('DF.FROMCSV로 인라인 CSV 적재');
$ok = $r->rawCommand('DF.FROMCSV', $key, $csvText);
if ($ok !== true) {
    $err = $r->getLastError() ?: '(알 수 없는 오류)';
    if (stripos($err, 'unknown command') !== false) {
        fwrite(STDERR, "DF.FROMCSV를 모르는 커맨드라고 함 — 서버가 --features polars 없이 빌드됐을 가능성이 높음.\n");
        fwrite(STDERR, "cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.\n");
        exit(1);
    }
    fwrite(STDERR, "DF.FROMCSV 실패: $err\n");
    exit(1);
}

section('DF.SHAPE / DF.COLUMNS로 구조 확인');
echo 'DF.SHAPE inventory -> [' . implode(', ', $r->rawCommand('DF.SHAPE', $key)) . "]\n";
echo 'DF.COLUMNS inventory -> ' . implode(', ', $r->rawCommand('DF.COLUMNS', $key)) . "\n";

section('DF.QUERY로 카테고리별 재고 가치 집계');
// RESP2에는 전용 Double 타입이 없어서 숫자 응답도 bulk string으로 온다 —
// 이 포크의 Reply::Double 인코딩 규칙(resp.rs) 때문.
$rows = $r->rawCommand(
    'DF.QUERY',
    $key,
    'SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category'
);
foreach ($rows as [$category, $totalValue]) {
    printf("  %-12s total_value=%s\n", $category, $totalValue);
}

section('DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)');
echo $r->rawCommand('DF.PRETTY', $key) . "\n";

section('DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교');
$exported = $r->rawCommand('DF.TOCSV', $key);
echo $exported;
echo '원본과 동일? -> ' . ($exported === $csvText ? 'true' : 'false') . "\n";

echo "\n정리: " . $r->del($key) . "개 키 삭제\n";
