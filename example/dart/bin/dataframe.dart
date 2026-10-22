// DataFrame(DF.*) 커맨드 둘러보기 — 자기 완결적인 버전.
//
// 작은 재고 CSV를 코드 안에 직접 담아 DF.FROMCSV로 그 자리에서 적재한다 —
// 서버가 --features polars로 빌드됐기만 하면 사전 준비 없이 바로 돌려볼 수
// 있다. Dart에는 polars 같은 네이티브 DataFrame 라이브러리가 없으므로,
// 여기서는 DF.QUERY의 RESP 행 배열을 직접 다루고 DF.PRETTY로 보기 좋은 표를
// 받는다 — example/python/dataframe_csv.py,
// example/golang/dataframe와 같은 시나리오.
//
// 실행:
//   dart run bin/dataframe.dart
import 'dart:io';
import 'package:redis/redis.dart';

void section(String title) => print('\n--- $title ---');

(String, int) addr() {
  final a = Platform.environment['REDISX_ADDR'] ?? '127.0.0.1:2417';
  final parts = a.split(':');
  return (parts[0], int.parse(parts[1]));
}

const csvText = 'name,category,price,qty\n'
    'widget,tools,9.99,100\n'
    'gadget,electronics,29.99,50\n'
    'gizmo,electronics,19.99,75\n'
    'bolt,tools,0.99,500\n'
    'screw,tools,0.49,1000\n';

Future<void> main() async {
  final (host, port) = addr();
  final conn = RedisConnection();
  final cmd = await conn.connect(host, port);

  const key = 'inventory';
  await cmd.send_object(['DEL', key]);

  section('DF.FROMCSV로 인라인 CSV 적재');
  try {
    await cmd.send_object(['DF.FROMCSV', key, csvText]);
  } catch (e) {
    stderr.writeln('DF.FROMCSV 실패: $e');
    stderr.writeln('unknown command라면 서버가 --features polars 없이 빌드된 것 — '
        'cargo build --release -p redisx-server --features polars 로 다시 빌드할 것.');
    await conn.close();
    exit(1);
  }

  section('DF.SHAPE / DF.COLUMNS로 구조 확인');
  print('DF.SHAPE inventory -> ${await cmd.send_object(['DF.SHAPE', key])}');
  print('DF.COLUMNS inventory -> ${await cmd.send_object(['DF.COLUMNS', key])}');

  section('DF.QUERY로 카테고리별 재고 가치 집계');
  // RESP2에는 전용 Double 타입이 없어서 숫자 셀도 string으로 온다
  // (이 포크의 Reply::Double 인코딩 규칙) — 그대로 문자열로 출력한다.
  final rows = await cmd.send_object([
    'DF.QUERY',
    key,
    'SELECT category, SUM(price * qty) AS total_value FROM self GROUP BY category ORDER BY category',
  ]) as List;
  for (final row in rows) {
    final cells = row as List;
    print('  ${cells[0].toString().padRight(12)} total_value=${cells[1]}');
  }

  section('DF.PRETTY로 사람이 보기 좋은 표 출력 (서버가 이미 포맷해서 보내준다)');
  print(await cmd.send_object(['DF.PRETTY', key]));

  section('DF.TOCSV로 다시 CSV 텍스트로 내보내서 원본과 비교');
  final exported = await cmd.send_object(['DF.TOCSV', key]) as String;
  print(exported);
  print('원본과 동일? -> ${exported == csvText}');

  final deleted = await cmd.send_object(['DEL', key]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
