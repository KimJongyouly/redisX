// HyperLogLog(PF*) 둘러보기 — "몇 명이 봤는지" 근사치로 세는 자료구조.
//
// 유니크 방문자 수를 세려고 Set에 전부 담으면 방문자 수만큼 메모리가 커지지만,
// HyperLogLog(PFADD/PFCOUNT)는 몇 KB 고정 메모리로 카디널리티(유일 값 개수)를
// 오차 범위 내에서 근사한다 — 그 "근사치"라는 걸 실제 Set과 나란히 세어서
// 직접 확인해본다. PFMERGE로 여러 날짜의 방문자 로그를 합치는 것도 함께 본다.
// example/python/hyperloglog.py, example/golang/hyperloglog와
// 같은 시나리오.
//
// 실행:
//   dart run bin/hyperloglog.dart
import 'dart:io';
import 'dart:math';
import 'package:redis/redis.dart';

void section(String title) => print('\n--- $title ---');

(String, int) addr() {
  final a = Platform.environment['REDISX_ADDR'] ?? '127.0.0.1:2417';
  final parts = a.split(':');
  return (parts[0], int.parse(parts[1]));
}

Future<void> main() async {
  final (host, port) = addr();
  final conn = RedisConnection();
  final cmd = await conn.connect(host, port);

  const hllMon = 'visitors:mon:hll', hllTue = 'visitors:tue:hll', hllWeek = 'visitors:week:hll';
  const exactSet = 'visitors:mon:exact';
  await cmd.send_object(['DEL', hllMon, hllTue, hllWeek, exactSet]);

  section('PFADD로 방문자 ID를 하나씩 추가하면서, 정확한 Set에도 똑같이 넣기');
  final rnd = Random(0);
  // 일부러 겹치는 방문자가 있도록(재방문) 10,000번의 방문 이벤트 중 실제로는
  // 3,000명 정도의 유니크 방문자만 나오게 만든다.
  for (var i = 0; i < 10000; i++) {
    final visitorId = 'user-${rnd.nextInt(3000) + 1}';
    await cmd.send_object(['PFADD', hllMon, visitorId]);
    await cmd.send_object(['SADD', exactSet, visitorId]);
  }

  final exactCount = await cmd.send_object(['SCARD', exactSet]) as int;
  final approxCount = await cmd.send_object(['PFCOUNT', hllMon]) as int;
  final errorPct = (approxCount - exactCount).abs() / exactCount * 100;
  print('정확한 유니크 방문자 수 (SCARD)   -> $exactCount');
  print('HyperLogLog 근사치 (PFCOUNT)      -> $approxCount');
  print('오차율                             -> ${errorPct.toStringAsFixed(2)}% (표준 오차는 약 0.81%)');

  section('메모리 비교 (방문자 수가 늘어나도 HLL은 거의 고정)');
  print('Set 메모리 사용량 근사치 (MEMORY USAGE) -> ${await cmd.send_object(['MEMORY', 'USAGE', exactSet])} bytes');
  print('HLL 메모리 사용량 근사치 (MEMORY USAGE) -> ${await cmd.send_object(['MEMORY', 'USAGE', hllMon])} bytes');

  section('PFMERGE로 이틀치 방문자 로그 합치기');
  for (var i = 0; i < 5000; i++) {
    await cmd.send_object(['PFADD', hllTue, 'user-${rnd.nextInt(4000) + 2000}']); // 월요일과 일부 겹침
  }
  await cmd.send_object(['PFMERGE', hllWeek, hllMon, hllTue]);
  print('월요일 근사 방문자 수 -> ${await cmd.send_object(['PFCOUNT', hllMon])}');
  print('화요일 근사 방문자 수 -> ${await cmd.send_object(['PFCOUNT', hllTue])}');
  print('합산(PFMERGE) 근사 방문자 수 -> ${await cmd.send_object(['PFCOUNT', hllWeek])} (단순 합보다 적은 건 겹치는 방문자가 있어서)');

  final deleted = await cmd.send_object(['DEL', hllMon, hllTue, hllWeek, exactSet]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
