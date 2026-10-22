// ZSet(정렬 집합) 커맨드 둘러보기 — 점수가 있는 리더보드.
//
// ZADD로 점수를 매기고, ZRANGE/ZREVRANGE로 순위를 뽑고, ZINCRBY로 점수를
// 올리고, ZRANGEBYSCORE로 구간을 조회하고, 두 리더보드를 ZUNIONSTORE로
// 합산하는 것까지 — example/python/zset.py, example/golang/zset와
// 같은 시나리오.
//
// 실행:
//   dart run bin/zset.dart
import 'dart:io';
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

  const board = 'leaderboard', weekly = 'leaderboard:weekly', combined = 'leaderboard:combined';
  await cmd.send_object(['DEL', board, weekly, combined]);

  section('ZADD로 점수 매기기');
  await cmd.send_object(['ZADD', board, '120', 'alice', '98', 'bob', '150', 'carol', '110', 'dave']);
  print('전체 랭킹 (ZREVRANGE, 점수 포함) -> ${await cmd.send_object(['ZREVRANGE', board, '0', '-1', 'WITHSCORES'])}');

  section('ZSCORE / ZREVRANK로 한 명만 조회');
  print('carol 점수 (ZSCORE) -> ${await cmd.send_object(['ZSCORE', board, 'carol'])}');
  print('carol 등수, 0부터 (ZREVRANK) -> ${await cmd.send_object(['ZREVRANK', board, 'carol'])}');

  section('ZINCRBY로 점수 올리기 (보너스 획득)');
  await cmd.send_object(['ZINCRBY', board, '30', 'bob']);
  print('bob 보너스 이후 전체 랭킹 -> ${await cmd.send_object(['ZREVRANGE', board, '0', '-1', 'WITHSCORES'])}');

  section('ZRANGEBYSCORE로 특정 점수 구간만 뽑기');
  print('100점 이상 120점 이하 -> ${await cmd.send_object(['ZRANGEBYSCORE', board, '100', '120', 'WITHSCORES'])}');

  section('ZPOPMAX / ZPOPMIN으로 1등/꼴등 뽑아서 제거');
  print('ZPOPMAX -> ${await cmd.send_object(['ZPOPMAX', board])}');
  print('ZPOPMIN -> ${await cmd.send_object(['ZPOPMIN', board])}');
  print('남은 랭킹 -> ${await cmd.send_object(['ZREVRANGE', board, '0', '-1', 'WITHSCORES'])}');

  section('ZUNIONSTORE로 이번 주 보너스 점수와 합산');
  // 방금 뽑아서 지워진 것 복원
  await cmd.send_object(['ZADD', board, '120', 'alice', '128', 'bob', '110', 'dave']);
  await cmd.send_object(['ZADD', weekly, '20', 'alice', '5', 'bob', '40', 'eve']);
  await cmd.send_object(['ZUNIONSTORE', combined, '2', board, weekly]);
  print('합산 랭킹 (ZUNIONSTORE) -> ${await cmd.send_object(['ZREVRANGE', combined, '0', '-1', 'WITHSCORES'])}');

  final deleted = await cmd.send_object(['DEL', board, weekly, combined]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
