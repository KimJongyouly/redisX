// Set 커맨드 둘러보기 — 태그/멤버십 같은 "순서 없는 유일 값 모음".
//
// 두 글 사이의 공통 태그(SINTER), 합쳐진 태그(SUNION), 한쪽에만 있는
// 태그(SDIFF)처럼 집합 연산이 자연스러운 상황을 예로 든다. SPOP으로
// 무작위로 하나 뽑고, SMOVE로 다른 집합에 옮기는 것도 함께 본다 —
// example/python/set.py, example/golang/set와 같은 시나리오.
//
// 실행:
//   dart run bin/set.dart
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

  const postA = 'post:1:tags', postB = 'post:2:tags', archive = 'tags:archive';
  await cmd.send_object(['DEL', postA, postB, archive]);

  section('SADD로 글마다 태그 달기');
  await cmd.send_object(['SADD', postA, 'rust', 'redis', 'database']);
  await cmd.send_object(['SADD', postB, 'rust', 'web', 'async']);
  print('post:1:tags -> ${await cmd.send_object(['SMEMBERS', postA])}');
  print('post:2:tags -> ${await cmd.send_object(['SMEMBERS', postB])}');

  section('SINTER / SUNION / SDIFF로 집합 연산');
  print('둘 다 쓴 태그 (SINTER) -> ${await cmd.send_object(['SINTER', postA, postB])}');
  print('둘 중 하나라도 쓴 태그 (SUNION) -> ${await cmd.send_object(['SUNION', postA, postB])}');
  print('post:1에만 있는 태그 (SDIFF) -> ${await cmd.send_object(['SDIFF', postA, postB])}');

  section('SINTERSTORE로 교집합을 새 키에 저장');
  const commonKey = 'tags:common';
  await cmd.send_object(['DEL', commonKey]);
  await cmd.send_object(['SINTERSTORE', commonKey, postA, postB]);
  print('tags:common -> ${await cmd.send_object(['SMEMBERS', commonKey])}');

  section('SISMEMBER / SCARD');
  print("post:1에 'redis' 태그가 있나? -> ${await cmd.send_object(['SISMEMBER', postA, 'redis'])}");
  print('post:1 태그 개수 (SCARD) -> ${await cmd.send_object(['SCARD', postA])}');

  section('SMOVE로 태그 하나를 보관함으로 옮기기');
  await cmd.send_object(['SMOVE', postA, archive, 'database']);
  print('post:1:tags -> ${await cmd.send_object(['SMEMBERS', postA])}');
  print('tags:archive -> ${await cmd.send_object(['SMEMBERS', archive])}');

  section('SPOP으로 무작위로 하나 뽑아서 제거');
  final popped = await cmd.send_object(['SPOP', postB]);
  print('뽑힌 태그 -> $popped / 남은 post:2:tags -> ${await cmd.send_object(['SMEMBERS', postB])}');

  final deleted = await cmd.send_object(['DEL', postA, postB, archive, commonKey]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
