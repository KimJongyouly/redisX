// Hash 커맨드 둘러보기 — 하나의 키 안에 여러 필드를 묶어서 쓰는 패턴.
//
// 사용자 프로필처럼 "엔티티 하나 = 해시 하나"로 모델링할 때 쓰는 전형적인
// Hash 사용법이다. HSET/HGETALL로 통째로 읽고 쓰고, HINCRBY로 필드 하나만
// 원자적으로 증가시키고, HRANDFIELD로 무작위 추첨도 해본다 —
// example/python/hash.py, example/golang/hash와 같은 시나리오.
//
// 실행:
//   dart run bin/hash.dart
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

  const userKey = 'user:42';
  await cmd.send_object(['DEL', userKey]);

  section('HSET으로 여러 필드를 한 번에 채우기');
  await cmd.send_object(['HSET', userKey, 'name', 'jiyeon', 'level', '5', 'coins', '100', 'logins', '0']);
  print('HGETALL user:42 -> ${await cmd.send_object(['HGETALL', userKey])}');

  section('HGET / HMGET으로 필드 일부만 읽기');
  print('HGET user:42 name -> ${await cmd.send_object(['HGET', userKey, 'name'])}');
  print('HMGET user:42 name level -> ${await cmd.send_object(['HMGET', userKey, 'name', 'level'])}');

  section('HINCRBY로 코인/로그인 횟수를 원자적으로 증가');
  await cmd.send_object(['HINCRBY', userKey, 'coins', '50']);
  await cmd.send_object(['HINCRBY', userKey, 'logins', '1']);
  print('HGETALL user:42 -> ${await cmd.send_object(['HGETALL', userKey])}');

  section('HEXISTS / HDEL');
  print('HEXISTS user:42 level -> ${await cmd.send_object(['HEXISTS', userKey, 'level'])}');
  await cmd.send_object(['HDEL', userKey, 'logins']);
  print('HDEL 이후 HGETALL -> ${await cmd.send_object(['HGETALL', userKey])}');

  section('HRANDFIELD로 여러 유저 중 무작위 추첨 흉내내기');
  const raffle = 'raffle:entries';
  await cmd.send_object(['DEL', raffle]);
  await cmd.send_object(['HSET', raffle, 'alice', '1', 'bob', '1', 'carol', '1', 'dave', '1']);
  final winner = await cmd.send_object(['HRANDFIELD', raffle]);
  print('당첨자 한 명 -> $winner');
  print('HRANDFIELD -3 (중복 허용 3명 뽑기) -> ${await cmd.send_object(['HRANDFIELD', raffle, '-3'])}');

  final deleted = await cmd.send_object(['DEL', userKey, raffle]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
