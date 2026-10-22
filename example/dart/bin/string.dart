// String(키/값) 커맨드 둘러보기.
//
// 기본 SET/GET부터, 문자열을 바이트 버퍼처럼 다루는 APPEND/GETRANGE/SETRANGE,
// 여러 키를 한 번에 다루는 MSET/MGET, 카운터로 쓰는 INCR/INCRBY, 그리고
// 만료(TTL) 관련 커맨드까지 — example/python/redis_string.py,
// example/golang/string와 같은 시나리오를 Dart(redis 패키지, pub.dev)로
// 옮긴 것이다. redisX에는 전용 SDK가 없으므로 RESP 커맨드를 그대로 보내는
// 범용 클라이언트(send_object)를 쓴다.
//
// 실행:
//   dart run bin/string.dart
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

  final demoKeys = ['greeting', 'page:views', 'user:1:session', 'k1', 'k2', 'k3'];
  await cmd.send_object(['DEL', ...demoKeys]);

  section('SET / GET');
  await cmd.send_object(['SET', 'greeting', 'hello world']);
  print('GET greeting -> ${await cmd.send_object(['GET', 'greeting'])}');

  section('APPEND / GETRANGE / SETRANGE (바이트 버퍼처럼 다루기)');
  await cmd.send_object(['APPEND', 'greeting', '!']);
  print('APPEND 이후: ${await cmd.send_object(['GET', 'greeting'])}');
  print('GETRANGE greeting 0 4 -> ${await cmd.send_object(['GETRANGE', 'greeting', '0', '4'])}');
  // "world"(5글자)+"!" 자리를 "redisX"로 덮어씀
  await cmd.send_object(['SETRANGE', 'greeting', '6', 'redisX']);
  print('SETRANGE 이후: ${await cmd.send_object(['GET', 'greeting'])}');

  section('MSET / MGET (여러 키 한 번에)');
  await cmd.send_object(['MSET', 'k1', 'v1', 'k2', 'v2', 'k3', 'v3']);
  print('MGET k1 k2 k3 -> ${await cmd.send_object(['MGET', 'k1', 'k2', 'k3'])}');

  section('INCR / INCRBY (카운터)');
  await cmd.send_object(['DEL', 'page:views']);
  for (var i = 0; i < 3; i++) {
    await cmd.send_object(['INCR', 'page:views']);
  }
  await cmd.send_object(['INCRBY', 'page:views', '10']);
  print('page:views -> ${await cmd.send_object(['GET', 'page:views'])}');

  section('EXPIRE / TTL / PERSIST');
  await cmd.send_object(['SET', 'user:1:session', 'token-abc']);
  await cmd.send_object(['EXPIRE', 'user:1:session', '2']);
  print('설정 직후 TTL -> ${await cmd.send_object(['TTL', 'user:1:session'])}초');
  await Future.delayed(const Duration(milliseconds: 2200));
  final exists = await cmd.send_object(['EXISTS', 'user:1:session']);
  print('2.2초 후 EXISTS -> ${exists == 1} (TTL이 지나 자동 삭제됨)');

  await cmd.send_object(['SET', 'user:1:session', 'token-def', 'EX', '60']);
  await cmd.send_object(['PERSIST', 'user:1:session']);
  print('PERSIST 이후 TTL -> ${await cmd.send_object(['TTL', 'user:1:session'])} (-1 = 만료 없음)');

  final deleted = await cmd.send_object(['DEL', ...demoKeys]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
