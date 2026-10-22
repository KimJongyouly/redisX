// List 커맨드 둘러보기 — 간단한 "할 일 큐"로 사용해본다.
//
// LPUSH/RPUSH로 양쪽 끝에 넣고, LRANGE로 전체를 보고, LINSERT/LREM/LTRIM으로
// 가운데를 건드리고, LPOS로 위치를 찾고, LMOVE로 다른 리스트(처리 완료 큐)로
// 옮기는 흐름 — example/python/list.py, example/golang/list와
// 같은 시나리오.
//
// 실행:
//   dart run bin/list.dart
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

  const queue = 'tasks:todo', done = 'tasks:done';
  await cmd.send_object(['DEL', queue, done]);

  section('RPUSH로 작업 넣기 (먼저 들어온 게 먼저 처리되도록 오른쪽에 쌓는다)');
  await cmd.send_object(['RPUSH', queue, 'reboot-db', 'send-email', 'resize-image']);
  print('LRANGE tasks:todo 0 -1 -> ${await cmd.send_object(['LRANGE', queue, '0', '-1'])}');

  section('LPUSH로 긴급 작업을 맨 앞에 끼워넣기');
  await cmd.send_object(['LPUSH', queue, 'URGENT:rotate-logs']);
  print('LRANGE tasks:todo 0 -1 -> ${await cmd.send_object(['LRANGE', queue, '0', '-1'])}');

  section('LINSERT로 특정 작업 바로 뒤에 끼워넣기');
  await cmd.send_object(['LINSERT', queue, 'AFTER', 'send-email', 'send-email-retry']);
  print('LRANGE tasks:todo 0 -1 -> ${await cmd.send_object(['LRANGE', queue, '0', '-1'])}');

  section('LPOS로 작업 위치 찾기, LREM으로 제거');
  final pos = await cmd.send_object(['LPOS', queue, 'send-email-retry']);
  print('send-email-retry의 위치 -> $pos');
  await cmd.send_object(['LREM', queue, '1', 'send-email-retry']); // 결국 재시도는 취소하기로 함
  print('LREM 이후 LRANGE -> ${await cmd.send_object(['LRANGE', queue, '0', '-1'])}');

  section('LPOP으로 맨 앞 작업을 꺼내 처리하고, LMOVE로 완료 큐에 쌓기');
  final task = await cmd.send_object(['LPOP', queue]);
  print('처리할 작업 -> $task');
  await cmd.send_object(['RPUSH', done, task]);
  await cmd.send_object(['LMOVE', queue, done, 'LEFT', 'RIGHT']); // 다음 작업도 바로 완료 큐로 옮겨본다
  print('tasks:todo -> ${await cmd.send_object(['LRANGE', queue, '0', '-1'])}');
  print('tasks:done -> ${await cmd.send_object(['LRANGE', done, '0', '-1'])}');

  section('LTRIM으로 완료 큐를 최근 N개만 남기기 (오래된 기록 정리)');
  await cmd.send_object(['RPUSH', done, 'old-task-1', 'old-task-2', 'old-task-3']);
  await cmd.send_object(['LTRIM', done, '-2', '-1']);
  print('LTRIM 이후 tasks:done -> ${await cmd.send_object(['LRANGE', done, '0', '-1'])}');

  final deleted = await cmd.send_object(['DEL', queue, done]);
  print('\n정리: $deleted 개 키 삭제');

  await conn.close();
}
