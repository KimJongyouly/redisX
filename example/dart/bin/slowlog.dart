// SLOWLOG 둘러보기 — redisX의 SLOWLOG는 메모리가 아니라 파일 기반이다.
//
// 실제 Redis의 SLOWLOG는 최근 N개를 메모리 링 버퍼에 들고 있지만, redisX가
// 포팅한 원본 C 포크는 그 설계를 통째로 버리고 "느린 커맨드를 파일 한 줄로
// 남기는" 방식으로 바꿨다(자세한 내용은 저장소 루트의 CHANGE.md 1절 참고).
// 그래서 redisX에서 SLOWLOG를 쓰려면 slowlog-file-path를 실제 경로로
// 설정해야 하고(기본값은 ""로 비활성화), SLOWLOG GET/LEN은 매번 그 파일을
// 읽어서 답한다 — 메모리에 아무것도 들고 있지 않다. example/python,
// example/golang의 slowlog 예제와 같은 시나리오.
//
// 실행:
//   dart run bin/slowlog.dart
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

  section('현재 설정 확인 (기본값은 비활성화 상태)');
  print('slowlog-file-path -> ${await cmd.send_object(['CONFIG', 'GET', 'slowlog-file-path'])}');
  print('slowlog-log-slower-than (마이크로초) -> ${await cmd.send_object(['CONFIG', 'GET', 'slowlog-log-slower-than'])}');

  section('CONFIG SET으로 파일 경로를 지정하고, 임계값을 0으로 낮춰서 전부 기록되게 하기');
  final logPath = '${Directory.systemTemp.path}/redisx-slowlog-demo-dart.log';
  final logFile = File(logPath);
  if (logFile.existsSync()) logFile.deleteSync();
  await cmd.send_object(['CONFIG', 'SET', 'slowlog-file-path', logPath]);
  await cmd.send_object(['CONFIG', 'SET', 'slowlog-log-slower-than', '0']); // 0 = 사실상 모든 커맨드를 "느린 커맨드"로 기록

  section('몇 가지 커맨드를 실행해서 로그에 쌓기');
  await cmd.send_object(['SET', 'k', 'v']);
  await cmd.send_object(['GET', 'k']);
  await cmd.send_object(['LPUSH', 'mylist', 'a', 'b', 'c']);
  await cmd.send_object(['DEL', 'mylist', 'k']);

  section('SLOWLOG LEN / GET으로 읽기');
  print('SLOWLOG LEN -> ${await cmd.send_object(['SLOWLOG', 'LEN'])}');
  final entries = await cmd.send_object(['SLOWLOG', 'GET', '5']) as List;
  for (final e in entries) {
    final id = e[0], durationUs = e[2], argv = e[3], peer = e[4];
    print('  #$id  ${durationUs}us  $peer  ${(argv as List).join(' ')}');
  }

  section('실제 로그 파일 내용 (바로 이 파일을 읽어서 위 응답을 만든 것)');
  print(logFile.readAsStringSync().trimRight());

  section('SLOWLOG RESET으로 파일 비우기');
  await cmd.send_object(['SLOWLOG', 'RESET']);
  // 임계값이 여전히 0이라 RESET 자신도 "느린 커맨드"로 기록된다 — 그래서
  // 0이 아니라 1이 나온다(바로 이 LEN 호출 직전에 실행된 RESET 한 줄).
  print('RESET 이후 SLOWLOG LEN -> ${await cmd.send_object(['SLOWLOG', 'LEN'])}');

  section('정리 — 다음에 실행했을 때도 모든 커맨드가 기록되지 않도록 원래 설정으로 되돌리기');
  await cmd.send_object(['CONFIG', 'SET', 'slowlog-log-slower-than', '10000']); // redisX 기본값
  await cmd.send_object(['CONFIG', 'SET', 'slowlog-file-path', '']); // 기본값(비활성화)으로 복원
  if (logFile.existsSync()) logFile.deleteSync();
  print('완료');

  await conn.close();
}
