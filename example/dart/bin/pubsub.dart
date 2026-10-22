// Pub/Sub(발행/구독) 둘러보기.
//
// SUBSCRIBE/PSUBSCRIBE로 채널을 구독하고, 별도 연결에서 PUBLISH로 메시지를
// 보내 받는 흐름 — 키스페이스와 무관한 메시징 기능이라 다른 예제들과 달리
// 지우고 시작할 "키"가 없다. `redis` 패키지(pub.dev)는 구독 전용
// `PubSub(command)` 클래스를 두고 있고, `.getStream()`이 돌려주는 Dart
// `Stream`에서 메시지를 하나씩 꺼내 쓴다. 구독에 쓴 연결은 그 스트림
// 전용이 되니 발행은 별도 연결을 하나 더 연다.
//
// 실행:
//   dart run bin/pubsub.dart
import 'dart:async';
import 'dart:io';
import 'package:redis/redis.dart';

void section(String title) => print('\n--- $title ---');

(String, int) addr() {
  final a = Platform.environment['REDISX_ADDR'] ?? '127.0.0.1:2417';
  final parts = a.split(':');
  return (parts[0], int.parse(parts[1]));
}

const channel = 'notifications';
const pattern = 'notif.*';
const patternChannel = 'notif.sports';

Future<void> main() async {
  final (host, port) = addr();
  final subConn = RedisConnection();
  final subCmd = await subConn.connect(host, port);
  final pubConn = RedisConnection();
  final pubCmd = await pubConn.connect(host, port);

  section('SUBSCRIBE / PSUBSCRIBE (구독자는 전용 연결 하나)');
  final sub = PubSub(subCmd);
  final queue = StreamIterator(sub.getStream());
  sub.subscribe([channel]);
  sub.psubscribe([pattern]);
  await queue.moveNext();
  print('구독 확인 -> ${queue.current}');
  await queue.moveNext();
  print('패턴 구독 확인 -> ${queue.current}');

  section('PUBLISH (발행자는 별도 연결)');
  var n = await pubCmd.send_object(['PUBLISH', channel, '서버 점검이 10분 후 시작됩니다']);
  print('PUBLISH $channel -> 받은 구독자 수: $n');
  await queue.moveNext();
  var msg = queue.current; // [message, channel, payload]
  print('구독자가 받은 메시지 -> ${msg[1]} : ${msg[2]}');

  section('패턴 매칭으로 받기 (PSUBSCRIBE notif.* 가 notif.sports 도 받음)');
  n = await pubCmd.send_object(['PUBLISH', patternChannel, '손흥민 결승골!']);
  print('PUBLISH $patternChannel -> 받은 구독자 수: $n');
  await queue.moveNext();
  msg = queue.current; // [pmessage, pattern, channel, payload]
  print('패턴으로 받은 메시지 -> pattern=${msg[1]} channel=${msg[2]} : ${msg[3]}');

  section('PUBSUB CHANNELS / NUMSUB / NUMPAT (구독 상태 조회)');
  print('PUBSUB CHANNELS -> ${await pubCmd.send_object([
        'PUBSUB',
        'CHANNELS',
        '*',
      ])}');
  print('PUBSUB NUMSUB -> ${await pubCmd.send_object([
        'PUBSUB',
        'NUMSUB',
        channel,
      ])}');
  print('PUBSUB NUMPAT -> ${await pubCmd.send_object(['PUBSUB', 'NUMPAT'])}');

  section('UNSUBSCRIBE / PUNSUBSCRIBE');
  sub.unsubscribe([channel]);
  sub.punsubscribe([pattern]);
  await queue.moveNext();
  print('구독 해제 확인 -> ${queue.current}');
  await queue.moveNext();
  print('패턴 구독 해제 확인 -> ${queue.current}');
  await Future.delayed(const Duration(milliseconds: 200)); // 서버가 두 UNSUBSCRIBE를 전부 처리할 시간
  n = await pubCmd.send_object(['PUBLISH', channel, '이제 아무도 안 듣는다']);
  print('구독 해제 후 PUBLISH -> 받은 구독자 수: $n (0이어야 정상)');

  await subConn.close();
  await pubConn.close();
  print('\n정리 완료');
}
