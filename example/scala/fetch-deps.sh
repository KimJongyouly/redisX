#!/usr/bin/env bash
# Jedis(표준 동기 Java Redis 클라이언트, Kotlin에서도 그대로 씀)와 그 런타임
# 의존성을 Maven Central에서 직접 내려받는다 — 이 저장소엔 Maven/Gradle이
# 없어서 빌드 도구 없이 scalac/scala만으로 돌릴 수 있게 하는 가장 간단한
# 방법이다. 다운로드한 jar들은 .gitignore 대상이라(lib/) 커밋 전에 매번 이
# 스크립트를 한 번 실행해야 한다.
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p lib

BASE=https://repo1.maven.org/maven2
JEDIS_VERSION=5.2.0
SLF4J_VERSION=1.7.36

fetch() {
  local path="$1" out="$2"
  if [ -f "lib/$out" ]; then
    echo "lib/$out already present, skipping"
    return
  fi
  echo "fetching $out ..."
  curl -sS --fail -o "lib/$out" "$BASE/$path"
}

fetch "redis/clients/jedis/$JEDIS_VERSION/jedis-$JEDIS_VERSION.jar" "jedis-$JEDIS_VERSION.jar"
fetch "org/slf4j/slf4j-api/$SLF4J_VERSION/slf4j-api-$SLF4J_VERSION.jar" "slf4j-api-$SLF4J_VERSION.jar"
fetch "org/slf4j/slf4j-nop/$SLF4J_VERSION/slf4j-nop-$SLF4J_VERSION.jar" "slf4j-nop-$SLF4J_VERSION.jar"
fetch "org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" "commons-pool2-2.12.0.jar"

echo "done — lib/ populated. Run ./build.sh next."
