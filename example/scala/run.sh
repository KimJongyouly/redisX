#!/usr/bin/env bash
# ./run.sh StringExample — lib/의 jar들을 받아온 뒤(fetch-deps.sh) 실행.
# `scala run`(scala-cli 기반 Scala 3 통합 CLI)이 컴파일+실행을 한 번에
# 처리하므로 별도 build.sh/출력 디렉터리가 필요 없다 — 첫 실행 때만 scala-cli
# 자체 툴체인을 내려받고(네트워크 필요, 한 번만), 이후로는 캐시를 쓴다.
set -euo pipefail
cd "$(dirname "$0")"

if ! ls lib/*.jar >/dev/null 2>&1; then
  echo "lib/ 비어있음 — 먼저 ./fetch-deps.sh 실행" >&2
  exit 1
fi

name="${1:?사용법: ./run.sh StringExample}"
LIBCP=$(ls lib/*.jar | tr '\n' ':')
exec scala run --classpath "$LIBCP" "src/$name.scala"
