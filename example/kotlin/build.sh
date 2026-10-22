#!/usr/bin/env bash
# lib/의 jar들을 받아온 뒤(fetch-deps.sh), src/*.kt 각각을 독립 실행 가능한
# jar로 컴파일한다. -include-runtime으로 Kotlin stdlib 자체를 jar 안에
# 번들링하므로, 실행 시 classpath에 kotlinc 설치 경로를 몰라도 된다.
set -euo pipefail
cd "$(dirname "$0")"

if ! ls lib/*.jar >/dev/null 2>&1; then
  echo "lib/ 비어있음 — 먼저 ./fetch-deps.sh 실행" >&2
  exit 1
fi

LIBCP=$(ls lib/*.jar | tr '\n' ':')
mkdir -p out

for src in src/*.kt; do
  name=$(basename "$src" .kt)
  echo "compiling $name ..."
  kotlinc -cp "$LIBCP" "$src" -include-runtime -d "out/$name.jar"
done

echo "done — run.sh로 실행"
