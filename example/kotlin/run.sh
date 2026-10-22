#!/usr/bin/env bash
# ./run.sh StringExample  — out/StringExample.jar를 빌드했다는 전제 하에 실행.
# Kotlin은 top-level fun main()이 있는 파일 FooExample.kt를 컴파일하면
# 클래스 이름이 자동으로 FooExampleKt가 된다(파일명 + "Kt").
set -euo pipefail
cd "$(dirname "$0")"

name="${1:?사용법: ./run.sh StringExample}"
exec java -cp "out/$name.jar:lib/*" "${name}Kt"
