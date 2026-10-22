#!/usr/bin/env bash
# Compiles every example under src/ into out/ using only lib/*.jar on the
# classpath (no Maven/Gradle). Run ./fetch-deps.sh first if lib/ is empty.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p out
javac -cp "lib/*" -d out src/*.java
echo "compiled into out/"
