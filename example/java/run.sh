#!/usr/bin/env bash
# Runs one compiled example class against redisX. Usage:
#   ./run.sh StringExample
#   REDISX_ADDR=127.0.0.1:2417 ./run.sh DataframeExample   # same as the default, shown explicitly
set -euo pipefail
cd "$(dirname "$0")"
if [ $# -ne 1 ]; then
  echo "usage: $0 <ExampleClassName>  (e.g. StringExample, ListExample, HashExample, SetExample, ZsetExample, HyperloglogExample, SlowlogExample, DataframeExample)" >&2
  exit 1
fi
java -cp "lib/*:out" "$1"
