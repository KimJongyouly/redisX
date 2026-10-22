#!/usr/bin/env bash
# setup_cluster.sh가 띄운 세 노드를 각 폴더의 server.pid를 보고 종료한다.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES=(node1 node2 node3)

for node in "${NODES[@]}"; do
    pid_file="$SCRIPT_DIR/$node/server.pid"
    if [[ -f "$pid_file" ]]; then
        pid="$(cat "$pid_file")"
        if kill "$pid" 2>/dev/null; then
            echo "  $node (pid $pid) 종료 요청"
        else
            echo "  $node (pid $pid) 이미 종료된 것으로 보임"
        fi
        rm -f "$pid_file"
    else
        echo "  $node: server.pid 없음 (안 떠 있거나 이미 정리됨)"
    fi
done
