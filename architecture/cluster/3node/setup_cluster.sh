#!/usr/bin/env bash
# cluster/3node 세 노드를 띄우고, MEET으로 묶고, 16384슬롯을 3등분해서
# 할당하는 것까지 한 번에 한다. 실행 후에는 바로 cluster-aware 클라이언트로
# 테스트할 수 있는 상태가 된다.
#
# 사용법:
#   cd redisX/architecture/cluster/3node
#   ./setup_cluster.sh
#
# redisx-server/redisx-cli가 PATH에 없다면 환경변수로 경로를 지정한다:
#   REDISX_SERVER_BIN=/path/to/redisx-server REDISX_CLI_BIN=/path/to/redisx-cli ./setup_cluster.sh
#
# 멈추려면: ./stop_cluster.sh (또는 각 노드 폴더의 server.pid를 보고 kill)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_BIN="${REDISX_SERVER_BIN:-redisx-server}"
CLI_BIN="${REDISX_CLI_BIN:-redisx-cli}"

NODES=(node1 node2 node3)
PORTS=(7301 7302 7303)
CPORTS=(17301 17302 17303)

echo "== 1) 세 노드 기동 =="
for i in "${!NODES[@]}"; do
    node="${NODES[$i]}"
    port="${PORTS[$i]}"
    dir="$SCRIPT_DIR/$node"
    rm -f "$dir/dump.rdb"
    (
        cd "$dir"
        "$SERVER_BIN" "./redisx-$node.conf" > server.log 2>&1 &
        echo $! > server.pid
    )
    echo "  $node: port=$port, pid=$(cat "$dir/server.pid")"
done

echo "== 2) 포트가 뜰 때까지 대기 =="
for port in "${PORTS[@]}"; do
    for _ in $(seq 1 50); do
        "$CLI_BIN" -p "$port" PING > /dev/null 2>&1 && break
        sleep 0.2
    done
    "$CLI_BIN" -p "$port" PING > /dev/null 2>&1 || { echo "포트 $port 가 기동되지 않음 — ${NODES[0]}/server.log 등을 확인할 것"; exit 1; }
done
echo "  모든 노드 PONG 확인"

echo "== 3) CLUSTER MEET (node1 기준으로 node2, node3를 소개) =="
"$CLI_BIN" -p "${PORTS[0]}" CLUSTER MEET 127.0.0.1 "${PORTS[1]}" "${CPORTS[1]}"
"$CLI_BIN" -p "${PORTS[0]}" CLUSTER MEET 127.0.0.1 "${PORTS[2]}" "${CPORTS[2]}"

echo "== 4) 가십이 전이적으로 퍼질 시간을 잠깐 대기 =="
sleep 2

echo "== 5) 16384 슬롯을 3등분해서 ADDSLOTSRANGE =="
"$CLI_BIN" -p "${PORTS[0]}" CLUSTER ADDSLOTSRANGE 0 5460
"$CLI_BIN" -p "${PORTS[1]}" CLUSTER ADDSLOTSRANGE 5461 10922
"$CLI_BIN" -p "${PORTS[2]}" CLUSTER ADDSLOTSRANGE 10923 16383

echo "== 6) 슬롯 할당이 가십으로 퍼질 시간을 잠깐 대기 =="
sleep 2

echo "== 7) 상태 확인 (node1에서 본 CLUSTER NODES) =="
"$CLI_BIN" -p "${PORTS[0]}" CLUSTER NODES

echo
echo "완료. 클러스터 인식 클라이언트로 접속해서 확인할 것:"
echo "  redisx-cli -c -p ${PORTS[0]} SET foo bar"
echo "  redisx-cli -c -p ${PORTS[0]} GET foo"
echo "멈추려면: ./stop_cluster.sh"
