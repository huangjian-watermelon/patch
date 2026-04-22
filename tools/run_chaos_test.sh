#!/usr/bin/env bash
set -euo pipefail

# 用法:
#   tools/run_chaos_test.sh [seconds]
# 依赖:
#   - forwarder / retrans / client 二进制可执行
#   - forwarder.json / retrans.json / client.json 配置存在
# 结果:
#   - logs/server.log logs/client.log
#   - logs/kpi_report.json

DURATION_SEC="${1:-60}"
LOG_DIR="logs"
mkdir -p "${LOG_DIR}"

SERVER_LOG="${LOG_DIR}/server.log"
FORWARDER_LOG="${LOG_DIR}/forwarder.log"
CLIENT_LOG="${LOG_DIR}/client.log"

: > "${SERVER_LOG}"
: > "${FORWARDER_LOG}"
: > "${CLIENT_LOG}"

./retrans retrans.json >"${SERVER_LOG}" 2>&1 &
RETRANS_PID=$!

sleep 1

./forwarder forwarder.json >"${FORWARDER_LOG}" 2>&1 &
FORWARDER_PID=$!

sleep 1

./client client.json >"${CLIENT_LOG}" 2>&1 &
CLIENT_PID=$!

cleanup() {
  kill "${CLIENT_PID}" >/dev/null 2>&1 || true
  kill "${FORWARDER_PID}" >/dev/null 2>&1 || true
  kill "${RETRANS_PID}" >/dev/null 2>&1 || true
  wait "${CLIENT_PID}" >/dev/null 2>&1 || true
  wait "${FORWARDER_PID}" >/dev/null 2>&1 || true
  wait "${RETRANS_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep "${DURATION_SEC}"

cleanup
trap - EXIT

python3 tools/kpi_from_logs.py \
  --client-log "${CLIENT_LOG}" \
  --server-log "${SERVER_LOG}" \
  --out "${LOG_DIR}/kpi_report.json"

echo "KPI written to ${LOG_DIR}/kpi_report.json"
