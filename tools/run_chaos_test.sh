#!/usr/bin/env bash
set -euo pipefail

# 用法:
#   tools/run_chaos_test.sh [seconds]
# 依赖:
#   - patchServer / tsRequestClient 二进制可执行
#   - patch_server.json / ts_request_client.json 配置存在
# 结果:
#   - logs/server.log logs/client.log
#   - logs/kpi_report.json

DURATION_SEC="${1:-60}"
LOG_DIR="logs"
mkdir -p "${LOG_DIR}"

SERVER_LOG="${LOG_DIR}/server.log"
CLIENT_LOG="${LOG_DIR}/client.log"

: > "${SERVER_LOG}"
: > "${CLIENT_LOG}"

./patchServer patch_server.json >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

sleep 1

./tsRequestClient ts_request_client.json >"${CLIENT_LOG}" 2>&1 &
CLIENT_PID=$!

cleanup() {
  kill "${CLIENT_PID}" >/dev/null 2>&1 || true
  kill "${SERVER_PID}" >/dev/null 2>&1 || true
  wait "${CLIENT_PID}" >/dev/null 2>&1 || true
  wait "${SERVER_PID}" >/dev/null 2>&1 || true
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
