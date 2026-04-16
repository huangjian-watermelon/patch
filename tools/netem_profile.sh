#!/usr/bin/env bash
set -euo pipefail

# 示例:
#   sudo tools/netem_profile.sh apply eth0
#   sudo tools/netem_profile.sh clear eth0
#
# 默认注入:
#   loss 1%, delay 20ms 5ms, reorder 5% 25%

ACTION="${1:-}"
IFACE="${2:-eth0}"

if [[ -z "${ACTION}" ]]; then
  echo "Usage: $0 <apply|clear> <iface>"
  exit 1
fi

if [[ "${ACTION}" == "apply" ]]; then
  tc qdisc replace dev "${IFACE}" root netem loss 1% delay 20ms 5ms reorder 5% 25%
  tc -s qdisc show dev "${IFACE}"
elif [[ "${ACTION}" == "clear" ]]; then
  tc qdisc del dev "${IFACE}" root || true
  tc -s qdisc show dev "${IFACE}"
else
  echo "Unknown action: ${ACTION}"
  exit 1
fi
