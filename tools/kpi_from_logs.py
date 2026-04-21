#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


def read_text(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8", errors="ignore")


def parse_metrics(client_log: str, server_log: str) -> dict:
    missing_packets = sum(int(x) for x in re.findall(r"missing start_seq\s*=\s*\d+\s*count\s*=\s*(\d+)", client_log))
    request_packets = len(re.findall(r"\[RetransRequest\] request seq=", client_log))
    timeout_packets = len(re.findall(r"\[timeout\] skip seq=", client_log))
    resent_packets = len(re.findall(r"\[RetransServer\] resend ok, seq=", server_log))

    recovered_snapshots = [int(x) for x in re.findall(r"recovered_packets=(\d+)", client_log)]
    recovered_packets = max(recovered_snapshots) if recovered_snapshots else 0

    recovery_rate = (recovered_packets / missing_packets) if missing_packets > 0 else 1.0
    timeout_rate = (timeout_packets / missing_packets) if missing_packets > 0 else 0.0

    return {
        "missing_packets_total": missing_packets,
        "request_packets_total": request_packets,
        "resent_packets_total": resent_packets,
        "recovered_packets_total": recovered_packets,
        "timeout_packets_total": timeout_packets,
        "recovery_rate": round(recovery_rate, 6),
        "timeout_rate": round(timeout_rate, 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract retransmission KPI from server/client logs.")
    parser.add_argument("--client-log", required=True)
    parser.add_argument("--server-log", required=True)
    parser.add_argument("--out", default="kpi_report.json")
    args = parser.parse_args()

    client_text = read_text(args.client_log)
    server_text = read_text(args.server_log)

    metrics = parse_metrics(client_text, server_text)

    out_path = Path(args.out)
    out_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
