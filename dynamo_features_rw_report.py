#!/usr/bin/env python3
"""
DynamoDB Read/Write Report for tatari_features tables.

Queries CloudWatch for ConsumedReadCapacityUnits and ConsumedWriteCapacityUnits
across all {prod,staging,dev}.tatari_features.* tables and produces a
per-table and per-environment summary in GB/day.

Usage:
    python3 dynamo_features_rw_report.py [--region REGION] [--hours HOURS]

Requirements:
    - AWS CLI configured with appropriate credentials
    - Python 3.7+
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone


def run_aws(cmd: list[str]) -> dict:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: {' '.join(cmd)}\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def get_matching_tables(region: str) -> list[str]:
    data = run_aws(["aws", "dynamodb", "list-tables", "--region", region, "--output", "json"])
    prefixes = ("prod.tatari_features", "staging.tatari_features", "dev.tatari_features")
    return sorted(t for t in data["TableNames"] if t.startswith(prefixes))


def get_metric(region: str, table: str, metric_name: str,
               start_time: str, end_time: str, period: int) -> float:
    data = run_aws([
        "aws", "cloudwatch", "get-metric-statistics",
        "--region", region,
        "--namespace", "AWS/DynamoDB",
        "--metric-name", metric_name,
        "--dimensions", f"Name=TableName,Value={table}",
        "--start-time", start_time,
        "--end-time", end_time,
        "--period", str(period),
        "--statistics", "Sum",
        "--output", "json",
    ])
    return sum(dp.get("Sum", 0.0) for dp in data.get("Datapoints", []))


def main():
    parser = argparse.ArgumentParser(description="DynamoDB tatari_features R/W report")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--hours", type=int, default=720, help="Lookback window in hours (default: 24)")
    args = parser.parse_args()

    region = args.region
    hours = args.hours

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    period = hours * 3600  # single datapoint spanning the full window

    print(f"Fetching tables from {region}...", file=sys.stderr)
    tables = get_matching_tables(region)
    print(f"Found {len(tables)} tables. Querying CloudWatch metrics...", file=sys.stderr)

    # Collect metrics ---------------------------------------------------------
    # { table: { "read_cu": float, "write_cu": float, "read_gb": float, "write_gb": float } }
    results: dict[str, dict] = {}

    for i, table in enumerate(tables, 1):
        print(f"  [{i}/{len(tables)}] {table}", file=sys.stderr)

        read_cu = get_metric(region, table, "ConsumedReadCapacityUnits",
                             start_str, end_str, period)
        write_cu = get_metric(region, table, "ConsumedWriteCapacityUnits",
                              start_str, end_str, period)

        # 1 WCU = 1 KB,  1 RCU = 4 KB
        results[table] = {
            "read_cu": read_cu,
            "write_cu": write_cu,
            "read_gb": (read_cu * 4.0) / (1024 * 1024),   # RCU * 4KB -> GB
            "write_gb": (write_cu * 1.0) / (1024 * 1024),  # WCU * 1KB -> GB
        }

    # Report ------------------------------------------------------------------
    col_table = 62
    hdr = (f"  {'Table':<{col_table}} {'Read CUs':>14} {'Read GB':>10}"
           f" {'Write CUs':>14} {'Write GB':>10} {'Total GB':>10}")
    sep = (f"  {'─' * col_table} {'─' * 14} {'─' * 10}"
           f" {'─' * 14} {'─' * 10} {'─' * 10}")

    window_label = f"{hours}h" if hours != 24 else "day"
    print()
    print("=" * len(hdr))
    print(f"  DynamoDB Read / Write Report  —  Last {hours} hours")
    print(f"  Window: {start_str}  →  {end_str}")
    print("=" * len(hdr))

    grand = {"read_cu": 0, "write_cu": 0, "read_gb": 0, "write_gb": 0}

    for env in ("prod", "staging", "dev"):
        prefix = f"{env}.tatari_features"
        env_tables = {k: v for k, v in results.items() if k.startswith(prefix)}
        if not env_tables:
            continue

        print(f"\n{'─' * len(hdr)}")
        print(f"  Environment: {env.upper()}")
        print(f"{'─' * len(hdr)}")
        print(hdr)
        print(sep)

        env_totals = {"read_cu": 0, "write_cu": 0, "read_gb": 0, "write_gb": 0}

        for table in sorted(env_tables):
            m = env_tables[table]
            short = table.replace(f"{prefix}.", "")
            total_gb = m["read_gb"] + m["write_gb"]

            for k in env_totals:
                env_totals[k] += m[k]

            fmt_rcu = f"{m['read_cu']:>14,.0f}" if m['read_cu'] else f"{'0':>14}"
            fmt_wcu = f"{m['write_cu']:>14,.0f}" if m['write_cu'] else f"{'0':>14}"
            fmt_rgb = f"{m['read_gb']:>10.4f}" if m['read_gb'] else f"{'0.0000':>10}"
            fmt_wgb = f"{m['write_gb']:>10.4f}" if m['write_gb'] else f"{'0.0000':>10}"
            fmt_tgb = f"{total_gb:>10.4f}" if total_gb else f"{'0.0000':>10}"

            print(f"  {short:<{col_table}} {fmt_rcu} {fmt_rgb} {fmt_wcu} {fmt_wgb} {fmt_tgb}")

        print(sep)
        env_total_gb = env_totals["read_gb"] + env_totals["write_gb"]
        label = f"SUBTOTAL ({env.upper()})"
        print(f"  {label:<{col_table}} {env_totals['read_cu']:>14,.0f} {env_totals['read_gb']:>10.4f}"
              f" {env_totals['write_cu']:>14,.0f} {env_totals['write_gb']:>10.4f}"
              f" {env_total_gb:>10.4f}")

        for k in grand:
            grand[k] += env_totals[k]

    grand_total_gb = grand["read_gb"] + grand["write_gb"]
    print(f"\n{'=' * len(hdr)}")
    label = "GRAND TOTAL (ALL ENVIRONMENTS)"
    print(f"  {label:<{col_table}} {grand['read_cu']:>14,.0f} {grand['read_gb']:>10.4f}"
          f" {grand['write_cu']:>14,.0f} {grand['write_gb']:>10.4f}"
          f" {grand_total_gb:>10.4f}")
    print(f"{'=' * len(hdr)}")
    print(f"\n  Notes:")
    print(f"    • 1 WCU  = 1 KB written   → Write GB = WCUs  / 1,048,576")
    print(f"    • 1 RCU  = 4 KB read       → Read GB  = RCUs × 4 / 1,048,576")
    print(f"    • RCUs reflect strongly-consistent-equivalent units")
    print(f"      (eventually consistent reads consume 0.5 RCU per 4 KB)")
    print()


if __name__ == "__main__":
    main()