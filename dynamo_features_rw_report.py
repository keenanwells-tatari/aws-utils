#!/usr/bin/env python3
"""
DynamoDB Read/Write Report for DynamoDB tables.

Queries CloudWatch for ConsumedReadCapacityUnits and ConsumedWriteCapacityUnits
and produces a per-table and per-environment summary in GB/day.

By default queries all {prod,staging,dev}.tatari_features.* tables.
Use --tables to specify short table names (without the env prefix); they will
be looked up as {prod,staging,dev}.tatari_features.<name> across each environment.

Usage:
    # Default: all tatari_features tables
    python3 dynamo_features_rw_report.py [--region REGION] [--hours HOURS]

    # Specific tables (short names — looked up across all envs)
    python3 dynamo_features_rw_report.py --tables linear_performance_history broadcast_calendar

    # Mix with other flags
    python3 dynamo_features_rw_report.py --tables squares_cubes --hours 48

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
    parser = argparse.ArgumentParser(description="DynamoDB R/W report")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--hours", type=int, default=720, help="Lookback window in hours (default: 720)")
    parser.add_argument("--tables", nargs="+", metavar="TABLE",
                        help="Short table names (without env prefix). Each name is "
                             "expanded to {prod,staging,dev}.tatari_features.<name> "
                             "and looked up across all environments. "
                             "If omitted, discovers all tatari_features tables.")
    parser.add_argument("--envs", nargs="+", default=["prod", "staging", "dev"],
                        metavar="ENV",
                        help="Environments to query (default: prod staging dev)")
    args = parser.parse_args()

    region = args.region
    hours = args.hours
    envs = args.envs

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    period = hours * 3600  # single datapoint spanning the full window

    if args.tables:
        # Expand short names to full table names across each env
        tables = sorted(
            f"{env}.tatari_features.{name}"
            for env in envs
            for name in args.tables
        )
        print(f"Expanding {len(args.tables)} table(s) across {len(envs)} env(s) "
              f"→ {len(tables)} lookups.", file=sys.stderr)
    else:
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

    # Group tables by environment prefix, falling back to "OTHER" --------
    groups: dict[str, dict[str, dict]] = {}

    for table, m in results.items():
        env = next((e for e in envs if table.startswith(f"{e}.")), "other")
        groups.setdefault(env, {})[table] = m

    grand = {"read_cu": 0, "write_cu": 0, "read_gb": 0, "write_gb": 0}

    # Print in a stable order: requested envs first, then "other"
    group_order = [e for e in (*envs, "other") if e in groups]

    for env in group_order:
        env_tables = groups[env]
        # Strip env.tatari_features. prefix for display
        common_prefix = f"{env}.tatari_features." if env != "other" else ""

        print(f"\n{'─' * len(hdr)}")
        print(f"  {'Group' if env == 'other' else 'Environment'}: {env.upper()}")
        print(f"{'─' * len(hdr)}")
        print(hdr)
        print(sep)

        env_totals = {"read_cu": 0, "write_cu": 0, "read_gb": 0, "write_gb": 0}

        for table in sorted(env_tables):
            m = env_tables[table]
            short = table[len(common_prefix):] if common_prefix else table
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
    label = "GRAND TOTAL"
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


# linear_performance_history audience_similarity_score_streaming streaming_performance_history streaming_performance_history_by_creative
# total 5697.6606 GB