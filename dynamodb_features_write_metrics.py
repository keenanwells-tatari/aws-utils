import subprocess
import json
import datetime
import math
import argparse

parser = argparse.ArgumentParser(description="Fetch DynamoDB write traffic metrics and translate to rows/sec")
parser.add_argument(
    "--tables", "-t",
    nargs="+",
    type=str,
    default=None,
    help="Query one or more specific table names. By default, queries all prod.* tables.",
)
args = parser.parse_args()

region = "us-east-1"
end_time = datetime.datetime.now(datetime.UTC)
start_time = end_time - datetime.timedelta(days=30)

# Step 1: Get tables to query
if args.tables:
    prod_tables = args.tables
    print(f"Fetching describe-table + CloudWatch metrics for {len(prod_tables)} table(s): {', '.join(prod_tables)}")
else:
    result = subprocess.run(
        ["aws", "dynamodb", "list-tables", "--region", region, "--output", "json"],
        capture_output=True, text=True
    )
    all_tables = json.loads(result.stdout)["TableNames"]
    prod_tables = [t for t in all_tables if t.startswith("prod.")]
    print(f"Fetching describe-table + CloudWatch metrics for {len(prod_tables)} prod tables...")
print(f"CloudWatch period: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
print()

table_data = []

for table in prod_tables:
    # Step 2: Get item count and size from describe-table
    result = subprocess.run(
        ["aws", "dynamodb", "describe-table", "--table-name", table,
        "--region", region, "--output", "json"],
        capture_output=True, text=True
    )
    info = json.loads(result.stdout)["Table"]
    size_bytes = info.get("TableSizeBytes", 0)
    item_count = info.get("ItemCount", 0)
    avg_item_bytes = size_bytes / item_count if item_count > 0 else 0

    # Step 3: Get ConsumedWriteCapacityUnits from CloudWatch (1-hour periods, last 30 days)
    result = subprocess.run([
        "aws", "cloudwatch", "get-metric-statistics",
        "--namespace", "AWS/DynamoDB",
        "--metric-name", "ConsumedWriteCapacityUnits",
        "--dimensions", f"Name=TableName,Value={table}",
        "--start-time", start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "--end-time", end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "--period", "3600",
        "--statistics", "Sum",
        "--region", region,
        "--output", "json"
    ], capture_output=True, text=True)

    cw_data = json.loads(result.stdout)
    datapoints = cw_data.get("Datapoints", [])

    if datapoints:
        avg_wcu_per_sec = (sum(dp["Sum"] for dp in datapoints) / len(datapoints)) / 3600
        peak_wcu_per_sec = max(dp["Sum"] for dp in datapoints) / 3600
    else:
        avg_wcu_per_sec = 0
        peak_wcu_per_sec = 0

    # Step 4: Derive rows/sec
    # DynamoDB WCU: 1 WCU = 1 write/sec of up to 1 KB
    # For items > 1 KB, each write consumes ceil(item_size / 1024) WCUs
    wcu_per_item = max(1, math.ceil(avg_item_bytes / 1024)) if avg_item_bytes > 0 else 1

    avg_write_rows_per_sec = avg_wcu_per_sec / wcu_per_item if wcu_per_item else 0
    peak_write_rows_per_sec = peak_wcu_per_sec / wcu_per_item if wcu_per_item else 0

    table_data.append({
        "table": table,
        "size_bytes": size_bytes,
        "item_count": item_count,
        "avg_item_bytes": avg_item_bytes,
        "wcu_per_item": wcu_per_item,
        "avg_wcu_per_sec": avg_wcu_per_sec,
        "peak_wcu_per_sec": peak_wcu_per_sec,
        "avg_write_rows_per_sec": avg_write_rows_per_sec,
        "peak_write_rows_per_sec": peak_write_rows_per_sec,
    })

# Sort by peak write rows/sec descending
table_data.sort(key=lambda t: t["peak_write_rows_per_sec"], reverse=True)

# Determine column width for table names
max_name_len = max((len(t["table"]) for t in table_data), default=30)
col_w = max(max_name_len + 2, 30)
row_w = col_w + 14 + 14 + 12 + 9 + 10 + 11 + 11 + 12 + 8  # column widths + spacing

# Print write throughput table
print("WRITE THROUGHPUT: WCU -> Rows/sec Translation")
print("=" * row_w)
print(f"{'Table':<{col_w}} {'Size (B)':>14} {'Items':>14} {'Avg Item(B)':>12} {'WCU/Item':>9} {'Avg WCU/s':>10} {'Peak WCU/s':>11} {'Avg Rows/s':>11} {'Peak Rows/s':>12}")
print("-" * row_w)

total_avg_rows = 0
total_peak_rows = 0
total_size_bytes = 0
total_items = 0

for t in table_data:
    print(
        f"{t['table']:<{col_w}} "
        f"{t['size_bytes']:>14,} "
        f"{t['item_count']:>14,} "
        f"{t['avg_item_bytes']:>12,.0f} "
        f"{t['wcu_per_item']:>9} "
        f"{t['avg_wcu_per_sec']:>10.2f} "
        f"{t['peak_wcu_per_sec']:>11.2f} "
        f"{t['avg_write_rows_per_sec']:>11.1f} "
        f"{t['peak_write_rows_per_sec']:>12.1f}"
    )
    total_avg_rows += t["avg_write_rows_per_sec"]
    total_peak_rows += t["peak_write_rows_per_sec"]
    total_size_bytes += t["size_bytes"]
    total_items += t["item_count"]

print("-" * row_w)
print(f"{'TOTAL':<{col_w}} {total_size_bytes:>14,} {total_items:>14,} {'':>12} {'':>9} {'':>10} {'':>11} {total_avg_rows:>11.1f} {total_peak_rows:>12.1f}")

print("\n\nNOTES:")
print("  - WCU per Item = ceil(avg_item_bytes / 1024)  [DynamoDB: 1 WCU = 1 write up to 1 KB]")
print("  - Avg/Peak WCU/s from CloudWatch ConsumedWriteCapacityUnits (1-hr periods, 30 days)")
print("  - Rows/s = WCU_per_sec / WCU_per_item")
print("  - 'Peak Rows/s' is per-table peak; tables don't necessarily peak simultaneously")
print("  - TOTAL Peak is a worst-case sum assuming all tables peak at the same time")
