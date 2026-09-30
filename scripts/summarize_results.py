#!/usr/bin/env python3
"""Combine raw benchmark CSVs and compute descriptive statistics."""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: summarize_results.py RESULT_ROOT")
    root = Path(sys.argv[1])
    inputs = sorted(root.rglob("benchmark_results_*.csv"))
    if not inputs:
        raise SystemExit(f"No benchmark result CSVs found under {root}")

    rows = []
    fieldnames = None
    for path in inputs:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            fieldnames = fieldnames or reader.fieldnames
            rows.extend(reader)

    failed = [row for row in rows if row["success"].lower() != "true"]
    if failed:
        raise SystemExit(f"{len(failed)} failed trial(s) found; refusing to summarize")

    raw_path = root / "combined_raw.csv"
    with open(raw_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    groups = defaultdict(list)
    for row in rows:
        key = (
            row["runtime"], row["storage_backend"], row["workload"],
            int(row["workload_size"]),
        )
        groups[key].append(row)

    summary_fields = [
        "runtime", "storage_backend", "workload", "workload_size", "n",
        "write_time_mean_s", "write_time_stdev_s", "write_time_median_s",
        "read_time_mean_s", "read_time_stdev_s", "read_time_median_s",
        "total_time_mean_s", "total_time_stdev_s", "total_time_median_s",
        "write_throughput_mean_rps", "read_throughput_mean_rps",
    ]
    summary_path = root / "summary.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=summary_fields)
        writer.writeheader()
        for key in sorted(groups):
            group = groups[key]
            write = [float(row["write_time_s"]) for row in group]
            read = [float(row["read_time_s"]) for row in group]
            total = [float(row["total_time_s"]) for row in group]
            write_rps = [float(row["write_throughput_rps"]) for row in group]
            read_rps = [float(row["read_throughput_rps"]) for row in group]
            writer.writerow({
                "runtime": key[0],
                "storage_backend": key[1],
                "workload": key[2],
                "workload_size": key[3],
                "n": len(group),
                "write_time_mean_s": statistics.mean(write),
                "write_time_stdev_s": statistics.stdev(write) if len(write) > 1 else 0.0,
                "write_time_median_s": statistics.median(write),
                "read_time_mean_s": statistics.mean(read),
                "read_time_stdev_s": statistics.stdev(read) if len(read) > 1 else 0.0,
                "read_time_median_s": statistics.median(read),
                "total_time_mean_s": statistics.mean(total),
                "total_time_stdev_s": statistics.stdev(total) if len(total) > 1 else 0.0,
                "total_time_median_s": statistics.median(total),
                "write_throughput_mean_rps": statistics.mean(write_rps),
                "read_throughput_mean_rps": statistics.mean(read_rps),
            })

    print(f"raw_rows={len(rows)}")
    print(f"conditions={len(groups)}")
    print(f"combined={raw_path}")
    print(f"summary={summary_path}")


if __name__ == "__main__":
    main()
