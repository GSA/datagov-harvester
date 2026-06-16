#!/usr/bin/env python3

from __future__ import annotations

import glob
import json
import math
import re
import statistics
import sys
from pathlib import Path


def percentile_nearest_rank(values: list[float], percentile: float) -> float:
    """Return a nearest-rank percentile from an unsorted list."""
    if not values:
        raise ValueError("Cannot calculate a percentile without values")

    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def parse_tps(summary_path: Path) -> float | None:
    if not summary_path.exists():
        return None

    content = summary_path.read_text(encoding="utf-8")

    match = re.search(
        r"^tps = ([0-9]+(?:\.[0-9]+)?)",
        content,
        flags=re.MULTILINE,
    )

    return float(match.group(1)) if match else None


def main() -> int:
    if len(sys.argv) != 8:
        print(
            "Usage: summarize_pgbench.py "
            "<run-dir> <phase> <case-key> <query-label> "
            "<test-case-label> <clients> <threads>",
            file=sys.stderr,
        )
        return 1

    run_dir = Path(sys.argv[1])
    phase = sys.argv[2]
    case_key = sys.argv[3]
    query_label = sys.argv[4]
    test_case_label = sys.argv[5]
    clients = int(sys.argv[6])
    threads = int(sys.argv[7])

    latencies_us: list[float] = []
    failed_transactions = 0

    # With multiple pgbench worker threads, pgbench creates multiple log files.
    for filename in glob.glob(str(run_dir / "pgbench.*")):
        path = Path(filename)

        if not path.is_file():
            continue

        for line in path.read_text(encoding="utf-8").splitlines():
            fields = line.split()

            if len(fields) < 3:
                continue

            elapsed = fields[2]

            try:
                latencies_us.append(float(elapsed))
            except ValueError:
                # Values such as "failed", "deadlock", or "serialization".
                failed_transactions += 1

    if not latencies_us:
        print("No successful transaction latencies found.", file=sys.stderr)
        return 1

    # pgbench logs elapsed transaction time in microseconds.
    latencies_ms = [value / 1_000 for value in latencies_us]

    metrics = {
        "phase": phase,
        "case_key": case_key,
        "query": query_label,
        "test_case": test_case_label,
        "clients": clients,
        "threads": threads,
        "successful_transactions": len(latencies_ms),
        "failed_transactions": failed_transactions,
        "median_ms": statistics.median(latencies_ms),
        "p95_ms": percentile_nearest_rank(latencies_ms, 0.95),
        "mean_ms": statistics.fmean(latencies_ms),
        "minimum_ms": min(latencies_ms),
        "maximum_ms": max(latencies_ms),
        "tps": parse_tps(run_dir / "summary.txt"),
    }

    output_path = run_dir / "metrics.json"
    output_path.write_text(
        json.dumps(metrics, indent=2) + "\n",
        encoding="utf-8",
    )

    print()
    print(f"Results written to {output_path}")
    print(f"Median: {metrics['median_ms']:.3f} ms")
    print(f"P95:    {metrics['p95_ms']:.3f} ms")
    print(f"Mean:   {metrics['mean_ms']:.3f} ms")
    print(f"Min:    {metrics['minimum_ms']:.3f} ms")
    print(f"Max:    {metrics['maximum_ms']:.3f} ms")

    if metrics["tps"] is not None:
        print(f"TPS:    {metrics['tps']:.3f}")

    if failed_transactions:
        print(f"Failed: {failed_transactions}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
