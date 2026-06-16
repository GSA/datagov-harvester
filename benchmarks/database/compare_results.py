#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def read_metrics(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def format_ms(value: float) -> str:
    return f"{value:.2f} ms"


def main() -> int:
    results_root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")

    before_root = results_root / "before"
    after_root = results_root / "after"

    if not before_root.exists() or not after_root.exists():
        print(
            "Both results/before and results/after must exist.",
            file=sys.stderr,
        )
        return 1

    rows: list[str] = []

    for before_file in sorted(before_root.glob("*/metrics.json")):
        case_key = before_file.parent.name
        after_file = after_root / case_key / "metrics.json"

        if not after_file.exists():
            print(
                f"Skipping {case_key}: missing after metrics",
                file=sys.stderr,
            )
            continue

        before = read_metrics(before_file)
        after = read_metrics(after_file)

        before_median = float(before["median_ms"])
        after_median = float(after["median_ms"])

        improvement = (
            ((before_median - after_median) / before_median) * 100
            if before_median
            else 0
        )

        rows.append(
            "| "
            + " | ".join(
                [
                    str(before["query"]),
                    str(before["test_case"]),
                    format_ms(before_median),
                    format_ms(after_median),
                    f"{improvement:+.1f}%",
                    format_ms(float(before["p95_ms"])),
                    format_ms(float(after["p95_ms"])),
                    "—",
                ]
            )
            + " |"
        )

    print(
        "| Query | Test case | Before median | After median | "
        "Change | Before p95 | After p95 | Plan change |"
    )
    print("|---|---|---:|---:|---:|---:|---:|---|")

    for row in rows:
        print(row)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
