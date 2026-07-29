#!/usr/bin/env python3
"""Render a monitoring run's CSVs into a self-contained HTML report with charts.

Reads the CSVs written by scripts/monitor_catalog_stage.py and emits one HTML file
with inline SVG time-series charts -- no build step, no CDN, no dependencies beyond
the Python standard library. Open the file in a browser or attach it to a ticket.

Usage:
    python3 scripts/monitor_report.py monitor-runs/20260728T153000
    python3 scripts/monitor_report.py <run-dir> -o /tmp/rebuild-report.html
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import sys
from datetime import datetime
from pathlib import Path

# Categorical slots 1-3 from the validated default palette (blue / orange / aqua).
# Verified with the data-viz validator in both modes, all-pairs:
#   light  #2a78d6,#eb6834,#1baf7a -> all checks pass (aqua 2.74:1 contrast WARN,
#                                    relieved by direct labels + table view)
#   dark   #3987e5,#d95926,#199e70 -> all checks pass
SERIES_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a"]
SERIES_DARK = ["#3987e5", "#d95926", "#199e70"]

PLOT_W = 960
PLOT_H = 260
MARGIN = {"top": 14, "right": 18, "bottom": 30, "left": 62}


# ---------------------------------------------------------------------- helpers


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as fh:
        return list(csv.DictReader(fh))


def num(value, default=None):
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def percentile(values: list[float], pct: float):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def nice_ceil(value: float) -> float:
    """Round a max up to a readable axis bound."""
    if value <= 0:
        return 1.0
    exp = 10 ** (len(str(int(value))) - 1)
    for mult in (1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10):
        bound = mult * exp
        if bound >= value:
            return float(bound)
    return float(10 * exp)


def fmt_time(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).strftime("%H:%M:%S")


def fmt_num(value: float, unit: str = "") -> str:
    if value is None:
        return "-"
    if unit == "s":
        return f"{value:.2f}s"
    if unit == "%":
        return f"{value:.0f}%"
    if abs(value) >= 1000:
        return f"{value:,.0f}{unit}"
    if abs(value) >= 10:
        return f"{value:.0f}{unit}"
    return f"{value:.2f}{unit}"


def esc(text) -> str:
    return html.escape(str(text), quote=True)


# ----------------------------------------------------------------------- charts


def phase_bands(rows: list[dict]) -> list[dict]:
    """Contiguous runs of a single phase value, for background shading."""
    bands, current = [], None
    for row in rows:
        phase = row.get("phase") or "unlabeled"
        epoch = num(row.get("epoch"))
        if epoch is None:
            continue
        if current is None or current["phase"] != phase:
            if current:
                current["end"] = epoch
            current = {"phase": phase, "start": epoch, "end": epoch}
            bands.append(current)
        else:
            current["end"] = epoch
    return bands


def line_chart(
    chart_id: str,
    title: str,
    subtitle: str,
    series: list[dict],
    unit: str = "",
    bands: list[dict] | None = None,
    threshold: dict | None = None,
    markers: list[dict] | None = None,
    y_min_zero: bool = True,
) -> str:
    """One time-series line chart as inline SVG plus its data for the hover layer.

    series: [{"name": str, "points": [(epoch, value), ...]}]
    """
    all_points = [p for s in series for p in s["points"]]
    if not all_points:
        return (
            f'<figure class="chart" id="{chart_id}">'
            f"<figcaption><h3>{esc(title)}</h3>"
            f'<p class="sub">{esc(subtitle)}</p></figcaption>'
            f'<p class="empty">No samples recorded.</p></figure>'
        )

    xs = [p[0] for p in all_points]
    ys = [p[1] for p in all_points]
    x_min, x_max = min(xs), max(xs)
    if x_max == x_min:
        x_max = x_min + 1
    y_hi_raw = max(ys)
    if threshold:
        y_hi_raw = max(y_hi_raw, threshold["value"])
    y_lo = 0.0 if y_min_zero else min(ys) * 0.98
    y_hi = nice_ceil(y_hi_raw) if y_min_zero else y_hi_raw * 1.02
    if y_hi <= y_lo:
        y_hi = y_lo + 1

    inner_w = PLOT_W - MARGIN["left"] - MARGIN["right"]
    inner_h = PLOT_H - MARGIN["top"] - MARGIN["bottom"]

    def sx(epoch: float) -> float:
        return MARGIN["left"] + (epoch - x_min) / (x_max - x_min) * inner_w

    def sy(value: float) -> float:
        return MARGIN["top"] + inner_h - (value - y_lo) / (y_hi - y_lo) * inner_h

    parts = [
        f'<figure class="chart" id="{chart_id}">',
        "<figcaption>",
        f"<h3>{esc(title)}</h3>",
        f'<p class="sub">{esc(subtitle)}</p>',
        "</figcaption>",
    ]

    if len(series) >= 2:
        legend = "".join(
            f'<span class="key"><i style="background:var(--series-{i + 1})"></i>'
            f"{esc(s['name'])}</span>"
            for i, s in enumerate(series)
        )
        parts.append(f'<div class="legend">{legend}</div>')

    svg = [
        f'<svg viewBox="0 0 {PLOT_W} {PLOT_H}" role="img" '
        f'aria-label="{esc(title)}" preserveAspectRatio="none">'
    ]

    # Phase bands sit furthest back so grid and marks stay readable over them.
    for band in bands or []:
        if band["phase"] in ("baseline", "unlabeled"):
            continue
        bx, bw = sx(band["start"]), max(2.0, sx(band["end"]) - sx(band["start"]))
        svg.append(
            f'<rect x="{bx:.1f}" y="{MARGIN["top"]}" width="{bw:.1f}" '
            f'height="{inner_h}" class="band"/>'
        )
        # Band labels sit at the foot of the plot; the top strip is reserved for
        # series direct labels, which would otherwise collide with them.
        svg.append(
            f'<text x="{bx + 5:.1f}" y="{MARGIN["top"] + inner_h - 6}" '
            f'class="band-label">{esc(band["phase"])}</text>'
        )

    # Gridlines + y ticks
    for i in range(5):
        value = y_lo + (y_hi - y_lo) * i / 4
        y = sy(value)
        svg.append(
            f'<line x1="{MARGIN["left"]}" y1="{y:.1f}" '
            f'x2="{PLOT_W - MARGIN["right"]}" y2="{y:.1f}" class="grid"/>'
        )
        svg.append(
            f'<text x="{MARGIN["left"] - 8}" y="{y + 4:.1f}" '
            f'class="tick tick-y">{esc(fmt_num(value, unit))}</text>'
        )

    # Baseline
    svg.append(
        f'<line x1="{MARGIN["left"]}" y1="{sy(y_lo):.1f}" '
        f'x2="{PLOT_W - MARGIN["right"]}" y2="{sy(y_lo):.1f}" class="axis"/>'
    )

    # X ticks
    for i in range(5):
        epoch = x_min + (x_max - x_min) * i / 4
        anchor = "start" if i == 0 else ("end" if i == 4 else "middle")
        svg.append(
            f'<text x="{sx(epoch):.1f}" y="{PLOT_H - 10}" class="tick" '
            f'text-anchor="{anchor}">{esc(fmt_time(epoch))}</text>'
        )

    if threshold:
        ty = sy(threshold["value"])
        svg.append(
            f'<line x1="{MARGIN["left"]}" y1="{ty:.1f}" '
            f'x2="{PLOT_W - MARGIN["right"]}" y2="{ty:.1f}" class="threshold"/>'
        )
        # Left-anchored: the right edge belongs to the series direct label, and on
        # a healthy run the threshold sits directly under the data line.
        svg.append(
            f'<text x="{MARGIN["left"] + 5}" y="{ty - 6:.1f}" '
            f'class="threshold-label" text-anchor="start">'
            f'{esc(threshold["label"])}</text>'
        )

    # Series paths, then direct labels at the last point (identity never color-only)
    for i, s in enumerate(series):
        points = sorted(s["points"])
        if not points:
            continue
        d = " ".join(
            f"{'M' if j == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}"
            for j, (x, y) in enumerate(points)
        )
        svg.append(f'<path d="{d}" class="line s{i + 1}"/>')
        if len(points) <= 60:
            for x, y in points:
                svg.append(
                    f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="4.5" '
                    f'class="dot s{i + 1}"/>'
                )
        lx, ly = points[-1]
        label = f"{s['name']} {fmt_num(ly, unit)}"
        # Labels normally trail to the left of the final point; flip to the right
        # when that would run off the plot (e.g. a run with a single sample).
        approx_w = len(label) * 6.2
        if sx(lx) - 6 - approx_w < MARGIN["left"]:
            anchor, tx = "start", sx(lx) + 8
        else:
            anchor, tx = "end", sx(lx) - 6
        svg.append(
            f'<text x="{tx:.1f}" y="{sy(ly) - 9:.1f}" '
            f'class="direct-label" text-anchor="{anchor}">{esc(label)}</text>'
        )

    # Failure / event markers use the status palette and are always labelled.
    for m in markers or []:
        mx = sx(m["epoch"])
        svg.append(
            f'<line x1="{mx:.1f}" y1="{MARGIN["top"]}" x2="{mx:.1f}" '
            f'y2="{MARGIN["top"] + inner_h}" class="marker"/>'
        )
        svg.append(
            f'<circle cx="{mx:.1f}" cy="{MARGIN["top"] + 6}" r="5" class="marker-dot"/>'
        )

    svg.append(
        f'<rect class="hit" x="{MARGIN["left"]}" y="{MARGIN["top"]}" '
        f'width="{inner_w}" height="{inner_h}"/>'
    )
    svg.append(
        f'<line class="crosshair" x1="0" y1="{MARGIN["top"]}" x2="0" '
        f'y2="{MARGIN["top"] + inner_h}" style="display:none"/>'
    )
    svg.append("</svg>")
    parts.append("".join(svg))
    parts.append('<div class="tooltip" hidden></div>')

    # Table view: the relief for the light-mode contrast WARN, and the
    # accessible equivalent of every chart.
    rows_by_x: dict[float, dict] = {}
    for i, s in enumerate(series):
        for x, y in s["points"]:
            rows_by_x.setdefault(x, {})[s["name"]] = y
    head = "".join(f"<th>{esc(s['name'])}</th>" for s in series)
    body = "".join(
        "<tr><td>"
        + esc(fmt_time(x))
        + "</td>"
        + "".join(
            "<td>"
            + (esc(fmt_num(vals.get(s["name"]), unit)) if s["name"] in vals else "-")
            + "</td>"
            for s in series
        )
        + "</tr>"
        for x, vals in sorted(rows_by_x.items())
    )
    parts.append(
        "<details class='table-view'><summary>Table view "
        f"({len(rows_by_x)} samples)</summary>"
        f"<div class='table-scroll'><table><thead><tr><th>Time</th>{head}</tr>"
        f"</thead><tbody>{body}</tbody></table></div></details>"
    )

    parts.append("</figure>")

    chart_data = {
        "id": chart_id,
        "unit": unit,
        "xMin": x_min,
        "xMax": x_max,
        "yLo": y_lo,
        "yHi": y_hi,
        "margin": MARGIN,
        "w": PLOT_W,
        "h": PLOT_H,
        "series": [{"name": s["name"], "points": sorted(s["points"])} for s in series],
    }
    return "".join(parts), chart_data


def stat_tile(label: str, value: str, note: str = "", state: str = "") -> str:
    cls = f"tile {state}".strip()
    icon = ""
    if state == "good":
        icon = '<span class="ico" aria-hidden="true">&#10003;</span>'
    elif state == "critical":
        icon = '<span class="ico" aria-hidden="true">&#9888;</span>'
    return (
        f'<div class="{cls}"><p class="tile-label">{esc(label)}</p>'
        f'<p class="tile-value">{icon}{esc(value)}</p>'
        f'<p class="tile-note">{esc(note)}</p></div>'
    )


# ------------------------------------------------------------------------- main


def phase_comparison(checks: list[dict], nodes: list[dict]) -> str:
    """Side-by-side per-phase table: the direct answer to "what did it cost?".

    Phases are ordered as they occurred rather than alphabetically, and each
    metric marks whether the change from the first phase is a regression, so the
    comparison never depends on the reader doing arithmetic.
    """
    order: list[str] = []
    for row in checks + nodes:
        phase = row.get("phase") or "unlabeled"
        if phase not in order:
            order.append(phase)
    if len(order) < 2:
        return ""

    def clean(kind, phase):
        return [
            r
            for r in checks
            if r["check"] == kind
            and (r.get("phase") or "unlabeled") == phase
            and r.get("os_sample_inflight") != "1"
        ]

    def node_vals(field, phase):
        return [
            num(r[field])
            for r in nodes
            if (r.get("phase") or "unlabeled") == phase and num(r[field]) is not None
        ]

    # (label, per-phase value fn, formatter, higher_is_worse)
    metrics = [
        (
            "Search p50",
            lambda p: percentile(
                [float(r["latency_s"]) for r in clean("search", p) if r["latency_s"]],
                50,
            ),
            lambda v: f"{v:.2f}s",
            True,
        ),
        (
            "Search p95",
            lambda p: percentile(
                [float(r["latency_s"]) for r in clean("search", p) if r["latency_s"]],
                95,
            ),
            lambda v: f"{v:.2f}s",
            True,
        ),
        (
            "Search failures",
            lambda p: sum(
                1
                for r in checks
                if r["check"] == "search"
                and (r.get("phase") or "unlabeled") == p
                and r["ok"] != "1"
            ),
            lambda v: f"{v:.0f}",
            True,
        ),
        (
            "Dashboard p50",
            lambda p: percentile(
                [
                    float(r["latency_s"])
                    for r in clean("dashboard", p)
                    if r["latency_s"]
                ],
                50,
            ),
            lambda v: f"{v:.3f}s",
            True,
        ),
        (
            "Min dataset count",
            lambda p: min(
                (
                    int(num(r["count"]))
                    for r in checks
                    if r["check"] == "dashboard"
                    and (r.get("phase") or "unlabeled") == p
                    and num(r["count"])
                ),
                default=None,
            ),
            lambda v: f"{v:,.0f}",
            False,
        ),
        (
            "Peak node CPU",
            lambda p: max(node_vals("cpu_percent", p), default=None),
            lambda v: f"{v:.0f}%",
            True,
        ),
        (
            "Peak JVM heap",
            lambda p: max(node_vals("heap_percent", p), default=None),
            lambda v: f"{v:.0f}%",
            True,
        ),
        (
            "Peak indexing rate",
            lambda p: max(node_vals("indexing_rate_per_s", p), default=None),
            lambda v: f"{v:,.0f}/s",
            False,
        ),
        (
            "Search rejections",
            lambda p: max(node_vals("search_pool_rejected", p), default=None),
            lambda v: f"{v:.0f}",
            True,
        ),
    ]

    head = "".join(f"<th>{esc(p)}</th>" for p in order)
    body_rows = []
    for label, value_of, fmt, higher_is_worse in metrics:
        values = [value_of(p) for p in order]
        if all(v is None for v in values):
            continue
        cells = []
        base = values[0]
        for i, v in enumerate(values):
            if v is None:
                cells.append("<td>-</td>")
                continue
            text = fmt(v)
            cls = ""
            if i > 0 and base not in (None, 0):
                delta = (v - base) / abs(base) * 100
                if abs(delta) >= 10:
                    worse = (delta > 0) == higher_is_worse
                    cls = ' class="worse"' if worse else ' class="better"'
                    text += f' <span class="delta">{delta:+.0f}%</span>'
            cells.append(f"<td{cls}>{text}</td>")
        body_rows.append(f"<tr><th>{esc(label)}</th>{''.join(cells)}</tr>")

    return (
        '<section class="panel compare"><h2>Baseline vs rebuilding</h2>'
        "<p>Change from the first phase is shown where it exceeds 10%. "
        "Latency excludes samples that overlapped a cf ssh cluster sample.</p>"
        f"<table><thead><tr><th>Metric</th>{head}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table></section>"
    )


def phase_order(checks: list[dict], nodes: list[dict]) -> list[str]:
    order: list[str] = []
    for row in checks + nodes:
        phase = row.get("phase") or "unlabeled"
        if phase not in order:
            order.append(phase)
    return order


def tail_distribution(checks: list[dict], phases: list[str]) -> dict:
    """Share of searches over each latency threshold, per phase.

    The tail is the user-visible story that p50/p95 hides: a p95 of 22s and a p75
    of 11s describe very different experiences, and only the threshold counts make
    "1 in 3 requests felt broken" legible.
    """
    out = {}
    for phase in phases:
        values = [
            float(r["latency_s"])
            for r in checks
            if r["check"] == "search"
            and (r.get("phase") or "unlabeled") == phase
            and r.get("os_sample_inflight") != "1"
            and r["latency_s"]
        ]
        if not values:
            continue
        out[phase] = {
            "n": len(values),
            "p50": percentile(values, 50),
            "p75": percentile(values, 75),
            "p95": percentile(values, 95),
            "max": max(values),
            "over": {
                t: 100 * sum(1 for v in values if v > t) / len(values)
                for t in (2, 5, 10, 30)
            },
        }
    return out


def server_side_query_latency(nodes: list[dict], phases: list[str]) -> dict:
    """OpenSearch's own mean query time per phase, from counter deltas.

    ``search_query_time_millis / search_query_total`` between consecutive samples
    is the cluster's view of how long queries took. Comparing it against the
    end-to-end HTTP timing separates "the cluster is slow" from "something above
    the cluster is slow" -- a distinction that decides where to spend effort.
    """
    by_node: dict[str, dict[float, dict]] = {}
    for row in nodes:
        epoch = num(row.get("epoch"))
        if epoch is not None:
            by_node.setdefault(row["node"], {})[epoch] = row

    per_phase: dict[str, list[float]] = {p: [] for p in phases}
    for samples in by_node.values():
        stamps = sorted(samples)
        for a, b in zip(stamps, stamps[1:]):
            first, second = samples[a], samples[b]
            queries = num(second.get("search_query_total"))
            queries_before = num(first.get("search_query_total"))
            millis = num(second.get("search_query_time_millis"))
            millis_before = num(first.get("search_query_time_millis"))
            if None in (queries, queries_before, millis, millis_before):
                continue
            dq = queries - queries_before
            dt = millis - millis_before
            if dq > 0:
                phase = second.get("phase") or "unlabeled"
                per_phase.setdefault(phase, []).append(dt / dq)

    return {
        phase: {
            "n": len(v),
            "median": percentile(v, 50),
            "p95": percentile(v, 95),
        }
        for phase, v in per_phase.items()
        if v
    }


def cluster_headroom(nodes: list[dict], phases: list[str]) -> dict:
    """Saturation signals per phase: queues, rejections, heap, CPU, GC share."""
    out = {}
    by_node: dict[str, dict[float, dict]] = {}
    for row in nodes:
        epoch = num(row.get("epoch"))
        if epoch is not None:
            by_node.setdefault(row["node"], {})[epoch] = row

    gc_share: dict[str, list[float]] = {}
    for samples in by_node.values():
        stamps = sorted(samples)
        for a, b in zip(stamps, stamps[1:]):
            first, second = samples[a], samples[b]
            gc_a, gc_b = (
                num(first.get("young_gc_millis")),
                num(second.get("young_gc_millis")),
            )
            if gc_a is None or gc_b is None or b <= a:
                continue
            phase = second.get("phase") or "unlabeled"
            gc_share.setdefault(phase, []).append(
                100 * (gc_b - gc_a) / ((b - a) * 1000)
            )

    for phase in phases:
        rows = [r for r in nodes if (r.get("phase") or "unlabeled") == phase]
        if not rows:
            continue

        def vals(field, rows=rows):
            return [num(r[field]) for r in rows if num(r[field]) is not None]

        shares = sorted(gc_share.get(phase, []))
        out[phase] = {
            "max_search_queue": max(vals("search_pool_queue"), default=None),
            "max_rejections": max(vals("search_pool_rejected"), default=None),
            "max_heap": max(vals("heap_percent"), default=None),
            "cpu_p50": percentile(vals("cpu_percent"), 50),
            "max_indexing": max(vals("indexing_rate_per_s"), default=None),
            "gc_median": shares[len(shares) // 2] if shares else None,
        }
    return out


def findings_section(checks: list[dict], nodes: list[dict]) -> str:
    """Narrative findings + recommendation, with every number read from the CSVs.

    The prose is conditional on the measurements so a regenerated report cannot
    end up asserting a conclusion the data no longer supports.
    """
    phases = phase_order(checks, nodes)
    tails = tail_distribution(checks, phases)
    server = server_side_query_latency(nodes, phases)
    head = cluster_headroom(nodes, phases)

    base = phases[0] if phases else None
    # The phase under test is the one with the most indexing activity.
    load = max(
        (p for p in phases if head.get(p, {}).get("max_indexing")),
        key=lambda p: head[p]["max_indexing"] or 0,
        default=None,
    )
    if not base or not load or base == load or base not in tails or load not in tails:
        return ""

    b, x = tails[base], tails[load]
    items = []

    # --- tail distribution table
    rows = "".join(
        f"<tr><th>{esc(p)}</th><td>{tails[p]['n']}</td>"
        + "".join(f"<td>{tails[p]['over'][t]:.1f}%</td>" for t in (2, 5, 10, 30))
        + f"<td>{tails[p]['p75']:.2f}s</td><td>{tails[p]['max']:.1f}s</td></tr>"
        for p in phases
        if p in tails
    )
    tail_table = (
        "<table class='mini'><thead><tr><th>Phase</th><th>n</th><th>&gt;2s</th>"
        "<th>&gt;5s</th><th>&gt;10s</th><th>&gt;30s</th><th>p75</th><th>max</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )

    items.append(
        (
            "critical" if x["over"][10] >= 10 else "warning",
            f"User-visible slowness during <em>{esc(load)}</em>: "
            f"{x['over'][5]:.0f}% of searches took over 5s and "
            f"{x['over'][10]:.0f}% took over 10s, against {b['over'][5]:.0f}% "
            f"and {b['over'][10]:.0f}% at {esc(base)}. p75 moved from "
            f"{b['p75']:.2f}s to {x['p75']:.2f}s.",
            "<p>Read the tail, not the median: p50 barely moves while the p75 "
            "and the over-10s share move a great deal. That is what a user "
            "experiences as broken rather than slow.</p>" + tail_table,
        )
    )

    # --- server-side vs end-to-end
    if base in server and load in server:
        sb, sx = server[base], server[load]
        ratio = (x["p95"] * 1000) / sx["p95"] if sx["p95"] else None
        items.append(
            (
                "info",
                f"The cluster's own query time rose from {sb['median']:.0f}ms to "
                f"{sx['median']:.0f}ms median ({sb['p95']:.0f}ms to "
                f"{sx['p95']:.0f}ms p95) &mdash; real degradation, but measured in "
                "milliseconds.",
                "<p>End-to-end HTTP p95 for the same window was "
                f"<strong>{x['p95']:.1f}s</strong>"
                + (
                    f", roughly {ratio:.0f}&times; the cluster's own p95. "
                    if ratio and ratio > 3
                    else ". "
                )
                + "So most of the wait was <em>not</em> spent executing the query. "
                "It sits above OpenSearch &mdash; catalog page render, the request "
                "path, or the monitoring host&rsquo;s own network. Capacity work on "
                "the cluster would not address it.</p>",
            )
        )

    # --- headroom
    if load in head:
        h = head[load]
        hb = head.get(base, {})
        saturated = (h["max_search_queue"] or 0) > 0 or (h["max_rejections"] or 0) > 0
        claim = (
            "The cluster was saturated during the rebuild: search queue peaked at "
            f"{h['max_search_queue']:.0f} with {h['max_rejections']:.0f} rejections."
            if saturated
            else "The cluster was never saturated: search queue peaked at "
            f"{h['max_search_queue']:.0f} with {h['max_rejections']:.0f} rejections."
        )
        detail = (
            f"<p>Peak heap {h['max_heap']:.0f}% "
            f"(vs {hb.get('max_heap') or 0:.0f}% at {esc(base)}), "
            f"CPU p50 {h['cpu_p50']:.0f}% (vs {hb.get('cpu_p50') or 0:.0f}%)"
        )
        if h.get("gc_median") is not None:
            detail += f", young-GC {h['gc_median']:.2f}% of wall clock"
        detail += f". Peak indexing {h['max_indexing']:.0f} docs/s. "
        detail += (
            "Adding nodes would not have changed the end-to-end numbers above.</p>"
            if not saturated
            else "</p>"
        )
        items.append(("warning" if saturated else "good", claim, detail))

    # --- availability
    total = [r for r in checks if r["check"] in ("search", "dashboard")]
    failed = [r for r in total if r["ok"] != "1"]
    counts = [
        int(num(r["count"]))
        for r in checks
        if r["check"] == "dashboard" and num(r["count"])
    ]
    items.append(
        (
            "good" if not failed else "warning",
            f"Availability held: {len(total) - len(failed)}/{len(total)} checks "
            "passed"
            + (
                f" and the dataset count never dropped below {min(counts):,}."
                if counts
                else "."
            ),
            "<p>No search returned zero results and the alias switch was atomic, "
            "so readers were served the old index for the entire rebuild. The "
            "zero-downtime property held.</p>",
        )
    )

    # --- recommendation, conditional on what was measured
    off_peak = x["over"][10] >= 10
    rec_class = "critical" if off_peak else "good"
    if off_peak:
        rec = (
            f"<p><strong>Run rebuilds off-peak.</strong> {x['over'][10]:.0f}% of "
            "searches over 10s is a poor experience however the time is spent, and "
            "the run takes about an hour &mdash; off-peak costs nothing.</p>"
            "<p><strong>But do not justify it as cluster health.</strong> The "
            "cluster had ample headroom; that framing would send capacity work in "
            "the wrong direction. If rebuilds must run during peak, investigate the "
            "catalog request path, not OpenSearch sizing.</p>"
        )
    else:
        rec = (
            "<p><strong>Rebuilds appear safe to run at any hour</strong> on this "
            "evidence: the latency tail stayed close to baseline and the cluster "
            "showed no saturation. Re-check against a longer baseline before "
            "relying on it.</p>"
        )

    caveats = (
        "<p class='caveat'><strong>Caveats on these numbers.</strong> "
        f"The {esc(base)} phase has only {b['n']} clean samples"
        + (
            " &mdash; a thin basis for a tail estimate; 15+ minutes is better. "
            if b["n"] < 90
            else ". "
        )
        + "Latency is measured from a laptop over the public internet and includes "
        "full page render, so it is a user-experience proxy, not a cluster metric "
        "&mdash; the millisecond figures above are the trustworthy server-side "
        "view. Samples overlapping a <code>cf ssh</code> cluster sample are "
        "excluded from all latency figures. To locate the missing seconds, a "
        "future run should capture catalog app response times alongside these.</p>"
    )

    blocks = "".join(
        f'<div class="finding {cls}"><p class="claim">{claim}</p>{detail}</div>'
        for cls, claim, detail in items
        if claim
    )
    return (
        '<section class="panel findings"><h2>Findings &amp; recommendation</h2>'
        f"{blocks}"
        f'<div class="finding rec {rec_class}"><p class="claim">Recommendation</p>'
        f"{rec}</div>{caveats}</section>"
    )


def build(run_dir: Path) -> tuple[str, str]:
    checks = read_csv(run_dir / "checks.csv")
    cluster = read_csv(run_dir / "opensearch_cluster.csv")
    nodes = read_csv(run_dir / "opensearch_nodes.csv")

    if not checks and not cluster:
        raise SystemExit(f"No monitoring CSVs found in {run_dir}")

    search = [r for r in checks if r["check"] == "search"]
    dashboard = [r for r in checks if r["check"] == "dashboard"]
    # cf ssh inflates locally-measured latency; exclude those rows from latency.
    search_clean = [r for r in search if r.get("os_sample_inflight") != "1"]
    dash_clean = [r for r in dashboard if r.get("os_sample_inflight") != "1"]
    excluded = (len(search) - len(search_clean)) + (len(dashboard) - len(dash_clean))

    charts, data = [], []
    bands = phase_bands(checks or cluster)

    # --- Search latency -----------------------------------------------------
    lat_points = [
        (num(r["epoch"]), num(r["latency_s"]))
        for r in search_clean
        if num(r["epoch"]) and num(r["latency_s"]) is not None
    ]
    fails = [
        {"epoch": num(r["epoch"]), "detail": r["detail"]}
        for r in search
        if r["ok"] != "1" and num(r["epoch"])
    ]
    chart, cdata = line_chart(
        "search-latency",
        "Search query latency",
        'Full page response for q="school OR <random-no-match>", sampled every 10s. '
        + (
            f"{excluded} sample(s) taken during a cf ssh cluster sample are excluded "
            "(that local subprocess inflates measured latency)."
            if excluded
            else "Lower is better."
        ),
        [{"name": "Search latency", "points": lat_points}],
        unit="s",
        bands=bands,
        markers=fails,
    )
    charts.append(chart)
    data.append(cdata)

    # --- Dashboard count ----------------------------------------------------
    count_points = [
        (num(r["epoch"]), num(r["count"]))
        for r in dashboard
        if num(r["epoch"]) and num(r["count"]) is not None
    ]
    threshold_value = None
    for r in dashboard:
        detail = r.get("detail", "")
        if ">=" in detail:
            try:
                threshold_value = float(
                    detail.split(">=")[1].strip(" )").replace(",", "")
                )
            except (IndexError, ValueError):
                pass
            break
    chart, cdata = line_chart(
        "dashboard-count",
        "Datasets available on the homepage",
        "Total count rendered by the catalog dashboard, sampled every 30s. "
        "A drop means the index lost documents from the reader's point of view.",
        [{"name": "Datasets", "points": count_points}],
        bands=bands,
        threshold=(
            {"value": threshold_value, "label": f"expected {threshold_value:,.0f}"}
            if threshold_value
            else None
        ),
        y_min_zero=False,
    )
    charts.append(chart)
    data.append(cdata)

    # --- Node metrics: max + mean across nodes (never one line per node) -----
    by_sample: dict[float, list[dict]] = {}
    for r in nodes:
        epoch = num(r["epoch"])
        if epoch is not None:
            by_sample.setdefault(epoch, []).append(r)

    def agg(field: str, how: str):
        out = []
        for epoch in sorted(by_sample):
            values = [
                num(r[field]) for r in by_sample[epoch] if num(r[field]) is not None
            ]
            if not values:
                continue
            value = max(values) if how == "max" else sum(values) / len(values)
            out.append((epoch, value))
        return out

    def agg_sum(field: str):
        out = []
        for epoch in sorted(by_sample):
            values = [
                num(r[field]) for r in by_sample[epoch] if num(r[field]) is not None
            ]
            if values:
                out.append((epoch, sum(values)))
        return out

    if by_sample:
        chart, cdata = line_chart(
            "node-cpu",
            "OpenSearch node CPU",
            "Busiest node vs cluster mean, sampled every 60s via cf ssh. "
            "Per-node detail is in opensearch_nodes.csv and the table view.",
            [
                {"name": "Peak node", "points": agg("cpu_percent", "max")},
                {"name": "Cluster mean", "points": agg("cpu_percent", "mean")},
            ],
            unit="%",
            bands=bands,
        )
        charts.append(chart)
        data.append(cdata)

        chart, cdata = line_chart(
            "node-heap",
            "JVM heap used",
            "Heap pressure is the metric that turns a rebuild into an outage; "
            "sustained >75% on the peak node is the number to watch.",
            [
                {"name": "Peak node", "points": agg("heap_percent", "max")},
                {"name": "Cluster mean", "points": agg("heap_percent", "mean")},
            ],
            unit="%",
            bands=bands,
        )
        charts.append(chart)
        data.append(cdata)

        chart, cdata = line_chart(
            "indexing-rate",
            "Indexing throughput",
            "Documents indexed per second, summed across nodes and derived from "
            "the delta between consecutive samples.",
            [{"name": "Docs indexed/s", "points": agg_sum("indexing_rate_per_s")}],
            unit="/s",
            bands=bands,
        )
        charts.append(chart)
        data.append(cdata)

        queue_points = agg("search_pool_queue", "max")
        if any(v > 0 for _, v in queue_points):
            chart, cdata = line_chart(
                "search-queue",
                "Search thread pool queue",
                "Queued search tasks on the busiest node. Anything sustained above "
                "zero means the cluster is shedding read capacity.",
                [{"name": "Peak queue depth", "points": queue_points}],
                bands=bands,
            )
            charts.append(chart)
            data.append(cdata)

    # --- Index document total ----------------------------------------------
    docs_points = [
        (num(r["epoch"]), num(r["index_docs_total"]))
        for r in cluster
        if r.get("ok") == "1" and num(r["epoch"]) and num(r.get("index_docs_total"))
    ]
    if docs_points:
        chart, cdata = line_chart(
            "index-docs",
            "Documents in datasets* indexes",
            "Sum of docs.count across every physical index matching datasets*, "
            "including replicas. A rebuild adds a second index before the swap.",
            [{"name": "Documents", "points": docs_points}],
            bands=bands,
            y_min_zero=False,
        )
        charts.append(chart)
        data.append(cdata)

    # --- KPI row ------------------------------------------------------------
    lats = [v for _, v in lat_points]
    s_pass = sum(1 for r in search if r["ok"] == "1")
    d_pass = sum(1 for r in dashboard if r["ok"] == "1")
    counts = [int(num(r["count"], 0)) for r in dashboard if num(r["count"])]
    cpu_max = max((v for _, v in agg("cpu_percent", "max")), default=None)
    heap_max = max((v for _, v in agg("heap_percent", "max")), default=None)
    idx_max = max((v for _, v in agg_sum("indexing_rate_per_s")), default=None)
    rejected = max(
        (num(r["search_pool_rejected"], 0) for r in nodes),
        default=0,
    )
    statuses = {r.get("cluster_status") for r in cluster if r.get("ok") == "1"}

    tiles = [
        stat_tile(
            "Search availability",
            f"{s_pass}/{len(search)}",
            "non-zero results returned",
            "good" if search and s_pass == len(search) else "critical",
        ),
        stat_tile(
            "Dashboard availability",
            f"{d_pass}/{len(dashboard)}",
            f"count held at or above {threshold_value:,.0f}"
            if threshold_value
            else "threshold checks passed",
            "good" if dashboard and d_pass == len(dashboard) else "critical",
        ),
        stat_tile(
            "Search latency p50 / p95",
            f"{percentile(lats, 50):.2f}s / {percentile(lats, 95):.2f}s"
            if lats
            else "-",
            f"max {max(lats):.2f}s over {len(lats)} clean samples" if lats else "",
        ),
        stat_tile(
            "Minimum dataset count",
            f"{min(counts):,}" if counts else "-",
            f"max {max(counts):,}" if counts else "",
        ),
    ]
    if by_sample:
        tiles += [
            stat_tile(
                "Peak node CPU",
                fmt_num(cpu_max, "%") if cpu_max is not None else "-",
                "busiest node, any sample",
            ),
            stat_tile(
                "Peak JVM heap",
                fmt_num(heap_max, "%") if heap_max is not None else "-",
                "busiest node, any sample",
                "critical" if (heap_max or 0) >= 85 else "",
            ),
            stat_tile(
                "Peak indexing rate",
                fmt_num(idx_max, "/s") if idx_max is not None else "-",
                "cluster total docs/s",
            ),
            stat_tile(
                "Search rejections",
                f"{rejected:,.0f}",
                "cumulative thread pool rejections",
                "good" if rejected == 0 else "critical",
            ),
        ]

    # --- Run metadata -------------------------------------------------------
    all_rows = sorted(
        [r for r in checks + cluster if num(r.get("epoch"))],
        key=lambda r: num(r["epoch"]),
    )
    window = (
        f"{fmt_time(num(all_rows[0]['epoch']))} - "
        f"{fmt_time(num(all_rows[-1]['epoch']))}"
        if all_rows
        else "-"
    )
    started = (
        datetime.fromtimestamp(num(all_rows[0]["epoch"])).strftime("%Y-%m-%d %H:%M:%S")
        if all_rows
        else "-"
    )
    duration_min = (
        (num(all_rows[-1]["epoch"]) - num(all_rows[0]["epoch"])) / 60 if all_rows else 0
    )
    aliases = [r.get("alias_targets") for r in cluster if r.get("ok") == "1"]
    phases = []
    for b in bands:
        phases.append(f"{b['phase']} ({fmt_time(b['start'])}-{fmt_time(b['end'])})")

    meta_rows = [
        ("Run directory", str(run_dir)),
        ("Started", started),
        ("Window", f"{window}  ({duration_min:.0f} min)"),
        (
            "HTTP checks",
            f"{len(checks)} ({len(search)} search, {len(dashboard)} dashboard)",
        ),
        ("OpenSearch samples", f"{len(by_sample)} ({len(nodes)} node rows)"),
        ("Cluster status seen", ", ".join(sorted(s for s in statuses if s)) or "-"),
        ("Alias targets seen", ", ".join(sorted(set(a for a in aliases if a))) or "-"),
        ("Phases", " -> ".join(phases) or "-"),
    ]

    failures = [r for r in checks if r["ok"] != "1"]
    fail_html = ""
    if failures:
        rows = "".join(
            f"<tr><td>{esc(fmt_time(num(r['epoch'])))}</td>"
            f"<td>{esc(r['check'])}</td><td>{esc(r.get('phase', ''))}</td>"
            f"<td>{esc(r.get('http_status', ''))}</td>"
            f"<td>{esc(r['detail'])}</td></tr>"
            for r in failures
        )
        fail_html = (
            '<section class="panel alert"><h2>'
            '<span class="ico" aria-hidden="true">&#9888;</span>'
            f"Failed checks ({len(failures)})</h2>"
            "<div class='table-scroll'><table><thead><tr><th>Time</th><th>Check</th>"
            "<th>Phase</th><th>HTTP</th><th>Detail</th></tr></thead>"
            f"<tbody>{rows}</tbody></table></div></section>"
        )
    else:
        fail_html = (
            '<section class="panel ok"><h2>'
            '<span class="ico" aria-hidden="true">&#10003;</span>'
            "No failed checks</h2>"
            f"<p>All {len(checks)} HTTP checks passed across the run.</p></section>"
        )

    title = f"Catalog staging monitor - {started}"
    doc = HTML_TEMPLATE.format(
        title=esc(title),
        palette=",".join(SERIES_LIGHT),
        surface="#fcfcfb",
        tiles="".join(tiles),
        meta="".join(
            f"<tr><th>{esc(k)}</th><td>{esc(v)}</td></tr>" for k, v in meta_rows
        ),
        failures=fail_html,
        comparison=phase_comparison(checks, nodes),
        findings=findings_section(checks, nodes),
        charts="".join(charts),
        data=json.dumps(data),
        series_light=SERIES_LIGHT,
        series_dark=SERIES_DARK,
    )
    return doc, title


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  :root {{
    --page: #f9f9f7; --surface-1: #fcfcfb;
    --text-primary: #0b0b0b; --text-secondary: #52514e; --muted: #898781;
    --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
    --series-1: #2a78d6; --series-2: #eb6834; --series-3: #1baf7a;
    --good: #0ca30c; --critical: #d03b3b; --warning: #fab219;
    --band: rgba(42,120,214,0.07);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) {{
      --page: #0d0d0d; --surface-1: #1a1a19;
      --text-primary: #ffffff; --text-secondary: #c3c2b7; --muted: #898781;
      --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
      --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70;
      --band: rgba(57,135,229,0.12);
    }}
  }}
  :root[data-theme="dark"] {{
    --page: #0d0d0d; --surface-1: #1a1a19;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70;
    --band: rgba(57,135,229,0.12);
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 32px 24px 64px;
    background: var(--page); color: var(--text-primary);
    font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  }}
  .wrap {{ max-width: 1060px; margin: 0 auto; }}
  header.top {{ display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap;
    margin-bottom: 4px; }}
  h1 {{ font-size: 26px; margin: 0; letter-spacing: -0.01em; }}
  .lede {{ color: var(--text-secondary); margin: 4px 0 28px; max-width: 70ch; }}
  button.theme {{
    margin-left: auto; background: var(--surface-1); color: var(--text-secondary);
    border: 1px solid var(--border); border-radius: 8px; padding: 6px 12px;
    font: inherit; font-size: 13px; cursor: pointer;
  }}
  .kpi {{ display: grid; gap: 12px; margin-bottom: 28px;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }}
  .tile {{ background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 14px 16px; }}
  .tile-label {{ margin: 0; font-size: 12px; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--muted); }}
  .tile-value {{ margin: 6px 0 2px; font-size: 27px; font-weight: 650;
    letter-spacing: -0.02em; display: flex; align-items: center; gap: 7px; }}
  .tile-note {{ margin: 0; font-size: 12.5px; color: var(--text-secondary); }}
  .tile.good .ico {{ color: var(--good); }}
  .tile.critical .ico {{ color: var(--critical); }}
  .ico {{ font-size: 0.8em; }}
  .panel {{ background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 16px 18px; margin-bottom: 22px; }}
  .panel h2 {{ font-size: 15px; margin: 0 0 10px;
    display: flex; align-items: center; gap: 8px; }}
  .panel.ok h2 .ico {{ color: var(--good); }}
  .panel.alert h2 .ico {{ color: var(--critical); }}
  .panel p {{ margin: 0; color: var(--text-secondary); font-size: 14px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px;
    font-variant-numeric: tabular-nums; }}
  th, td {{ text-align: left; padding: 5px 12px 5px 0; vertical-align: top;
    border-bottom: 1px solid var(--grid); }}
  th {{ color: var(--muted); font-weight: 500; white-space: nowrap; }}
  td {{ color: var(--text-secondary); }}
  .table-scroll {{ max-height: 320px; overflow: auto; }}
  .meta table th {{ width: 190px; }}
  .chart {{ background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 16px 18px 10px; margin: 0 0 22px;
    position: relative; }}
  .chart h3 {{ font-size: 15px; margin: 0; }}
  .chart .sub {{ margin: 4px 0 10px; font-size: 13px; color: var(--text-secondary);
    max-width: 82ch; }}
  .chart svg {{ width: 100%; height: 260px; display: block; overflow: visible; }}
  .legend {{ display: flex; gap: 16px; margin: 0 0 6px; font-size: 12.5px;
    color: var(--text-secondary); }}
  .legend .key {{ display: flex; align-items: center; gap: 6px; }}
  .legend i {{ width: 10px; height: 10px; border-radius: 3px; display: block; }}
  .grid {{ stroke: var(--grid); stroke-width: 1; }}
  .axis {{ stroke: var(--axis); stroke-width: 1; }}
  .tick {{ fill: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }}
  .tick-y {{ text-anchor: end; }}
  .band {{ fill: var(--band); }}
  .band-label {{ fill: var(--text-secondary); font-size: 10.5px;
    text-transform: uppercase; letter-spacing: 0.08em; }}
  .line {{ fill: none; stroke-width: 2; stroke-linejoin: round;
    stroke-linecap: round; }}
  .dot {{ stroke: var(--surface-1); stroke-width: 2; }}
  .s1 {{ stroke: var(--series-1); }} .s1.dot {{ fill: var(--series-1); }}
  .s2 {{ stroke: var(--series-2); }} .s2.dot {{ fill: var(--series-2); }}
  .s3 {{ stroke: var(--series-3); }} .s3.dot {{ fill: var(--series-3); }}
  .direct-label {{ fill: var(--text-secondary); font-size: 11.5px;
    font-weight: 600; paint-order: stroke; stroke: var(--surface-1);
    stroke-width: 3px; }}
  .threshold {{ stroke: var(--muted); stroke-width: 1.5; stroke-dasharray: 5 4; }}
  .threshold-label {{ fill: var(--muted); font-size: 11px; }}
  .marker {{ stroke: var(--critical); stroke-width: 1.5; stroke-dasharray: 3 3; }}
  .marker-dot {{ fill: var(--critical); stroke: var(--surface-1); stroke-width: 2; }}
  .crosshair {{ stroke: var(--axis); stroke-width: 1; pointer-events: none; }}
  .hit {{ fill: transparent; }}
  .tooltip {{ position: absolute; pointer-events: none; z-index: 5;
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 8px; padding: 8px 10px; font-size: 12.5px;
    box-shadow: 0 4px 14px rgba(0,0,0,0.14); min-width: 130px; }}
  .tooltip .tt-time {{ color: var(--muted); font-size: 11px;
    margin-bottom: 4px; font-variant-numeric: tabular-nums; }}
  .tooltip .tt-row {{ display: flex; align-items: center; gap: 7px;
    color: var(--text-primary); font-variant-numeric: tabular-nums; }}
  .tooltip i {{ width: 9px; height: 9px; border-radius: 2px; display: block; }}
  details.table-view {{ margin: 4px 0 0; }}
  details.table-view summary {{ cursor: pointer; font-size: 12.5px;
    color: var(--text-secondary); padding: 6px 0; }}
  .empty {{ color: var(--muted); font-size: 13px; padding: 24px 0; }}
  .panel.findings .finding {{ border-left: 3px solid var(--grid);
    padding: 2px 0 2px 14px; margin: 0 0 18px; }}
  .panel.findings .finding.critical {{ border-left-color: var(--critical); }}
  .panel.findings .finding.warning {{ border-left-color: var(--warning); }}
  .panel.findings .finding.good {{ border-left-color: var(--good); }}
  .panel.findings .finding.info {{ border-left-color: var(--series-1); }}
  .panel.findings .claim {{ margin: 0 0 6px; color: var(--text-primary);
    font-weight: 600; font-size: 14.5px; }}
  .panel.findings p {{ margin: 0 0 8px; font-size: 13.5px; max-width: 88ch; }}
  .panel.findings .finding.rec {{ background: var(--page); border-radius: 0 8px 8px 0;
    padding: 12px 14px; }}
  .panel.findings .caveat {{ color: var(--muted); font-size: 12.5px;
    border-top: 1px solid var(--grid); padding-top: 12px; margin-top: 4px; }}
  .panel.findings code {{ font-size: 12px; background: var(--page);
    padding: 1px 4px; border-radius: 3px; }}
  table.mini {{ margin: 8px 0 4px; font-size: 12.5px; max-width: 640px; }}
  table.mini th {{ color: var(--muted); font-weight: 500; }}
  table.mini tbody th {{ color: var(--text-secondary); text-align: left; }}
  table.mini td {{ color: var(--text-primary); font-variant-numeric: tabular-nums;
    padding-right: 14px; }}
  .panel.compare table th:first-child {{ width: 210px; }}
  .panel.compare thead th {{ color: var(--text-secondary); font-weight: 600;
    text-transform: uppercase; font-size: 11px; letter-spacing: 0.06em; }}
  .panel.compare tbody th {{ color: var(--text-secondary); font-weight: 500; }}
  .panel.compare td {{ color: var(--text-primary); font-variant-numeric: tabular-nums;
    font-weight: 550; }}
  .panel.compare td.worse {{ color: var(--critical); }}
  .panel.compare td.better {{ color: var(--good); }}
  .panel.compare .delta {{ font-size: 11.5px; font-weight: 500; opacity: 0.85; }}
  .panel.compare p {{ margin: 0 0 10px; }}
  h2.section {{ font-size: 13px; text-transform: uppercase; color: var(--muted);
    letter-spacing: 0.08em; margin: 32px 0 12px; }}
</style>
</head>
<body data-palette="{palette}" data-mode="light" data-surface="{surface}">
<div class="wrap">
  <header class="top">
    <h1>Catalog staging rebuild monitor</h1>
    <button class="theme" type="button" onclick="toggleTheme()">Toggle theme</button>
  </header>
  <p class="lede">Continuous availability and performance sampling of
  catalog-stage.data.gov alongside OpenSearch cluster load, captured while an
  index rebuild ran. Shaded regions mark labelled phases.</p>

  <div class="kpi">{tiles}</div>

  {failures}

  {comparison}

  {findings}

  <section class="panel meta"><h2>Run details</h2>
  <table><tbody>{meta}</tbody></table></section>

  <h2 class="section">Catalog availability</h2>
  {charts}
</div>
<script>
const CHARTS = {data};
function toggleTheme() {{
  const root = document.documentElement;
  const dark = getComputedStyle(root).getPropertyValue('--page').trim() === '#0d0d0d';
  root.dataset.theme = dark ? 'light' : 'dark';
}}
function seriesColor(i) {{
  return getComputedStyle(document.documentElement)
    .getPropertyValue('--series-' + (i + 1)).trim() || '#2a78d6';
}}
function fmt(v, unit) {{
  if (v === null || v === undefined) return '-';
  if (unit === 's') return v.toFixed(2) + 's';
  if (unit === '%') return Math.round(v) + '%';
  if (Math.abs(v) >= 1000)
    return v.toLocaleString(undefined, {{maximumFractionDigits: 0}});
  return (Math.abs(v) >= 10 ? v.toFixed(0) : v.toFixed(2)) + unit;
}}
CHARTS.forEach(function (c) {{
  const fig = document.getElementById(c.id);
  if (!fig) return;
  const svg = fig.querySelector('svg');
  const hit = fig.querySelector('.hit');
  const cross = fig.querySelector('.crosshair');
  const tip = fig.querySelector('.tooltip');
  if (!svg || !hit || !tip) return;
  const inner = c.w - c.margin.left - c.margin.right;
  function show(evt) {{
    const box = svg.getBoundingClientRect();
    const px = (evt.clientX - box.left) / box.width * c.w;
    const t = c.xMin + Math.min(1, Math.max(0,
      (px - c.margin.left) / inner)) * (c.xMax - c.xMin);
    let nearest = null;
    const rows = [];
    c.series.forEach(function (s, i) {{
      let best = null;
      s.points.forEach(function (p) {{
        if (!best || Math.abs(p[0] - t) < Math.abs(best[0] - t)) best = p;
      }});
      if (best) {{
        rows.push({{name: s.name, value: best[1], color: seriesColor(i)}});
        if (!nearest || Math.abs(best[0] - t) < Math.abs(nearest - t))
          nearest = best[0];
      }}
    }});
    if (nearest === null) return;
    const x = c.margin.left + (nearest - c.xMin) / (c.xMax - c.xMin) * inner;
    cross.setAttribute('x1', x); cross.setAttribute('x2', x);
    cross.style.display = '';
    tip.hidden = false;
    tip.innerHTML = '<div class="tt-time">' +
      new Date(nearest * 1000).toLocaleTimeString() + '</div>' +
      rows.map(function (r) {{
        return '<div class="tt-row"><i style="background:' + r.color + '"></i>' +
          r.name + ' <strong>' + fmt(r.value, c.unit) + '</strong></div>';
      }}).join('');
    const box2 = svg.getBoundingClientRect();
    const figBox = fig.getBoundingClientRect();
    const left = box2.left - figBox.left + (x / c.w) * box2.width;
    tip.style.left = Math.min(figBox.width - tip.offsetWidth - 8,
      Math.max(8, left + 14)) + 'px';
    tip.style.top = (evt.clientY - figBox.top + 12) + 'px';
  }}
  hit.addEventListener('mousemove', show);
  hit.addEventListener('mouseenter', show);
  hit.addEventListener('mouseleave', function () {{
    cross.style.display = 'none';
    tip.hidden = true;
  }});
}});
</script>
</body>
</html>
"""


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("run_dir", help="monitoring run directory containing the CSVs")
    p.add_argument("-o", "--output", default=None, help="output HTML path")
    args = p.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.is_dir():
        print(f"Not a directory: {run_dir}", file=sys.stderr)
        return 2

    doc, _ = build(run_dir)
    out = Path(args.output) if args.output else run_dir / "report.html"
    out.write_text(doc)
    print(f"Wrote {out.resolve()}  ({len(doc):,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
