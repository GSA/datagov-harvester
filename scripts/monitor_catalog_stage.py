#!/usr/bin/env python3
"""Monitor catalog-stage.data.gov and the staging OpenSearch cluster during a
rebuild-index, writing chart-ready CSV.

Three independent samplers run concurrently, each on its own interval:

  SEARCH      10s  `school OR <random-no-match>` query; records latency + hit count
  DASHBOARD   30s  homepage total dataset count; must stay >= --min-count
  OPENSEARCH  60s  cluster/node CPU, memory, heap, GC, thread pools, indexing rates
                   (via `cf ssh` into the staging harvester app -- takes ~10s/sample)

Output (all in --outdir, default ./monitor-runs/<start-timestamp>/):

  checks.csv              one row per HTTP check (dashboard + search, long format)
  opensearch_cluster.csv  one row per OpenSearch sample (cluster-level)
  opensearch_nodes.csv    one row per node per OpenSearch sample
  raw/os-<epoch>.json     full JSON snapshot per OpenSearch sample
  monitor.log             human-readable running log (same content as stdout)

Mark rebuild phases for the charts by writing to the phase file at any time:

    echo rebuilding > <outdir>/phase.txt
    echo post-rebuild > <outdir>/phase.txt

Every row records the current phase value, so charts can shade the rebuild window.

Usage:
    python3 scripts/monitor_catalog_stage.py                    # start monitoring
    python3 scripts/monitor_catalog_stage.py --once             # one of each, exit
    python3 scripts/monitor_catalog_stage.py --no-opensearch    # HTTP checks only
    python3 scripts/monitor_catalog_stage.py --report DIR       # summarize a run

Auth comes from CATALOG_USER / CATALOG_PASS (defaults to staging basic-auth creds).
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import random
import re
import shutil
import ssl
import string
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://catalog-stage.data.gov/"
MIN_COUNT = 547_883
CF_APP = os.environ.get("CF_APP_NAME", "datagov-harvest")
CF_INSTANCE = os.environ.get("CF_APP_INSTANCE", "0")
METRICS_PAYLOAD = Path(__file__).resolve().parent / "opensearch_metrics_json.py"

# Homepage:
#   <span class="text-heavy text-secondary font-sans-lg">547,883</span>
#   datasets available on Data.gov.
DASHBOARD_RE = re.compile(
    r'<span class="text-heavy text-secondary font-sans-lg">\s*([\d,]+)\s*</span>\s*'
    r"datasets available on Data\.gov",
    re.IGNORECASE,
)
# Search results toolbar: Found [over] <strong>10000</strong> datasets matching "..."
SEARCH_RE = re.compile(
    r"Found\s+(?:over\s+)?<strong>\s*([\d,]+)\s*</strong>\s*datasets?\s+matching",
    re.IGNORECASE,
)
# Zero results: <p ... id="no-datasets-alert">Found <strong>0</strong> datasets.
SEARCH_ZERO_RE = re.compile(
    r'id="no-datasets-alert"|Found\s+<strong>\s*0\s*</strong>\s*datasets', re.IGNORECASE
)

CHECK_FIELDS = [
    "timestamp",
    "epoch",
    "elapsed_s",
    "phase",
    "check",
    "ok",
    "http_status",
    "count",
    "latency_s",
    "os_sample_inflight",
    "query",
    "detail",
]
CLUSTER_FIELDS = [
    "timestamp",
    "epoch",
    "elapsed_s",
    "phase",
    "ok",
    "sample_latency_s",
    "cluster_status",
    "nodes_total",
    "data_nodes",
    "active_shards",
    "active_shards_percent",
    "relocating_shards",
    "initializing_shards",
    "unassigned_shards",
    "pending_tasks",
    "task_max_waiting_millis",
    "alias_targets",
    "physical_indices",
    "index_docs_total",
    "index_store",
    "error",
]
NODE_FIELDS = [
    "timestamp",
    "epoch",
    "elapsed_s",
    "phase",
    "node",
    "roles",
    "cpu_percent",
    "load_1m",
    "load_5m",
    "load_15m",
    "ram_percent",
    "heap_percent",
    "heap_used_bytes",
    "heap_max_bytes",
    "disk_used_percent",
    "young_gc_count",
    "young_gc_millis",
    "old_gc_count",
    "old_gc_millis",
    "indexing_index_total",
    "indexing_current",
    "indexing_time_millis",
    "indexing_rate_per_s",
    "search_query_total",
    "search_query_current",
    "search_query_time_millis",
    "search_rate_per_s",
    "search_latency_ms_avg",
    "merges_current",
    "refresh_total",
    "docs_count",
    "store_size_bytes",
    "search_pool_active",
    "search_pool_queue",
    "search_pool_rejected",
    "write_pool_active",
    "write_pool_queue",
    "write_pool_rejected",
]

PRINT_LOCK = threading.Lock()
STOP = threading.Event()
START_MONO = time.monotonic()

# `cf ssh` is a heavyweight local subprocess (SSH handshake + tunnel) and measurably
# inflates concurrently-measured HTTP latency on this machine -- observed ~1s search
# requests spiking to ~12s while a sample was in flight. Checks taken during a sample
# are tagged os_sample_inflight=1 so latency analysis can exclude them.
OS_SAMPLE_INFLIGHT = threading.Event()


# --------------------------------------------------------------------------- io


class CsvSink:
    """Append-only CSV with a header, flushed per row, safe across threads."""

    def __init__(self, path: Path, fields: list[str]):
        self.path = path
        self.fields = fields
        self._lock = threading.Lock()
        new = not path.exists() or path.stat().st_size == 0
        self._fh = path.open("a", newline="")
        self._writer = csv.DictWriter(
            self._fh, fieldnames=fields, extrasaction="ignore"
        )
        if new:
            self._writer.writeheader()
            self._fh.flush()

    def write(self, row: dict):
        with self._lock:
            self._writer.writerow(row)
            self._fh.flush()

    def close(self):
        with self._lock:
            self._fh.close()


class Tee:
    """Write log lines to stdout and a file."""

    def __init__(self, path: Path):
        self._fh = path.open("a")

    def line(self, text: str):
        with PRINT_LOCK:
            print(text, flush=True)
            self._fh.write(text + "\n")
            self._fh.flush()

    def close(self):
        self._fh.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def now_display() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def stamp() -> dict:
    """Common leading columns for every CSV row."""
    return {
        "timestamp": now_iso(),
        "epoch": round(time.time(), 3),
        "elapsed_s": round(time.monotonic() - START_MONO, 1),
    }


def inflight_flag() -> int:
    """1 when a cf ssh OpenSearch sample overlapped this check (latency suspect)."""
    return 1 if OS_SAMPLE_INFLIGHT.is_set() else 0


def read_phase(phase_file: Path) -> str:
    try:
        return phase_file.read_text().strip() or "unlabeled"
    except OSError:
        return "unlabeled"


# ------------------------------------------------------------------- http checks


def random_token(length: int = 24) -> str:
    """Token long/random enough that it can never match a real dataset, and
    different every call so nothing is served from a query cache."""
    alphabet = string.ascii_lowercase + string.digits
    return "zz" + "".join(random.choice(alphabet) for _ in range(length))


def http_get(url: str, user: str, password: str, timeout: int = 60):
    """Return (status, body, latency_s, error) -- error is None on success."""
    req = urllib.request.Request(url)
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    req.add_header("User-Agent", "datagov-harvester-stage-monitor/1.0")
    req.add_header("Cache-Control", "no-cache")
    started = time.monotonic()
    try:
        with urllib.request.urlopen(
            req, timeout=timeout, context=ssl.create_default_context()
        ) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            latency = time.monotonic() - started
            if resp.status != 200:
                return resp.status, body, latency, f"HTTP {resp.status}"
            return resp.status, body, latency, None
    except urllib.error.HTTPError as e:
        return e.code, None, time.monotonic() - started, f"HTTP {e.code} {e.reason}"
    except Exception as e:  # timeout, TLS, DNS, connection reset
        return None, None, time.monotonic() - started, f"{type(e).__name__}: {e}"


def check_dashboard(cfg) -> dict:
    inflight = inflight_flag()
    status, body, latency, err = http_get(cfg.base, cfg.user, cfg.password)
    row = {
        **stamp(),
        "check": "dashboard",
        "http_status": status or "",
        "latency_s": round(latency, 3) if latency is not None else "",
        "os_sample_inflight": inflight | inflight_flag(),
        "query": "",
    }
    if err:
        return {**row, "ok": 0, "count": "", "detail": err}
    m = DASHBOARD_RE.search(body)
    if not m:
        return {
            **row,
            "ok": 0,
            "count": "",
            "detail": "dashboard count element not found in page",
        }
    count = int(m.group(1).replace(",", ""))
    if count < cfg.min_count:
        return {
            **row,
            "ok": 0,
            "count": count,
            "detail": f"count {count:,} < expected {cfg.min_count:,}",
        }
    return {
        **row,
        "ok": 1,
        "count": count,
        "detail": f"datasets available (>= {cfg.min_count:,})",
    }


def check_search(cfg) -> dict:
    query = f"{cfg.term} OR {random_token()}"
    url = cfg.base + "?" + urllib.parse.urlencode({"q": query, "sort": "relevance"})
    inflight = inflight_flag()
    status, body, latency, err = http_get(url, cfg.user, cfg.password)
    row = {
        **stamp(),
        "check": "search",
        "http_status": status or "",
        "latency_s": round(latency, 3) if latency is not None else "",
        "os_sample_inflight": inflight | inflight_flag(),
        "query": query,
    }
    if err:
        return {**row, "ok": 0, "count": "", "detail": err}
    m = SEARCH_RE.search(body)
    if not m:
        if SEARCH_ZERO_RE.search(body):
            return {**row, "ok": 0, "count": 0, "detail": "zero results"}
        return {**row, "ok": 0, "count": "", "detail": "result count not found in page"}
    count = int(m.group(1).replace(",", ""))
    if count <= 0:
        return {**row, "ok": 0, "count": count, "detail": "zero results"}
    return {**row, "ok": 1, "count": count, "detail": "hits"}


# -------------------------------------------------------------- opensearch sample

REMOTE_COMMAND = r"""
set -o errexit
set -o nounset
set -o pipefail

export PATH="$HOME/deps/0/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/deps/0/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cd "$HOME/app"
. "$HOME/app/.profile" >/dev/null 2>&1

if ! command -v python >/dev/null 2>&1; then
  echo "Python was not found in the Cloud Foundry buildpack PATH." >&2
  exit 1
fi

exec python -
"""


def fetch_opensearch(payload: Path, app: str, instance: str, timeout: int = 150):
    """Stream the metrics payload into the CF app; return (snapshot, latency, error)."""
    started = time.monotonic()
    OS_SAMPLE_INFLIGHT.set()
    try:
        proc = subprocess.run(
            ["cf", "ssh", app, "-i", instance, "-T", "-c", REMOTE_COMMAND],
            stdin=payload.open("rb"),
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None, time.monotonic() - started, f"cf ssh timed out after {timeout}s"
    except Exception as e:
        return None, time.monotonic() - started, f"{type(e).__name__}: {e}"
    finally:
        OS_SAMPLE_INFLIGHT.clear()

    latency = time.monotonic() - started
    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace").strip()

    # The JSON object is the last non-empty stdout line; cf prints banners first.
    payload_line = next(
        (ln for ln in reversed(stdout.splitlines()) if ln.strip().startswith("{")), None
    )
    if payload_line is None:
        detail = stderr.splitlines()[-1] if stderr else f"exit {proc.returncode}"
        return None, latency, f"no JSON from cf ssh: {detail}"
    try:
        return json.loads(payload_line), latency, None
    except json.JSONDecodeError as e:
        return None, latency, f"malformed JSON from cf ssh: {e}"


def to_number(value):
    """cat APIs return strings; make them numeric for charting where possible."""
    if value is None or value == "":
        return ""
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def cluster_row(snap: dict, latency: float, phase: str) -> dict:
    health = snap.get("cluster_health", {})
    indices = snap.get("cat_indices", []) or []
    docs_total = 0
    for idx in indices:
        try:
            docs_total += int(idx.get("docs.count") or 0)
        except (TypeError, ValueError):
            pass
    return {
        **stamp(),
        "phase": phase,
        "ok": 1,
        "sample_latency_s": round(latency, 2),
        "cluster_status": health.get("status", ""),
        "nodes_total": health.get("number_of_nodes", ""),
        "data_nodes": health.get("number_of_data_nodes", ""),
        "active_shards": health.get("active_shards", ""),
        "active_shards_percent": health.get("active_shards_percent_as_number", ""),
        "relocating_shards": health.get("relocating_shards", ""),
        "initializing_shards": health.get("initializing_shards", ""),
        "unassigned_shards": health.get("unassigned_shards", ""),
        "pending_tasks": health.get("number_of_pending_tasks", ""),
        "task_max_waiting_millis": health.get("task_max_waiting_in_queue_millis", ""),
        "alias_targets": ";".join(
            f"{t['index']}{'(w)' if t.get('is_write_index') else ''}"
            for t in snap.get("alias_targets", [])
        )
        or "none",
        "physical_indices": ";".join(
            f"{i.get('index')}={i.get('docs.count')}" for i in indices
        ),
        "index_docs_total": docs_total,
        "index_store": ";".join(
            f"{i.get('index')}={i.get('store.size')}" for i in indices
        ),
        "error": "",
    }


def node_rows(snap: dict, phase: str, prev: dict) -> list[dict]:
    """Build per-node rows, deriving rates against the previous sample."""
    base = stamp()
    cat_by_name = {n.get("name"): n for n in (snap.get("cat_nodes") or [])}
    pools = snap.get("thread_pools", {}) or {}
    rows = []
    for name, stats in sorted((snap.get("node_stats") or {}).items()):
        cat = cat_by_name.get(name, {})
        pool = pools.get(name, {})
        search_pool = pool.get("search", {})
        write_pool = pool.get("write", {})

        index_total = stats.get("indexing_index_total")
        query_total = stats.get("search_query_total")
        query_millis = stats.get("search_query_time_millis")

        index_rate = search_rate = ""
        last = prev.get(name)
        if last:
            dt = base["epoch"] - last["epoch"]
            if dt > 0:
                if isinstance(index_total, int) and isinstance(last["index"], int):
                    index_rate = round(max(0, index_total - last["index"]) / dt, 2)
                if isinstance(query_total, int) and isinstance(last["query"], int):
                    search_rate = round(max(0, query_total - last["query"]) / dt, 2)
        prev[name] = {
            "epoch": base["epoch"],
            "index": index_total,
            "query": query_total,
        }

        avg_query_ms = ""
        if (
            isinstance(query_total, int)
            and query_total > 0
            and isinstance(query_millis, int)
        ):
            avg_query_ms = round(query_millis / query_total, 2)

        rows.append(
            {
                **base,
                "phase": phase,
                "node": name,
                "roles": cat.get("node.role", ""),
                "cpu_percent": to_number(cat.get("cpu"))
                if cat.get("cpu") not in (None, "")
                else stats.get("os_cpu_percent", ""),
                "load_1m": to_number(cat.get("load_1m")) or stats.get("os_load_1m", ""),
                "load_5m": to_number(cat.get("load_5m")) or stats.get("os_load_5m", ""),
                "load_15m": to_number(cat.get("load_15m"))
                or stats.get("os_load_15m", ""),
                "ram_percent": to_number(cat.get("ram.percent"))
                if cat.get("ram.percent") not in (None, "")
                else stats.get("os_mem_used_percent", ""),
                "heap_percent": stats.get("heap_used_percent", ""),
                "heap_used_bytes": stats.get("heap_used_bytes", ""),
                "heap_max_bytes": stats.get("heap_max_bytes", ""),
                "disk_used_percent": to_number(cat.get("disk.used_percent")),
                "young_gc_count": stats.get("young_gc_count", ""),
                "young_gc_millis": stats.get("young_gc_millis", ""),
                "old_gc_count": stats.get("old_gc_count", ""),
                "old_gc_millis": stats.get("old_gc_millis", ""),
                "indexing_index_total": index_total if index_total is not None else "",
                "indexing_current": stats.get("indexing_current", ""),
                "indexing_time_millis": stats.get("indexing_index_time_millis", ""),
                "indexing_rate_per_s": index_rate,
                "search_query_total": query_total if query_total is not None else "",
                "search_query_current": stats.get("search_query_current", ""),
                "search_query_time_millis": (
                    query_millis if query_millis is not None else ""
                ),
                "search_rate_per_s": search_rate,
                "search_latency_ms_avg": avg_query_ms,
                "merges_current": stats.get("merges_current", ""),
                "refresh_total": stats.get("refresh_total", ""),
                "docs_count": stats.get("docs_count", ""),
                "store_size_bytes": stats.get("store_size_bytes", ""),
                "search_pool_active": to_number(search_pool.get("active")),
                "search_pool_queue": to_number(search_pool.get("queue")),
                "search_pool_rejected": to_number(search_pool.get("rejected")),
                "write_pool_active": to_number(write_pool.get("active")),
                "write_pool_queue": to_number(write_pool.get("queue")),
                "write_pool_rejected": to_number(write_pool.get("rejected")),
            }
        )
    return rows


# ------------------------------------------------------------------------ runners


def emit_check(log: Tee, row: dict):
    count = row.get("count")
    latency = row.get("latency_s")
    log.line(
        f"{now_display():<26} "
        f"{row['check'].upper():<10} "
        f"{'PASS' if row['ok'] else 'FAIL':<5} "
        f"{(f'{count:,}' if isinstance(count, int) else '-'):>10} "
        f"{(f'{latency:.3f}s' if isinstance(latency, float) else '-'):>9}  "
        f"{row['detail']}" + (f"  q=\"{row['query']}\"" if row.get("query") else "")
    )


def run_check_loop(interval, fn, cfg, sink: CsvSink, log: Tee):
    while not STOP.is_set():
        try:
            row = fn(cfg)
            row["phase"] = read_phase(cfg.phase_file)
            sink.write(row)
            emit_check(log, row)
        except Exception as exc:  # a sampler must never die silently
            log.line(
                f"{now_display():<26} {'MONITOR':<10} check loop error, "
                f"continuing: {type(exc).__name__}: {exc}"
            )
        if STOP.wait(interval):
            return


def run_opensearch_loop(interval, cfg, cluster_sink, node_sink, log: Tee):
    prev: dict = {}
    while not STOP.is_set():
        try:
            _opensearch_sample(cfg, cluster_sink, node_sink, log, prev)
        except Exception as exc:  # a sampler must never die silently
            log.line(
                f"{now_display():<26} {'OPENSEARCH':<10} sample loop error, "
                f"continuing: {type(exc).__name__}: {exc}"
            )
        if STOP.wait(interval):
            return


def _opensearch_sample(cfg, cluster_sink, node_sink, log: Tee, prev: dict):
    """Take one cluster sample and write its cluster + per-node rows."""
    phase = read_phase(cfg.phase_file)
    snap, latency, err = fetch_opensearch(cfg.payload, cfg.app, cfg.instance)
    if err:
        cluster_sink.write(
            {
                **stamp(),
                "phase": phase,
                "ok": 0,
                "sample_latency_s": round(latency, 2),
                "error": err,
            }
        )
        log.line(
            f"{now_display():<26} {'OPENSEARCH':<10} {'FAIL':<5} "
            f"{'-':>10} {latency:>8.2f}s  {err}"
        )
    else:
        crow = cluster_row(snap, latency, phase)
        cluster_sink.write(crow)
        nrows = node_rows(snap, phase, prev)
        for row in nrows:
            node_sink.write(row)
        if cfg.raw_dir:
            # Never let an unwritable snapshot kill the sampler: the CSV rows
            # are the product, the raw JSON is a convenience. (Learned the
            # hard way -- moving the run directory mid-run silently killed
            # this thread and cost the cluster metrics for a whole rebuild.)
            try:
                cfg.raw_dir.mkdir(parents=True, exist_ok=True)
                raw = cfg.raw_dir / f"os-{int(time.time())}.json"
                raw.write_text(json.dumps(snap, separators=(",", ":")))
            except OSError as exc:
                log.line(
                    f"{now_display():<26} {'OPENSEARCH':<10} raw snapshot "
                    f"not written: {exc}"
                )

        cpus = [r["cpu_percent"] for r in nrows if isinstance(r["cpu_percent"], int)]
        heaps = [r["heap_percent"] for r in nrows if isinstance(r["heap_percent"], int)]
        idx_rates = [
            r["indexing_rate_per_s"]
            for r in nrows
            if isinstance(r["indexing_rate_per_s"], float)
        ]
        rejected = sum(
            r["search_pool_rejected"]
            for r in nrows
            if isinstance(r["search_pool_rejected"], int)
        )
        detail = (
            f"{crow['cluster_status']} "
            f"cpu max/avg {max(cpus, default=0)}%/"
            f"{(sum(cpus) / len(cpus) if cpus else 0):.0f}% "
            f"heap max {max(heaps, default=0)}% "
            f"idx {sum(idx_rates):.0f}/s "
            f"docs {crow['index_docs_total']:,} "
            f"alias {crow['alias_targets']} "
            f"rejected {rejected}"
        )
        log.line(
            f"{now_display():<26} {'OPENSEARCH':<10} {'PASS':<5} "
            f"{crow['index_docs_total']:>10,} {latency:>8.2f}s  {detail}"
        )


# ------------------------------------------------------------------------- report


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def report(outdir: Path) -> int:
    checks = outdir / "checks.csv"
    if not checks.exists():
        print(f"No checks.csv in {outdir}", file=sys.stderr)
        return 1
    rows = list(csv.DictReader(checks.open()))
    print(f"Run: {outdir}")
    print(f"Rows: {len(rows)}")
    if rows:
        print(f"Window: {rows[0]['timestamp']}  ->  {rows[-1]['timestamp']}")
    for kind in ("dashboard", "search"):
        for phase in sorted({r.get("phase") or "unlabeled" for r in rows}):
            subset = [
                r
                for r in rows
                if r["check"] == kind and (r.get("phase") or "unlabeled") == phase
            ]
            if not subset:
                continue
            passed = sum(1 for r in subset if r["ok"] == "1")
            # Exclude checks that overlapped a cf ssh sample: that subprocess
            # inflates locally-measured latency and is not a server-side signal.
            clean = [r for r in subset if r.get("os_sample_inflight") != "1"]
            tainted = len(subset) - len(clean)
            lats = [float(r["latency_s"]) for r in clean if r["latency_s"]]
            counts = [int(r["count"]) for r in subset if r["count"]]
            print(f"\n{kind.upper()} [{phase}]  n={len(subset)}")
            print(f"  pass/fail:  {passed}/{len(subset) - passed}")
            if tainted:
                print(f"  excluded:   {tainted} row(s) overlapped a cf ssh sample")
            if lats:
                print(
                    f"  latency:    min {min(lats):.3f}s"
                    f"  p50 {percentile(lats, 50):.3f}s"
                    f"  p95 {percentile(lats, 95):.3f}s"
                    f"  p99 {percentile(lats, 99):.3f}s"
                    f"  max {max(lats):.3f}s"
                )
            if counts:
                print(f"  count:      min {min(counts):,}  max {max(counts):,}")
            for r in subset:
                if r["ok"] != "1":
                    print(f"  FAIL {r['timestamp']}  {r['detail']}")

    nodes = outdir / "opensearch_nodes.csv"
    if nodes.exists():
        nrows = list(csv.DictReader(nodes.open()))
        print(
            f"\nOPENSEARCH  samples={len({r['timestamp'] for r in nrows})} "
            f"node-rows={len(nrows)}"
        )
        for phase in sorted({r.get("phase") or "unlabeled" for r in nrows}):
            subset = [r for r in nrows if (r.get("phase") or "unlabeled") == phase]
            cpus = [float(r["cpu_percent"]) for r in subset if r["cpu_percent"]]
            heaps = [float(r["heap_percent"]) for r in subset if r["heap_percent"]]
            idx = [
                float(r["indexing_rate_per_s"])
                for r in subset
                if r["indexing_rate_per_s"]
            ]
            rej = [
                float(r["search_pool_rejected"])
                for r in subset
                if r["search_pool_rejected"]
            ]
            print(f"  [{phase}] n={len(subset)}")
            if cpus:
                print(
                    f"    cpu %:      p50 {percentile(cpus, 50):.0f}  "
                    f"p95 {percentile(cpus, 95):.0f}  max {max(cpus):.0f}"
                )
            if heaps:
                print(
                    f"    heap %:     p50 {percentile(heaps, 50):.0f}  "
                    f"p95 {percentile(heaps, 95):.0f}  max {max(heaps):.0f}"
                )
            if idx:
                print(
                    f"    idx docs/s: p50 {percentile(idx, 50):.1f}  "
                    f"max {max(idx):.1f}"
                )
            if rej:
                print(f"    search rejections (max cumulative): {max(rej):.0f}")
    return 0


# --------------------------------------------------------------------------- main


class Config:
    pass


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--url", default=BASE_URL)
    p.add_argument("--min-count", type=int, default=MIN_COUNT)
    p.add_argument("--term", default="school", help="matching term in the OR query")
    p.add_argument("--search-interval", type=float, default=10.0)
    p.add_argument("--dashboard-interval", type=float, default=30.0)
    p.add_argument("--opensearch-interval", type=float, default=60.0)
    p.add_argument("--outdir", default=None, help="default ./monitor-runs/<timestamp>")
    p.add_argument(
        "--no-opensearch", action="store_true", help="skip cf ssh cluster sampling"
    )
    p.add_argument(
        "--no-raw", action="store_true", help="do not keep raw JSON snapshots"
    )
    p.add_argument("--cf-app", default=CF_APP)
    p.add_argument("--cf-instance", default=CF_INSTANCE)
    p.add_argument(
        "--payload",
        default=str(METRICS_PAYLOAD),
        help="metrics script streamed into the CF app",
    )
    p.add_argument(
        "--phase", default="baseline", help="initial phase label written to phase.txt"
    )
    p.add_argument("--once", action="store_true", help="one sample of each, then exit")
    p.add_argument("--report", metavar="DIR", help="summarize an existing run and exit")
    args = p.parse_args()

    if args.report:
        return report(Path(args.report))

    outdir = Path(
        args.outdir or Path("monitor-runs") / datetime.now().strftime("%Y%m%dT%H%M%S")
    )
    outdir.mkdir(parents=True, exist_ok=True)

    cfg = Config()
    cfg.base = args.url if args.url.endswith("/") else args.url + "/"
    cfg.min_count = args.min_count
    cfg.term = args.term
    cfg.user = os.environ.get("CATALOG_USER", "admin")
    cfg.password = os.environ.get("CATALOG_PASS", "datagovteam")
    cfg.phase_file = outdir / "phase.txt"
    cfg.app = args.cf_app
    cfg.instance = args.cf_instance
    cfg.payload = Path(args.payload).resolve()
    cfg.raw_dir = None if args.no_raw else outdir / "raw"
    if cfg.raw_dir:
        cfg.raw_dir.mkdir(exist_ok=True)
    if not cfg.phase_file.exists():
        cfg.phase_file.write_text(args.phase + "\n")

    use_os = not args.no_opensearch
    if use_os:
        if not cfg.payload.is_file():
            print(f"Metrics payload not found: {cfg.payload}", file=sys.stderr)
            return 2
        if shutil.which("cf") is None:
            print("cf CLI not found; rerun with --no-opensearch", file=sys.stderr)
            return 2

    log = Tee(outdir / "monitor.log")
    check_sink = CsvSink(outdir / "checks.csv", CHECK_FIELDS)
    cluster_sink = CsvSink(outdir / "opensearch_cluster.csv", CLUSTER_FIELDS)
    node_sink = CsvSink(outdir / "opensearch_nodes.csv", NODE_FIELDS)

    log.line(f"Monitoring {cfg.base}")
    log.line(f"  output dir  {outdir.resolve()}")
    log.line(
        f"  phase file  {cfg.phase_file}  " f"(currently: {read_phase(cfg.phase_file)})"
    )
    log.line(
        f"  SEARCH      every {args.search_interval:g}s  "
        f'q="{cfg.term} OR <random-no-match>"'
    )
    log.line(
        f"  DASHBOARD   every {args.dashboard_interval:g}s  "
        f"expect >= {cfg.min_count:,} datasets"
    )
    log.line(
        f"  OPENSEARCH  every {args.opensearch_interval:g}s  "
        f"via cf ssh {cfg.app}/{cfg.instance}"
        if use_os
        else "  OPENSEARCH  disabled"
    )
    log.line("")
    log.line(
        f"{'TIMESTAMP':<26} {'CHECK':<10} {'RES':<5} {'COUNT':>10} "
        f"{'LATENCY':>9}  DETAIL"
    )
    log.line("-" * 110)

    try:
        if args.once:
            for fn in (check_dashboard, check_search):
                row = fn(cfg)
                row["phase"] = read_phase(cfg.phase_file)
                check_sink.write(row)
                emit_check(log, row)
            if use_os:
                run_once = threading.Thread(
                    target=lambda: run_opensearch_loop(
                        0, cfg, cluster_sink, node_sink, log
                    ),
                    daemon=True,
                )
                run_once.start()
                time.sleep(0.1)
                STOP.set()
                run_once.join(timeout=180)
            return 0

        threads = [
            threading.Thread(
                target=run_check_loop,
                args=(args.search_interval, check_search, cfg, check_sink, log),
                daemon=True,
            ),
            threading.Thread(
                target=run_check_loop,
                args=(args.dashboard_interval, check_dashboard, cfg, check_sink, log),
                daemon=True,
            ),
        ]
        if use_os:
            threads.append(
                threading.Thread(
                    target=run_opensearch_loop,
                    args=(args.opensearch_interval, cfg, cluster_sink, node_sink, log),
                    daemon=True,
                )
            )
        for t in threads:
            t.start()
        while any(t.is_alive() for t in threads):
            time.sleep(0.3)
    except KeyboardInterrupt:
        STOP.set()
        log.line("\nStopped.")
        time.sleep(0.5)
    finally:
        for sink in (check_sink, cluster_sink, node_sink):
            sink.close()
        log.line(f"CSV written to {outdir.resolve()}")
        log.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
