#!/usr/bin/env python3
"""Emit one JSON snapshot of OpenSearch cluster metrics on stdout.

Machine-readable sibling of scripts/opensearch_status.py. Designed to be streamed
into a Cloud Foundry app over `cf ssh` (see monitor_catalog_stage.py), so the only
thing on stdout is a single JSON object -- everything else goes to stderr.

Standalone usage (inside the CF container, after sourcing .profile):
    python opensearch_metrics_json.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

from botocore.credentials import Credentials
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

LOGICAL_INDEX = os.getenv("OPENSEARCH_INDEX", "datasets")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set after sourcing the app profile")
    return value


def hostname(value: str) -> str:
    if "://" not in value:
        return value
    parsed = urlparse(value)
    if not parsed.hostname:
        raise RuntimeError(f"Could not parse OPENSEARCH_HOST={value!r}")
    return parsed.hostname


def create_client() -> OpenSearch:
    auth = AWSV4SignerAuth(
        Credentials(
            access_key=required_env("OPENSEARCH_ACCESS_KEY"),
            secret_key=required_env("OPENSEARCH_SECRET_KEY"),
        ),
        os.getenv("AWS_REGION", "us-gov-west-1"),
        "es",
    )
    return OpenSearch(
        hosts=[{"host": hostname(required_env("OPENSEARCH_HOST")), "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=60,
    )


def alias_targets(client: OpenSearch) -> list[dict]:
    """Which physical index/indexes the logical alias currently points at."""
    try:
        if not client.indices.exists_alias(name=LOGICAL_INDEX):
            return []
        aliases = client.indices.get_alias(name=LOGICAL_INDEX)
    except Exception:
        return []
    return [
        {
            "index": index_name,
            "is_write_index": bool(
                meta.get("aliases", {}).get(LOGICAL_INDEX, {}).get("is_write_index")
            ),
        }
        for index_name, meta in sorted(aliases.items())
    ]


def thread_pools(client: OpenSearch) -> dict:
    """{node_name: {pool_name: {active, queue, rejected, completed}}}"""
    rows = client.cat.thread_pool(
        thread_pool_patterns="search,write,index,bulk",
        format="json",
        h="node_name,name,active,queue,rejected,completed",
    )
    out: dict = {}
    for row in rows:
        out.setdefault(row.get("node_name", "?"), {})[row.get("name", "?")] = {
            "active": row.get("active"),
            "queue": row.get("queue"),
            "rejected": row.get("rejected"),
            "completed": row.get("completed"),
        }
    return out


def node_stats(client: OpenSearch) -> dict:
    """Per-node JVM / OS / indexing counters keyed by node name."""
    stats = client.nodes.stats(metric="jvm,os,indices,process")
    out: dict = {}
    for node in stats.get("nodes", {}).values():
        name = node.get("name", "?")
        jvm_mem = node.get("jvm", {}).get("mem", {})
        collectors = node.get("jvm", {}).get("gc", {}).get("collectors", {})
        os_stats = node.get("os", {})
        os_mem = os_stats.get("mem", {})
        os_cpu = os_stats.get("cpu", {})
        process_cpu = node.get("process", {}).get("cpu", {})
        indices = node.get("indices", {})
        out[name] = {
            "heap_used_bytes": jvm_mem.get("heap_used_in_bytes"),
            "heap_max_bytes": jvm_mem.get("heap_max_in_bytes"),
            "heap_used_percent": jvm_mem.get("heap_used_percent"),
            "young_gc_count": collectors.get("young", {}).get("collection_count"),
            "young_gc_millis": collectors.get("young", {}).get(
                "collection_time_in_millis"
            ),
            "old_gc_count": collectors.get("old", {}).get("collection_count"),
            "old_gc_millis": collectors.get("old", {}).get("collection_time_in_millis"),
            "os_cpu_percent": os_cpu.get("percent"),
            "os_load_1m": (os_cpu.get("load_average") or {}).get("1m"),
            "os_load_5m": (os_cpu.get("load_average") or {}).get("5m"),
            "os_load_15m": (os_cpu.get("load_average") or {}).get("15m"),
            "os_mem_used_percent": os_mem.get("used_percent"),
            "os_mem_total_bytes": os_mem.get("total_in_bytes"),
            "process_cpu_percent": process_cpu.get("percent"),
            "indexing_index_total": indices.get("indexing", {}).get("index_total"),
            "indexing_index_time_millis": indices.get("indexing", {}).get(
                "index_time_in_millis"
            ),
            "indexing_current": indices.get("indexing", {}).get("index_current"),
            "search_query_total": indices.get("search", {}).get("query_total"),
            "search_query_time_millis": indices.get("search", {}).get(
                "query_time_in_millis"
            ),
            "search_query_current": indices.get("search", {}).get("query_current"),
            "refresh_total": indices.get("refresh", {}).get("total"),
            "merges_current": indices.get("merges", {}).get("current"),
            "docs_count": indices.get("docs", {}).get("count"),
            "store_size_bytes": indices.get("store", {}).get("size_in_bytes"),
        }
    return out


def main() -> int:
    client = create_client()

    cat_nodes = client.cat.nodes(
        format="json",
        h=(
            "name,node.role,cpu,load_1m,load_5m,load_15m,"
            "ram.percent,heap.percent,disk.used_percent"
        ),
    )
    cat_indices = client.cat.indices(
        index=f"{LOGICAL_INDEX}*",
        format="json",
        s="index",
        h="health,status,index,docs.count,docs.deleted,store.size,pri.store.size,pri,rep",
    )
    cat_allocation = client.cat.allocation(
        format="json",
        h="node,shards,disk.indices,disk.used,disk.avail,disk.total,disk.percent",
    )

    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "logical_index": LOGICAL_INDEX,
        "alias_targets": alias_targets(client),
        "cluster_health": client.cluster.health(),
        "cat_nodes": cat_nodes,
        "cat_indices": cat_indices,
        "cat_allocation": cat_allocation,
        "node_stats": node_stats(client),
        "thread_pools": thread_pools(client),
    }

    json.dump(snapshot, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # keep stdout clean: JSON or nothing
        print(f"OpenSearch metrics collection failed: {exc}", file=sys.stderr)
        sys.exit(1)
