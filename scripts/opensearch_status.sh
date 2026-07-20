#!/usr/bin/env bash
#
# Query the bound OpenSearch cluster from a running Cloud Foundry app instance.
# The Python payload is streamed over SSH, so it does not need to be deployed.

set -o errexit
set -o nounset
set -o pipefail

app_name="${CF_APP_NAME:-datagov-harvest}"
app_instance="${CF_APP_INSTANCE:-0}"

if [ "$#" -gt 1 ]; then
  echo "Usage: $0 [APP_NAME]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  app_name="$1"
fi

if ! command -v cf >/dev/null 2>&1; then
  echo "The Cloud Foundry CLI (cf) is required." >&2
  exit 1
fi

echo "Cloud Foundry target:"
cf target
echo
echo "Querying OpenSearch through ${app_name} instance ${app_instance}..."

remote_command='
set -o errexit
set -o nounset
set -o pipefail

export PATH="$HOME/deps/0/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/deps/0/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cd "$HOME/app"
. "$HOME/app/.profile" >/dev/null

if ! command -v python >/dev/null 2>&1; then
  echo "Python was not found in the Cloud Foundry buildpack PATH." >&2
  exit 1
fi

exec python -
'

cf ssh "$app_name" -i "$app_instance" -T -c "$remote_command" <<'PYTHON'
from __future__ import annotations

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


def section(title: str, body: str) -> None:
    print(f"\n=== {title} ===")
    print(body.rstrip() if body else "(none)")


def human_bytes(value: int | None) -> str:
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


def jvm_table(stats: dict) -> str:
    lines = [
        f"{'node':<36} {'heap used':>12} {'heap max':>12} "
        f"{'heap %':>8} {'young GC':>12} {'old GC':>12}"
    ]
    for node in sorted(
        stats.get("nodes", {}).values(), key=lambda item: item.get("name", "")
    ):
        jvm = node.get("jvm", {})
        memory = jvm.get("mem", {})
        collectors = jvm.get("gc", {}).get("collectors", {})
        lines.append(
            f"{node.get('name', '?'):<36} "
            f"{human_bytes(memory.get('heap_used_in_bytes')):>12} "
            f"{human_bytes(memory.get('heap_max_in_bytes')):>12} "
            f"{memory.get('heap_used_percent', '-'):>8} "
            f"{collectors.get('young', {}).get('collection_count', '-'):>12} "
            f"{collectors.get('old', {}).get('collection_count', '-'):>12}"
        )
    return "\n".join(lines)


def logical_index_status(client: OpenSearch) -> str:
    if client.indices.exists_alias(name=LOGICAL_INDEX):
        aliases = client.indices.get_alias(name=LOGICAL_INDEX)
        lines = []
        for index_name, metadata in sorted(aliases.items()):
            alias = metadata.get("aliases", {}).get(LOGICAL_INDEX, {})
            lines.append(
                f"{LOGICAL_INDEX} -> {index_name} "
                f"(write index: {alias.get('is_write_index', False)})"
            )
        return "\n".join(lines)

    if client.indices.exists(index=LOGICAL_INDEX):
        return f"{LOGICAL_INDEX} is a concrete legacy index, not an alias"

    return f"{LOGICAL_INDEX} does not exist as an alias or concrete index"


def main() -> int:
    opensearch_host = hostname(required_env("OPENSEARCH_HOST"))
    access_key = required_env("OPENSEARCH_ACCESS_KEY")
    secret_key = required_env("OPENSEARCH_SECRET_KEY")
    region = os.getenv("AWS_REGION", "us-gov-west-1")

    auth = AWSV4SignerAuth(
        Credentials(access_key=access_key, secret_key=secret_key),
        region,
        "es",
    )
    client = OpenSearch(
        hosts=[{"host": opensearch_host, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=60,
    )

    print(f"Snapshot time: {datetime.now(timezone.utc).isoformat()}")
    print(f"Logical index: {LOGICAL_INDEX}")

    section("Current logical index", logical_index_status(client))
    section(
        f"Physical indexes matching {LOGICAL_INDEX}*",
        client.cat.indices(
            index=f"{LOGICAL_INDEX}*",
            v=True,
            s="index",
            h="health,status,index,docs.count,store.size,pri.store.size,pri,rep",
        ),
    )
    section("Cluster health", client.cat.health(v=True))
    section(
        "Disk allocation",
        client.cat.allocation(
            v=True,
            h=(
                "node,shards,disk.indices,disk.used,disk.avail,"
                "disk.total,disk.percent"
            ),
        ),
    )
    section(
        "Node CPU / memory (point-in-time)",
        client.cat.nodes(
            v=True,
            h=(
                "name,node.role,cpu,load_1m,load_5m,load_15m,"
                "ram.percent,heap.percent,disk.used_percent"
            ),
        ),
    )
    section("JVM heap and garbage collection", jvm_table(client.nodes.stats(metric="jvm")))
    section(
        "Search / write / index thread pools",
        client.cat.thread_pool(
            thread_pool_patterns="search,write,index",
            v=True,
            h="node_name,name,active,queue,rejected,completed",
        ),
    )

    print(
        "\nThese are point-in-time OpenSearch node metrics. Use CloudWatch for "
        "historical CPUUtilization, JVMMemoryPressure, latency, and indexing rates."
    )
    return 0


try:
    sys.exit(main())
except Exception as exc:
    print(f"OpenSearch status check failed: {exc}", file=sys.stderr)
    sys.exit(1)
PYTHON
