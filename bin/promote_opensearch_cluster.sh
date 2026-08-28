#!/bin/bash

set -euo pipefail

script_dir="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=bin/lib/cf_service_binding.sh
source "${script_dir}/lib/cf_service_binding.sh"

usage="Usage: promote_opensearch_cluster.sh <next_service> <canonical_service> [catalog_app]"
next_service=${1:-}
canonical_service=${2:-}
catalog_app=${3:-datagov-catalog}
retired_service="${canonical_service}-old"

if [[ -z "$next_service" || -z "$canonical_service" ]]; then
  echo "$usage" >&2
  exit 1
fi

if ! cf service "$next_service" >/dev/null 2>&1; then
  echo "No service instance named '${next_service}'." >&2
  exit 1
fi
if ! cf service "$canonical_service" >/dev/null 2>&1; then
  echo "No service instance named '${canonical_service}'." >&2
  exit 1
fi
if cf service "$retired_service" >/dev/null 2>&1; then
  echo "${retired_service} already exists; refusing a partial service-name swap." >&2
  exit 1
fi

if cf app "$catalog_app" >/dev/null 2>&1; then
  if ! cf_service_binding_exists "$catalog_app" "$next_service"; then
    echo "${catalog_app} is not bound to ${next_service}." >&2
    exit 1
  fi
else
  echo "No ${catalog_app} app in this space; catalog restart will be skipped."
fi

echo "Renaming ${canonical_service} to ${retired_service}."
cf rename-service "$canonical_service" "$retired_service"

echo "Renaming ${next_service} to ${canonical_service}."
if ! cf rename-service "$next_service" "$canonical_service"; then
  echo "Replacement rename failed; restoring ${retired_service} to ${canonical_service}." >&2
  if ! cf rename-service "$retired_service" "$canonical_service"; then
    echo "Automatic rollback also failed; the canonical service name must be restored manually." >&2
  fi
  exit 1
fi

echo "${canonical_service} now names the replacement cluster."
echo "${retired_service} remains available for running tasks and rollback."
