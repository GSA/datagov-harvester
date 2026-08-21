#!/bin/bash

set -euo pipefail

script_dir="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=bin/lib/opensearch_plan.sh
source "${script_dir}/lib/opensearch_plan.sh"

usage="Usage: provision_opensearch_cluster.sh <next_service> [retired_service] [plan]"
next_service=${1:-}
retired_service=${2:-datagov-catalog-opensearch-old}
plan=${3:-}

if [[ -z "$next_service" ]]; then
  echo "$usage" >&2
  exit 1
fi

if cf service "$next_service" >/dev/null 2>&1; then
  echo "${next_service} already exists; clean up or inspect the previous migration first." >&2
  exit 1
fi

if cf service "$retired_service" >/dev/null 2>&1; then
  echo "${retired_service} already exists; the previous cluster has not been cleaned up." >&2
  exit 1
fi

space=$(opensearch_current_space)
if [[ -z "$plan" ]]; then
  plan=$(opensearch_plan_for_space "$space")
fi
if [[ -z "$plan" ]]; then
  echo "No default OpenSearch plan for space '${space}'." >&2
  exit 1
fi

echo "Creating ${next_service} in ${space} with plan ${plan}."
cf create-service --wait aws-elasticsearch "$plan" "$next_service" \
  -c "{\"ElasticsearchVersion\":\"${OPENSEARCH_ENGINE_VERSION}\"}"
