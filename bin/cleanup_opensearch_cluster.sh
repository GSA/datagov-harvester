#!/bin/bash

set -euo pipefail

script_dir="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=bin/lib/cf_service_binding.sh
source "${script_dir}/lib/cf_service_binding.sh"

usage="Usage: cleanup_opensearch_cluster.sh <retired_service> [harvest_app] [catalog_app]"
retired_service=${1:-}
harvest_app=${2:-datagov-harvest}
catalog_app=${3:-datagov-catalog}
result_file="${GITHUB_WORKSPACE:-.}/.opensearch_cleanup_required"
rm -f "$result_file"

if [[ -z "$retired_service" ]]; then
  echo "$usage" >&2
  exit 1
fi

if ! cf service "$retired_service" >/dev/null 2>&1; then
  echo "No ${retired_service} service exists; nothing to clean up."
  exit 0
fi

reasons=()

if ! cf app "$harvest_app" >/dev/null 2>&1; then
  echo "No ${harvest_app} app in the targeted space." >&2
  exit 1
fi

harvest_guid=$(cf app "$harvest_app" --guid)
active_harvest_tasks=$(
  cf curl "/v3/apps/${harvest_guid}/tasks?states=PENDING,RUNNING,CANCELING&per_page=5000" |
    jq '[
      .resources[]
      | select(
          (.state == "PENDING" or .state == "RUNNING" or .state == "CANCELING")
          and (.name | startswith("harvest-job-"))
        )
    ] | length'
)
if [[ "$active_harvest_tasks" -gt 0 ]]; then
  reasons+=("${active_harvest_tasks} harvest task(s) are still using the retired cluster")
fi

if cf app "$catalog_app" >/dev/null 2>&1; then
  catalog_guid=$(cf app "$catalog_app" --guid)
  active_catalog_deployments=$(
    cf curl "/v3/deployments?app_guids=${catalog_guid}&status_values=ACTIVE" |
      jq '.resources | length'
  )
  if [[ "$active_catalog_deployments" -gt 0 ]]; then
    reasons+=("${catalog_app} still has an active deployment")
  fi
fi

if [[ ${#reasons[@]} -gt 0 ]]; then
  printf '%s\n' "${reasons[@]}" | tee "$result_file"
  echo "Retaining ${retired_service}; an O&M cleanup issue is required."
  exit 0
fi

for app_name in "$harvest_app" "$catalog_app"; do
  if cf app "$app_name" >/dev/null 2>&1 &&
    cf_service_binding_exists "$app_name" "$retired_service"; then
    echo "Unbinding ${app_name} from ${retired_service}."
    cf unbind-service "$app_name" "$retired_service"
  fi
done

echo "Deleting ${retired_service}."
cf delete-service "$retired_service" -f --wait
