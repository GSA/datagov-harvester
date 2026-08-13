#!/bin/bash

set -euo pipefail

catalog_app=${1:-datagov-catalog}

if ! cf app "$catalog_app" >/dev/null 2>&1; then
  echo "No ${catalog_app} app in this space; skipping catalog restart."
  exit 0
fi

if cf restart "$catalog_app" --strategy rolling; then
  echo "${catalog_app} completed its rolling restart."
  exit 0
fi

app_guid=$(cf app "$catalog_app" --guid)
active_deployments=$(
  cf curl "/v3/deployments?app_guids=${app_guid}&status_values=ACTIVE" |
    jq '.resources | length'
)
if [[ "$active_deployments" -gt 0 ]]; then
  echo "${catalog_app} already has an active deployment; treating it as the restart." >&2
  exit 0
fi

echo "${catalog_app} restart failed and no active deployment will complete it." >&2
exit 1
