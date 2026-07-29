#!/bin/bash

# Point one or more apps at an OpenSearch service instance and roll them.
#
# Apps are processed in the order given, each fully rolled before the next
# starts, so an operator can stop after the first if something looks wrong.
# Harvester should come first: it is the only writer, so moving it first means
# no write lands on the cluster being left behind once harvesting resumes.
#
# Uses the blocking form of `cf restart --strategy rolling` (no --no-wait), so
# this only succeeds once Cloud Foundry reports the replacement instances
# healthy. A failed start therefore fails the step instead of silently leaving
# the app on the old cluster.

set -euo pipefail

# shellcheck source=bin/lib/cf_env.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/cf_env.sh"

usage="Usage: cutover_opensearch_cluster.sh <service_instance> <app_name> [app_name...]"
service_name=${1:-}
shift || true
apps=("$@")

if [[ -z "$service_name" || ${#apps[@]} -eq 0 ]]; then
  echo "$usage" >&2
  exit 1
fi

# Pre-flight every app before changing any of them. `.profile` resolves
# credentials by instance name, so pointing an app at an unbound instance leaves
# OPENSEARCH_HOST empty, the .profile guard fails the start, and every instance
# in the rolling restart fails its health check. Checking first turns a
# half-finished cutover into a no-op.
if ! cf service "$service_name" > /dev/null 2>&1; then
  echo "No service instance named '$service_name' in this space." >&2
  echo "Provision it first, or check the name with 'cf services'." >&2
  exit 1
fi

unbound=()
for app in "${apps[@]}"; do
  if ! cf curl "/v3/service_credential_bindings?app_names=${app}&service_instance_names=${service_name}" \
    | jq -e '.pagination.total_results > 0' > /dev/null 2>&1; then
    unbound+=("$app")
  fi
done
if [[ ${#unbound[@]} -gt 0 ]]; then
  echo "Not bound to $service_name: ${unbound[*]}" >&2
  echo "Bind first (inert until the next restart):" >&2
  for app in "${unbound[@]}"; do
    echo "  cf bind-service $app $service_name" >&2
  done
  exit 1
fi

for app in "${apps[@]}"; do
  echo "=== $app -> $service_name ==="

  previous=$(cf_env_value "$app" OPENSEARCH_SERVICE_NAME)
  # Record the old value: this is what you pass back to this script to roll back.
  if [[ -n "$previous" ]]; then
    echo "  previous OPENSEARCH_SERVICE_NAME: $previous"
  else
    echo "  previous OPENSEARCH_SERVICE_NAME: (unset -- .profile default)"
  fi

  if [[ "$previous" == "$service_name" ]]; then
    echo "  already set to $service_name; restarting anyway to be certain the"
    echo "  running instances have picked it up."
  else
    cf set-env "$app" OPENSEARCH_SERVICE_NAME "$service_name"
  fi

  echo "  rolling restart..."
  cf restart "$app" --strategy rolling
  echo "  $app is now on $service_name."
done

echo ""
echo "Cutover complete for: ${apps[*]}"
echo "To roll back, re-run with the previous service instance name."
