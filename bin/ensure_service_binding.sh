#!/bin/bash

set -euo pipefail

script_dir="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=bin/lib/cf_service_binding.sh
source "${script_dir}/lib/cf_service_binding.sh"

usage="Usage: ensure_service_binding.sh <service_instance> <app_name> [app_name...]"
service_name=${1:-}
shift || true
apps=("$@")

if [[ -z "$service_name" || ${#apps[@]} -eq 0 ]]; then
  echo "$usage" >&2
  exit 1
fi

max_attempts=${BIND_HOST_MAX_ATTEMPTS:-10}
retry_seconds=${BIND_HOST_RETRY_SECONDS:-30}

for app_name in "${apps[@]}"; do
  if ! cf app "$app_name" >/dev/null 2>&1; then
    echo "No app named '${app_name}' in the targeted space." >&2
    exit 1
  fi

  if cf_service_binding_exists "$app_name" "$service_name"; then
    echo "${app_name} is already bound to ${service_name}."
  else
    echo "Binding ${app_name} to ${service_name}."
    cf bind-service "$app_name" "$service_name"
  fi

  host=$(cf_binding_credential "$app_name" "$service_name" host)
  attempt=0
  while [[ -z "$host" && $attempt -lt $max_attempts ]]; do
    attempt=$((attempt + 1))
    echo "${app_name} binding has an empty host; rebinding (${attempt}/${max_attempts})."
    sleep "$retry_seconds"
    cf unbind-service "$app_name" "$service_name"
    cf bind-service "$app_name" "$service_name"
    host=$(cf_binding_credential "$app_name" "$service_name" host)
  done

  if [[ -z "$host" ]]; then
    echo "${app_name} has no OpenSearch host after ${max_attempts} rebind attempts." >&2
    exit 1
  fi

  echo "${app_name} resolves ${service_name} at ${host}."
done
