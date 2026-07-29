#!/bin/bash

set -euo pipefail

# shellcheck source=bin/lib/cf_env.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/cf_env.sh"

usage="Usage: set_harvest_runner_capacity.sh <check|enable|disable> [app_name] [enabled_max_tasks]"
action=${1:-}
app_name=${2:-datagov-harvest}
max_tasks_env=HARVEST_RUNNER_MAX_TASKS

case "$action" in
  check)
    current_value=$(cf_env_value "$app_name" "$max_tasks_env")

    if [[ -n "$current_value" ]]; then
      echo "$max_tasks_env is currently set to $current_value."
    else
      echo "$max_tasks_env is not set in Cloud Foundry; the application defaults to 3."
    fi
    exit 0
    ;;
  enable)
    desired_value=${3:-3}
    if [[ ! "$desired_value" =~ ^[1-9][0-9]*$ ]]; then
      echo "enabled_max_tasks must be a positive integer." >&2
      exit 2
    fi
    ;;
  disable)
    desired_value=0
    ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac

cf set-env "$app_name" "$max_tasks_env" "$desired_value"

# Without --no-wait, cf waits until every instance in the rolling deployment
# has been replaced and is healthy.
cf restart "$app_name" --strategy rolling

echo "$max_tasks_env set to $desired_value."
echo "Confirmed the $app_name rolling restart completed."
