#!/bin/bash

set -euo pipefail

app_name=${1:?Usage: wait_for_harvest_tasks.sh <app_name> [timeout_seconds]}
timeout_seconds=${2:-7200}
poll_seconds=${HARVEST_TASK_POLL_SECONDS:-15}
quiet_seconds=${HARVEST_TASK_QUIET_SECONDS:-30}
deadline=$(( $(date +%s) + timeout_seconds ))
quiet_since=0

apk add --no-cache jq

app_guid=$(cf app "$app_name" --guid)

while true; do
  tasks=$(cf curl "/v3/apps/${app_guid}/tasks?states=PENDING,RUNNING,CANCELING&per_page=5000")
  active_count=$(
    echo "$tasks" |
      jq '[.resources[] | select(
        (.state == "PENDING" or .state == "RUNNING" or .state == "CANCELING")
        and (.name | startswith("harvest-job-"))
      )] | length'
  )

  if [[ "$active_count" -eq 0 ]]; then
    now=$(date +%s)
    if [[ "$quiet_since" -eq 0 ]]; then
      quiet_since=$now
      echo "No harvest tasks are running; confirming the queue stays drained..."
    elif [[ $(( now - quiet_since )) -ge "$quiet_seconds" ]]; then
      echo "No harvest tasks ran during the ${quiet_seconds}-second quiet period."
      exit 0
    fi
  else
    quiet_since=0
    echo "Waiting for $active_count active harvest task(s) to finish..."
  fi

  if [[ $(date +%s) -ge "$deadline" ]]; then
    echo "Timed out waiting for $active_count harvest task(s) to finish." >&2
    exit 1
  fi

  sleep "$poll_seconds"
done
