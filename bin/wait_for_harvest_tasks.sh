#!/bin/bash

set -euo pipefail

app_name=${1:?Usage: wait_for_harvest_tasks.sh <app_name> [timeout_seconds] [--force-kill-running-jobs]}
timeout_seconds=${2:-7200}
force_kill_running_jobs=false
if [[ "${3:-}" == "--force-kill-running-jobs" ]]; then
  force_kill_running_jobs=true
  timeout_seconds=${HARVEST_TASK_FORCE_KILL_TIMEOUT_SECONDS:-900}
elif [[ -n "${3:-}" ]]; then
  echo "Unknown option: $3" >&2
  exit 2
fi
poll_seconds=${HARVEST_TASK_POLL_SECONDS:-15}
quiet_seconds=${HARVEST_TASK_QUIET_SECONDS:-30}
deadline=$(( $(date +%s) + timeout_seconds ))
quiet_since=0
force_cancel_requested=false

apk add --no-cache jq

app_guid=$(cf app "$app_name" --guid)

while true; do
  tasks=$(cf curl "/v3/apps/${app_guid}/tasks?states=PENDING,RUNNING,CANCELING&per_page=5000")
  active_tasks=$(
    echo "$tasks" |
      jq -c '[.resources[] | select(
        (.state == "PENDING" or .state == "RUNNING" or .state == "CANCELING")
        and (.name | startswith("harvest-job-"))
      )]'
  )
  active_count=$(echo "$active_tasks" | jq 'length')

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
    if [[ "$force_cancel_requested" == true ]]; then
      echo "Waiting for $active_count canceled harvest task(s) to stop..."
    else
      echo "Waiting for $active_count active harvest task(s) to finish..."
    fi
  fi

  if [[ "$active_count" -gt 0 && $(date +%s) -ge "$deadline" ]]; then
    if [[ "$force_kill_running_jobs" == true && "$force_cancel_requested" == false ]]; then
      echo "Force-kill timeout reached; canceling $active_count active harvest task(s)..."
      while IFS=$'\t' read -r task_guid task_name; do
        if [[ -z "$task_guid" ]]; then
          continue
        fi
        echo "Canceling harvest task $task_name ($task_guid)..."
        cf curl -X POST "/v3/tasks/${task_guid}/actions/cancel" >/dev/null
      done < <(
        echo "$active_tasks" |
          jq -r '.[] | select(.state != "CANCELING") | [.guid, .name] | @tsv'
      )
      force_cancel_requested=true
    elif [[ "$force_kill_running_jobs" == false ]]; then
      echo "Timed out waiting for $active_count harvest task(s) to finish." >&2
      exit 1
    fi
  fi

  sleep "$poll_seconds"
done
