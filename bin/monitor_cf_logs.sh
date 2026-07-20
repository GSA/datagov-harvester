#!/bin/bash

set -euo pipefail

# Monitor a Cloud Foundry task until the API reports a terminal state.
#
# Usage: monitor_cf_logs.sh <app_name> <task_name> [warning_pattern]
#
# Exit codes:
#   0 - Task succeeded
#   1 - Task failed or could not be verified
#
# If warning_pattern matches one or more log lines, writes
# "warning_pattern_matched" to .cf_monitor_result in GITHUB_WORKSPACE (or the
# current directory) so CI can branch on the result without changing the task
# exit status.

app_to_monitor=${1:?Usage: monitor_cf_logs.sh <app_name> <task_name> [warning_pattern]}
task_to_monitor=${2:?Usage: monitor_cf_logs.sh <app_name> <task_name> [warning_pattern]}
warning_pattern=${3:-}
poll_seconds=${CF_TASK_MONITOR_POLL_SECONDS:-10}
timeout_seconds=${CF_TASK_MONITOR_TIMEOUT_SECONDS:-10800}
deadline=$(( $(date +%s) + timeout_seconds ))

result_file="${GITHUB_WORKSPACE:-.}/.cf_monitor_result"
rm -f "$result_file"

if ! command -v jq >/dev/null || ! command -v grep >/dev/null; then
  apk add --no-cache grep jq
fi

seen_logs_file=$(mktemp)
trap 'rm -f "$seen_logs_file"' EXIT

write_warning_result() {
  echo "warning_pattern_matched" > "$result_file"
}

emit_new_logs() {
  local recent_logs line

  if ! recent_logs=$(cf logs "$app_to_monitor" --recent 2>&1); then
    echo "Warning: could not read recent logs for $app_to_monitor; task state will still be verified through the Cloud Foundry API." >&2
    return
  fi

  while IFS= read -r line; do
    if grep -Fqx -- "$line" "$seen_logs_file"; then
      continue
    fi
    echo "$line" | tee -a "$seen_logs_file"
    if [[ -n "$warning_pattern" ]] && echo "$line" | grep -qF "$warning_pattern"; then
      write_warning_result
    fi
  done < <(
    echo "$recent_logs" |
      grep -F "[APP/TASK/$task_to_monitor/0]" ||
      true
  )
}

app_guid=$(cf app "$app_to_monitor" --guid)

while true; do
  tasks=$(
    cf curl \
      "/v3/apps/${app_guid}/tasks?names=${task_to_monitor}&order_by=-created_at&per_page=1"
  )
  task_count=$(echo "$tasks" | jq '.resources | length')

  emit_new_logs

  if [[ "$task_count" -eq 0 ]]; then
    if [[ $(date +%s) -ge "$deadline" ]]; then
      echo "Timed out waiting for Cloud Foundry task $task_to_monitor to appear." >&2
      exit 1
    fi
    echo "Waiting for Cloud Foundry task $task_to_monitor to appear..."
    sleep "$poll_seconds"
    continue
  fi

  task_state=$(echo "$tasks" | jq -r '.resources[0].state')
  task_guid=$(echo "$tasks" | jq -r '.resources[0].guid')
  task_result=$(echo "$tasks" | jq -r '.resources[0].result.failure_reason // empty')

  case "$task_state" in
    SUCCEEDED)
      echo "Cloud Foundry task $task_to_monitor ($task_guid) succeeded."
      exit 0
      ;;
    FAILED)
      echo "Cloud Foundry task $task_to_monitor ($task_guid) failed: ${task_result:-no failure reason reported}" >&2
      exit 1
      ;;
    PENDING|RUNNING|CANCELING)
      ;;
    *)
      echo "Cloud Foundry task $task_to_monitor ($task_guid) reported unknown state: $task_state" >&2
      exit 1
      ;;
  esac

  if [[ $(date +%s) -ge "$deadline" ]]; then
    echo "Timed out waiting for Cloud Foundry task $task_to_monitor; last state was $task_state." >&2
    exit 1
  fi

  echo "Waiting for Cloud Foundry task $task_to_monitor; current state is $task_state..."
  sleep "$poll_seconds"
done
