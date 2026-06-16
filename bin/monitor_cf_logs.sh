#!/bin/bash

# Monitor Cloud Foundry task logs until the task exits.
#
# Usage: monitor_cf_logs.sh <app_name> <task_name> [warning_pattern]
#
# Exit codes:
#   0 - Task exited 0 and warning_pattern did not match (or no pattern given)
#   1 - Task exited non-zero
#   2 - Task exited 0 but warning_pattern matched one or more log lines
#
# On exit 2, writes "index_batch_warning" to .cf_monitor_result in GITHUB_WORKSPACE
# (or the current directory) so CI can branch on the failure type.

app_to_monitor=$1
task_to_monitor=$2
warning_pattern=${3:-}

result_file="${GITHUB_WORKSPACE:-.}/.cf_monitor_result"
rm -f "$result_file"

warning_detected=false

# install a better version of grep which supports --line-buffered
apk add grep

write_warning_result() {
  echo "index_batch_warning" > "$result_file"
}

while read -r line ; do
  echo "$line"
  if [[ -n "$warning_pattern" ]] && echo "$line" | grep -qF "$warning_pattern"; then
    warning_detected=true
  fi
  if echo "$line" | grep -q "OUT Exit status 0"; then
    if [[ "$warning_detected" == true ]]; then
      write_warning_result
      exit 2
    fi
    exit 0
  elif echo "$line" | grep -q "OUT Exit status"; then
    exit 1
  fi
done < <(cf logs "$app_to_monitor" | grep --line-buffered "\[APP/TASK/$task_to_monitor/0\]")
