#!/bin/bash

# Monitor Cloud Foundry task logs until the task exits.
#
# Usage: monitor_cf_logs.sh <app_name> <task_name> [warning_pattern]
#
# Exit codes:
#   0 - Task exited 0
#   1 - Task exited non-zero
#
# If warning_pattern matches one or more log lines, writes
# "warning_pattern_matched" to .cf_monitor_result in GITHUB_WORKSPACE (or the
# current directory) so CI can branch on the result without changing the task
# exit status.

app_to_monitor=$1
task_to_monitor=$2
warning_pattern=${3:-}

result_file="${GITHUB_WORKSPACE:-.}/.cf_monitor_result"
rm -f "$result_file"

# install a better version of grep which supports --line-buffered
apk add grep

write_warning_result() {
  echo "warning_pattern_matched" > "$result_file"
}

while read -r line ; do
  echo "$line"
  if [[ -n "$warning_pattern" ]] && echo "$line" | grep -qF "$warning_pattern"; then
    write_warning_result
  fi
  if echo "$line" | grep -q "OUT Exit status 0"; then
    exit 0
  elif echo "$line" | grep -q "OUT Exit status"; then
    exit 1
  fi
done < <(cf logs "$app_to_monitor" | grep --line-buffered "\[APP/TASK/$task_to_monitor/0\]")
