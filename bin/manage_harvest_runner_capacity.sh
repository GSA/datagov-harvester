#!/bin/bash

set -euo pipefail

usage="Usage: manage_harvest_runner_capacity.sh <pause|restore> <app_name> <vars_file>"
action=${1:?$usage}
app_name=${2:?$usage}
vars_file=${3:?$usage}
poll_seconds=${CF_RESTART_POLL_SECONDS:-10}
timeout_seconds=${CF_RESTART_TIMEOUT_SECONDS:-900}
max_tasks_env=HARVEST_RUNNER_MAX_TASKS

if [[ ! -f "$vars_file" ]]; then
  echo "Vars file does not exist: $vars_file" >&2
  exit 1
fi

if ! configured_value=$(
  awk -v key="$max_tasks_env" '
    $0 ~ "^[[:space:]]*" key "[[:space:]]*:" {
      value = $0
      sub("^[[:space:]]*" key "[[:space:]]*:[[:space:]]*", "", value)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      configured_value = value
      count++
    }
    END {
      if (count == 1) {
        print configured_value
      } else {
        exit 1
      }
    }
  ' "$vars_file"
); then
  echo "Expected exactly one $max_tasks_env value in $vars_file." >&2
  exit 1
fi
if [[ ! "$configured_value" =~ ^[0-9]+$ || "$configured_value" -eq 0 ]]; then
  echo "Expected $max_tasks_env in $vars_file to be a positive integer; got '$configured_value'." >&2
  exit 1
fi

if ! command -v jq >/dev/null; then
  apk add --no-cache jq
fi

app_guid=$(cf app "$app_name" --guid)

restart_and_confirm() {
  local expected_is_set=$1
  local expected_value=${2:-}
  local deadline=$(( $(date +%s) + timeout_seconds ))
  local before_processes before_process_count before_process_guid
  local before_desired_instances before_stats before_running_instances
  local before_instance_guids before_instance_guid_count

  before_processes=$(cf curl "/v3/apps/${app_guid}/processes")
  before_process_count=$(echo "$before_processes" | jq '[.resources[] | select(.type == "web")] | length')
  before_process_guid=$(echo "$before_processes" | jq -r '[.resources[] | select(.type == "web")][0].guid // empty')
  before_desired_instances=$(echo "$before_processes" | jq -r '[.resources[] | select(.type == "web")][0].instances // 0')
  if [[ "$before_process_count" -ne 1 ||
        -z "$before_process_guid" ||
        "$before_desired_instances" -le 0 ]]; then
    echo "Could not identify one running web process before restarting $app_name." >&2
    exit 1
  fi

  before_stats=$(cf curl "/v3/processes/${before_process_guid}/stats")
  before_running_instances=$(echo "$before_stats" | jq '[.resources[] | select(.state == "RUNNING")] | length')
  before_instance_guids=$(echo "$before_stats" | jq -c '[.resources[].instance_guid // empty] | sort')
  before_instance_guid_count=$(echo "$before_instance_guids" | jq 'length')
  if [[ "$before_running_instances" -ne "$before_desired_instances" ||
        "$before_instance_guid_count" -ne "$before_desired_instances" ]]; then
    echo "All desired web instances must be running and report instance GUIDs before restart." >&2
    exit 1
  fi

  cf restart "$app_name" --strategy rolling

  while true; do
    local deployments processes process_count process_guid desired_instances stats
    local active_deployments observed_instances running_instances environment env_matches
    local instance_guids instance_guid_count reused_instance_guids

    deployments=$(cf curl "/v3/deployments?status_values=ACTIVE&per_page=5000")
    active_deployments=$(
      echo "$deployments" |
        jq --arg app_guid "$app_guid" \
          '[.resources[] | select(.relationships.app.data.guid == $app_guid)] | length'
    )

    processes=$(cf curl "/v3/apps/${app_guid}/processes")
    process_count=$(echo "$processes" | jq '[.resources[] | select(.type == "web")] | length')
    process_guid=$(echo "$processes" | jq -r '[.resources[] | select(.type == "web")][0].guid // empty')
    desired_instances=$(echo "$processes" | jq -r '[.resources[] | select(.type == "web")][0].instances // 0')

    observed_instances=0
    running_instances=0
    instance_guids='[]'
    instance_guid_count=0
    reused_instance_guids=$before_desired_instances
    if [[ "$process_count" -eq 1 && -n "$process_guid" ]]; then
      stats=$(cf curl "/v3/processes/${process_guid}/stats")
      observed_instances=$(echo "$stats" | jq '.resources | length')
      running_instances=$(echo "$stats" | jq '[.resources[] | select(.state == "RUNNING")] | length')
      instance_guids=$(echo "$stats" | jq -c '[.resources[].instance_guid // empty] | sort')
      instance_guid_count=$(echo "$instance_guids" | jq 'length')
      reused_instance_guids=$(
        jq -n \
          --argjson before "$before_instance_guids" \
          --argjson after "$instance_guids" \
          '$before | map(. as $guid | $after | index($guid)) | map(select(. != null)) | length'
      )
    fi

    environment=$(cf curl "/v3/apps/${app_guid}/env")
    if [[ "$expected_is_set" == true ]]; then
      if [[ $(echo "$environment" | jq -r --arg name "$max_tasks_env" '.environment_variables[$name] // empty') == "$expected_value" ]]; then
        env_matches=true
      else
        env_matches=false
      fi
    elif echo "$environment" | jq -e --arg name "$max_tasks_env" '.environment_variables | has($name)' >/dev/null; then
      env_matches=false
    else
      env_matches=true
    fi

    if [[ "$active_deployments" -eq 0 &&
          "$process_count" -eq 1 &&
          "$desired_instances" -gt 0 &&
          "$observed_instances" -eq "$desired_instances" &&
          "$running_instances" -eq "$desired_instances" &&
          "$instance_guid_count" -eq "$desired_instances" &&
          "$reused_instance_guids" -eq 0 &&
          "$env_matches" == true ]]; then
      echo "Confirmed all $desired_instances web instance(s) were replaced and are running."
      return
    fi

    if [[ $(date +%s) -ge "$deadline" ]]; then
      echo "Timed out confirming the rolling restart of $app_name." >&2
      echo "active_deployments=$active_deployments desired_instances=$desired_instances observed_instances=$observed_instances running_instances=$running_instances instance_guid_count=$instance_guid_count reused_instance_guids=$reused_instance_guids env_matches=$env_matches" >&2
      exit 1
    fi

    echo "Waiting for rolling restart: active_deployments=$active_deployments running_instances=$running_instances/$desired_instances reused_instance_guids=$reused_instance_guids"
    sleep "$poll_seconds"
  done
}

case "$action" in
  pause)
    environment=$(cf curl "/v3/apps/${app_guid}/env")
    if echo "$environment" | jq -e --arg name "$max_tasks_env" '.environment_variables | has($name)' >/dev/null; then
      current_value=$(echo "$environment" | jq -r --arg name "$max_tasks_env" '.environment_variables[$name]')
    else
      current_value="<unset>"
    fi
    if [[ "$current_value" != "$configured_value" ]]; then
      echo "Current $max_tasks_env value '$current_value' does not match canonical value '$configured_value' from $vars_file." >&2
      exit 1
    fi

    cf set-env "$app_name" "$max_tasks_env" 0
    restart_and_confirm true 0
    echo "Harvest task scheduling is disabled."
    ;;
  restore)
    environment=$(cf curl "/v3/apps/${app_guid}/env")
    if echo "$environment" | jq -e --arg name "$max_tasks_env" '.environment_variables | has($name)' >/dev/null; then
      current_value=$(echo "$environment" | jq -r --arg name "$max_tasks_env" '.environment_variables[$name]')
    else
      current_value="<unset>"
    fi

    if [[ "$current_value" == "$configured_value" ]]; then
      echo "Harvest runner capacity already matches $vars_file; nothing to restore."
      exit 0
    fi
    if [[ "$current_value" != 0 ]]; then
      echo "Refusing to restore $max_tasks_env: current value '$current_value' is neither paused nor canonical ('$configured_value')." >&2
      exit 1
    fi

    cf set-env "$app_name" "$max_tasks_env" "$configured_value"
    restart_and_confirm true "$configured_value"
    echo "Harvest runner capacity was restored."
    ;;
  *)
    echo "Unknown action: $action" >&2
    exit 2
    ;;
esac
