#!/bin/bash

set -euo pipefail

usage="Usage: set_harvest_runner_capacity.sh <enable|disable> [app_name] [secrets_service]"
action=${1:-}
app_name=${2:-datagov-harvest}
secrets_service=${3:-${app_name}-secrets}
credential_name=HARVEST_RUNNER_ENABLED
max_tasks_credential_name=HARVEST_RUNNER_MAX_TASKS
poll_seconds=${CF_RESTART_POLL_SECONDS:-5}
timeout_seconds=${CF_RESTART_TIMEOUT_SECONDS:-900}

case "$action" in
  enable)
    desired_value=true
    ;;
  disable)
    desired_value=false
    ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac

if ! command -v jq >/dev/null; then
  if command -v apk >/dev/null; then
    apk add --no-cache jq
  else
    echo "jq is required." >&2
    exit 1
  fi
fi

credentials_file=$(mktemp)
updated_credentials_file=$(mktemp)
trap 'rm -f "$credentials_file" "$updated_credentials_file"' EXIT
chmod 600 "$credentials_file" "$updated_credentials_file"

app_guid=$(cf app "$app_name" --guid)
if [[ -z "$app_guid" ]]; then
  echo "Could not find the GUID for $app_name." >&2
  exit 1
fi

service_guid=$(cf service "$secrets_service" --guid)
if [[ -z "$service_guid" ]]; then
  echo "Could not find the GUID for $secrets_service." >&2
  exit 1
fi

before_processes=$(cf curl "/v3/apps/${app_guid}/processes")
before_process_count=$(
  echo "$before_processes" |
    jq '[.resources[] | select(.type == "web")] | length'
)
before_process_guid=$(
  echo "$before_processes" |
    jq -r '[.resources[] | select(.type == "web")][0].guid // empty'
)
before_desired_instances=$(
  echo "$before_processes" |
    jq -r '[.resources[] | select(.type == "web")][0].instances // 0'
)
if [[ "$before_process_count" -ne 1 ||
      -z "$before_process_guid" ||
      "$before_desired_instances" -le 0 ]]; then
  echo "Could not identify one active web process for $app_name." >&2
  exit 1
fi

before_stats=$(cf curl "/v3/processes/${before_process_guid}/stats")
before_instance_guids=$(
  echo "$before_stats" |
    jq -c '[.resources[].instance_guid // empty] | unique | sort'
)
if [[ $(echo "$before_instance_guids" | jq 'length') -ne "$before_desired_instances" ]]; then
  echo "Every $app_name instance must report an instance GUID before restart." >&2
  exit 1
fi

cf curl "/v3/service_instances/${service_guid}/credentials" > "$credentials_file"
if ! jq -e 'type == "object"' "$credentials_file" >/dev/null; then
  echo "$secrets_service did not return a credential object." >&2
  exit 1
fi

configured_max_tasks=$(
  jq -r --arg name "$max_tasks_credential_name" \
    '.[$name] // "3" | tostring' "$credentials_file"
)
if [[ ! "$configured_max_tasks" =~ ^[1-9][0-9]*$ ]]; then
  echo "$max_tasks_credential_name must be a positive integer; got '$configured_max_tasks'." >&2
  exit 1
fi
if [[ "$desired_value" == "true" ]]; then
  effective_max_tasks=$configured_max_tasks
else
  effective_max_tasks=0
fi

current_value=$(
  jq -r --arg name "$credential_name" \
    'if has($name) then .[$name] | tostring else "<unset>" end' \
    "$credentials_file"
)
jq --arg name "$credential_name" --arg value "$desired_value" \
  '.[$name] = $value' "$credentials_file" > "$updated_credentials_file"

cf update-user-provided-service "$secrets_service" \
  -p "$updated_credentials_file"

confirmed_value=$(
  cf curl "/v3/service_instances/${service_guid}/credentials" |
    jq -r --arg name "$credential_name" '.[$name] // empty | tostring'
)
if [[ "$confirmed_value" != "$desired_value" ]]; then
  echo "Failed to confirm $credential_name=$desired_value in $secrets_service." >&2
  exit 1
fi

cf restart "$app_name" --strategy rolling

deadline=$(( $(date +%s) + timeout_seconds ))
while true; do
  deployments=$(cf curl "/v3/deployments?status_values=ACTIVE&per_page=5000")
  active_deployments=$(
    echo "$deployments" |
      jq --arg app_guid "$app_guid" \
        '[.resources[] | select(.relationships.app.data.guid == $app_guid)] | length'
  )

  processes=$(cf curl "/v3/apps/${app_guid}/processes")
  process_count=$(
    echo "$processes" |
      jq '[.resources[] | select(.type == "web")] | length'
  )
  process_guid=$(
    echo "$processes" |
      jq -r '[.resources[] | select(.type == "web")][0].guid // empty'
  )
  desired_instances=$(
    echo "$processes" |
      jq -r '[.resources[] | select(.type == "web")][0].instances // 0'
  )

  observed_instances=0
  running_instances=0
  instance_guids='[]'
  reused_instance_guids=$before_desired_instances
  logged_instances=0
  if [[ "$process_count" -eq 1 && -n "$process_guid" ]]; then
    stats=$(cf curl "/v3/processes/${process_guid}/stats")
    observed_instances=$(echo "$stats" | jq '.resources | length')
    running_instances=$(
      echo "$stats" |
        jq '[.resources[] | select(.state == "RUNNING")] | length'
    )
    instance_guids=$(
      echo "$stats" |
        jq -c '[.resources[].instance_guid // empty] | unique | sort'
    )
    reused_instance_guids=$(
      jq -n \
        --argjson before "$before_instance_guids" \
        --argjson current "$instance_guids" \
        '[ $before[] as $old | $current[] | select(. == $old) ] | length'
    )

    recent_logs=$(cf logs "$app_name" --recent 2>&1 || true)
    while IFS= read -r instance_guid; do
      marker="Harvester startup: ${max_tasks_credential_name}=${configured_max_tasks} ${credential_name}=${desired_value} EFFECTIVE_HARVEST_RUNNER_MAX_TASKS=${effective_max_tasks} CF_INSTANCE_GUID=${instance_guid}"
      if [[ "$recent_logs" == *"$marker"* ]]; then
        logged_instances=$((logged_instances + 1))
      fi
    done < <(echo "$instance_guids" | jq -r '.[]')
  fi

  if [[ "$active_deployments" -eq 0 &&
        "$process_count" -eq 1 &&
        "$desired_instances" -gt 0 &&
        "$observed_instances" -eq "$desired_instances" &&
        "$running_instances" -eq "$desired_instances" &&
        $(echo "$instance_guids" | jq 'length') -eq "$desired_instances" &&
        "$reused_instance_guids" -eq 0 &&
        "$logged_instances" -eq "$desired_instances" ]]; then
    echo "Confirmed all $desired_instances instance(s) restarted with $credential_name=$desired_value and effective max tasks $effective_max_tasks."
    break
  fi

  if [[ $(date +%s) -ge "$deadline" ]]; then
    echo "Timed out confirming the rolling restart of $app_name." >&2
    echo "active_deployments=$active_deployments running_instances=$running_instances/$desired_instances reused_instance_guids=$reused_instance_guids logged_instances=$logged_instances/$desired_instances" >&2
    exit 1
  fi

  echo "Waiting for restart confirmation: running_instances=$running_instances/$desired_instances reused_instance_guids=$reused_instance_guids logged_instances=$logged_instances/$desired_instances"
  sleep "$poll_seconds"
done

echo "$credential_name changed from $current_value to $desired_value."
echo "The $app_name rolling restart completed."
