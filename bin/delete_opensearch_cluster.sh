#!/bin/bash

# Delete an OpenSearch service instance, refusing if anything still points at it.
#
# Two callers, both destructive and irreversible, which is why the guard below is
# not optional:
#   - decommission: remove the retired cluster after a promotion is verified.
#   - teardown:     remove a half-built replacement cluster after a failed rebuild.
#
# The guard is what keeps the second caller safe. A workflow condition deciding
# "the rebuild failed, so delete the replacement" is a piece of YAML that can be
# wrong; this check reads the apps themselves. If any app resolves this instance as
# its live OpenSearch cluster, deleting it would take search down, so the script
# refuses regardless of what the caller believes.
#
# `cf delete-service` fails while bindings exist, so every bound app is unbound
# first -- but only after the guard has passed.
#
# See docs/ops/migrate-opensearch-cluster.md.

set -euo pipefail

usage="Usage: delete_opensearch_cluster.sh <service_instance> [app_name...]"
service_name=${1:-}
shift || true
apps=("$@")
if [[ ${#apps[@]} -eq 0 ]]; then
  apps=(datagov-harvest datagov-catalog)
fi

if [[ -z "$service_name" ]]; then
  echo "$usage" >&2
  exit 1
fi

if ! cf service "$service_name" > /dev/null 2>&1; then
  # Idempotent: a re-dispatched workflow must not fail because the previous run
  # already got here.
  echo "No service instance named '${service_name}'; nothing to delete."
  exit 0
fi

echo "=== delete ${service_name} ==="

# --- Guard: is this instance live for anyone? ------------------------------
#
# Read the resolved host from inside each container rather than trusting `cf env`,
# which cannot see OPENSEARCH_HOST (.profile exports it at container start). This is
# the same technique as bin/report_opensearch_cluster.sh.
#
# A container we cannot read is treated as "in use". Failing closed matters more
# than convenience here: the alternative is deleting a cluster because cf ssh was
# briefly unavailable.
in_use=()
for app in "${apps[@]}"; do
  if ! cf app "$app" > /dev/null 2>&1; then
    echo "  ${app}: not in this space, skipping."
    continue
  fi

  # One round trip: read both the host the app resolved AND this instance's own
  # host out of the same VCAP_SERVICES, then compare. Comparing hosts rather than
  # instance names matters because a promotion renames instances, so the name an
  # app was configured with may no longer be the name it is serving from.
  resolved=$(
    cf ssh "$app" -c "cd app 2>/dev/null || cd /home/vcap/app
      instance_host=\$(echo \$VCAP_SERVICES | jq -r '..|objects|select(.name==\"${service_name}\")|.credentials.host // empty' | head -n 1)
      if ( source .profile ) >/dev/null 2>&1; then
        source .profile >/dev/null 2>&1
        printf 'ok|%s|%s' \"\$OPENSEARCH_HOST\" \"\$instance_host\"
      else
        printf 'profile-failed||'
      fi" 2>/dev/null | tr -d '\r' || true
  )

  case "$resolved" in
    ok\|*)
      rest=${resolved#ok|}
      live_host=${rest%%|*}
      instance_host=${rest#*|}
      if [[ -n "$instance_host" && "$instance_host" == "$live_host" ]]; then
        in_use+=("$app")
        echo "  ${app}: LIVE on this instance (${live_host})."
      else
        echo "  ${app}: not using this instance."
      fi
      ;;
    *)
      in_use+=("$app")
      echo "  ${app}: could not read the container; assuming it is in use." >&2
      ;;
  esac
done

if [[ ${#in_use[@]} -gt 0 ]]; then
  echo "" >&2
  echo "Refusing to delete ${service_name}: still serving ${in_use[*]}." >&2
  echo "Move the app(s) off it first -- deleting it now would take search down." >&2
  exit 1
fi

# --- Unbind, then delete ---------------------------------------------------
for app in "${apps[@]}"; do
  if ! cf app "$app" > /dev/null 2>&1; then
    continue
  fi
  if cf curl "/v3/service_credential_bindings?app_names=${app}&service_instance_names=${service_name}" \
    | jq -e '.pagination.total_results > 0' > /dev/null 2>&1; then
    echo "  unbinding ${app} from ${service_name}..."
    cf unbind-service "$app" "$service_name"
  fi
done

echo "  deleting ${service_name}..."
cf delete-service "$service_name" -f --wait

echo ""
echo "Deleted ${service_name}."
