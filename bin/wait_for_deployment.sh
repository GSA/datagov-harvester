#!/bin/bash

# Wait for an app's in-flight Cloud Foundry deployment to finish.
#
# Every deploy in this repo pushes with `--strategy rolling --no-wait` (see
# .github/actions/deploy/action.yml), so the push command -- and therefore the
# GitHub job -- returns while the rollout is still in progress. That is fine for a
# deploy on its own, but not when something downstream has to run *the code that
# was just pushed*.
#
# The case that forces this script to exist: `flask search rebuild-index` runs via
# `cf run-task`, which uses the app's *current* droplet. If the rollout has not
# finished, that may still be the previous droplet, and the rebuild would then
# write documents in the OLD shape into the NEW cluster. Both verification gates
# would pass anyway, because `flask search compare` only checks id sets and
# last_harvested_date -- never document shape. So the failure would be silent, and
# would only surface later as wrong search results.
#
# Usage: wait_for_deployment.sh <app_name> [timeout_seconds]
#
# Exits 0 once no ACTIVE deployment remains, non-zero on timeout. Read-only.

set -euo pipefail

app_name=${1:?Usage: wait_for_deployment.sh <app_name> [timeout_seconds]}
timeout_seconds=${2:-1800}
poll_seconds=${DEPLOYMENT_POLL_SECONDS:-15}
# A short quiet period, for the same reason wait_for_harvest_tasks.sh has one: CF
# briefly reports no ACTIVE deployment between superseding a deployment and
# starting its replacement, and a restart cron colliding with this deploy does
# exactly that. Returning in that gap would hand the caller the old droplet.
quiet_seconds=${DEPLOYMENT_QUIET_SECONDS:-20}
deadline=$(( $(date +%s) + timeout_seconds ))
quiet_since=0

# The cg-cli-tools image is Alpine and does not always carry jq. Guarded rather
# than an unconditional `apk add` so this stays runnable locally and under test.
if ! command -v jq > /dev/null 2>&1; then
  apk add --no-cache jq
fi

app_guid=$(cf app "$app_name" --guid)

echo "=== waiting for ${app_name} deployments to finish ==="

while true; do
  # Scoped by app_guids so a deployment of some other app in the same space --
  # datagov-harvest-proxy, or catalog -- does not hold this up.
  active_count=$(
    cf curl "/v3/deployments?app_guids=${app_guid}&status_values=ACTIVE" |
      jq '.resources | length'
  )

  if [[ "$active_count" -eq 0 ]]; then
    now=$(date +%s)
    if [[ "$quiet_since" -eq 0 ]]; then
      quiet_since=$now
      echo "  no active deployment; confirming it stays that way..."
    elif [[ $(( now - quiet_since )) -ge "$quiet_seconds" ]]; then
      echo "  ${app_name} has no active deployment after a ${quiet_seconds}s quiet period."
      echo "  The running instances are serving the droplet that was just pushed."
      exit 0
    fi
  else
    quiet_since=0
    echo "  waiting for ${active_count} active deployment(s) of ${app_name}..."
  fi

  if [[ $(date +%s) -ge "$deadline" ]]; then
    echo "" >&2
    echo "Timed out after ${timeout_seconds}s waiting for ${app_name} to finish deploying." >&2
    echo "Anything that depends on the new code must NOT run: a task started now" >&2
    echo "could still get the previous droplet. Check the deployment with:" >&2
    echo "  cf app ${app_name}" >&2
    echo "  cf curl \"/v3/deployments?app_guids=${app_guid}&status_values=ACTIVE\"" >&2
    exit 1
  fi

  sleep "$poll_seconds"
done
