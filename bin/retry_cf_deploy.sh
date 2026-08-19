#!/bin/bash

set -uo pipefail

max_attempts=${CF_DEPLOY_RETRY_ATTEMPTS:-10}
sleep_seconds=${CF_DEPLOY_RETRY_SLEEP_SECONDS:-30}

if [[ $# -eq 0 ]]; then
  echo "Usage: retry_cf_deploy.sh <command and args...>" >&2
  echo "Wraps any command that pushes or restarts an app, so the scheduled" >&2
  echo "app-restart cron cannot fail the release by holding a deployment." >&2
  exit 2
fi

for attempt in $(seq 1 "$max_attempts"); do
  log_file=$(mktemp)
  "$@" 2>&1 | tee "$log_file"
  status=${PIPESTATUS[0]}

  if [[ "$status" -eq 0 ]]; then
    rm -f "$log_file"
    exit 0
  fi

  if ! grep -q "deployment is in flight" "$log_file"; then
    rm -f "$log_file"
    exit "$status"
  fi
  rm -f "$log_file"

  if [[ "$attempt" -eq "$max_attempts" ]]; then
    echo "Gave up after $max_attempts attempts: deployment still in flight." >&2
    exit "$status"
  fi

  echo "Attempt $attempt/$max_attempts hit an in-flight deployment (likely the scheduled app restart); retrying in ${sleep_seconds}s..."
  sleep "$sleep_seconds"
done
