#!/bin/bash

# Report which OpenSearch cluster each app has actually resolved.
#
# `cf env` is not enough on its own: it shows OPENSEARCH_SERVICE_NAME (set with
# `cf set-env`) but NOT OPENSEARCH_HOST, which `.profile` exports inside the
# container at start. So `cf env` tells you what the app was *told* to use, and
# this tells you what it *resolved* -- which is what you want after a cutover,
# a rename, or when diagnosing a failed start.
#
# Read-only. Safe to run at any time.

set -euo pipefail

# shellcheck source=bin/lib/cf_env.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib/cf_env.sh"

usage="Usage: report_opensearch_cluster.sh <app_name> [app_name...]"
apps=("$@")
failed=0

if [[ ${#apps[@]} -eq 0 ]]; then
  echo "$usage" >&2
  exit 1
fi

for app in "${apps[@]}"; do
  echo "=== $app ==="

  configured=$(cf_env_value "$app" OPENSEARCH_SERVICE_NAME)
  if [[ -n "$configured" ]]; then
    echo "  OPENSEARCH_SERVICE_NAME (cf env): $configured"
  else
    echo "  OPENSEARCH_SERVICE_NAME (cf env): unset -- using the .profile default"
  fi

  # cf ssh runs outside the buildpack's profile scripts, so source .profile to
  # get the same exports the app process sees.
  #
  # .profile aborts with exit 1 when it cannot resolve the named instance, which
  # would kill this shell before it printed anything -- indistinguishable from
  # cf ssh being unavailable, and that is precisely the failure this script
  # exists to diagnose. So capture .profile's own stderr and emit an explicit
  # marker either way, rather than inferring from empty output.
  # Two passes, because .profile both exports the values we want AND calls
  # `exit 1` when it cannot resolve the instance:
  #   1. Run it in a SUBSHELL to capture that exit status and its stderr. Doing
  #      this first means the failure is reported instead of silently killing
  #      the remote shell -- the case this script exists to diagnose.
  #   2. Only if it succeeded, source it in THIS shell to read the exports. A
  #      command substitution here would discard them.
  resolved=$(
    cf ssh "$app" -c 'cd app 2>/dev/null || cd /home/vcap/app
      err=$(mktemp)
      if ( source .profile ) >/dev/null 2>"$err"; then
        source .profile >/dev/null 2>&1
        printf "ok|%s|%s" "$OPENSEARCH_HOST" "$OPENSEARCH_NEXT_HOST"
      else
        printf "profile-failed|%s" "$(tail -n 1 "$err")"
      fi
      rm -f "$err"' 2>/dev/null | tr -d '\r' || true
  )

  case "$resolved" in
    ok\|*)
      resolved=${resolved#ok|}
      live_host=${resolved%%|*}
      next_host=${resolved#*|}
      echo "  resolved OPENSEARCH_HOST:      $live_host"
      if [[ -n "$next_host" ]]; then
        echo "  resolved OPENSEARCH_NEXT_HOST: $next_host"
      else
        echo "  resolved OPENSEARCH_NEXT_HOST: (none -- no replacement cluster bound)"
      fi
      ;;
    profile-failed\|*)
      # The app cannot start in this state; the message names the instance it
      # could not find.
      echo "  .profile FAILED: ${resolved#profile-failed|}" >&2
      echo "  This app cannot start until OPENSEARCH_SERVICE_NAME names a bound instance." >&2
      failed=1
      ;;
    *)
      echo "  could not read the container (is cf ssh enabled for this app?)" >&2
      failed=1
      ;;
  esac
done

exit "$failed"
