#!/bin/bash

# Promote the replacement cluster to canonical: move both apps onto it and give it
# the canonical service-instance name.
#
# The ordering here is the whole point of the script. The obvious sequence (delete
# the old instance, rename the new one, then repoint the apps) leaves both apps
# naming an instance that does not exist, and every new container start fails
# .profile's empty-host guard for as long as that lasts. This order instead moves
# the cluster *underneath* a name that never stops resolving:
#
#   1. rename <canonical> -> <canonical>-old.   (frees the canonical name)
#   2. rename <next> -> <canonical>.            (canonical now IS the new cluster)
#   3. roll the harvester.                      (re-resolves; picks up the new host)
#   4. roll catalog.                            (same)
#   5. unset OPENSEARCH_NEXT_SERVICE_NAME.      (housekeeping, off the hot path)
#
# NEITHER app is repointed with cf set-env, because neither needs to be: both
# resolve the canonical name, and after step 2 that name is the new cluster. The
# whole migration is two renames plus two restarts.
#
# The restarts in 3 and 4 are mandatory, not cosmetic. A rename is metadata-only --
# the AWS endpoint and credentials do not change -- and .profile resolves the host
# exactly once, at container start. So a running instance keeps talking to whatever
# endpoint it resolved at boot, and would stay on the old cluster indefinitely
# without a restart. Both are blocking rolling restarts, so existing instances keep
# serving if a start fails.
#
# An earlier version set OPENSEARCH_SERVICE_NAME=<next> on the harvester before the
# renames and unset it after. That was removed: the two cancelled out, and between
# them the variable named <next>, which step 2 had just renamed out of existence --
# a window spanning two commands and a full rolling restart, in which any container
# start (a cron restart, a crash, CF rescheduling) failed the empty-host guard. The
# only surviving exposure is between steps 1 and 2, which is a single command.
#
# Why catalog needs no cf set-env either: datagov-catalog's .profile resolves
# ${APP_NAME}-opensearch -- literally datagov-catalog-opensearch -- and does NOT
# read OPENSEARCH_SERVICE_NAME. Setting that variable on catalog is a silent no-op.
# The rename in step 2 is the only thing that can move catalog.
#
# Does NOT delete the old cluster: that is bin/delete_opensearch_cluster.sh, run
# only after verification, so rollback survives this whole script. To roll back,
# reverse the renames or use bin/cutover_opensearch_cluster.sh.
#
# See docs/ops/migrate-opensearch-cluster.md.

set -euo pipefail

script_dir="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=bin/lib/cf_env.sh
source "${script_dir}/lib/cf_env.sh"

usage="Usage: promote_opensearch_cluster.sh <next_service> <canonical_service> [harvest_app] [catalog_app]"
next_service=${1:-}
canonical_service=${2:-}
harvest_app=${3:-datagov-harvest}
catalog_app=${4:-datagov-catalog}
retired_service="${canonical_service}-old"
# How long to wait between catalog restart attempts. Overridable so tests do not
# have to sit through the real backoff.
catalog_retry_seconds=${CATALOG_RESTART_RETRY_SECONDS:-60}

if [[ -z "$next_service" || -z "$canonical_service" ]]; then
  echo "$usage" >&2
  exit 1
fi
if [[ "$next_service" == "$canonical_service" ]]; then
  echo "next and canonical are both '${next_service}'; nothing to promote." >&2
  echo "This cluster has already been promoted." >&2
  exit 1
fi

# --- Pre-flight ------------------------------------------------------------
#
# Validate everything before mutating anything, so a failure here is a no-op
# rather than a half-promoted cluster. Renaming is cheap to start and awkward to
# unwind, so all four preconditions are checked up front.

if ! cf service "$next_service" > /dev/null 2>&1; then
  echo "No service instance named '${next_service}' in this space." >&2
  exit 1
fi
if ! cf service "$canonical_service" > /dev/null 2>&1; then
  echo "No service instance named '${canonical_service}' in this space." >&2
  echo "Nothing to retire -- has this migration already been promoted?" >&2
  exit 1
fi
if cf service "$retired_service" > /dev/null 2>&1; then
  echo "'${retired_service}' already exists, so the rename in step 2 would fail." >&2
  echo "A previous migration left it behind; delete it first with" >&2
  echo "  bin/delete_opensearch_cluster.sh ${retired_service}" >&2
  exit 1
fi

# The harvester must exist -- it is the app this script repoints and restarts.
if ! cf app "$harvest_app" > /dev/null 2>&1; then
  echo "No app named '${harvest_app}' in this space." >&2
  exit 1
fi

# Catalog is optional: a space may not run it. Detect that now rather than failing at
# the restart in step 5, which happens *after* the renames.
catalog_present=yes
if ! cf app "$catalog_app" > /dev/null 2>&1; then
  catalog_present=no
  echo "NOTE: no app named '${catalog_app}' in this space; skipping its restart." >&2
fi

# Every app that will exist must be bound to the replacement instance. Bindings
# survive renames, so a binding missing here stays missing after step 3, and the app
# then cannot resolve the canonical name at all -- .profile exits 1 and the app will
# not start. This is the check that catches create_cloudgov_services.sh having
# downgraded a failed catalog bind to a warning.
apps_to_check=("$harvest_app")
if [[ "$catalog_present" == yes ]]; then
  apps_to_check+=("$catalog_app")
fi
unbound=()
for app in "${apps_to_check[@]}"; do
  if ! cf curl "/v3/service_credential_bindings?app_names=${app}&service_instance_names=${next_service}" \
    | jq -e '.pagination.total_results > 0' > /dev/null 2>&1; then
    unbound+=("$app")
  fi
done
if [[ ${#unbound[@]} -gt 0 ]]; then
  echo "Not bound to ${next_service}: ${unbound[*]}" >&2
  echo "Bind first (inert until the next restart):" >&2
  for app in "${unbound[@]}"; do
    echo "  cf bind-service $app $next_service" >&2
  done
  exit 1
fi

echo "Promoting ${next_service} to ${canonical_service}."
echo "  ${canonical_service} will be retired as ${retired_service} (NOT deleted)."
echo ""

# --- 1 & 2. Swap the names -------------------------------------------------
#
# Adjacent on purpose: between them the canonical name does not exist, so keep the
# gap to a single command. Running instances of both apps are unaffected either way,
# because a rename does not change the endpoint they already resolved.
#
# Report the harvester's current override, if any, purely so the log records the
# state this ran against -- at rest it should be unset or already the canonical name.
current=$(cf_env_value "$harvest_app" OPENSEARCH_SERVICE_NAME)
if [[ -n "$current" ]]; then
  echo "${harvest_app} OPENSEARCH_SERVICE_NAME: ${current}"
  if [[ "$current" != "$canonical_service" ]]; then
    # Anything else means an operator or an aborted run left an override behind. It
    # would survive this script and keep the app pinned to a name we are about to
    # move, so refuse rather than produce a half-migrated app.
    echo "Refusing to promote: ${harvest_app} is pinned to '${current}', not" >&2
    echo "'${canonical_service}'. Clear it first so the rename takes effect:" >&2
    echo "  cf unset-env ${harvest_app} OPENSEARCH_SERVICE_NAME" >&2
    exit 1
  fi
else
  echo "${harvest_app} OPENSEARCH_SERVICE_NAME: (unset -- .profile default)"
fi
echo ""

echo "=== 1/5 rename ${canonical_service} -> ${retired_service} ==="
cf rename-service "$canonical_service" "$retired_service"

echo "=== 2/5 rename ${next_service} -> ${canonical_service} ==="
cf rename-service "$next_service" "$canonical_service"

# Confirm the name actually moved. Everything after this assumes the canonical name
# resolves again, and "renamed" vs "silently did not" is worth one cheap check at the
# one point in the script where the window is open.
if ! cf service "$canonical_service" > /dev/null 2>&1; then
  echo "'${canonical_service}' does not resolve after the rename." >&2
  echo "The window is still open. Recover with:" >&2
  echo "  cf rename-service ${retired_service} ${canonical_service}" >&2
  exit 1
fi

# --- 3. Move the writer ----------------------------------------------------
#
# The harvester has been resolving the canonical name all along; step 2 changed which
# cluster that is. The restart is what makes it re-resolve -- without it the app keeps
# using the endpoint it captured at its last boot.
#
# The harvester goes first because it is the only writer: once harvesting resumes, no
# write should land on the cluster being left behind.
echo "=== 3/5 restart ${harvest_app} onto ${canonical_service} ==="
cf restart "$harvest_app" --strategy rolling

# --- 4. Move the reader ----------------------------------------------------
#
# Retried rather than fatal. Catalog is restarted by a cron in its own repo every 15
# minutes, so two rolling deployments can collide and one supersedes the other --
# which makes `cf restart` return non-zero even though nothing is wrong. The rename
# has already landed at this point, so catalog converges on its own cron regardless;
# failing here would report a broken migration that is in fact complete.
if [[ "$catalog_present" == yes ]]; then
  echo "=== 4/5 restart ${catalog_app} onto ${canonical_service} ==="
  catalog_rolled=no
  for attempt in 1 2 3; do
    if cf restart "$catalog_app" --strategy rolling; then
      catalog_rolled=yes
      break
    fi
    echo "  attempt ${attempt} failed (a restart cron may have been mid-deployment);" >&2
    echo "  retrying in ${catalog_retry_seconds}s..." >&2
    sleep "$catalog_retry_seconds"
  done
  if [[ "$catalog_rolled" != yes ]]; then
    echo "WARNING: could not roll ${catalog_app} after 3 attempts. Its own restart" >&2
    echo "cron will pick up the rename within ~15 minutes. Confirm with:" >&2
    echo "  bin/report_opensearch_cluster.sh ${catalog_app}" >&2
  fi
else
  echo "=== 4/5 skipped: ${catalog_app} is not in this space ==="
fi

# --- 5. Housekeeping -------------------------------------------------------
#
# OPENSEARCH_NEXT_SERVICE_NAME now names the cluster that just became live, and
# `--cluster next` refuses to run when next and live resolve to the same host. Clear
# it so a later rebuild can name a genuinely new replacement.
#
# Deliberately last: it is off the critical path, and .profile has no guard on this
# variable (unlike OPENSEARCH_SERVICE_NAME), so a stale value cannot stop an app from
# starting. No restart needed -- nothing reads it until the next rebuild task.
echo "=== 5/5 clear ${harvest_app} OPENSEARCH_NEXT_SERVICE_NAME ==="
cf unset-env "$harvest_app" OPENSEARCH_NEXT_SERVICE_NAME

echo ""
echo "Promotion complete: ${canonical_service} is now the replacement cluster."
echo "The old cluster survives as ${retired_service} -- verify, then delete it with:"
echo "  bin/delete_opensearch_cluster.sh ${retired_service} ${harvest_app} ${catalog_app}"
echo "To roll back before deleting, reverse the renames and restart both apps."
