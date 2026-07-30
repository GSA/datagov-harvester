#!/bin/bash

# Promote the replacement cluster to canonical: move both apps onto it and give it
# the canonical service-instance name.
#
# The ordering here is the whole point of the script. The obvious sequence (delete
# the old instance, rename the new one, then repoint the apps) leaves both apps
# naming an instance that does not exist, and every new container start fails
# .profile's empty-host guard for as long as that lasts. This order instead keeps a
# real, bound instance behind every name an app resolves:
#
#   1. harvester -> <next> explicitly, and roll it.       (canonical still = old)
#   2. rename <canonical> -> <canonical>-old.             (frees the canonical name)
#   3. rename <next> -> <canonical>.                      (catalog now resolves NEW)
#   4. unset the harvester's overrides, and roll it.      (falls back to the
#                                                          .profile default, which
#                                                          is <canonical> = NEW)
#   5. roll catalog so it picks up the renamed instance.
#
# Steps 2->3 and 3->4 are adjacent command pairs, so the residual exposure is
# seconds and affects only *new* container starts; running instances are unaffected
# because VCAP_SERVICES is read at container start and a rename is metadata-only
# (the AWS endpoint and credentials do not change). Every restart is a blocking
# rolling restart, so existing instances keep serving if a start fails.
#
# Why catalog needs no cf set-env: datagov-catalog's .profile resolves
# ${APP_NAME}-opensearch -- literally datagov-catalog-opensearch -- and does NOT
# read OPENSEARCH_SERVICE_NAME. Setting that variable on catalog is a silent no-op.
# The rename in step 3 is the only thing that moves catalog.
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

# Both apps must be bound to the replacement instance. Bindings survive renames,
# so a binding missing here stays missing after step 3, and the app then cannot
# resolve the canonical name at all -- .profile exits 1 and the app will not start.
unbound=()
for app in "$harvest_app" "$catalog_app"; do
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

# --- 1. Move the writer first ----------------------------------------------
#
# The harvester is the only writer. Moving it first means that once harvesting
# resumes no write lands on the cluster being left behind.
echo "=== 1/5 ${harvest_app} -> ${next_service} ==="
previous=$(cf_env_value "$harvest_app" OPENSEARCH_SERVICE_NAME)
if [[ -n "$previous" ]]; then
  echo "  previous OPENSEARCH_SERVICE_NAME: $previous"
else
  echo "  previous OPENSEARCH_SERVICE_NAME: (unset -- .profile default)"
fi
cf set-env "$harvest_app" OPENSEARCH_SERVICE_NAME "$next_service"
echo "  rolling restart..."
cf restart "$harvest_app" --strategy rolling

# --- 2 & 3. Swap the names -------------------------------------------------
#
# Adjacent on purpose: between them the canonical name does not exist, so keep the
# gap to one command. Running instances are unaffected either way.
echo "=== 2/5 rename ${canonical_service} -> ${retired_service} ==="
cf rename-service "$canonical_service" "$retired_service"

echo "=== 3/5 rename ${next_service} -> ${canonical_service} ==="
cf rename-service "$next_service" "$canonical_service"

# --- 4. Return the harvester to a bare steady state ------------------------
#
# unset rather than set: OPENSEARCH_SERVICE_NAME defaults to the canonical name in
# .profile, and the canonical name is now the new cluster. Leaving the override at
# <next> would point at a name that no longer exists.
#
# OPENSEARCH_NEXT_SERVICE_NAME must go too. It also names the now-promoted cluster,
# and `--cluster next` refuses to run when next and live resolve to the same host.
echo "=== 4/5 clear ${harvest_app} overrides (falls back to ${canonical_service}) ==="
cf unset-env "$harvest_app" OPENSEARCH_SERVICE_NAME
cf unset-env "$harvest_app" OPENSEARCH_NEXT_SERVICE_NAME
echo "  rolling restart..."
cf restart "$harvest_app" --strategy rolling

# --- 5. Move the reader ----------------------------------------------------
#
# Catalog has been resolving the canonical name all along; the rename in step 3 is
# what changed which cluster that is. It needs a restart to see it.
echo "=== 5/5 restart ${catalog_app} onto ${canonical_service} ==="
cf restart "$catalog_app" --strategy rolling

echo ""
echo "Promotion complete: ${canonical_service} is now the replacement cluster."
echo "The old cluster survives as ${retired_service} -- verify, then delete it with:"
echo "  bin/delete_opensearch_cluster.sh ${retired_service} ${harvest_app} ${catalog_app}"
echo "To roll back before deleting, reverse the renames and restart both apps."
