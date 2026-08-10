#!/bin/bash

# Provision the replacement OpenSearch cluster and expose it to the harvester.
#
#   create the instance -> bind both consumers -> confirm each binding carries a
#   real cluster endpoint
#
# Deliberately NOT idempotent about the instance: an existing `<canonical>-next`
# means a migration is already in flight (or failed partway), and adopting it would
# point a second rebuild at the same cluster. It refuses instead. Resuming is
# `start_at=rebuild`, which skips this stage rather than re-running it.
#
# Completely inert with respect to live traffic. It touches no serving app at all:
# binding does not affect a running instance (Cloud Foundry refreshes VCAP_SERVICES
# only at container start), and there is no restart here. The rebuild reaches the
# new cluster because `.profile` derives its name as "<canonical>-next" and
# `cf run-task` starts a fresh container that reads current bindings.
#
# See docs/ops/migrate-opensearch-cluster.md.

set -euo pipefail

script_dir="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=bin/lib/cf_env.sh
source "${script_dir}/lib/cf_env.sh"
# shellcheck source=bin/lib/opensearch_plan.sh
source "${script_dir}/lib/opensearch_plan.sh"

usage="Usage: provision_opensearch_cluster.sh <service_instance> [plan] [app_name...]"
service_name=${1:-}
plan=${2:-}
shift 2 2>/dev/null || shift $#
apps=("$@")
if [[ ${#apps[@]} -eq 0 ]]; then
  apps=(datagov-harvest datagov-catalog)
fi
# The harvester is the only app that needs the NEXT credentials: it runs the
# rebuild. Catalog only ever reads the cluster the canonical name resolves to.
harvest_app=${apps[0]}

# How long to keep rebinding while the broker has not published an endpoint yet.
# Overridable so tests do not sleep. 10 x 30s covers the gap seen in staging with
# a wide margin; past that the instance is wrong, not slow.
bind_host_max_attempts=${BIND_HOST_MAX_ATTEMPTS:-10}
bind_host_retry_seconds=${BIND_HOST_RETRY_SECONDS:-30}

if [[ -z "$service_name" ]]; then
  echo "$usage" >&2
  exit 1
fi

space=$(opensearch_current_space)
if [[ -z "$plan" ]]; then
  plan=$(opensearch_plan_for_space "$space")
fi
if [[ -z "$plan" ]]; then
  echo "No default OpenSearch plan for space '${space}'; pass one explicitly." >&2
  exit 1
fi

echo "=== provision ${service_name} (space ${space}) ==="

# Refuse an instance that already exists rather than adopting it.
#
# The name is fixed, so a leftover `<canonical>-next` is not a resumable state --
# it is either a migration already in flight or the wreckage of one that failed.
# Adopting it would start a second rebuild writing into the same cluster as the
# first, and two concurrent backfills into one index interleave into a result that
# passes neither verification nor reason.
#
# So this fails before `cf create-service`, which is the cheapest possible place to
# stop: nothing has been provisioned, nothing is billing, and the live cluster is
# untouched. Resuming a genuine half-finished migration is what `start_at=rebuild`
# is for -- it skips this stage entirely rather than re-running it.
if cf service "$service_name" > /dev/null 2>&1; then
  echo "" >&2
  echo "${service_name} already exists." >&2
  echo "" >&2
  echo "Refusing to provision over it: a second rebuild would write into the same" >&2
  echo "cluster as the one already running and corrupt both. Either" >&2
  echo "" >&2
  echo "  - a migration is in flight    -> let it finish;" >&2
  echo "  - one failed partway          -> resume it with start_at=rebuild, which" >&2
  echo "                                   skips provisioning; or" >&2
  echo "  - it is genuinely abandoned   -> delete it first:" >&2
  echo "      bin/delete_opensearch_cluster.sh ${service_name} ${apps[*]}" >&2
  exit 1
fi

echo "  creating ${service_name} (plan ${plan}, ${OPENSEARCH_ENGINE_VERSION})."
echo "  AWS quotes 15-30 minutes per node, so an es-large can take hours."
cf create-service --wait aws-elasticsearch "$plan" "$service_name" \
  -c "{\"ElasticsearchVersion\":\"${OPENSEARCH_ENGINE_VERSION}\"}"

# Bind here rather than in manifest.yml: a manifest cannot reference an instance
# that does not exist yet, and manifest application is additive, so a binding made
# here survives later deploys.
#
# Bind BOTH consumers. Catalog reads the cluster too, and a migration that binds
# only the harvester would leave catalog unable to resolve the instance once it is
# renamed to the canonical name.
for app in "${apps[@]}"; do
  if cf curl "/v3/service_credential_bindings?app_names=${app}&service_instance_names=${service_name}" \
    | jq -e '.pagination.total_results > 0' > /dev/null 2>&1; then
    echo "  ${app} is already bound to ${service_name}."
  else
    echo "  binding ${app} to ${service_name}..."
    cf bind-service "$app" "$service_name"
  fi

  # A binding can succeed and still carry an empty `host`.
  #
  # `cf create-service --wait` returns, and the instance reports "create
  # succeeded / status is ready", before the aws-broker has populated the
  # cluster endpoint. A bind in that window is accepted and stores
  # `host: ""` (and `uri: "https://"`) with valid access_key/secret_key --
  # permanently, because binding credentials are captured once. Observed in
  # staging 2026-08-10: the bind ran ~6s after --wait returned, both apps got
  # an empty host, and the rebuild failed on `--cluster next requires
  # OPENSEARCH_NEXT_HOST`. A larger plan means more nodes and a wider window.
  #
  # A restart cannot fix this: .profile reads VCAP_SERVICES, which reflects the
  # stored binding. Only rebinding re-reads the broker. So poll, and rebind when
  # the endpoint appears.
  host=$(cf_binding_credential "$app" "$service_name" host)
  attempt=0
  while [[ -z "$host" && $attempt -lt $bind_host_max_attempts ]]; do
    attempt=$((attempt + 1))
    echo "  ${app}: binding has an empty host (broker endpoint not ready);" \
         "rebinding, attempt ${attempt}/${bind_host_max_attempts}..."
    sleep "$bind_host_retry_seconds"
    cf unbind-service "$app" "$service_name"
    cf bind-service "$app" "$service_name"
    host=$(cf_binding_credential "$app" "$service_name" host)
  done

  if [[ -z "$host" ]]; then
    # Fail here rather than let the rebuild fail later: at this point nothing has
    # moved, so the live cluster is untouched and this is a clean, resumable stop.
    echo "" >&2
    echo "${app} is bound to ${service_name} but the binding has no host after" >&2
    echo "${bind_host_max_attempts} rebind attempts. The broker has not published an" >&2
    echo "endpoint for this instance. Check it, then re-run with start_at=rebuild:" >&2
    echo "  cf service ${service_name}" >&2
    exit 1
  fi
  echo "  ${app} resolves ${service_name} at ${host}."
done

# No `cf set-env` and no restart.
#
# .profile derives the replacement's name as "${OPENSEARCH_SERVICE_NAME}-next" and
# resolves it if bound, so the binding above is the entire handoff. The rebuild runs
# via `cf run-task`, and a task starts a fresh container that reads current
# bindings -- verified in staging 2026-08-10, where a task saw this instance with no
# env var set and no restart performed.
#
# The removed set-env + rolling restart were therefore pure cost: ~70s of rolling
# restart per run, on the live app, in a window where nothing yet reads the
# replacement. They also made this stage the only part of the build phase that
# touched a serving app at all.
echo ""
echo "Provisioned ${service_name} and bound it to: ${apps[*]}"
echo "Nothing reads it yet -- both apps still serve from the live cluster."
