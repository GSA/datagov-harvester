#!/bin/bash

# Provision the replacement OpenSearch cluster and expose it to the harvester.
#
#   create the instance (if absent) -> bind both consumers -> set
#   OPENSEARCH_NEXT_SERVICE_NAME on the harvester and roll it
#
# Idempotent throughout, so a workflow that failed later on can be re-dispatched
# from the start without paying for provisioning twice. Every step checks the
# state it is about to create.
#
# This is deliberately inert with respect to live traffic: binding does not affect
# a running app (Cloud Foundry only refreshes VCAP_SERVICES on restart), and the
# only restart here is the harvester picking up the NEXT credentials, which
# nothing reads until `rebuild-index --cluster next` runs. See
# docs/ops/migrate-opensearch-cluster.md.

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

if cf service "$service_name" > /dev/null 2>&1; then
  echo "  ${service_name} already exists; leaving it alone."
else
  echo "  creating ${service_name} (plan ${plan}, ${OPENSEARCH_ENGINE_VERSION})."
  echo "  AWS quotes 15-30 minutes per node, so an es-large can take hours."
  cf create-service --wait aws-elasticsearch "$plan" "$service_name" \
    -c "{\"ElasticsearchVersion\":\"${OPENSEARCH_ENGINE_VERSION}\"}"
fi

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
    continue
  fi
  echo "  binding ${app} to ${service_name}..."
  cf bind-service "$app" "$service_name"
done

# Expose the NEXT credentials so `rebuild-index --cluster next` can resolve them.
# .profile reads this variable and exports OPENSEARCH_NEXT_HOST/ACCESS_KEY/SECRET_KEY.
previous=$(cf_env_value "$harvest_app" OPENSEARCH_NEXT_SERVICE_NAME)
if [[ "$previous" == "$service_name" ]]; then
  echo "  ${harvest_app} already has OPENSEARCH_NEXT_SERVICE_NAME=${service_name}."
  echo "  restarting anyway so the running instances are certain to have it."
else
  cf set-env "$harvest_app" OPENSEARCH_NEXT_SERVICE_NAME "$service_name"
fi

# Blocking rolling restart (no --no-wait): if .profile cannot resolve the new
# instance the start fails, and this step must fail with it rather than report
# success on an app that cannot see the replacement cluster.
echo "  rolling restart of ${harvest_app}..."
cf restart "$harvest_app" --strategy rolling

echo ""
echo "Provisioned ${service_name} and exposed it to ${harvest_app}."
echo "Nothing reads it yet -- both apps still serve from the live cluster."
