#!/bin/sh

set -e

# If an argument was provided, use it as the service name prefix.
# Otherwise default to "datagov-harvest".
app_name=${1:-datagov-harvest}

# shellcheck source=bin/lib/opensearch_plan.sh
. "$(dirname "$0")/bin/lib/opensearch_plan.sh"

# Get the current space and trim leading whitespace
space=$(opensearch_current_space)

# create email service
cf service "${app_name}-smtp"  > /dev/null 2>&1 || cf create-service --wait aws-ses domain "${app_name}-smtp" -c '{"admin_email": "datagovhelp@gsa.gov"}'

# create the secrets service if necessary
cf service "${app_name}-secrets"  > /dev/null 2>&1 || cf cups "${app_name}-secrets"

# Plan per space and engine version come from bin/lib/opensearch_plan.sh, shared
# with bin/provision_opensearch_cluster.sh so the two cannot drift.
opensearch_plan=$(opensearch_plan_for_space "$space")
opensearch_version=$OPENSEARCH_ENGINE_VERSION

# create the OpenSearch service if necessary
if [ -n "$opensearch_plan" ]; then
    cf service "datagov-catalog-opensearch" > /dev/null 2>&1 || cf create-service --wait aws-elasticsearch "$opensearch_plan" "datagov-catalog-opensearch" -c "{\"ElasticsearchVersion\":\"${opensearch_version}\"}"
fi

# Create the replacement OpenSearch cluster used to rebuild the index without
# loading the live one. Opt-in only: each cluster costs real resource credits, so
# a routine deploy must never create a second one. Set CREATE_OPENSEARCH_NEXT=1
# and see docs/ops/migrate-opensearch-cluster.md.
case "${CREATE_OPENSEARCH_NEXT:-}" in
  ""|0|false|no) create_next=no ;;
  *)             create_next=yes ;;
esac
if [ "$create_next" = yes ]; then
    # Named after catalog, like the live instance, because both apps bind it --
    # naming it after harvest would misrepresent who uses it. Overridable so a
    # future migration can pick its own name.
    #
    # The "-next" suffix is temporary: the decommission step renames this
    # instance to the canonical name once the old one is deleted. That rename
    # has a cost -- see the downtime warning in
    # docs/ops/migrate-opensearch-cluster.md.
    next_service="${OPENSEARCH_NEXT_SERVICE_NAME:-datagov-catalog-opensearch-next}"
    # Matches the live plan and engine version by default. Override the plan when
    # the point of the migration is to resize, which the cloud.gov broker cannot
    # do in place.
    next_plan=${OPENSEARCH_NEXT_PLAN:-$opensearch_plan}
    if [ -z "$next_plan" ]; then
        echo "No default OpenSearch plan for space '${space}'; set OPENSEARCH_NEXT_PLAN." >&2
        exit 1
    fi

    echo "Provisioning ${next_service} (plan ${next_plan}, ${opensearch_version})."
    echo "This can take 15-30 minutes per node."
    cf service "$next_service" > /dev/null 2>&1 || cf create-service --wait aws-elasticsearch "$next_plan" "$next_service" -c "{\"ElasticsearchVersion\":\"${opensearch_version}\"}"

    # Bind here rather than in manifest.yml: a manifest cannot reference an
    # instance that does not exist yet, and binding is additive so it survives
    # later deploys. Credentials only reach an app after a restart, so this is
    # inert until the operator restarts it.
    #
    # Bind BOTH consumers. Catalog reads the cluster too, and a cutover that
    # moves only the harvester would leave catalog searching the old one.
    for bind_app in "$app_name" "${OPENSEARCH_CATALOG_APP:-datagov-catalog}"; do
        if cf services | grep -q "^${next_service} .*[[:space:]]${bind_app}\([,[:space:]]\|$\)"; then
            echo "${bind_app} is already bound to ${next_service}."
            continue
        fi
        # Tolerate a missing app (a space may not run catalog) without aborting
        # the whole script under `set -e`, but say so rather than failing silently.
        cf bind-service "$bind_app" "$next_service" || \
            echo "WARNING: could not bind ${bind_app} to ${next_service}; bind it manually before cutover." >&2
    done
fi

# Production and staging should use bigger DB instances
if [ "$space" = "prod" ] || [ "$space" = "staging" ]; then
    cf service "${app_name}-db"    > /dev/null 2>&1 || cf create-service --wait aws-rds xlarge-gp-psql "${app_name}-db"
else
    cf service "${app_name}-db"    > /dev/null 2>&1 || cf create-service --wait aws-rds medium-gp-psql "${app_name}-db"
fi
