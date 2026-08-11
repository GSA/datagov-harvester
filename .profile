#!/bin/bash

set -o errexit
set -o pipefail

function vcap_get_service_by_name () {
  local service_name path
  service_name="$1"
  path="$2"
  echo $VCAP_SERVICES | jq --raw-output --arg service_name "$service_name" ".[][] | select(.name == \$service_name) | ($path | if . == null then empty else . end)"
}

function vcap_get_service () {
  local path name
  name="$1"
  path="$2"
  vcap_get_service_by_name "${APP_NAME}-${name}" "$path"
}

export APP_NAME=$(echo $VCAP_APPLICATION | jq -r '.application_name')

# GA (google analytics)
export GA_CREDENTIALS==$(vcap_get_service secrets .credentials.GA_CREDENTIALS)

# POSTGRES DB CREDS
export URI=$(vcap_get_service db .credentials.uri)
export DATABASE_URI=$(echo $URI | sed 's/postgres:\/\//postgresql+psycopg:\/\//g')

# CF CREDS for CF TASKS API
export CF_SERVICE_AUTH=$(vcap_get_service secrets .credentials.CF_SERVICE_AUTH)
export CF_SERVICE_USER=$(vcap_get_service secrets .credentials.CF_SERVICE_USER)

export FLASK_APP_SECRET_KEY=$(vcap_get_service secrets .credentials.FLASK_APP_SECRET_KEY)
export HARVEST_API_TOKEN=$(vcap_get_service secrets .credentials.HARVEST_API_TOKEN)
export OPENID_PRIVATE_KEY=$(vcap_get_service secrets .credentials.OPENID_PRIVATE_KEY)
export HARVEST_RUNNER_MAX_TASKS=${HARVEST_RUNNER_MAX_TASKS:-3}

# New Relic
export NEW_RELIC_LICENSE_KEY=$(vcap_get_service secrets .credentials.NEW_RELIC_LICENSE_KEY)

# SMTP Settings
export HARVEST_SMTP_SERVER=$(vcap_get_service smtp .credentials.smtp_server)
export HARVEST_SMTP_STARTTLS=True
export HARVEST_SMTP_USER=$(vcap_get_service smtp .credentials.smtp_user)
export HARVEST_SMTP_PASSWORD=$(vcap_get_service smtp .credentials.smtp_password)
export HARVEST_SMTP_SENDER=harvester@$(vcap_get_service smtp .credentials.domain_arn | grep -o "ses-[[:alnum:]]\+.appmail.cloud.gov")
export HARVEST_SMTP_RECIPIENT=datagovteam@gsa.gov

# OpenSearch host and credentials.
#
# The cluster is shared with catalog, so the instance is not named after this
# app. The live cluster is always the instance with the canonical name: a
# migration moves the *name* onto the new cluster with `cf rename-service`, so
# this never has to be repointed. Overridable only to debug against a specific
# instance. See docs/ops/migrate-opensearch-cluster.md.
export OPENSEARCH_SERVICE_NAME=${OPENSEARCH_SERVICE_NAME:-datagov-catalog-opensearch}
export OPENSEARCH_HOST=$(vcap_get_service_by_name "$OPENSEARCH_SERVICE_NAME" .credentials.host)
export OPENSEARCH_ACCESS_KEY=$(vcap_get_service_by_name "$OPENSEARCH_SERVICE_NAME" .credentials.access_key)
export OPENSEARCH_SECRET_KEY=$(vcap_get_service_by_name "$OPENSEARCH_SERVICE_NAME" .credentials.secret_key)

# Fail the start rather than boot an app that silently indexes nothing: the
# harvest path treats an empty OPENSEARCH_HOST as "OpenSearch not configured"
# and turns every index/delete into a no-op without logging an error. A typo in
# OPENSEARCH_SERVICE_NAME at cutover would otherwise pass the rolling-restart
# health check and go unnoticed until the next compare.
if [ -z "$OPENSEARCH_HOST" ]; then
  echo "OPENSEARCH_HOST is empty: no bound service instance named '$OPENSEARCH_SERVICE_NAME'" >&2
  exit 1
fi

# The replacement cluster that `flask search rebuild-index --cluster next` fills.
#
# The name is fixed, exactly like the canonical name above and like every other
# service this app resolves (`-db`, `-secrets`, `-smtp`). Nothing sets this with
# `cf set-env`: a migration binds an instance called `<canonical>-next`, and this
# resolves it if it is bound and leaves the NEXT_* variables empty if it is not.
#
# That "if bound" is the whole mechanism, and it needs no restart to take effect:
# `cf run-task` starts a fresh container that reads the current bindings, so the
# rebuild task sees a newly bound replacement immediately. (Verified in staging
# 2026-08-10: a task saw the bound instance with no set-env and no restart.) A
# long-running web instance still needs a restart to notice, which is why the
# promote's rolling restarts remain mandatory -- but nothing on the rebuild path
# depends on one.
#
# Guarded with `|| true` because this script runs under `set -o errexit` and jq
# exits non-zero when no bound instance matches, which is the normal state at rest.
export OPENSEARCH_NEXT_SERVICE_NAME="${OPENSEARCH_SERVICE_NAME}-next"
export OPENSEARCH_NEXT_HOST=$(vcap_get_service_by_name "$OPENSEARCH_NEXT_SERVICE_NAME" .credentials.host || true)
export OPENSEARCH_NEXT_ACCESS_KEY=$(vcap_get_service_by_name "$OPENSEARCH_NEXT_SERVICE_NAME" .credentials.access_key || true)
export OPENSEARCH_NEXT_SECRET_KEY=$(vcap_get_service_by_name "$OPENSEARCH_NEXT_SERVICE_NAME" .credentials.secret_key || true)

echo "Setting CA Bundle.."
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# egress proxy
echo "Setting up egress proxy.."
if [ -z ${proxy_url+x} ]; then
  echo "Egress proxy is not connected."
else
  echo "Egress proxy is enabled, excluding internal domains.."
  # Both clusters must bypass the proxy: the live one serves queries and the
  # replacement one receives the backfill.
  export no_proxy=".apps.internal,${OPENSEARCH_HOST}${OPENSEARCH_NEXT_HOST:+,${OPENSEARCH_NEXT_HOST}}"
  export http_proxy=$proxy_url
  export https_proxy=$proxy_url
fi

# migrations are handled in app-start.sh
