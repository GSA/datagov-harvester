#!/bin/bash

set -o errexit
set -o pipefail

function vcap_get_service () {
  local path name service_name
  name="$1"
  path="$2"
  service_name=${APP_NAME}-${name}
  if [ "$name" = "opensearch" ]; then
    service_name=datagov-catalog-opensearch
  fi
  echo "$VCAP_SERVICES" | jq --raw-output --arg service_name "$service_name" ".[][] | select(.name == \$service_name) | $path"
}

function require_vcap_value () {
  local service="$1"
  local credential_path="$2"
  local label="$3"
  local _val

  _val=$(vcap_get_service "$service" "$credential_path")
  if [ "$_val" = "null" ] || [ -z "$_val" ]; then
    echo "ERROR: ${label} not found in VCAP_SERVICES. Aborting startup." >&2
    exit 1
  fi
  printf '%s' "$_val"
}

function require_vcap_secret () {
  local service="$1"
  local credential_path="$2"
  local env_var="$3"
  local label="${4:-$env_var}"
  local _val

  _val=$(require_vcap_value "$service" "$credential_path" "$label")
  printf -v "$env_var" '%s' "$_val"
  export "$env_var"
}

export APP_NAME=$(echo "$VCAP_APPLICATION" | jq -r '.application_name')

# GA (google analytics)
require_vcap_secret secrets .credentials.GA_CREDENTIALS GA_CREDENTIALS

# POSTGRES DB CREDS
require_vcap_secret db .credentials.uri URI DATABASE_URI
export DATABASE_URI=$(echo "$URI" | sed 's/postgres:\/\//postgresql+psycopg:\/\//g')

# CF CREDS for CF TASKS API
require_vcap_secret secrets .credentials.CF_SERVICE_AUTH CF_SERVICE_AUTH
require_vcap_secret secrets .credentials.CF_SERVICE_USER CF_SERVICE_USER
require_vcap_secret secrets .credentials.FLASK_APP_SECRET_KEY FLASK_APP_SECRET_KEY
require_vcap_secret secrets .credentials.OPENID_PRIVATE_KEY OPENID_PRIVATE_KEY

# New Relic
require_vcap_secret secrets .credentials.NEW_RELIC_LICENSE_KEY NEW_RELIC_LICENSE_KEY

# SMTP Settings
export HARVEST_SMTP_SERVER=$(vcap_get_service smtp .credentials.smtp_server)
export HARVEST_SMTP_STARTTLS=True
require_vcap_secret smtp .credentials.smtp_user HARVEST_SMTP_USER
require_vcap_secret smtp .credentials.smtp_password HARVEST_SMTP_PASSWORD
export HARVEST_SMTP_SENDER=harvester@$(vcap_get_service smtp .credentials.domain_arn | grep -o "ses-[[:alnum:]]\+.appmail.cloud.gov")
export HARVEST_SMTP_RECIPIENT=datagovhelp@gsa.gov

# OpenSearch host and credentials
export OPENSEARCH_HOST=$(vcap_get_service opensearch .credentials.host)
require_vcap_secret opensearch .credentials.access_key OPENSEARCH_ACCESS_KEY
require_vcap_secret opensearch .credentials.secret_key OPENSEARCH_SECRET_KEY

echo "Setting CA Bundle.."
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# egress proxy
echo "Setting up egress proxy.."
if [ -z ${proxy_url+x} ]; then
  echo "Egress proxy is not connected."
else
  echo "Egress proxy is enabled, excluding internal domains.."
  export no_proxy=".apps.internal,${OPENSEARCH_HOST}"
  export http_proxy=$proxy_url
  export https_proxy=$proxy_url
fi

# migrations are handled in app-start.sh
