#!/bin/bash

set -o errexit
set -o pipefail

function vcap_get_service () {
  local path name
  name="$1"
  path="$2"
  service_name=${APP_NAME}-${name}
  echo $VCAP_SERVICES | jq --raw-output --arg service_name "$service_name" ".[][] | select(.name == \$service_name) | $path"
}

export APP_NAME=$(echo $VCAP_APPLICATION | jq -r '.application_name')

# POSTGRES DB CREDS
export URI=$(vcap_get_service db .credentials.uri)
export DATABASE_URI=$(echo $URI | sed 's/postgres:\/\//postgresql+psycopg:\/\//g')

# CF CREDS for CF TASKS API
export CF_SERVICE_AUTH=$(vcap_get_service secrets .credentials.CF_SERVICE_AUTH)
export CF_SERVICE_USER=$(vcap_get_service secrets .credentials.CF_SERVICE_USER)

export FLASK_APP_SECRET_KEY=$(vcap_get_service secrets .credentials.FLASK_APP_SECRET_KEY)
export OPENID_PRIVATE_KEY=$(vcap_get_service secrets .credentials.OPENID_PRIVATE_KEY)

export CKAN_API_TOKEN=$(vcap_get_service secrets .credentials.CKAN_API_TOKEN)

# New Relic
export NEW_RELIC_LICENSE_KEY=$(vcap_get_service secrets .credentials.NEW_RELIC_LICENSE_KEY)

# SMTP Settings
export HARVEST_SMTP_SERVER=$(vcap_get_service smtp .credentials.smtp_server)
export HARVEST_SMTP_STARTTLS=True
export HARVEST_SMTP_USER=$(vcap_get_service smtp .credentials.smtp_user)
export HARVEST_SMTP_PASSWORD=$(vcap_get_service smtp .credentials.smtp_password)
export HARVEST_SMTP_SENDER=harvester@$(vcap_get_service smtp .credentials.domain_arn | grep -o "ses-[[:alnum:]]\+.ssb.data.gov")
export HARVEST_SMTP_RECIPIENT=datagovhelp@gsa.gov

# migrations are handled in app-start.sh
