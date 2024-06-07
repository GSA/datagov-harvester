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
export DATABASE_URI=$(echo $URI | sed 's/postgres:\/\//postgresql:\/\//g')

# CF CREDS for CF TASKS API
export CF_API_URL=$(vcap_get_service secrets .credentials.CF_API_URL)
export CF_SERVICE_AUTH=$(vcap_get_service secrets .credentials.CF_SERVICE_AUTH)
export CF_SERVICE_USER=$(vcap_get_service secrets .credentials.CF_SERVICE_USER)

export LM_RUNNER_APP_GUID=$(vcap_get_service secrets .credentials.LM_RUNNER_APP_GUID)

export FLASK_APP_SECRET_KEY=$(vcap_get_service secrets .credentials.FLASK_APP_SECRET_KEY)
export OPENID_PRIVATE_KEY=$(vcap_get_service secrets .credentials.OPENID_PRIVATE_KEY)

flask db upgrade