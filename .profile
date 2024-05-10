#!/bin/bash

set -o errexit
set -o pipefail

function vcap_get_service () {
  local path name
  name="$1"
  path="$2"
  service_name=${APP_NAME}-${name}
  echo $VCAP_SERVICES | jq --raw-output --arg service_name "$service_name" ".[][] | $path"
}

export APP_NAME=$(echo $VCAP_APPLICATION | jq -r '.application_name')

# POSTGRES DB CREDS
export URI=$(vcap_get_service aws-rds .credentials.uri)
export DATABASE_URI=$(echo $URI | sed 's/postgres:\/\//postgresql:\/\//g')

# CF CREDS for CF TASKS API
export CF_API_URL='https://api.fr.cloud.gov'
export CF_SERVICE_USER=$(vcap_get_service cloud-gov-service-account .credentials.username)
export CF_SERVICE_AUTH=$(vcap_get_service cloud-gov-service-account .credentials.password)

export LM_RUNNER_APP_GUID=$(vcap_get_service user-provided .credentials.LM_RUNNER_APP_GUID)

flask db upgrade