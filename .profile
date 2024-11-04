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
export REAL_NAME=$(echo $VCAP_APPLICATION | jq -r '.application_name')
if [[ $APP_NAME = "datagov-harvest-admin" ]] || \
   [[ $APP_NAME = "datagov-harvest-runner" ]]
then
  APP_NAME=datagov-harvest
fi

# POSTGRES DB CREDS
export URI=$(vcap_get_service db .credentials.uri)
export DATABASE_URI=$(echo $URI | sed 's/postgres:\/\//postgresql:\/\//g')

# CF CREDS for CF TASKS API
export CF_SERVICE_AUTH=$(vcap_get_service secrets .credentials.CF_SERVICE_AUTH)
export CF_SERVICE_USER=$(vcap_get_service secrets .credentials.CF_SERVICE_USER)

export FLASK_APP_SECRET_KEY=$(vcap_get_service secrets .credentials.FLASK_APP_SECRET_KEY)
export OPENID_PRIVATE_KEY=$(vcap_get_service secrets .credentials.OPENID_PRIVATE_KEY)

export CKAN_API_TOKEN=$(vcap_get_service secrets .credentials.CKAN_API_TOKEN)

# New Relic
export NEW_RELIC_LICENSE_KEY=$(vcap_get_service secrets .credentials.NEW_RELIC_LICENSE_KEY)


if [[ $REAL_NAME = "datagov-harvest-admin" ]]; then
  flask db upgrade
fi
