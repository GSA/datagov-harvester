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

export db_username=$(vcap_get_service aws-rds .credentials.username)
export db_password=$(vcap_get_service aws-rds .credentials.password)
export db_host=$(vcap_get_service aws-rds .credentials.host)
export db_port=$(vcap_get_service aws-rds .credentials.port)
export db_name=$(vcap_get_service aws-rds .credentials.db_name)

export DATABASE_URI=postgresql://$db_username:$db_password@$db_host:$db_port/$db_name