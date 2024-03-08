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

export DATABASE_URI=$(vcap_get_service aws-rds .credentials.uri)