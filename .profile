#!/bin/bash

##############################################################################
# NOTE: When adding commands to this file, be mindful of sensitive output.
# Since these logs are publicly available in github actions, we don't want
# to leak anything.
##############################################################################

set -o errexit
set -o pipefail

echo "airflow config setup..."

function vcap_get_service () {
  local path name
  name="$1"
  path="$2"
  #TODO FIX THIS
  service_name=test-airflow-${name}
  echo $VCAP_SERVICES | jq --raw-output --arg service_name "$service_name" ".[][] | select(.name == \$service_name) | $path"
}

export APP_NAME=$(echo $VCAP_APPLICATION | jq -r '.application_name')

# # Create a staging area for secrets and files
# CONFIG_DIR=$(mktemp -d)
# SHARED_DIR=$(mktemp -d)

# Extract credentials from VCAP_SERVICES
export REDIS_HOST=$(vcap_get_service redis .credentials.host)
export REDIS_PASSWORD=$(vcap_get_service redis .credentials.password)
export REDIS_PORT=$(vcap_get_service redis .credentials.port)
export AIRFLOW__CELERY__BROKER_URL=$(vcap_get_service redis .credentials.uri)
export BROKER_URL=$(vcap_get_service redis .credentials.uri)
export AIRFLOW__CELERY__RESULT_BACKEND="db+$(vcap_get_service db .credentials.uri)"
export SAML2_PRIVATE_KEY=$(vcap_get_service secrets .credentials.SAML2_PRIVATE_KEY)

# remote s3 for logs
export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="s3_connection_logging"  # name of conn id in web ui?
# export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=$(vcap_get_service s3 .credentials.uri)
export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://$(vcap_get_service s3 .credentials.endpoing)/$(vcap_get_service s3 .credentials.bucket)"

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(vcap_get_service db .credentials.uri)
# this appears to already be available via the manigfest
#export AIRFLOW__CORE__DAGS_FOLDER=$()

# export NEW_RELIC_LICENSE_KEY=$(vcap_get_service secrets .credentials.NEW_RELIC_LICENSE_KEY)

echo "Setup airflow webserver admin.."
# TODO obviously fix this by adding real cred handling
# airflow users create --role Admin --username <usernam> --password <password> --email <unique@email.com> --firstname <first> --lastname <last> 
