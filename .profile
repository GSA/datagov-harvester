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

export AIRFLOW__CELERY__BROKER_URL="$(vcap_get_service redis .credentials.uri)/0"
export BROKER_URL=$AIRFLOW__CELERY__BROKER_URL

AIRFLOW__CELERY__RESULT_BACKEND="db+$(vcap_get_service db .credentials.uri)"
export AIRFLOW__CELERY__RESULT_BACKEND=${AIRFLOW__CELERY__RESULT_BACKEND/'postgres'/'postgresql+psycopg2'}
# export AIRFLOW__CELERY__RESULT_BACKEND=$AIRFLOW__CELERY__BROKER_URL

export FLOWER_PORT="$PORT"
# export SAML2_PRIVATE_KEY=$(vcap_get_service secrets .credentials.SAML2_PRIVATE_KEY)

# remote s3 for logs
export AIRFLOW__LOGGING__REMOTE_LOGGING="true"
export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="s3conn"  # name of conn id in web ui?
# export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=$(vcap_get_service s3 .credentials.uri)
export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://$(vcap_get_service s3 .credentials.endpoing)/$(vcap_get_service s3 .credentials.bucket)/logs"
export AIRFLOW__LOGGING__ENCRYPT_S3_LOGS="false"

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(vcap_get_service db .credentials.uri)"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN/'postgres'/'postgresql+psycopg2'}

# TODO connections can be provided here:
# https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#storing-connections-in-environment-variables
