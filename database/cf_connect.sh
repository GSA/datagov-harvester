#!/bin/bash

set -o errexit
set -o pipefail

APP=${APP:-datagov-harvest}

CREDS=$(cf curl "/v3/apps/$(cf app "$APP" --guid)/env" | jq -e '.system_env_json.VCAP_SERVICES["aws-rds"][0].credentials')
DB_HOSTNAME=$(jq -r .host <<< "$CREDS")
DB_PORT=$(jq -r .port <<< "$CREDS")

cf ssh -L "15432:$DB_HOSTNAME:$DB_PORT" "$APP"
