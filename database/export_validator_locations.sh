#!/bin/bash
# Rebuild the validator's place-name SQLite file from the harvest DB in the
# targeted cf space (use prod: the validator should match prod harvests).
# Run ./cf_connect.sh in another shell first (it holds the tunnel on 15432).

set -o errexit
set -o pipefail

APP=${APP:-datagov-harvest}

echo "Get DB connection info from $APP in $(cf target | grep space: | awk '{print $2}')"

CREDS=$(cf curl "/v3/apps/$(cf app "$APP" --guid)/env" | jq -e '.system_env_json.VCAP_SERVICES["aws-rds"][0].credentials')
DB_NAME=$(jq -r .db_name <<< "$CREDS")
DB_USERNAME=$(jq -r .username <<< "$CREDS")
export PGPASSWORD=$(jq -r .password <<< "$CREDS")

SCRIPT_DIR=$(dirname "$0")

cd "$SCRIPT_DIR/.."
poetry run python scripts/refresh_location_names.py "postgresql+psycopg://$DB_USERNAME@localhost:15432/$DB_NAME"
