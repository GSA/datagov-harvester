#!/bin/bash

set -o errexit
set -o pipefail

echo "Get DB connection info"

DB_NAME=$(cf env datagov-harvest-admin | grep -m 1 '"name":' | sed -E 's/.*"name": "(.*)".*/\1/')

DB_USERNAME=$(cf env datagov-harvest-admin | grep '"username":' | sed -E 's/.*"username": "(.*)".*/\1/')

PGPASSWORD=$(cf env datagov-harvest-admin | grep '"password":' | sed -E 's/.*"password": "(.*)".*/\1/')

# Usage: ./load_csv.sh <csv_file> <table_name>
CSV_FILE=${1:-"locations.csv"}
TABLE_NAME=locations

# Get the directory of the current script
SCRIPT_DIR=$(dirname "$0")

echo "Loading CSV file into $TABLE_NAME..."

psql "postgresql://$DB_USERNAME:$PGPASSWORD@localhost:15432/$DB_NAME" -c "\COPY $TABLE_NAME FROM '$SCRIPT_DIR/$CSV_FILE' WITH CSV HEADER QUOTE '\"';"

if [ $? -eq 0 ]; then
    echo "CSV file loaded successfully into $TABLE_NAME."
else
    echo "Failed to load CSV file into $TABLE_NAME."
fi
