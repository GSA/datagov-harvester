#!/bin/bash

# Usage: ./load_csv.sh <csv_file> <table_name>
CSV_FILE=$1
TABLE_NAME=$2

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <csv_file> <table_name>"
    exit 1
fi

# Get the directory of the current script
SCRIPT_DIR=$(dirname "$0")

# Check if DATABASE_URI is set
if [ -z "$DATABASE_URI" ]; then
    echo "DATABASE_URI is not set in the environment variables."
    exit 1
fi

# Load the CSV file into the PostgreSQL table
psql $DATABASE_URI -c "\COPY $TABLE_NAME FROM '$SCRIPT_DIR/$CSV_FILE' WITH CSV HEADER"

if [ $? -eq 0 ]; then
    echo "CSV file loaded successfully into $TABLE_NAME."
else
    echo "Failed to load CSV file into $TABLE_NAME."
fi
