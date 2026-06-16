#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  run_pgbench.sh \
    <phase> \
    <case-key> \
    <query-label> \
    <test-case-label> \
    <sql-template> \
    [source-id]

Example:
  run_pgbench.sh \
    before \
    latest-large \
    "Latest successful records by source" \
    "Large source" \
    queries/latest_records_by_source.sql.template \
    00000000-0000-0000-0000-000000000000
EOF
}

if [ "$#" -lt 5 ]; then
    usage
    exit 1
fi

PHASE="$1"
CASE_KEY="$2"
QUERY_LABEL="$3"
TEST_CASE_LABEL="$4"
SQL_TEMPLATE="$5"
SOURCE_ID="${6:-}"

CLIENTS="${CLIENTS:-1}"
THREADS="${THREADS:-1}"
WARMUP_TRANSACTIONS="${WARMUP_TRANSACTIONS:-5}"
MEASURE_TRANSACTIONS="${MEASURE_TRANSACTIONS:-50}"
PROTOCOL="${PROTOCOL:-simple}"
RESULTS_ROOT="${RESULTS_ROOT:-results}"

# Use the host port published by Docker.
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"

: "${PGDATABASE:?PGDATABASE must be set}"
: "${PGUSER:?PGUSER must be set}"

export PGHOST PGPORT PGDATABASE PGUSER

for command in pgbench psql python3 sed grep; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "Required command not found: $command" >&2
        exit 1
    fi
done

if [ ! -f "$SQL_TEMPLATE" ]; then
    echo "SQL template does not exist: $SQL_TEMPLATE" >&2
    exit 1
fi

RUN_DIR="${RESULTS_ROOT}/${PHASE}/${CASE_KEY}"
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"

RUN_DIR_ABS="$(cd "$RUN_DIR" && pwd)"
RENDERED_SQL="${RUN_DIR_ABS}/query.sql"

if grep -q '__SOURCE_ID__' "$SQL_TEMPLATE"; then
    if [ -z "$SOURCE_ID" ]; then
        echo "This query requires a source ID." >&2
        exit 1
    fi

    # Harvest source IDs are UUID-shaped strings. Validate before inserting
    # the value into the SQL template.
    if ! [[ "$SOURCE_ID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then
        echo "Invalid source ID: $SOURCE_ID" >&2
        exit 1
    fi

    sed "s/__SOURCE_ID__/${SOURCE_ID}/g" \
        "$SQL_TEMPLATE" > "$RENDERED_SQL"
else
    cp "$SQL_TEMPLATE" "$RENDERED_SQL"
fi

echo "Checking database connection..."
psql \
    -X \
    --set ON_ERROR_STOP=1 \
    --command 'SELECT 1;' \
    >/dev/null

echo "Warm-up: ${WARMUP_TRANSACTIONS} transaction(s) per client"

pgbench \
    --no-vacuum \
    --file "$RENDERED_SQL" \
    --client "$CLIENTS" \
    --jobs "$THREADS" \
    --transactions "$WARMUP_TRANSACTIONS" \
    --protocol "$PROTOCOL" \
    "$PGDATABASE" \
    >/dev/null

echo "Measured run: ${MEASURE_TRANSACTIONS} transaction(s) per client"

(
    cd "$RUN_DIR_ABS"

    pgbench \
        --no-vacuum \
        --file "$RENDERED_SQL" \
        --client "$CLIENTS" \
        --jobs "$THREADS" \
        --transactions "$MEASURE_TRANSACTIONS" \
        --protocol "$PROTOCOL" \
        --log \
        --log-prefix pgbench \
        "$PGDATABASE" \
        2>&1 | tee summary.txt
)

python3 "$(dirname "$0")/summarize_pgbench.py" \
    "$RUN_DIR_ABS" \
    "$PHASE" \
    "$CASE_KEY" \
    "$QUERY_LABEL" \
    "$TEST_CASE_LABEL" \
    "$CLIENTS" \
    "$THREADS"