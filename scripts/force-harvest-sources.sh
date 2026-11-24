
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://harvest.data.gov"
PER_PAGE=100
DRY_RUN="${DRY_RUN:-0}"
IDS_FILE="harvest_source_ids.txt"

# Require API token for actual job creation
if [[ "$DRY_RUN" != "1" ]]; then
    : "${HARVESTER_ADMIN_API_KEY_PROD:?HARVESTER_ADMIN_API_KEY_PROD must be set}"
fi

# Temporary file for collecting IDs
tmp_file=$(mktemp)
trap 'rm -f "$tmp_file"' EXIT

page=0
expected_total=0

echo "Fetching harvest sources..."

while true; do
    echo "Fetching page $page..."

    response=$(curl -fsS --retry 3 --get \
        --data-urlencode "page=$page" \
        --data-urlencode "per_page=$PER_PAGE" \
        --data-urlencode "order_by=id" \
        --data-urlencode "envelope=true" \
        "$BASE_URL/api/v1/harvest_sources/")

    # Verify response structure
    if ! jq -e '
        (.data | type == "array") and
        (.links | type == "object") and
        (.meta.total | type == "number")
    ' <<< "$response" > /dev/null; then
        echo "Invalid API response on page $page" >&2
        exit 1
    fi

    if [[ "$page" -eq 0 ]]; then
        expected_total=$(jq -r '.meta.total' <<< "$response")
    fi

    # Collect source IDs
    jq -r '.data[].id' <<< "$response" >> "$tmp_file"

    # Stop when there is no next page
    next=$(jq -r '.links.next // empty' <<< "$response")
    [[ -z "$next" ]] && break

    page=$((page + 1))
done

# Remove duplicates
sort -u "$tmp_file" > "$IDS_FILE"

total=$(wc -l < "$IDS_FILE" | tr -d ' ')

echo "Found $total unique harvest sources."

# Avoid submitting jobs if pagination was incomplete
if [[ "$total" -ne "$expected_total" ]]; then
    echo "Expected $expected_total sources, got $total. Aborting." >&2
    exit 1
fi

echo "IDs saved to $IDS_FILE"

success=0
failed=0

# Create a force-harvest job for every source
while IFS= read -r id; do
    [[ -z "$id" ]] && continue

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "[DRY RUN] Would create job for $id"
        continue
    fi

    payload=$(jq -n \
        --arg id "$id" \
        --arg timestamp "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
        '{
            harvest_source_id: $id,
            status: "new",
            job_type: "force-harvest",
            date_created: $timestamp
        }')

    if http_code=$(curl -sS \
        -o /dev/null \
        -w '%{http_code}' \
        -H "X-API-Key: ${HARVESTER_ADMIN_API_KEY_PROD}" \
        "$BASE_URL/api/v1/harvest_job/add" \
        --json "$payload") && [[ "$http_code" == 2?? ]]; then

        echo "Created job for $id"
        success=$((success + 1))
    else
        echo "Failed for $id (HTTP ${http_code:-unknown})" >&2
        failed=$((failed + 1))

    fi

    echo "sleeping for 10 seconds"
    sleep 10

done < "$IDS_FILE"

echo "Deleting temporary source ID file."
rm -f "$IDS_FILE"

echo "Completed: $success successful, $failed failed."

[[ "$failed" -eq 0 ]]
