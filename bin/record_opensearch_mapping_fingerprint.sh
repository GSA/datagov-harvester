#!/bin/bash

set -euo pipefail

# Record the schema fingerprint an index was just rebuilt with.
#
# Usage: record_opensearch_mapping_fingerprint.sh <fingerprint> [harvest_app]
#
# Run this only after a rebuild has been validated and promoted. Until it runs,
# the recorded fingerprint stays stale, which is what makes a failed migration
# retry on the next release instead of being silently skipped.

usage="Usage: record_opensearch_mapping_fingerprint.sh <fingerprint> [harvest_app]"
fingerprint=${1:-}
harvest_app=${2:-datagov-harvest}

env_var=OPENSEARCH_MAPPING_FINGERPRINT

if [[ -z "$fingerprint" ]]; then
  echo "$usage" >&2
  exit 1
fi

if ! cf app "$harvest_app" >/dev/null 2>&1; then
  echo "No ${harvest_app} app in the targeted space." >&2
  exit 1
fi

echo "Recording ${env_var}=${fingerprint} on ${harvest_app}."
cf set-env "$harvest_app" "$env_var" "$fingerprint" >/dev/null
