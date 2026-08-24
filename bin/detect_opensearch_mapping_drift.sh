#!/bin/bash

set -euo pipefail

# Decide whether a space's OpenSearch index was built with this revision's schema.
#
# Usage: detect_opensearch_mapping_drift.sh <expected_fingerprint> [harvest_app]
#
# The fingerprint of the schema an index was last built with is recorded on the
# harvester app as OPENSEARCH_MAPPING_FINGERPRINT. A recorded value that differs
# from this revision's means the live index predates the schema the new code
# expects, so it has to be rebuilt before that code serves traffic.
#
# Writes the reason to .opensearch_mapping_drift in GITHUB_WORKSPACE (or the
# current directory) when a rebuild is required, so CI can branch on the result
# the same way it branches on deferred cluster cleanup. Exits non-zero only when
# the recorded fingerprint cannot be read at all.
#
# Nothing is recorded the first time this runs in a space. The index is adopted
# as-is rather than rebuilt: whichever release last touched it built it, and
# rebuilding every space once to learn a value we can simply write down would
# cost hours of cluster and database load for no schema change.

usage="Usage: detect_opensearch_mapping_drift.sh <expected_fingerprint> [harvest_app]"
expected_fingerprint=${1:-}
harvest_app=${2:-datagov-harvest}

env_var=OPENSEARCH_MAPPING_FINGERPRINT
result_file="${GITHUB_WORKSPACE:-.}/.opensearch_mapping_drift"
rm -f "$result_file"

if [[ -z "$expected_fingerprint" ]]; then
  echo "$usage" >&2
  exit 1
fi

if ! cf app "$harvest_app" >/dev/null 2>&1; then
  echo "No ${harvest_app} app in the targeted space." >&2
  exit 1
fi

harvest_guid=$(cf app "$harvest_app" --guid)

if ! environment=$(cf curl --fail "/v3/apps/${harvest_guid}/environment_variables"); then
  echo "Unable to read environment variables for ${harvest_app}." >&2
  exit 1
fi

if ! recorded_fingerprint=$(
  jq -r --arg name "$env_var" '.var[$name] // ""' <<<"$environment"
); then
  echo "Unable to parse environment variables for ${harvest_app}." >&2
  exit 1
fi

if [[ -z "$recorded_fingerprint" ]]; then
  echo "No ${env_var} recorded for ${harvest_app}; adopting the live index."
  echo "Recording ${expected_fingerprint} without a rebuild."
  cf set-env "$harvest_app" "$env_var" "$expected_fingerprint" >/dev/null
  exit 0
fi

if [[ "$recorded_fingerprint" == "$expected_fingerprint" ]]; then
  echo "Index schema is current (${expected_fingerprint}); no rebuild required."
  exit 0
fi

{
  echo "The live index was built with schema ${recorded_fingerprint}."
  echo "This revision declares schema ${expected_fingerprint}."
} | tee "$result_file"
echo "A rebuild is required before ${harvest_app} serves this revision."
