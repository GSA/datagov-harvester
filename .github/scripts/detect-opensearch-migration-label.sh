#!/usr/bin/env bash

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"

head_sha=${1:?Usage: detect-opensearch-migration-label.sh <head_sha> [label]}
label=${2:-force re-index recommended}
gh_api_max_attempts=${GH_API_MAX_ATTEMPTS:-3}
gh_api_retry_seconds=${GH_API_RETRY_SECONDS:-2}

emit() {
  echo "$1"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "$1" >>"$GITHUB_OUTPUT"
  fi
}

pull_requests_with_label() {
  local attempt matched

  for ((attempt = 1; attempt <= gh_api_max_attempts; attempt++)); do
    if matched=$(
      gh api "repos/${GITHUB_REPOSITORY}/commits/${head_sha}/pulls" \
        --jq ".[] | select(.labels[]?.name == \"${label}\") | .number"
    ); then
      printf '%s' "$matched"
      return 0
    fi

    if [[ "$attempt" -eq "$gh_api_max_attempts" ]]; then
      echo "Failed to inspect pull requests for commit ${head_sha} after ${attempt} attempts." >&2
      return 1
    fi
    echo "Unable to inspect pull requests for commit ${head_sha}; retrying (${attempt}/${gh_api_max_attempts})." >&2
    sleep "$gh_api_retry_seconds"
  done
}

if ! matches=$(pull_requests_with_label); then
  exit 1
fi

if [[ -n "$matches" ]]; then
  pr_numbers=$(printf '%s' "$matches" | sort -un | paste -sd, -)
  emit "migration_needed=true"
  emit "pr_numbers=${pr_numbers}"
  echo "OpenSearch migration required by PR(s): ${pr_numbers}."
else
  emit "migration_needed=false"
  emit "pr_numbers="
  echo "No OpenSearch migration label found on ${head_sha}."
fi
