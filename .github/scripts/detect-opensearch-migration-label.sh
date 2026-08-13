#!/usr/bin/env bash

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"

head_sha=${1:?Usage: detect-opensearch-migration-label.sh <head_sha> <workflow_file> <branch> [label]}
workflow_file=${2:?Usage: detect-opensearch-migration-label.sh <head_sha> <workflow_file> <branch> [label]}
branch=${3:?Usage: detect-opensearch-migration-label.sh <head_sha> <workflow_file> <branch> [label]}
label=${4:-force re-index recommended}
fallback_workflow_file=${FALLBACK_WORKFLOW_FILE:-}

emit() {
  echo "$1"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "$1" >>"$GITHUB_OUTPUT"
  fi
}

workflow_watermark() {
  local candidate_workflow exclude_head jq_filter
  candidate_workflow="$1"
  exclude_head=${2:-false}
  jq_filter='.workflow_runs[0].head_sha // empty'

  if [[ "$exclude_head" == "true" ]]; then
    jq_filter=".workflow_runs[] | select(.head_sha != \"${head_sha}\") | .head_sha"
  fi

  gh api -X GET \
    "repos/${GITHUB_REPOSITORY}/actions/workflows/${candidate_workflow}/runs" \
    -f "branch=${branch}" -f status=success -f per_page=10 \
    --jq "$jq_filter" |
    sed -n '1p'
}

base_sha=$(workflow_watermark "$workflow_file")

if [[ -z "$base_sha" && -n "$fallback_workflow_file" ]]; then
  echo "No successful ${workflow_file} run; using ${fallback_workflow_file} to bootstrap the watermark."
  base_sha=$(workflow_watermark "$fallback_workflow_file" true)
fi

if [[ -z "$base_sha" ]]; then
  echo "No successful release run on ${branch} is available as a watermark." >&2
  exit 1
fi

compare=$(gh api "repos/${GITHUB_REPOSITORY}/compare/${base_sha}...${head_sha}")
status=$(jq -r '.status' <<<"$compare")
total_commits=$(jq -r '.total_commits' <<<"$compare")
listed_commits=$(jq -r '.commits | length' <<<"$compare")

if [[ "$status" != "ahead" && "$status" != "identical" ]]; then
  echo "Cannot inspect ${base_sha}...${head_sha}: compare status is '${status}'." >&2
  exit 1
fi

if [[ "$total_commits" -gt "$listed_commits" ]]; then
  echo "The compare response is truncated (${listed_commits}/${total_commits} commits)." >&2
  exit 1
fi

matches=""
while read -r sha; do
  [[ -z "$sha" ]] && continue
  matched=$(
    gh api "repos/${GITHUB_REPOSITORY}/commits/${sha}/pulls" \
      --jq ".[] | select(.labels[]?.name == \"${label}\") | .number" ||
      true
  )
  if [[ -n "$matched" ]]; then
    matches+="${matched}"$'\n'
  fi
done < <(jq -r '.commits[].sha' <<<"$compare")

emit "base_sha=${base_sha}"
if [[ -n "$matches" ]]; then
  pr_numbers=$(
    printf '%s' "$matches" |
      sed '/^$/d' |
      sort -un |
      paste -sd, -
  )
  emit "migration_needed=true"
  emit "pr_numbers=${pr_numbers}"
  echo "OpenSearch migration required by PR(s): ${pr_numbers}."
else
  emit "migration_needed=false"
  emit "pr_numbers="
  echo "No OpenSearch migration label found."
fi
