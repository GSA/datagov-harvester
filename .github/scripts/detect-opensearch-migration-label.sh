#!/usr/bin/env bash

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"

head_sha=${1:?Usage: detect-opensearch-migration-label.sh <head_sha> <workflow_file> <branch> [label]}
workflow_file=${2:?Usage: detect-opensearch-migration-label.sh <head_sha> <workflow_file> <branch> [label]}
branch=${3:?Usage: detect-opensearch-migration-label.sh <head_sha> <workflow_file> <branch> [label]}
label=${4:-force re-index recommended}

emit() {
  echo "$1"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "$1" >>"$GITHUB_OUTPUT"
  fi
}

base_sha=$(
  gh api -X GET \
    "repos/${GITHUB_REPOSITORY}/actions/workflows/${workflow_file}/runs" \
    -f "branch=${branch}" -f status=success -f per_page=1 \
    --jq '.workflow_runs[0].head_sha // empty'
)

if [[ -z "$base_sha" ]]; then
  echo "No successful ${workflow_file} run on ${branch} is available as a watermark." >&2
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
