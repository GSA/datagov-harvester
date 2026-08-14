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

workflow_candidates() {
  local candidate_workflow exclude_head jq_filter
  candidate_workflow="$1"
  exclude_head=${2:-false}
  jq_filter='.workflow_runs[].head_sha // empty'

  if [[ "$exclude_head" == "true" ]]; then
    jq_filter=".workflow_runs[] | select(.head_sha != \"${head_sha}\") | .head_sha"
  fi

  gh api -X GET \
    "repos/${GITHUB_REPOSITORY}/actions/workflows/${candidate_workflow}/runs" \
    -f "branch=${branch}" -f status=success -f per_page=10 \
    --jq "$jq_filter"
}

base_sha=""
compare=""
rejected_status=""

# Candidates arrive newest first, but a force-push orphans the runs it rewrote.
# Keep walking until one is still an ancestor of HEAD.
select_watermark() {
  local candidates candidate candidate_compare status
  candidates="$1"

  while read -r candidate; do
    [[ -z "$candidate" ]] && continue
    candidate_compare=$(gh api "repos/${GITHUB_REPOSITORY}/compare/${candidate}...${head_sha}")
    status=$(jq -r '.status' <<<"$candidate_compare")
    if [[ "$status" == "ahead" || "$status" == "identical" ]]; then
      base_sha="$candidate"
      compare="$candidate_compare"
      return 0
    fi
    rejected_status="$status"
    echo "Skipping watermark ${candidate}: compare status is '${status}'." >&2
  done <<<"$candidates"

  return 1
}

primary_candidates=$(workflow_candidates "$workflow_file")

if [[ -n "$primary_candidates" ]]; then
  if ! select_watermark "$primary_candidates"; then
    echo "No ${workflow_file} run on ${branch} is an ancestor of ${head_sha}: compare status is '${rejected_status}'." >&2
    exit 1
  fi
elif [[ -n "$fallback_workflow_file" ]]; then
  echo "No successful ${workflow_file} run; using ${fallback_workflow_file} to bootstrap the watermark."
  fallback_candidates=$(workflow_candidates "$fallback_workflow_file" true)
  if ! select_watermark "$fallback_candidates"; then
    # Nothing released on this lineage yet, so there is no range to diff. Treat
    # it as a clean bootstrap instead of wedging every future push.
    echo "No ${fallback_workflow_file} run on ${branch} is an ancestor of ${head_sha}; bootstrapping without a diff."
    emit "base_sha=${head_sha}"
    emit "migration_needed=false"
    emit "pr_numbers="
    exit 0
  fi
else
  echo "No successful release run on ${branch} is available as a watermark." >&2
  exit 1
fi

total_commits=$(jq -r '.total_commits' <<<"$compare")
listed_commits=$(jq -r '.commits | length' <<<"$compare")

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
