#!/usr/bin/env bash

# Decide whether this push to `main` owes an OpenSearch index rebuild.
#
# A PR merged with the `force re-index recommended` label means the OpenSearch
# mapping or document shape changed, so the index has to be rebuilt or search
# results go stale. Nothing else detects this: MAPPINGS carries no version or hash,
# and OpenSearchClient._ensure_index() only creates an index when one is *absent*,
# so a mapping change deployed against an existing index is a silent no-op.
#
#   Prints to stdout (and to $GITHUB_OUTPUT when set):
#     reindex_needed=true|false
#     base_sha=<sha>          the commit this range was measured from
#     pr_numbers=<n,n,...>    labelled PRs found (empty when none)
#
# WHY A WATERMARK AND NOT $GITHUB_EVENT before...after
#
# deploy.yml is serialized by a concurrency group with the default `queue: single`,
# which allows exactly one pending run: when a third merge arrives, the *pending*
# run is cancelled and replaced. That collapse is correct for deploys (you want the
# newest code) but it silently drops a reindex obligation:
#
#   merge B (labelled) -> run B goes pending
#   merge C            -> run C cancels run B, deploys HEAD (which contains B!)
#
# Using the push event's own range does not help, because run C's `before` is B's
# sha, so B falls outside C's range. Instead measure from the head_sha of the last
# *successful* run of this workflow. A cancelled or failed run never advances that
# watermark, so the obligation stays detectable until a run actually completes.
#
# FAIL CLOSED. Every ambiguity exits non-zero rather than guessing, because the
# watermark does not advance on failure -- so a refusal loses nothing and can be
# retried, while a wrong "false" ships a schema change with no reindex.

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
head_sha=${1:?Usage: detect-reindex-label.sh <head_sha> [workflow_file] [label] [branch]}
workflow_file=${2:-deploy.yml}
label=${3:-force re-index recommended}
# The branch whose successful runs form the watermark. Must match the branch this
# workflow actually deploys from, or the watermark comes from an unrelated history:
# deploy.yml is main, commit.yml is develop.
branch=${4:-main}

emit() {
  echo "$1"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "$1" >> "$GITHUB_OUTPUT"
  fi
}

# --- the watermark ---------------------------------------------------------
#
# Only `status=success` counts. `conclusion` on a run cancelled while pending is
# `cancelled`, so such a run is invisible here and its commits get re-examined by
# whichever run succeeds next -- which is the whole point.
base_sha=$(
  gh api "repos/${GITHUB_REPOSITORY}/actions/workflows/${workflow_file}/runs" \
    -f "branch=${branch}" -f status=success -f per_page=1 \
    --jq '.workflow_runs[0].head_sha // empty' 2>/dev/null || true
)

if [[ -z "$base_sha" ]]; then
  echo "No previous successful run of ${workflow_file} on ${branch} to measure from." >&2
  echo "" >&2
  echo "Refusing to guess whether a reindex is owed. Dispatch this workflow" >&2
  echo "manually with reindex=force or reindex=skip to state it explicitly; that" >&2
  echo "run then becomes the watermark for every run after it." >&2
  exit 1
fi

# --- the commit range -----------------------------------------------------
compare=$(gh api "repos/${GITHUB_REPOSITORY}/compare/${base_sha}...${head_sha}")
status=$(echo "$compare" | jq -r '.status')
total_commits=$(echo "$compare" | jq -r '.total_commits')
listed_commits=$(echo "$compare" | jq -r '.commits | length')

# `ahead` is the normal case; `identical` happens on a re-run of an already
# successful commit. `behind` or `diverged` means history was rewritten under us,
# and the range would silently omit commits.
if [[ "$status" != "ahead" && "$status" != "identical" ]]; then
  echo "Cannot measure the commit range: ${base_sha}...${head_sha} is '${status}'," >&2
  echo "not 'ahead' or 'identical'. History was probably force-pushed." >&2
  echo "Dispatch manually with reindex=force or reindex=skip." >&2
  exit 1
fi

# The compare endpoint caps `.commits` at 250 even when total_commits exceeds it,
# so a large range would look clean simply because the labelled commit was cut off.
if [[ "$total_commits" -gt "$listed_commits" ]]; then
  echo "Commit range is truncated: ${total_commits} commits but only" >&2
  echo "${listed_commits} listed by the compare API. A labelled PR could be in the" >&2
  echo "part that was cut off, so this cannot be decided automatically." >&2
  echo "Dispatch manually with reindex=force or reindex=skip." >&2
  exit 1
fi

echo "Measuring ${base_sha:0:8}...${head_sha:0:8} (${total_commits} commit(s), ${status})."

# --- the labels -----------------------------------------------------------
#
# Look up PRs per commit rather than parsing merge-commit subjects: this repo has
# squash, merge-commit and rebase merges in its history, and only the API resolves
# all three. `|| true` keeps a commit with no associated PR (a direct push) from
# aborting the loop under `set -e`.
# Initialized empty, and every expansion below uses the `-` default so `set -u`
# cannot abort on the no-matches path (bash treats an empty array as unset).
pr_numbers=()
while read -r sha; do
  [[ -z "$sha" ]] && continue
  matched=$(
    gh api "repos/${GITHUB_REPOSITORY}/commits/${sha}/pulls" \
      --jq ".[] | select(.labels[]?.name == \"${label}\") | .number" 2>/dev/null || true
  )
  while read -r number; do
    [[ -z "$number" ]] && continue
    # De-duplicate: a squashed PR's commits all resolve to the same PR number.
    # `${pr_numbers[*]-}` (not `${pr_numbers[*]}`) because bash treats an unset
    # empty array as an unbound variable under `set -u`, which would abort here on
    # the very first match.
    if [[ ! " ${pr_numbers[*]-} " == *" ${number} "* ]]; then
      pr_numbers+=("$number")
    fi
  done <<< "$matched"
done <<< "$(echo "$compare" | jq -r '.commits[].sha')"

emit "base_sha=${base_sha}"

if [[ ${#pr_numbers[@]-0} -gt 0 ]]; then
  joined=$(IFS=,; echo "${pr_numbers[*]-}")
  emit "reindex_needed=true"
  emit "pr_numbers=${joined}"
  echo ""
  echo "REINDEX REQUIRED: PR(s) ${joined} carry '${label}'."
  echo "The OpenSearch index will be rebuilt after each environment deploys."
else
  emit "reindex_needed=false"
  emit "pr_numbers="
  echo ""
  echo "No PR in this range carries '${label}'; deploying without a reindex."
fi
