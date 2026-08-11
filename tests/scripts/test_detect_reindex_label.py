"""Tests for .github/scripts/detect-reindex-label.sh.

This script decides whether a push to `main` owes an index rebuild. Getting it wrong
in the "false" direction ships a schema change with no reindex and degrades search
silently, so the properties pinned here are: it finds the label across a range, it
de-duplicates, and it FAILS rather than guessing whenever the range is unreliable.
"""

import json
import os
import subprocess
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / ".github"
    / "scripts"
    / "detect-reindex-label.sh"
)

LABEL = "force re-index recommended"
BASE = "1111111111111111111111111111111111111111"
HEAD = "2222222222222222222222222222222222222222"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_detect(
    tmp_path,
    *arguments,
    watermark=BASE,
    compare=None,
    pulls_by_sha=None,
):
    """Run the script against a fake `gh`.

    ``watermark`` is the head_sha of the last successful run (empty string = none).
    ``compare`` is the compare-API payload. ``pulls_by_sha`` maps a commit sha to the
    payload of ``/commits/<sha>/pulls``.
    """
    if compare is None:
        compare = {
            "status": "ahead",
            "total_commits": 1,
            "commits": [{"sha": "aaa"}],
        }
    if pulls_by_sha is None:
        pulls_by_sha = {}

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (tmp_path / "watermark").write_text(watermark)
    (tmp_path / "compare.json").write_text(json.dumps(compare))
    (tmp_path / "pulls.json").write_text(json.dumps(pulls_by_sha))

    # The real script calls `gh api ... --jq <filter>`; the fake serves the right
    # payload per endpoint and shells out to real jq so the filters are exercised
    # rather than stubbed.
    _write_executable(
        fake_bin / "gh",
        """#!/bin/bash
endpoint=""
jq_filter=""
prev=""
for arg in "$@"; do
  case "$prev" in
    --jq) jq_filter="$arg" ;;
  esac
  case "$arg" in
    repos/*) endpoint="$arg" ;;
  esac
  prev="$arg"
done

emit() {
  if [[ -n "$jq_filter" ]]; then
    echo "$1" | jq -r "$jq_filter"
  else
    echo "$1"
  fi
}

# Record the full argv of every call so a test can assert which branch and workflow
# file were actually queried -- getting those wrong silently measures from an
# unrelated history.
echo "$*" >> "$FAKE_DIR/gh-calls"

case "$endpoint" in
  *"/actions/workflows/"*)
    wm=$(cat "$FAKE_DIR/watermark")
    if [[ -z "$wm" ]]; then
      emit '{"workflow_runs":[]}'
    else
      emit "{\\"workflow_runs\\":[{\\"head_sha\\":\\"$wm\\"}]}"
    fi
    ;;
  *"/compare/"*)
    emit "$(cat "$FAKE_DIR/compare.json")"
    ;;
  *"/commits/"*"/pulls")
    sha=$(echo "$endpoint" | sed -E 's#.*/commits/([^/]+)/pulls#\\1#')
    payload=$(jq -c --arg sha "$sha" '.[$sha] // []' "$FAKE_DIR/pulls.json")
    emit "$payload"
    ;;
  *)
    echo "Unexpected gh endpoint: $endpoint" >&2
    exit 1
    ;;
esac
""",
    )

    output_file = tmp_path / "github-output"
    output_file.touch()
    (tmp_path / "gh-calls").touch()
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DIR": str(tmp_path),
        "GITHUB_REPOSITORY": "GSA/datagov-harvester",
        "GITHUB_OUTPUT": str(output_file),
    }
    args = arguments or (HEAD,)
    result = subprocess.run(
        [str(SCRIPT), *args],
        capture_output=True,
        env=env,
        text=True,
        timeout=60,
    )
    return result, output_file.read_text()


def _gh_calls(tmp_path):
    return (tmp_path / "gh-calls").read_text()


def _labelled(number, *labels):
    return [{"number": number, "labels": [{"name": name} for name in labels]}]


def test_detects_the_label_and_reports_the_pr(tmp_path):
    result, output = _run_detect(
        tmp_path,
        pulls_by_sha={"aaa": _labelled(42, LABEL)},
    )

    assert result.returncode == 0, result.stderr
    assert "reindex_needed=true" in output
    assert "pr_numbers=42" in output
    assert "REINDEX REQUIRED" in result.stdout


def test_reports_false_when_no_pr_carries_the_label(tmp_path):
    result, output = _run_detect(
        tmp_path,
        pulls_by_sha={"aaa": _labelled(42, "dependencies", "python")},
    )

    assert result.returncode == 0, result.stderr
    assert "reindex_needed=false" in output
    assert "pr_numbers=" in output


def test_finds_a_label_anywhere_in_the_range_not_just_at_head(tmp_path):
    """The collapse case this exists for: an earlier labelled merge whose own run was
    cancelled while pending must still be found by the run that supersedes it."""
    result, output = _run_detect(
        tmp_path,
        compare={
            "status": "ahead",
            "total_commits": 3,
            "commits": [{"sha": "aaa"}, {"sha": "bbb"}, {"sha": "ccc"}],
        },
        pulls_by_sha={"bbb": _labelled(77, LABEL)},
    )

    assert result.returncode == 0, result.stderr
    assert "reindex_needed=true" in output
    assert "pr_numbers=77" in output


def test_deduplicates_a_squashed_prs_commits(tmp_path):
    """Every commit of a squash-merged PR resolves to the same PR number."""
    result, output = _run_detect(
        tmp_path,
        compare={
            "status": "ahead",
            "total_commits": 2,
            "commits": [{"sha": "aaa"}, {"sha": "bbb"}],
        },
        pulls_by_sha={
            "aaa": _labelled(55, LABEL),
            "bbb": _labelled(55, LABEL),
        },
    )

    assert result.returncode == 0, result.stderr
    assert "pr_numbers=55" in output


def test_reports_every_labelled_pr_in_the_range(tmp_path):
    result, output = _run_detect(
        tmp_path,
        compare={
            "status": "ahead",
            "total_commits": 2,
            "commits": [{"sha": "aaa"}, {"sha": "bbb"}],
        },
        pulls_by_sha={
            "aaa": _labelled(1, LABEL),
            "bbb": _labelled(2, LABEL),
        },
    )

    assert result.returncode == 0, result.stderr
    assert "pr_numbers=1,2" in output


def test_tolerates_a_commit_with_no_associated_pr(tmp_path):
    """A direct push to main has no PR; that must not abort the scan."""
    result, output = _run_detect(
        tmp_path,
        compare={
            "status": "ahead",
            "total_commits": 2,
            "commits": [{"sha": "aaa"}, {"sha": "bbb"}],
        },
        pulls_by_sha={"bbb": _labelled(9, LABEL)},
    )

    assert result.returncode == 0, result.stderr
    assert "pr_numbers=9" in output


def test_fails_closed_without_a_watermark(tmp_path):
    """Nothing to measure from. Refusing loses nothing -- the watermark only advances
    on success, so a later run re-examines the same commits."""
    result, _ = _run_detect(tmp_path, watermark="")

    assert result.returncode == 1
    assert "No previous successful run" in result.stderr
    assert "reindex=force" in result.stderr


def test_fails_closed_on_rewritten_history(tmp_path):
    result, _ = _run_detect(
        tmp_path,
        compare={"status": "diverged", "total_commits": 1, "commits": [{"sha": "a"}]},
    )

    assert result.returncode == 1
    assert "diverged" in result.stderr


def test_fails_closed_on_a_truncated_range(tmp_path):
    """The compare API caps `.commits` at 250, so a big range could look clean only
    because the labelled commit was cut off."""
    result, _ = _run_detect(
        tmp_path,
        compare={
            "status": "ahead",
            "total_commits": 300,
            "commits": [{"sha": "aaa"}],
        },
    )

    assert result.returncode == 1
    assert "truncated" in result.stderr


def test_accepts_an_identical_range_on_a_rerun(tmp_path):
    """Re-running an already successful commit is legitimate, not an error."""
    result, output = _run_detect(
        tmp_path,
        compare={"status": "identical", "total_commits": 0, "commits": []},
    )

    assert result.returncode == 0, result.stderr
    assert "reindex_needed=false" in output


def test_reports_the_base_it_measured_from(tmp_path):
    """The watermark goes in the output so a run's range is auditable after the fact."""
    result, output = _run_detect(tmp_path)

    assert result.returncode == 0, result.stderr
    assert f"base_sha={BASE}" in output


def test_defaults_to_deploy_yml_on_main(tmp_path):
    result, _ = _run_detect(tmp_path)

    assert result.returncode == 0, result.stderr
    calls = _gh_calls(tmp_path)
    assert "actions/workflows/deploy.yml/runs" in calls
    assert "branch=main" in calls


def test_measures_from_the_requested_workflow_and_branch(tmp_path):
    """commit.yml deploys development from `develop`, so its watermark must come from
    that branch's runs. Reading main's history instead would span unrelated commits
    and could silently miss -- or invent -- a reindex obligation."""
    result, _ = _run_detect(
        tmp_path,
        HEAD,
        "commit.yml",
        LABEL,
        "develop",
    )

    assert result.returncode == 0, result.stderr
    calls = _gh_calls(tmp_path)
    assert "actions/workflows/commit.yml/runs" in calls
    assert "branch=develop" in calls
    assert "branch=main" not in calls


def test_a_custom_label_is_honoured(tmp_path):
    """The label is a parameter so a caller can look for something else without
    editing the script -- and so the tests can exercise a label that really exists."""
    result, output = _run_detect(
        tmp_path,
        HEAD,
        "deploy.yml",
        "some-other-label",
        pulls_by_sha={"aaa": _labelled(3, "some-other-label")},
    )

    assert result.returncode == 0, result.stderr
    assert "reindex_needed=true" in output
    assert "pr_numbers=3" in output
