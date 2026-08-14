import json
import os
import subprocess
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / ".github"
    / "scripts"
    / "detect-opensearch-migration-label.sh"
)

BASE = "1111111111111111111111111111111111111111"
HEAD = "9999999999999999999999999999999999999999"
ORPHAN = "2222222222222222222222222222222222222222"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _shas(value):
    return value if isinstance(value, str) else " ".join(value)


def _run(
    tmp_path,
    *,
    commits=("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",),
    labeled_shas=(),
    base_sha=BASE,
    fallback_base_sha="",
    fallback_workflow_file="",
    status="ahead",
    total_commits=None,
    compare_statuses=None,
):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "gh-calls"
    output_file = tmp_path / "outputs"
    compare_dir = tmp_path / "compare"
    compare_dir.mkdir()

    def _compare(compare_status):
        return json.dumps(
            {
                "status": compare_status,
                "total_commits": (
                    len(commits) if total_commits is None else total_commits
                ),
                "commits": [{"sha": sha} for sha in commits],
            }
        )

    (compare_dir / "default.json").write_text(_compare(status))
    for sha, sha_status in (compare_statuses or {}).items():
        (compare_dir / f"{sha}.json").write_text(_compare(sha_status))

    _write_executable(
        fake_bin / "gh",
        """#!/bin/bash
echo "$*" >> "$GH_CALLS_FILE"
if [[ "$*" == *"/actions/workflows/"* ]]; then
  if [[ -n "$GH_FALLBACK_WORKFLOW_FILE" ]] &&
    [[ "$*" == *"/${GH_FALLBACK_WORKFLOW_FILE}/"* ]]; then
    printf '%s\\n' $GH_FALLBACK_BASE_SHA
  else
    printf '%s\\n' $GH_BASE_SHA
  fi
elif [[ "$*" == *"/compare/"* ]]; then
  args="$*"
  ref=${args##*/compare/}
  base=${ref%%...*}
  if [[ -f "$GH_COMPARE_DIR/${base}.json" ]]; then
    cat "$GH_COMPARE_DIR/${base}.json"
  else
    cat "$GH_COMPARE_DIR/default.json"
  fi
elif [[ "$*" == *"/commits/"*"/pulls"* ]]; then
  for sha in $GH_LABELED_SHAS; do
    if [[ "$*" == *"/commits/${sha}/pulls"* ]]; then
      echo 42
    fi
  done
else
  echo "Unexpected gh call: $*" >&2
  exit 1
fi
""",
    )

    result = subprocess.run(
        [str(SCRIPT), HEAD, "deploy.yml", "main"],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "GITHUB_REPOSITORY": "GSA/datagov-harvester",
            "GITHUB_OUTPUT": str(output_file),
            "GH_CALLS_FILE": str(calls_file),
            "GH_BASE_SHA": _shas(base_sha),
            "GH_FALLBACK_BASE_SHA": _shas(fallback_base_sha),
            "GH_FALLBACK_WORKFLOW_FILE": fallback_workflow_file,
            "GH_COMPARE_DIR": str(compare_dir),
            "GH_LABELED_SHAS": " ".join(labeled_shas),
            "FALLBACK_WORKFLOW_FILE": fallback_workflow_file,
        },
        text=True,
        timeout=10,
    )
    outputs = output_file.read_text() if output_file.exists() else ""
    calls = calls_file.read_text() if calls_file.exists() else ""
    return result, outputs, calls


def test_detects_a_labeled_pr_since_the_last_success(tmp_path):
    sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    result, outputs, _ = _run(tmp_path, commits=(sha,), labeled_shas=(sha,))

    assert result.returncode == 0, result.stderr
    assert "migration_needed=true" in outputs
    assert "pr_numbers=42" in outputs
    assert f"base_sha={BASE}" in outputs


def test_reports_no_migration_for_unlabeled_commits(tmp_path):
    result, outputs, _ = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "migration_needed=false" in outputs
    assert "pr_numbers=" in outputs


def test_uses_the_last_successful_run_as_the_watermark(tmp_path):
    result, _, calls = _run(tmp_path)

    assert result.returncode == 0
    watermark_call = next(
        line for line in calls.splitlines() if "/actions/workflows/" in line
    )
    assert "-X GET" in watermark_call
    assert "branch=main" in watermark_call
    assert "status=success" in watermark_call


def test_fails_without_a_successful_release_watermark(tmp_path):
    result, outputs, _ = _run(tmp_path, base_sha="")

    assert result.returncode == 1
    assert "No successful" in result.stderr
    assert outputs == ""


def test_bootstraps_from_a_previous_release_workflow(tmp_path):
    result, outputs, calls = _run(
        tmp_path,
        base_sha="",
        fallback_base_sha=BASE,
        fallback_workflow_file="commit.yml",
    )

    assert result.returncode == 0, result.stderr
    assert f"base_sha={BASE}" in outputs
    assert "/actions/workflows/deploy.yml/runs" in calls
    assert "/actions/workflows/commit.yml/runs" in calls
    fallback_call = next(
        line
        for line in calls.splitlines()
        if "/actions/workflows/commit.yml/runs" in line
    )
    assert f'select(.head_sha != "{HEAD}")' in fallback_call


def test_fails_when_the_compare_response_is_truncated(tmp_path):
    result, outputs, _ = _run(tmp_path, total_commits=251)

    assert result.returncode == 1
    assert "truncated" in result.stderr
    assert outputs == ""


def test_fails_when_history_is_not_ahead_or_identical(tmp_path):
    result, outputs, _ = _run(tmp_path, status="diverged")

    assert result.returncode == 1
    assert "compare status is 'diverged'" in result.stderr
    assert outputs == ""


def test_walks_past_a_watermark_orphaned_by_a_force_push(tmp_path):
    result, outputs, _ = _run(
        tmp_path,
        base_sha=(ORPHAN, BASE),
        compare_statuses={ORPHAN: "diverged", BASE: "ahead"},
    )

    assert result.returncode == 0, result.stderr
    assert f"base_sha={BASE}" in outputs
    assert f"Skipping watermark {ORPHAN}" in result.stderr


def test_bootstrap_tolerates_a_rewritten_fallback_history(tmp_path):
    result, outputs, _ = _run(
        tmp_path,
        base_sha="",
        fallback_base_sha=ORPHAN,
        fallback_workflow_file="commit.yml",
        compare_statuses={ORPHAN: "diverged"},
    )

    assert result.returncode == 0, result.stderr
    assert f"base_sha={HEAD}" in outputs
    assert "migration_needed=false" in outputs
    assert "pr_numbers=" in outputs
    assert "bootstrapping without a diff" in result.stdout


def test_bootstrap_tolerates_an_empty_fallback_history(tmp_path):
    result, outputs, _ = _run(
        tmp_path,
        base_sha="",
        fallback_base_sha="",
        fallback_workflow_file="commit.yml",
    )

    assert result.returncode == 0, result.stderr
    assert f"base_sha={HEAD}" in outputs
    assert "migration_needed=false" in outputs


def test_identical_commit_is_a_valid_unlabeled_rerun(tmp_path):
    result, outputs, _ = _run(tmp_path, commits=(), status="identical", total_commits=0)

    assert result.returncode == 0
    assert "migration_needed=false" in outputs


def test_deduplicates_a_pr_associated_with_multiple_commits(tmp_path):
    commits = (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    result, outputs, _ = _run(tmp_path, commits=commits, labeled_shas=commits)

    assert result.returncode == 0
    assert outputs.count("pr_numbers=42") == 1
