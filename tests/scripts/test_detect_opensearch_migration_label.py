import os
import subprocess
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / ".github"
    / "scripts"
    / "detect-opensearch-migration-label.sh"
)

HEAD = "9999999999999999999999999999999999999999"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(
    tmp_path,
    *,
    labeled=False,
    pull_api_failures=0,
    head_sha=HEAD,
):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "gh-calls"
    output_file = tmp_path / "outputs"
    pull_attempts_file = tmp_path / "pull-attempts"
    pull_attempts_file.write_text("0")

    _write_executable(
        fake_bin / "gh",
        """#!/bin/bash
echo "$*" >> "$GH_CALLS_FILE"
if [[ "$*" == *"/commits/"*"/pulls"* ]]; then
  attempts=$(cat "$GH_PULL_ATTEMPTS_FILE")
  attempts=$((attempts + 1))
  echo "$attempts" > "$GH_PULL_ATTEMPTS_FILE"
  if [[ "$attempts" -le "$GH_PULL_API_FAILURES" ]]; then
    echo "Simulated pull request API failure" >&2
    exit 1
  fi
  if [[ "$GH_LABELED" == "true" ]]; then
    echo 42
  fi
else
  echo "Unexpected gh call: $*" >&2
  exit 1
fi
""",
    )

    result = subprocess.run(
        [str(SCRIPT), head_sha],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "GITHUB_REPOSITORY": "GSA/datagov-harvester",
            "GITHUB_OUTPUT": str(output_file),
            "GH_CALLS_FILE": str(calls_file),
            "GH_LABELED": "true" if labeled else "false",
            "GH_PULL_API_FAILURES": str(pull_api_failures),
            "GH_PULL_ATTEMPTS_FILE": str(pull_attempts_file),
            "GH_API_MAX_ATTEMPTS": "3",
            "GH_API_RETRY_SECONDS": "0",
        },
        text=True,
        timeout=10,
    )
    outputs = output_file.read_text() if output_file.exists() else ""
    calls = calls_file.read_text() if calls_file.exists() else ""
    return result, outputs, calls


def test_detects_the_label_on_the_triggering_commit(tmp_path):
    result, outputs, calls = _run(tmp_path, labeled=True)

    assert result.returncode == 0, result.stderr
    assert "migration_needed=true" in outputs
    assert "pr_numbers=42" in outputs
    assert f"/commits/{HEAD}/pulls" in calls


def test_reports_no_migration_when_unlabeled(tmp_path):
    result, outputs, _ = _run(tmp_path, labeled=False)

    assert result.returncode == 0, result.stderr
    assert "migration_needed=false" in outputs
    assert "pr_numbers=" in outputs


def test_retries_a_transient_pull_request_api_failure(tmp_path):
    result, outputs, calls = _run(tmp_path, labeled=True, pull_api_failures=1)

    assert result.returncode == 0, result.stderr
    assert "migration_needed=true" in outputs
    assert calls.count(f"/commits/{HEAD}/pulls") == 2
    assert "retrying (1/3)" in result.stderr


def test_fails_closed_after_repeated_pull_request_api_failures(tmp_path):
    result, outputs, calls = _run(tmp_path, pull_api_failures=3)

    assert result.returncode == 1
    assert outputs == ""
    assert calls.count(f"/commits/{HEAD}/pulls") == 3
    assert f"commit {HEAD} after 3 attempts" in result.stderr
