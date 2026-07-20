import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "monitor_cf_logs.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _setup_fake_cf(tmp_path, states, logs="", logs_fail=False, curl_fail=False):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    state_count_file = tmp_path / "state-count"
    state_count_file.write_text("0")

    _write_executable(fake_bin / "apk", "#!/bin/bash\nexit 0\n")
    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
set -euo pipefail

echo "$*" >> "$CF_CALLS_FILE"

case "$1" in
  app)
    echo "app-guid"
    ;;
  logs)
    if [[ "$CF_LOGS_FAIL" == "true" ]]; then
      echo "log stream unavailable" >&2
      exit 1
    fi
    printf '%s\\n' "$CF_TASK_LOGS"
    ;;
  curl)
    if [[ "$CF_CURL_FAIL" == "true" ]]; then
      echo "task API unavailable" >&2
      exit 42
    fi
    IFS=',' read -r -a states <<< "$CF_TASK_STATES"
    state_count=$(<"$CF_STATE_COUNT_FILE")
    state_index=$state_count
    if [[ "$state_index" -ge "${#states[@]}" ]]; then
      state_index=$((${#states[@]} - 1))
    fi
    state=${states[$state_index]}
    echo "$((state_count + 1))" > "$CF_STATE_COUNT_FILE"
    if [[ "$state" == "FAILED" ]]; then
      result='{"failure_reason":"task failed"}'
    else
      result='{}'
    fi
    printf '{"resources":[{"guid":"task-guid","state":"%s","result":%s}]}\\n' \
      "$state" "$result"
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "CF_CALLS_FILE": str(calls_file),
        "CF_STATE_COUNT_FILE": str(state_count_file),
        "CF_TASK_STATES": ",".join(states),
        "CF_TASK_LOGS": logs,
        "CF_LOGS_FAIL": str(logs_fail).lower(),
        "CF_CURL_FAIL": str(curl_fail).lower(),
        "CF_TASK_MONITOR_POLL_SECONDS": "0",
        "CF_TASK_MONITOR_TIMEOUT_SECONDS": "5",
        "GITHUB_WORKSPACE": str(tmp_path),
    }
    return env, calls_file


def _run(env, warning_pattern=None):
    arguments = [str(SCRIPT), "datagov-harvest", "search-rebuild"]
    if warning_pattern is not None:
        arguments.append(warning_pattern)
    return subprocess.run(
        arguments,
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )


def test_monitor_polls_task_api_until_success_and_reports_warning(tmp_path):
    log_line = (
        "2026-07-20T10:00:00Z [APP/TASK/search-rebuild/0] OUT "
        "failed to index in this batch"
    )
    env, calls_file = _setup_fake_cf(
        tmp_path,
        ["RUNNING", "SUCCEEDED"],
        logs=log_line,
    )

    result = _run(env, "failed to index in this batch")

    assert result.returncode == 0
    assert "task-guid) succeeded" in result.stdout
    assert result.stdout.count(log_line) == 1
    assert (tmp_path / ".cf_monitor_result").read_text().strip() == (
        "warning_pattern_matched"
    )
    calls = calls_file.read_text()
    assert calls.count("curl /v3/apps/app-guid/tasks?") == 2


def test_monitor_returns_failure_from_task_api(tmp_path):
    env, _ = _setup_fake_cf(tmp_path, ["FAILED"])

    result = _run(env)

    assert result.returncode == 1
    assert "failed: task failed" in result.stderr


def test_monitor_uses_task_api_when_logs_are_unavailable(tmp_path):
    env, _ = _setup_fake_cf(tmp_path, ["SUCCEEDED"], logs_fail=True)

    result = _run(env)

    assert result.returncode == 0
    assert "could not read recent logs" in result.stderr
    assert "task-guid) succeeded" in result.stdout


def test_monitor_fails_when_task_state_cannot_be_verified(tmp_path):
    env, _ = _setup_fake_cf(tmp_path, ["RUNNING"], curl_fail=True)

    result = _run(env)

    assert result.returncode != 0
    assert "task API unavailable" in result.stderr
