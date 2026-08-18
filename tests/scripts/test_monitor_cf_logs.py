import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "monitor_cf_logs.sh"
APP = "datagov-harvest"
TASK = "search-rebuild-123-1"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(
    tmp_path,
    *,
    states=("SUCCEEDED",),
    log_lines="",
    warning_pattern="",
    task_found=True,
    lookup_failures=0,
    poll_failures=0,
):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    counter_dir = tmp_path / "counters"
    counter_dir.mkdir()

    _write_executable(fake_bin / "apk", "#!/bin/bash\nexit 0\n")
    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
set -euo pipefail

echo "$*" >> "$CF_CALLS_FILE"

increment_counter() {
  local name=$1
  local counter_file="$CF_COUNTER_DIR/$name"
  local count=0

  if [[ -f "$counter_file" ]]; then
    count=$(cat "$counter_file")
  fi
  count=$((count + 1))
  echo "$count" > "$counter_file"
  echo "$count"
}

case "$1" in
  app)
    [[ "$2" == "$CF_APP" && "$3" == "--guid" ]]
    echo "app-guid"
    ;;
  logs)
    [[ "$2" == "$CF_APP" ]]
    printf '%s\\n' "$CF_LOG_LINES"
    ;;
  curl)
    [[ "$2" == "--fail" ]]
    endpoint=$3
    if [[ "$endpoint" == /v3/apps/*/tasks* ]]; then
      count=$(increment_counter lookup)
      if [[ "$count" -le "$CF_LOOKUP_FAILURES" ]]; then
        exit 1
      fi
      if [[ "$CF_TASK_FOUND" == "true" ]]; then
        printf '{"resources":[{"guid":"task-guid","name":"%s"}]}\\n' "$CF_TASK"
      else
        echo '{"resources":[]}'
      fi
    elif [[ "$endpoint" == "/v3/tasks/task-guid" ]]; then
      count=$(increment_counter poll)
      if [[ "$count" -le "$CF_POLL_FAILURES" ]]; then
        exit 1
      fi
      read -r -a states <<< "$CF_TASK_STATES"
      index=$((count - CF_POLL_FAILURES - 1))
      if [[ "$index" -ge "${#states[@]}" ]]; then
        index=$((${#states[@]} - 1))
      fi
      printf '{"state":"%s"}\\n' "${states[$index]}"
    else
      echo "Unexpected cf curl endpoint: $endpoint" >&2
      exit 1
    fi
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    arguments = [str(SCRIPT), APP, TASK]
    if warning_pattern:
        arguments.append(warning_pattern)

    result = subprocess.run(
        arguments,
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "GITHUB_WORKSPACE": str(tmp_path),
            "CF_APP": APP,
            "CF_TASK": TASK,
            "CF_TASK_FOUND": "true" if task_found else "false",
            "CF_TASK_STATES": " ".join(states),
            "CF_LOG_LINES": log_lines,
            "CF_LOOKUP_FAILURES": str(lookup_failures),
            "CF_POLL_FAILURES": str(poll_failures),
            "CF_CALLS_FILE": str(calls_file),
            "CF_COUNTER_DIR": str(counter_dir),
            "CF_TASK_POLL_SECONDS": "0",
            "CF_TASK_LOOKUP_TIMEOUT_SECONDS": "0",
            "CF_TASK_MAX_POLL_ERRORS": "3",
            "CF_TASK_LOG_SETTLE_SECONDS": "0",
        },
        text=True,
        timeout=10,
    )
    result_file = tmp_path / ".cf_monitor_result"
    warning_result = result_file.read_text() if result_file.exists() else ""
    calls = calls_file.read_text() if calls_file.exists() else ""
    return result, warning_result, calls


def _task_log(message):
    return f"2026-08-14T12:00:00Z [APP/TASK/{TASK}/0] OUT {message}"


def test_succeeds_after_log_eof_only_when_cf_reports_success(tmp_path):
    result, _, _ = _run(tmp_path, states=("SUCCEEDED",))

    assert result.returncode == 0, result.stderr
    assert "state: SUCCEEDED" in result.stdout


def test_fails_after_log_eof_when_cf_reports_failure(tmp_path):
    result, _, calls = _run(tmp_path, states=("FAILED",))

    assert result.returncode == 1
    assert "state: FAILED" in result.stdout
    assert f"logs {APP} --recent" in calls


def test_cf_state_overrides_a_successful_exit_log_line(tmp_path):
    result, _, _ = _run(
        tmp_path,
        states=("FAILED",),
        log_lines=_task_log("Exit status 0"),
    )

    assert result.returncode == 1


def test_polls_until_cf_reports_a_terminal_state(tmp_path):
    result, _, calls = _run(
        tmp_path,
        states=("PENDING", "RUNNING", "SUCCEEDED"),
    )

    assert result.returncode == 0, result.stderr
    assert calls.count("curl --fail /v3/tasks/task-guid") == 3


def test_fails_after_repeated_task_state_api_errors(tmp_path):
    result, _, calls = _run(
        tmp_path,
        states=("SUCCEEDED",),
        poll_failures=3,
    )

    assert result.returncode == 1
    assert calls.count("curl --fail /v3/tasks/task-guid") == 3
    assert "after 3 consecutive attempts" in result.stderr


def test_fails_when_the_named_task_cannot_be_found(tmp_path):
    result, _, _ = _run(tmp_path, task_found=False)

    assert result.returncode == 1
    assert "was not found" in result.stderr


def test_records_a_warning_without_changing_a_successful_task_result(tmp_path):
    warning = "failed to index in this batch"
    result, warning_result, _ = _run(
        tmp_path,
        states=("SUCCEEDED",),
        log_lines=_task_log(warning),
        warning_pattern=warning,
    )

    assert result.returncode == 0, result.stderr
    assert warning_result == "warning_pattern_matched\n"
