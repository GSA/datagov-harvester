import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "retry_cf_deploy.sh"

FAKE_CF_PUSH = """#!/bin/bash
set -euo pipefail

count=0
if [[ -f "$COUNTER_FILE" ]]; then
  count=$(cat "$COUNTER_FILE")
fi
count=$((count + 1))
echo "$count" > "$COUNTER_FILE"

if [[ "$count" -le "${FAIL_TIMES:-0}" ]]; then
  echo "${FAIL_MESSAGE:-boom}" >&2
  exit "${FAIL_EXIT_CODE:-1}"
fi

echo "cf push succeeded on attempt $count"
"""


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(tmp_path, *, fail_times=0, fail_message="boom", fail_exit_code=1, **env_overrides):
    fake_cf_push = tmp_path / "fake_cf_push.sh"
    _write_executable(fake_cf_push, FAKE_CF_PUSH)
    counter_file = tmp_path / "counter"

    env = {
        **os.environ,
        "COUNTER_FILE": str(counter_file),
        "FAIL_TIMES": str(fail_times),
        "FAIL_MESSAGE": fail_message,
        "FAIL_EXIT_CODE": str(fail_exit_code),
        "CF_DEPLOY_RETRY_SLEEP_SECONDS": "0",
        **env_overrides,
    }

    result = subprocess.run(
        [str(SCRIPT), str(fake_cf_push)],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )
    attempts = int(counter_file.read_text()) if counter_file.exists() else 0
    return result, attempts


def test_succeeds_on_first_attempt_without_retrying(tmp_path):
    result, attempts = _run(tmp_path, fail_times=0)

    assert result.returncode == 0, result.stderr
    assert attempts == 1


def test_retries_an_in_flight_deployment_until_it_succeeds(tmp_path):
    result, attempts = _run(
        tmp_path,
        fail_times=2,
        fail_message="Cannot update this process while a deployment is in flight.",
    )

    assert result.returncode == 0, result.stderr
    assert attempts == 3
    assert "retrying in 0s" in result.stdout


def test_does_not_retry_an_unrelated_failure(tmp_path):
    result, attempts = _run(
        tmp_path,
        fail_times=99,
        fail_message="Some other cf error",
    )

    assert result.returncode == 1
    assert attempts == 1


def test_gives_up_after_the_configured_attempt_limit(tmp_path):
    result, attempts = _run(
        tmp_path,
        fail_times=99,
        fail_message="Cannot update this process while a deployment is in flight.",
        CF_DEPLOY_RETRY_ATTEMPTS="3",
    )

    assert result.returncode == 1
    assert attempts == 3
    assert "Gave up after 3 attempts" in result.stderr
