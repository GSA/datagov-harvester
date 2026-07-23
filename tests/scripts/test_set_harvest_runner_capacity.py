import json
import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "set_harvest_runner_capacity.sh"
CAPACITY_ENV = "HARVEST_RUNNER_MAX_TASKS"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _setup_fake_cf(tmp_path, initial_environment=None):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    environment_file = tmp_path / "cf-environment"
    restart_count_file = tmp_path / "cf-restart-count"
    environment_file.write_text(
        json.dumps({"environment_variables": initial_environment or {}})
    )
    restart_count_file.write_text("0")

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
  set-env)
    jq --arg name "$3" --arg value "$4" \
      '.environment_variables[$name] = $value' \
      "$CF_ENVIRONMENT_FILE" > "$CF_ENVIRONMENT_FILE.tmp"
    mv "$CF_ENVIRONMENT_FILE.tmp" "$CF_ENVIRONMENT_FILE"
    ;;
  restart)
    if [[ "${CF_RESTART_EXIT_CODE:-0}" != "0" ]]; then
      exit "$CF_RESTART_EXIT_CODE"
    fi
    restart_count=$(<"$CF_RESTART_COUNT_FILE")
    if [[ "${CF_KEEP_INSTANCE_GUIDS:-false}" != "true" ]]; then
      echo "$((restart_count + 1))" > "$CF_RESTART_COUNT_FILE"
    fi
    ;;
  curl)
    case "$2" in
      /v3/apps/app-guid/env)
        cat "$CF_ENVIRONMENT_FILE"
        ;;
      /v3/deployments*)
        echo '{"resources":[]}'
        ;;
      /v3/apps/app-guid/processes)
        echo '{"resources":[{"guid":"web-process","type":"web","instances":2}]}'
        ;;
      /v3/processes/web-process/stats)
        restart_count=$(<"$CF_RESTART_COUNT_FILE")
        printf '%s\\n' \
          '{"resources":['\
          '{"state":"RUNNING","instance_guid":"instance-'"$restart_count"'-0"},'\
          '{"state":"RUNNING","instance_guid":"instance-'"$restart_count"'-1"}]}'
        ;;
      *)
        echo "Unexpected cf curl path: $2" >&2
        exit 1
        ;;
    esac
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
        "CF_ENVIRONMENT_FILE": str(environment_file),
        "CF_RESTART_COUNT_FILE": str(restart_count_file),
        "CF_RESTART_POLL_SECONDS": "0",
        "CF_RESTART_TIMEOUT_SECONDS": "5",
    }
    return env, calls_file, environment_file


def _run(env, *arguments):
    return subprocess.run(
        [str(SCRIPT), *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )


def _capacity(environment_file):
    return json.loads(environment_file.read_text())["environment_variables"].get(
        CAPACITY_ENV
    )


@pytest.mark.parametrize(
    ("arguments", "expected_capacity"),
    [(("disable",), "0"), (("enable",), "3"), (("enable", "app", "5"), "5")],
)
def test_toggle_updates_capacity_and_verifies_restart(
    tmp_path, arguments, expected_capacity
):
    env, calls_file, environment_file = _setup_fake_cf(
        tmp_path, {CAPACITY_ENV: "2"}
    )

    result = _run(env, *arguments)

    assert result.returncode == 0, result.stderr
    assert _capacity(environment_file) == expected_capacity
    assert (
        f"set-env {arguments[1] if len(arguments) > 1 else 'datagov-harvest'} "
        f"{CAPACITY_ENV} {expected_capacity}"
    ) in calls_file.read_text()
    assert "Confirmed all 2 web instance(s) were replaced" in result.stdout


def test_enable_turns_on_harvesting_when_already_disabled(tmp_path):
    env, _, environment_file = _setup_fake_cf(tmp_path, {CAPACITY_ENV: "0"})

    result = _run(env, "enable")

    assert result.returncode == 0, result.stderr
    assert _capacity(environment_file) == "3"


def test_check_reports_only_current_capacity(tmp_path):
    env, _, _ = _setup_fake_cf(
        tmp_path,
        {CAPACITY_ENV: "5", "SENSITIVE_VALUE": "do-not-print"},
    )

    result = _run(env, "check")

    assert result.returncode == 0, result.stderr
    assert f"{CAPACITY_ENV} is currently set to 5." in result.stdout
    assert "do-not-print" not in result.stdout


def test_check_reports_default_when_capacity_is_unset(tmp_path):
    env, _, _ = _setup_fake_cf(tmp_path)

    result = _run(env, "check")

    assert result.returncode == 0, result.stderr
    assert "the application defaults to 3" in result.stdout


@pytest.mark.parametrize("max_tasks", ["0", "-1", "not-a-number"])
def test_enable_rejects_invalid_capacity_without_calling_cf(tmp_path, max_tasks):
    env, calls_file, _ = _setup_fake_cf(tmp_path)

    result = _run(env, "enable", "datagov-harvest", max_tasks)

    assert result.returncode == 2
    assert "must be a positive integer" in result.stderr
    assert not calls_file.exists()


def test_fails_when_restart_fails(tmp_path):
    env, _, environment_file = _setup_fake_cf(
        tmp_path, {CAPACITY_ENV: "3"}
    )
    env["CF_RESTART_EXIT_CODE"] = "1"

    result = _run(env, "disable")

    assert result.returncode == 1
    assert _capacity(environment_file) == "0"
    assert "Confirmed" not in result.stdout


def test_restart_requires_every_instance_guid_to_change(tmp_path):
    env, _, _ = _setup_fake_cf(tmp_path, {CAPACITY_ENV: "3"})
    env["CF_KEEP_INSTANCE_GUIDS"] = "true"
    env["CF_RESTART_TIMEOUT_SECONDS"] = "0"

    result = _run(env, "disable")

    assert result.returncode == 1
    assert "Timed out confirming the rolling restart" in result.stderr
    assert "reused_instance_guids=2" in result.stderr


def test_rejects_unknown_action_without_calling_cf(tmp_path):
    env, calls_file, _ = _setup_fake_cf(tmp_path)

    result = _run(env, "unknown")

    assert result.returncode == 2
    assert "Usage:" in result.stderr
    assert not calls_file.exists()
