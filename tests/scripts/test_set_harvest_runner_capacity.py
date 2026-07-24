import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "set_harvest_runner_capacity.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _setup_fake_cf(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
set -euo pipefail

echo "$*" >> "$CF_CALLS_FILE"

case "$1" in
  set-env)
    [[ "$2" == "datagov-harvest" && "$3" == "HARVEST_RUNNER_MAX_TASKS" ]]
    ;;
  restart)
    [[ "$2" == "datagov-harvest" && "$3" == "--strategy" && "$4" == "rolling" ]]
    exit "${CF_RESTART_EXIT_CODE:-0}"
    ;;
  env)
    [[ "$2" == "datagov-harvest" ]]
    printf '%s\n' "${CF_ENV_OUTPUT:-User-Provided:}"
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
    }
    return env, calls_file


@pytest.mark.parametrize(
    ("action", "max_tasks"),
    [("disable", "0"), ("enable", "3")],
)
def test_updates_only_max_tasks_and_restarts(tmp_path, action, max_tasks):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), action],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    calls = calls_file.read_text()
    assert f"set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS {max_tasks}" in calls
    assert "restart datagov-harvest --strategy rolling" in calls
    assert f"HARVEST_RUNNER_MAX_TASKS set to {max_tasks}" in result.stdout
    assert "Confirmed the datagov-harvest rolling restart completed." in result.stdout


def test_check_reports_current_max_tasks_without_exposing_other_environment_values(
    tmp_path,
):
    env, calls_file = _setup_fake_cf(tmp_path)
    env["CF_ENV_OUTPUT"] = """System-Provided:
SENSITIVE_VALUE: do-not-print
User-Provided:
HARVEST_RUNNER_MAX_TASKS: 5"""

    result = subprocess.run(
        [str(SCRIPT), "check"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert calls_file.read_text() == "env datagov-harvest\n"
    assert "HARVEST_RUNNER_MAX_TASKS is currently set to 5." in result.stdout
    assert "do-not-print" not in result.stdout


def test_check_reports_default_when_max_tasks_is_not_set(tmp_path):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), "check"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert calls_file.read_text() == "env datagov-harvest\n"
    assert (
        "HARVEST_RUNNER_MAX_TASKS is not set in Cloud Foundry; "
        "the application defaults to 3."
    ) in result.stdout


def test_enable_uses_custom_max_tasks(tmp_path):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), "enable", "datagov-harvest", "5"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS 5" in (
        calls_file.read_text()
    )
    assert "HARVEST_RUNNER_MAX_TASKS set to 5" in result.stdout


def test_enable_sets_default_capacity(tmp_path):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), "enable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS 3" in (
        calls_file.read_text()
    )


def test_restarts_when_max_tasks_already_has_desired_value(tmp_path):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), "disable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "restart datagov-harvest --strategy rolling" in calls_file.read_text()
    assert "HARVEST_RUNNER_MAX_TASKS set to 0" in result.stdout


def test_fails_when_restart_fails(tmp_path):
    env, _ = _setup_fake_cf(tmp_path)
    env["CF_RESTART_EXIT_CODE"] = "1"

    result = subprocess.run(
        [str(SCRIPT), "disable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 1
    assert "Confirmed" not in result.stdout


@pytest.mark.parametrize("max_tasks", ["0", "-1", "not-a-number"])
def test_enable_rejects_invalid_max_tasks_without_calling_cf(tmp_path, max_tasks):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), "enable", "datagov-harvest", max_tasks],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 2
    assert "must be a positive integer" in result.stderr
    assert not calls_file.exists()


def test_rejects_unknown_action_without_calling_cf(tmp_path):
    env, calls_file = _setup_fake_cf(tmp_path)

    result = subprocess.run(
        [str(SCRIPT), "pause"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 2
    assert "Usage:" in result.stderr
    assert not calls_file.exists()
