import json
import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "set_harvest_runner_capacity.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _setup_fake_cf(tmp_path, app_environment):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    app_environment_file = tmp_path / "app-environment.json"
    restart_count_file = tmp_path / "restart-count"
    app_environment_file.write_text(json.dumps(app_environment))
    restart_count_file.write_text("0")

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
set -euo pipefail

echo "$*" >> "$CF_CALLS_FILE"

case "$1" in
  app)
    [[ "$2" == "datagov-harvest" && "$3" == "--guid" ]]
    echo "app-guid"
    ;;
  curl)
    case "$2" in
      /v3/apps/app-guid/processes)
        echo '{"resources":[{"guid":"web-process","type":"web","instances":2}]}'
        ;;
      /v3/processes/web-process/stats)
        restart_count=$(<"$CF_RESTART_COUNT_FILE")
        printf '%s\n' \
          '{"resources":['\
          '{"state":"RUNNING","instance_guid":"instance-'"$restart_count"'-0"},'\
          '{"state":"RUNNING","instance_guid":"instance-'"$restart_count"'-1"}]}'
        ;;
      /v3/deployments*)
        echo '{"resources":[]}'
        ;;
      *)
        echo "Unexpected cf curl path: $2" >&2
        exit 1
        ;;
    esac
    ;;
  set-env)
    [[ "$2" == "datagov-harvest" && "$3" == "HARVEST_RUNNER_MAX_TASKS" ]]
    updated_environment=$(mktemp)
    jq --arg name "$3" --arg value "$4" \
      '.[$name] = $value' "$CF_APP_ENVIRONMENT_FILE" > "$updated_environment"
    mv "$updated_environment" "$CF_APP_ENVIRONMENT_FILE"
    ;;
  restart)
    [[ "$2" == "datagov-harvest" && "$3" == "--strategy" && "$4" == "rolling" ]]
    restart_count=$(<"$CF_RESTART_COUNT_FILE")
    echo "$((restart_count + 1))" > "$CF_RESTART_COUNT_FILE"
    ;;
  logs)
    [[ "$2" == "datagov-harvest" && "$3" == "--recent" ]]
    if [[ "${CF_HIDE_STARTUP_LOGS:-false}" != "true" ]]; then
      restart_count=$(<"$CF_RESTART_COUNT_FILE")
      max_tasks=$(jq -r '.HARVEST_RUNNER_MAX_TASKS // "3"' "$CF_APP_ENVIRONMENT_FILE")
      marker="Harvester startup: HARVEST_RUNNER_MAX_TASKS=$max_tasks"
      echo "$marker CF_INSTANCE_GUID=instance-$restart_count-0"
      echo "$marker CF_INSTANCE_GUID=instance-$restart_count-1"
    fi
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
        "CF_APP_ENVIRONMENT_FILE": str(app_environment_file),
        "CF_RESTART_COUNT_FILE": str(restart_count_file),
        "CF_RESTART_POLL_SECONDS": "0",
        "CF_RESTART_TIMEOUT_SECONDS": "5",
    }
    return env, calls_file, app_environment_file


@pytest.mark.parametrize(
    ("action", "max_tasks"),
    [("disable", "0"), ("enable", "3")],
)
def test_updates_only_max_tasks_and_restarts(tmp_path, action, max_tasks):
    original_environment = {
        "OTHER_SETTING": "keep-me",
        "HARVEST_RUNNER_MAX_TASKS": "5",
    }
    env, calls_file, app_environment_file = _setup_fake_cf(
        tmp_path, original_environment
    )

    result = subprocess.run(
        [str(SCRIPT), action],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(app_environment_file.read_text()) == {
        **original_environment,
        "HARVEST_RUNNER_MAX_TASKS": max_tasks,
    }
    calls = calls_file.read_text()
    assert f"set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS {max_tasks}" in calls
    assert "restart datagov-harvest --strategy rolling" in calls
    assert "service_instances" not in calls
    assert "update-user-provided-service" not in calls
    assert f"HARVEST_RUNNER_MAX_TASKS set to {max_tasks}" in result.stdout
    assert (
        f"Confirmed all 2 instance(s) restarted with "
        f"HARVEST_RUNNER_MAX_TASKS={max_tasks}."
    ) in result.stdout


def test_enable_uses_custom_max_tasks(tmp_path):
    env, calls_file, app_environment_file = _setup_fake_cf(tmp_path, {})

    result = subprocess.run(
        [str(SCRIPT), "enable", "datagov-harvest", "5"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(app_environment_file.read_text()) == {
        "HARVEST_RUNNER_MAX_TASKS": "5"
    }
    assert "set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS 5" in (
        calls_file.read_text()
    )
    assert "HARVEST_RUNNER_MAX_TASKS set to 5" in result.stdout


def test_enable_sets_default_capacity(tmp_path):
    env, _, app_environment_file = _setup_fake_cf(
        tmp_path, {"OTHER_SETTING": "keep-me"}
    )

    result = subprocess.run(
        [str(SCRIPT), "enable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(app_environment_file.read_text()) == {
        "OTHER_SETTING": "keep-me",
        "HARVEST_RUNNER_MAX_TASKS": "3",
    }


def test_restarts_when_max_tasks_already_has_desired_value(tmp_path):
    env, calls_file, _ = _setup_fake_cf(
        tmp_path,
        {
            "HARVEST_RUNNER_MAX_TASKS": "0",
        },
    )

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


def test_fails_when_restarted_instances_do_not_log_capacity(tmp_path):
    env, _, _ = _setup_fake_cf(tmp_path, {"HARVEST_RUNNER_MAX_TASKS": "5"})
    env["CF_HIDE_STARTUP_LOGS"] = "true"
    env["CF_RESTART_TIMEOUT_SECONDS"] = "0"

    result = subprocess.run(
        [str(SCRIPT), "disable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 1
    assert "Timed out confirming the rolling restart" in result.stderr
    assert "reused_instance_guids=0" in result.stderr
    assert "logged_instances=0/2" in result.stderr


@pytest.mark.parametrize("max_tasks", ["0", "-1", "not-a-number"])
def test_enable_rejects_invalid_max_tasks_without_calling_cf(tmp_path, max_tasks):
    env, calls_file, _ = _setup_fake_cf(tmp_path, {})

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
    env, calls_file, _ = _setup_fake_cf(tmp_path, {})

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
