import json
import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "set_harvest_runner_capacity.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _setup_fake_cf(tmp_path, credentials):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    credentials_file = tmp_path / "service-credentials.json"
    restart_count_file = tmp_path / "restart-count"
    credentials_file.write_text(json.dumps(credentials))
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
  service)
    [[ "$2" == "datagov-harvest-secrets" && "$3" == "--guid" ]]
    echo "service-guid"
    ;;
  curl)
    case "$2" in
      /v3/service_instances/service-guid/credentials)
        cat "$CF_CREDENTIALS_FILE"
        ;;
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
  update-user-provided-service)
    [[ "$2" == "datagov-harvest-secrets" && "$3" == "-p" ]]
    cp "$4" "$CF_CREDENTIALS_FILE"
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
      max_tasks=$(jq -r '.HARVEST_RUNNER_MAX_TASKS // "3"' "$CF_CREDENTIALS_FILE")
      enabled=$(jq -r '.HARVEST_RUNNER_ENABLED' "$CF_CREDENTIALS_FILE")
      if [[ "$enabled" == "true" ]]; then
        effective_max_tasks=$max_tasks
      else
        effective_max_tasks=0
      fi
      marker="Harvester startup: HARVEST_RUNNER_MAX_TASKS=$max_tasks"
      marker="$marker HARVEST_RUNNER_ENABLED=$enabled"
      marker="$marker EFFECTIVE_HARVEST_RUNNER_MAX_TASKS=$effective_max_tasks"
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
        "CF_CREDENTIALS_FILE": str(credentials_file),
        "CF_RESTART_COUNT_FILE": str(restart_count_file),
        "CF_RESTART_POLL_SECONDS": "0",
        "CF_RESTART_TIMEOUT_SECONDS": "5",
    }
    return env, calls_file, credentials_file


@pytest.mark.parametrize(
    ("action", "enabled", "effective_max_tasks"),
    [("disable", "false", "0"), ("enable", "true", "5")],
)
def test_updates_only_enabled_flag_and_restarts(
    tmp_path, action, enabled, effective_max_tasks
):
    original_credentials = {
        "CF_SERVICE_USER": "service-user",
        "FLASK_APP_SECRET_KEY": "keep-me",
        "HARVEST_RUNNER_MAX_TASKS": "5",
    }
    env, calls_file, credentials_file = _setup_fake_cf(tmp_path, original_credentials)

    result = subprocess.run(
        [str(SCRIPT), action],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(credentials_file.read_text()) == {
        **original_credentials,
        "HARVEST_RUNNER_ENABLED": enabled,
    }
    calls = calls_file.read_text()
    assert (
        "service datagov-harvest-secrets --guid" in calls
        and "update-user-provided-service datagov-harvest-secrets -p " in calls
        and "restart datagov-harvest --strategy rolling" in calls
    )
    assert f"HARVEST_RUNNER_ENABLED changed from <unset> to {enabled}" in result.stdout
    assert (
        f"Confirmed all 2 instance(s) restarted with "
        f"HARVEST_RUNNER_ENABLED={enabled} and effective max tasks "
        f"{effective_max_tasks}."
    ) in result.stdout


def test_enable_uses_default_capacity_without_storing_it(tmp_path):
    env, _, credentials_file = _setup_fake_cf(
        tmp_path, {"FLASK_APP_SECRET_KEY": "keep-me"}
    )

    result = subprocess.run(
        [str(SCRIPT), "enable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(credentials_file.read_text()) == {
        "FLASK_APP_SECRET_KEY": "keep-me",
        "HARVEST_RUNNER_ENABLED": "true",
    }
    assert "effective max tasks 3" in result.stdout


def test_restarts_when_enabled_flag_already_has_desired_value(tmp_path):
    env, calls_file, _ = _setup_fake_cf(
        tmp_path,
        {
            "HARVEST_RUNNER_MAX_TASKS": "5",
            "HARVEST_RUNNER_ENABLED": "false",
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
    assert "changed from false to false" in result.stdout


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


def test_rejects_invalid_configured_capacity_without_updating_service(tmp_path):
    env, calls_file, _ = _setup_fake_cf(tmp_path, {"HARVEST_RUNNER_MAX_TASKS": "0"})

    result = subprocess.run(
        [str(SCRIPT), "disable"],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )

    assert result.returncode == 1
    assert "must be a positive integer" in result.stderr
    calls = calls_file.read_text()
    assert "update-user-provided-service" not in calls
    assert "restart" not in calls


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
