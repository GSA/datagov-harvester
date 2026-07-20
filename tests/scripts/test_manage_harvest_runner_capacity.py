import json
import os
import subprocess
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2] / "bin" / "manage_harvest_runner_capacity.sh"
)


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _setup_fake_cf(tmp_path, initial_environment):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    environment_file = tmp_path / "cf-environment"
    restart_count_file = tmp_path / "cf-restart-count"
    environment_file.write_text(
        json.dumps({"environment_variables": initial_environment})
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
    printf '{"environment_variables":{"%s":"%s"}}\\n' "$3" "$4" > "$CF_ENVIRONMENT_FILE"
    ;;
  unset-env)
    echo '{"environment_variables":{}}' > "$CF_ENVIRONMENT_FILE"
    ;;
  restart)
    restart_count=$(<"$CF_RESTART_COUNT_FILE")
    if [[ "${CF_KEEP_INSTANCE_GUIDS:-false}" != "true" ]]; then
      echo "$((restart_count + 1))" > "$CF_RESTART_COUNT_FILE"
    fi
    ;;
  curl)
    case "$2" in
      /v3/apps/app-guid/env)
        printf '%s\\n' "$(<"$CF_ENVIRONMENT_FILE")"
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


def _run(action, state_file, env):
    return subprocess.run(
        [str(SCRIPT), action, "datagov-harvest", str(state_file)],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )


def test_pause_and_restore_existing_capacity(tmp_path):
    env, calls_file, environment_file = _setup_fake_cf(
        tmp_path, {"HARVEST_RUNNER_MAX_TASKS": "3"}
    )
    state_file = tmp_path / "capacity-state.json"

    pause_result = _run("pause", state_file, env)

    assert pause_result.returncode == 0
    assert json.loads(state_file.read_text()) == {"was_set": True, "value": "3"}
    assert json.loads(environment_file.read_text())["environment_variables"] == {
        "HARVEST_RUNNER_MAX_TASKS": "0"
    }
    assert (
        "Confirmed all 2 web instance(s) were replaced and are running."
        in pause_result.stdout
    )

    restore_result = _run("restore", state_file, env)

    assert restore_result.returncode == 0
    assert not state_file.exists()
    assert json.loads(environment_file.read_text())["environment_variables"] == {
        "HARVEST_RUNNER_MAX_TASKS": "3"
    }
    calls = calls_file.read_text()
    assert "set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS 0" in calls
    assert "set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS 3" in calls
    assert calls.count("restart datagov-harvest --strategy rolling") == 2


def test_restore_unsets_capacity_when_it_was_initially_absent(tmp_path):
    env, calls_file, environment_file = _setup_fake_cf(tmp_path, {})
    state_file = tmp_path / "capacity-state.json"

    assert _run("pause", state_file, env).returncode == 0
    assert _run("restore", state_file, env).returncode == 0

    assert json.loads(environment_file.read_text())["environment_variables"] == {}
    assert (
        "unset-env datagov-harvest HARVEST_RUNNER_MAX_TASKS" in calls_file.read_text()
    )


def test_pause_refuses_to_overwrite_an_existing_zero_capacity(tmp_path):
    env, calls_file, _ = _setup_fake_cf(tmp_path, {"HARVEST_RUNNER_MAX_TASKS": "0"})
    state_file = tmp_path / "capacity-state.json"

    result = _run("pause", state_file, env)

    assert result.returncode == 1
    assert "positive integer" in result.stderr
    assert not state_file.exists()
    calls = calls_file.read_text()
    assert "set-env" not in calls
    assert "restart" not in calls


def test_pause_requires_every_instance_guid_to_change(tmp_path):
    env, _, _ = _setup_fake_cf(tmp_path, {"HARVEST_RUNNER_MAX_TASKS": "3"})
    env["CF_KEEP_INSTANCE_GUIDS"] = "true"
    env["CF_RESTART_TIMEOUT_SECONDS"] = "0"
    state_file = tmp_path / "capacity-state.json"

    result = _run("pause", state_file, env)

    assert result.returncode == 1
    assert "Timed out confirming the rolling restart" in result.stderr
    assert "reused_instance_guids=2" in result.stderr
