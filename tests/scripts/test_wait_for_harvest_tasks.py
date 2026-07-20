import json
import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "wait_for_harvest_tasks.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_wait_script(tmp_path, *arguments):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    canceled_file = tmp_path / "canceled"

    _write_executable(fake_bin / "apk", "#!/bin/bash\nexit 0\n")
    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
set -euo pipefail

echo "$*" >> "$CF_CALLS_FILE"

if [[ "$1" == "app" ]]; then
  echo "app-guid"
elif [[ "$1" == "curl" && "$2" == /v3/apps/* ]]; then
  if [[ -f "$CF_CANCELED_FILE" ]]; then
    echo '{"resources":[]}'
  else
    echo "$CF_ACTIVE_TASKS"
  fi
elif [[ "$1" == "curl" && "$2" == "-X" && "$3" == "POST" ]]; then
  touch "$CF_CANCELED_FILE"
  echo '{}'
else
  echo "Unexpected cf command: $*" >&2
  exit 1
fi
""",
    )

    active_tasks = {
        "resources": [
            {
                "guid": "harvest-task-guid",
                "name": "harvest-job-123-harvest",
                "state": "RUNNING",
            },
            {
                "guid": "other-task-guid",
                "name": "search-pause-123",
                "state": "RUNNING",
            },
        ]
    }
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "CF_ACTIVE_TASKS": json.dumps(active_tasks),
        "CF_CALLS_FILE": str(calls_file),
        "CF_CANCELED_FILE": str(canceled_file),
        "HARVEST_TASK_FORCE_KILL_TIMEOUT_SECONDS": "0",
        "HARVEST_TASK_POLL_SECONDS": "0",
        "HARVEST_TASK_QUIET_SECONDS": "0",
    }
    result = subprocess.run(
        [str(SCRIPT), "datagov-harvest", *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )
    return result, calls_file.read_text()


def test_timeout_abandons_without_canceling_tasks(tmp_path):
    result, calls = _run_wait_script(tmp_path, "0")

    assert result.returncode == 1
    assert "Timed out waiting for 1 harvest task(s) to finish." in result.stderr
    assert "actions/cancel" not in calls


def test_force_kill_cancels_harvest_tasks_and_waits_until_drained(tmp_path):
    result, calls = _run_wait_script(tmp_path, "7200", "--force-kill-running-jobs")

    assert result.returncode == 0
    assert "Force-kill timeout reached" in result.stdout
    assert "curl -X POST /v3/tasks/harvest-task-guid/actions/cancel" in calls
    assert "/v3/tasks/other-task-guid/actions/cancel" not in calls
    assert "No harvest tasks ran during the 0-second quiet period." in result.stdout
