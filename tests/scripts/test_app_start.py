import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "app-start.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_app_start(
    tmp_path,
    cf_instance_index="0",
    upgrade_exit=0,
    check_exit=0,
):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "calls"

    _write_executable(
        fake_bin / "flask",
        f"""#!/bin/bash
if [[ "$1" == "db" && "$2" == "upgrade" ]]; then
  echo "db upgrade" >> "$CALLS_FILE"
  exit {upgrade_exit}
elif [[ "$1" == "db" && "$2" == "check" ]]; then
  echo "db check" >> "$CALLS_FILE"
  exit {check_exit}
else
  echo "Unexpected flask command: $*" >&2
  exit 1
fi
""",
    )
    _write_executable(
        fake_bin / "newrelic-admin",
        """#!/bin/bash
echo "newrelic-admin $*" >> "$CALLS_FILE"
exit 0
""",
    )

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "CALLS_FILE": str(calls_file),
        "PORT": "8080",
    }
    if cf_instance_index is not None:
        env["CF_INSTANCE_INDEX"] = cf_instance_index
    else:
        env.pop("CF_INSTANCE_INDEX", None)

    result = subprocess.run(
        [str(SCRIPT)],
        capture_output=True,
        env=env,
        text=True,
        timeout=10,
    )
    calls = calls_file.read_text() if calls_file.exists() else ""
    return result, calls


def test_starts_gunicorn_when_migration_and_check_both_pass(tmp_path):
    result, calls = _run_app_start(tmp_path)

    assert result.returncode == 0
    assert "db upgrade" in calls
    assert "db check" in calls
    assert "newrelic-admin" in calls


def test_refuses_to_start_when_schema_drift_detected(tmp_path):
    result, calls = _run_app_start(tmp_path, check_exit=1)

    assert result.returncode == 1
    assert "Schema drift detected" in result.stderr
    assert "db check" in calls
    assert "newrelic-admin" not in calls


def test_non_zero_instance_skips_migration_and_check_entirely(tmp_path):
    result, calls = _run_app_start(
        tmp_path, cf_instance_index="1", upgrade_exit=1, check_exit=1
    )

    assert result.returncode == 0
    assert "db upgrade" not in calls
    assert "db check" not in calls
    assert "newrelic-admin" in calls
