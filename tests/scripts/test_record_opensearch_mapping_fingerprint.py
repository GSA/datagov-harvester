import os
import subprocess
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "bin"
    / "record_opensearch_mapping_fingerprint.sh"
)

HARVEST_APP = "datagov-harvest"
FINGERPRINT = "c" * 64
ENV_VAR = "OPENSEARCH_MAPPING_FINGERPRINT"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(tmp_path, *, app_exists=True, args=(FINGERPRINT, HARVEST_APP)):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
echo "$*" >> "$CF_CALLS_FILE"
case "$1" in
  app)
    [[ "$CF_APP_EXISTS" == "true" ]]
    ;;
  set-env)
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    result = subprocess.run(
        [str(SCRIPT), *args],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "CF_CALLS_FILE": str(calls_file),
            "CF_APP_EXISTS": "true" if app_exists else "false",
        },
        text=True,
        timeout=10,
    )
    return result, calls_file.read_text()


def test_records_the_fingerprint_on_the_harvester_app(tmp_path):
    result, calls = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert f"set-env {HARVEST_APP} {ENV_VAR} {FINGERPRINT}" in calls.splitlines()


def test_missing_app_fails(tmp_path):
    result, calls = _run(tmp_path, app_exists=False)

    assert result.returncode == 1
    assert "No datagov-harvest app" in result.stderr
    assert "set-env" not in calls


def test_missing_fingerprint_argument_prints_usage(tmp_path):
    result, calls = _run(tmp_path, args=())

    assert result.returncode == 1
    assert "Usage:" in result.stderr
    assert "set-env" not in calls
