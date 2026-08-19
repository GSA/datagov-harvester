import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "restart_catalog.sh"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(tmp_path, *, app_exists=True, restart_succeeds=True, active_deployments=0):
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
    [[ "$CF_APP_EXISTS" == "true" ]] || exit 1
    [[ "${3:-}" == "--guid" ]] && echo catalog-guid
    exit 0
    ;;
  restart)
    [[ "$CF_RESTART_SUCCEEDS" == "true" ]]
    ;;
  curl)
    printf '{"resources":['
    for ((i=0; i<CF_ACTIVE_DEPLOYMENTS; i++)); do
      [[ $i -gt 0 ]] && printf ','
      printf '{}'
    done
    echo ']}'
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    result = subprocess.run(
        [str(SCRIPT)],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "CF_CALLS_FILE": str(calls_file),
            "CF_APP_EXISTS": "true" if app_exists else "false",
            "CF_RESTART_SUCCEEDS": "true" if restart_succeeds else "false",
            "CF_ACTIVE_DEPLOYMENTS": str(active_deployments),
        },
        text=True,
        timeout=10,
    )
    return result, calls_file.read_text()


def test_completes_a_rolling_restart(tmp_path):
    result, calls = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "restart datagov-catalog --strategy rolling" in calls


def test_accepts_an_existing_catalog_deployment(tmp_path):
    result, _ = _run(tmp_path, restart_succeeds=False, active_deployments=1)

    assert result.returncode == 0
    assert "already has an active deployment" in result.stderr


def test_fails_when_restart_does_not_continue(tmp_path):
    result, _ = _run(tmp_path, restart_succeeds=False)

    assert result.returncode == 1
    assert "no active deployment" in result.stderr


def test_skips_a_missing_catalog(tmp_path):
    result, calls = _run(tmp_path, app_exists=False)

    assert result.returncode == 0
    assert "restart " not in calls
