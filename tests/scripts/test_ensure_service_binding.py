import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "ensure_service_binding.sh"
SERVICE = "datagov-catalog-opensearch-next"
APP = "datagov-harvest-next"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(tmp_path, *, app_exists=True, initially_bound=False, empty_reads=0):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()
    state_file = tmp_path / "state"
    state_file.write_text(f"bound={1 if initially_bound else 0}\nreads=0\n")

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
echo "$*" >> "$CF_CALLS_FILE"
source "$CF_STATE_FILE"
case "$1" in
  app)
    [[ "$CF_APP_EXISTS" == "true" ]]
    ;;
  bind-service)
    sed -i.bak 's/^bound=.*/bound=1/' "$CF_STATE_FILE"
    ;;
  unbind-service)
    sed -i.bak 's/^bound=.*/bound=0/' "$CF_STATE_FILE"
    ;;
  curl)
    if [[ "$2" == */details ]]; then
      if [[ $reads -lt $CF_EMPTY_READS ]]; then
        host=""
      else
        host="opensearch-next.example"
      fi
      sed -i.bak "s/^reads=.*/reads=$((reads + 1))/" "$CF_STATE_FILE"
      printf '{"credentials":{"host":"%s"}}\\n' "$host"
    elif [[ $bound -eq 1 ]]; then
      echo '{"resources":[{"guid":"binding-guid"}]}'
    else
      echo '{"resources":[]}'
    fi
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    result = subprocess.run(
        [str(SCRIPT), SERVICE, APP],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "CF_CALLS_FILE": str(calls_file),
            "CF_STATE_FILE": str(state_file),
            "CF_APP_EXISTS": "true" if app_exists else "false",
            "CF_EMPTY_READS": str(empty_reads),
            "BIND_HOST_RETRY_SECONDS": "0",
            "BIND_HOST_MAX_ATTEMPTS": "2",
        },
        text=True,
        timeout=10,
    )
    return result, calls_file.read_text()


def test_binds_and_verifies_the_host(tmp_path):
    result, calls = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert f"bind-service {APP} {SERVICE}" in calls
    assert "opensearch-next.example" in result.stdout


def test_rebinds_an_empty_host(tmp_path):
    result, calls = _run(tmp_path, initially_bound=True, empty_reads=1)

    assert result.returncode == 0, result.stderr
    assert f"unbind-service {APP} {SERVICE}" in calls
    assert "binding has an empty host" in result.stdout


def test_fails_when_the_host_never_appears(tmp_path):
    result, _ = _run(tmp_path, initially_bound=True, empty_reads=99)

    assert result.returncode == 1
    assert "no OpenSearch host" in result.stderr


def test_fails_for_a_missing_app(tmp_path):
    result, calls = _run(tmp_path, app_exists=False)

    assert result.returncode == 1
    assert "No app named" in result.stderr
    assert "bind-service" not in calls
