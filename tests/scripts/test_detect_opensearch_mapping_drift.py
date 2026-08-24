import os
import subprocess
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2] / "bin" / "detect_opensearch_mapping_drift.sh"
)

HARVEST_APP = "datagov-harvest"
EXPECTED = "a" * 64
ENV_VAR = "OPENSEARCH_MAPPING_FINGERPRINT"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(
    tmp_path,
    *,
    recorded=EXPECTED,
    app_exists=True,
    fail_env_read=False,
    args=(EXPECTED, HARVEST_APP),
):
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
    if [[ "$CF_APP_EXISTS" != "true" ]]; then
      exit 1
    fi
    if [[ "${3:-}" == "--guid" ]]; then
      echo harvest-guid
    fi
    ;;
  curl)
    if [[ "$CF_FAIL_ENV_READ" == "true" ]]; then
      exit 1
    fi
    if [[ -n "$CF_RECORDED" ]]; then
      printf '{"var":{"%s":"%s"}}\\n' "$CF_ENV_VAR" "$CF_RECORDED"
    else
      echo '{"var":{}}'
    fi
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
            "GITHUB_WORKSPACE": str(tmp_path),
            "CF_CALLS_FILE": str(calls_file),
            "CF_APP_EXISTS": "true" if app_exists else "false",
            "CF_FAIL_ENV_READ": "true" if fail_env_read else "false",
            "CF_RECORDED": recorded,
            "CF_ENV_VAR": ENV_VAR,
        },
        text=True,
        timeout=10,
    )
    sentinel = tmp_path / ".opensearch_mapping_drift"
    return result, calls_file.read_text(), sentinel


def _set_env_calls(calls):
    return [line for line in calls.splitlines() if line.startswith("set-env")]


def test_matching_fingerprint_requires_no_rebuild(tmp_path):
    result, calls, sentinel = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert not sentinel.exists()
    assert _set_env_calls(calls) == []
    assert "no rebuild required" in result.stdout


def test_differing_fingerprint_requests_a_rebuild(tmp_path):
    result, _, sentinel = _run(tmp_path, recorded="b" * 64)

    assert result.returncode == 0, result.stderr
    assert sentinel.exists()
    assert "b" * 64 in sentinel.read_text()
    assert EXPECTED in sentinel.read_text()


def test_unrecorded_fingerprint_adopts_the_live_index(tmp_path):
    """A space with no recorded value must not trigger a needless rebuild."""
    result, calls, sentinel = _run(tmp_path, recorded="")

    assert result.returncode == 0, result.stderr
    assert not sentinel.exists()
    assert _set_env_calls(calls) == [f"set-env {HARVEST_APP} {ENV_VAR} {EXPECTED}"]


def test_a_stale_sentinel_is_cleared(tmp_path):
    (tmp_path / ".opensearch_mapping_drift").write_text("from an earlier space\n")

    _, _, sentinel = _run(tmp_path)

    assert not sentinel.exists()


def test_missing_app_fails(tmp_path):
    result, _, sentinel = _run(tmp_path, app_exists=False)

    assert result.returncode == 1
    assert not sentinel.exists()
    assert "No datagov-harvest app" in result.stderr


def test_unreadable_environment_fails_rather_than_assuming_no_drift(tmp_path):
    result, _, sentinel = _run(tmp_path, fail_env_read=True)

    assert result.returncode == 1
    assert not sentinel.exists()
    assert "Unable to read environment variables" in result.stderr


def test_missing_fingerprint_argument_prints_usage(tmp_path):
    result, _, _ = _run(tmp_path, args=())

    assert result.returncode == 1
    assert "Usage:" in result.stderr
