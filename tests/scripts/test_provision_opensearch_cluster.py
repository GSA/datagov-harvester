import os
import subprocess
from pathlib import Path

BIN = Path(__file__).resolve().parents[2] / "bin"
SCRIPT = BIN / "provision_opensearch_cluster.sh"
PLAN_LIB = BIN / "lib" / "opensearch_plan.sh"

NEXT = "datagov-catalog-opensearch-next"
OLD = "datagov-catalog-opensearch-old"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _plan_for(space):
    result = subprocess.run(
        ["sh", "-c", f'. "{PLAN_LIB}"; opensearch_plan_for_space "{space}"'],
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip()


def _run(tmp_path, *, space="development", existing_services=(), plan=None):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
echo "$*" >> "$CF_CALLS_FILE"
case "$1" in
  target)
    printf 'space: %s\\n' "$CF_SPACE"
    ;;
  service)
    [[ " $CF_EXISTING_SERVICES " == *" $2 "* ]]
    ;;
  create-service)
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    arguments = [str(SCRIPT), NEXT, OLD]
    if plan is not None:
        arguments.append(plan)
    result = subprocess.run(
        arguments,
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "CF_CALLS_FILE": str(calls_file),
            "CF_SPACE": space,
            "CF_EXISTING_SERVICES": " ".join(existing_services),
        },
        text=True,
        timeout=10,
    )
    return result, calls_file.read_text()


def test_plan_matches_each_release_space():
    assert _plan_for("development") == "es-medium"
    assert _plan_for("staging") == "es-medium-ha"
    assert _plan_for("prod") == "es-large"
    assert _plan_for("sandbox") == ""


def test_provisions_the_space_default_plan(tmp_path):
    result, calls = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (
        f"create-service --wait aws-elasticsearch es-medium {NEXT} "
        '-c {"ElasticsearchVersion":"OpenSearch_2.11"}'
    ) in calls


def test_honors_an_explicit_plan(tmp_path):
    result, calls = _run(tmp_path, plan="es-large")

    assert result.returncode == 0, result.stderr
    assert f"create-service --wait aws-elasticsearch es-large {NEXT}" in calls


def test_refuses_an_existing_replacement(tmp_path):
    result, calls = _run(tmp_path, existing_services=(NEXT,))

    assert result.returncode == 1
    assert "already exists" in result.stderr
    assert "create-service" not in calls


def test_refuses_an_existing_retired_cluster(tmp_path):
    result, calls = _run(tmp_path, existing_services=(OLD,))

    assert result.returncode == 1
    assert "has not been cleaned up" in result.stderr
    assert "create-service" not in calls


def test_refuses_an_unknown_space_without_override(tmp_path):
    result, calls = _run(tmp_path, space="sandbox")

    assert result.returncode == 1
    assert "No default OpenSearch plan" in result.stderr
    assert "create-service" not in calls
