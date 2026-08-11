"""Tests for bin/delete_opensearch_cluster.sh.

This script is the last line of defence before an irreversible delete. Its callers
include a workflow condition deciding "the rebuild failed, so remove the replacement
cluster" -- a piece of YAML that can be wrong. So the tests centre on the guard:
it must refuse whenever an app is actually serving from the instance, and must fail
closed when it cannot tell.
"""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "delete_opensearch_cluster.sh"

TARGET = "datagov-catalog-opensearch-old"
LIVE_HOST = "vpc-live.us-gov-west-1.es.amazonaws.com"
OTHER_HOST = "vpc-other.us-gov-west-1.es.amazonaws.com"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_delete(
    tmp_path,
    *arguments,
    service_exists=True,
    resolved_host=OTHER_HOST,
    instance_host=LIVE_HOST,
    ssh_fails=False,
    bound=True,
):
    """Run the script against a stubbed cf and return (result, cf call log).

    ``resolved_host`` is what each app's .profile resolves as its LIVE cluster;
    ``instance_host`` is the host of the instance being deleted. Equal values mean
    the app is serving from it, which must be refused.
    """
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
echo "$*" >> "$CF_CALLS_FILE"

case "$1" in
  service)
    if [[ "$CF_SERVICE_EXISTS" == "true" ]]; then
      echo "name: $2"
      exit 0
    fi
    echo "not found" >&2
    exit 1
    ;;
  app)
    exit 0
    ;;
  ssh)
    if [[ "$CF_SSH_FAILS" == "true" ]]; then
      exit 1
    fi
    printf 'ok|%s|%s' "$CF_RESOLVED_HOST" "$CF_INSTANCE_HOST"
    ;;
  curl)
    if [[ "$CF_BOUND" == "true" ]]; then
      echo '{"pagination":{"total_results":1}}'
    else
      echo '{"pagination":{"total_results":0}}'
    fi
    ;;
  unbind-service|delete-service)
    exit 0
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
        "CF_SERVICE_EXISTS": "true" if service_exists else "false",
        "CF_RESOLVED_HOST": resolved_host,
        "CF_INSTANCE_HOST": instance_host,
        "CF_SSH_FAILS": "true" if ssh_fails else "false",
        "CF_BOUND": "true" if bound else "false",
    }
    result = subprocess.run(
        [str(SCRIPT), *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=30,
    )
    return result, calls_file.read_text()


def test_refuses_when_an_app_is_serving_from_the_instance(tmp_path):
    """The guard that makes the failure-teardown path safe."""
    result, calls = _run_delete(
        tmp_path,
        TARGET,
        "datagov-harvest",
        resolved_host=LIVE_HOST,
        instance_host=LIVE_HOST,
    )

    assert result.returncode == 1
    assert f"Refusing to delete {TARGET}" in result.stderr
    assert "would take search down" in result.stderr
    assert "delete-service" not in calls
    assert "unbind-service" not in calls


def test_refuses_when_the_container_cannot_be_read(tmp_path):
    """Fails closed: an unreadable container must not be taken as 'unused'."""
    result, calls = _run_delete(tmp_path, TARGET, "datagov-harvest", ssh_fails=True)

    assert result.returncode == 1
    assert "assuming it is in use" in result.stderr
    assert "delete-service" not in calls


def test_deletes_an_unused_instance_after_unbinding_every_app(tmp_path):
    result, calls = _run_delete(
        tmp_path,
        TARGET,
        "datagov-harvest",
        "datagov-catalog",
        resolved_host=OTHER_HOST,
        instance_host=LIVE_HOST,
    )

    assert result.returncode == 0, result.stderr
    lines = calls.strip().splitlines()
    unbinds = [line for line in lines if line.startswith("unbind-service")]
    assert unbinds == [
        f"unbind-service datagov-harvest {TARGET}",
        f"unbind-service datagov-catalog {TARGET}",
    ]
    # cf delete-service fails while bindings exist, so the delete must come last.
    assert lines[-1] == f"delete-service {TARGET} -f --wait"


def test_skips_unbinding_an_app_that_is_not_bound(tmp_path):
    result, calls = _run_delete(tmp_path, TARGET, "datagov-harvest", bound=False)

    assert result.returncode == 0, result.stderr
    assert "unbind-service" not in calls
    assert f"delete-service {TARGET} -f --wait" in calls


def test_missing_instance_is_a_no_op(tmp_path):
    """Idempotent, so a re-dispatched workflow does not fail on an already-done
    delete."""
    result, calls = _run_delete(tmp_path, TARGET, service_exists=False)

    assert result.returncode == 0
    assert "nothing to delete" in result.stdout
    assert "delete-service" not in calls


def test_requires_a_service_name(tmp_path):
    result, _ = _run_delete(tmp_path)

    assert result.returncode == 1
    assert "Usage:" in result.stderr
