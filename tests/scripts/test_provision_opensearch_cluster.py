"""Tests for bin/provision_opensearch_cluster.sh and bin/lib/opensearch_plan.sh.

Provisioning is the expensive, slow stage (AWS quotes 15-30 minutes per node), so the
properties worth pinning are that it is idempotent -- a re-dispatched workflow must not
pay for it twice -- and that it never silently picks the wrong plan.
"""

import os
import subprocess
from pathlib import Path

BIN = Path(__file__).resolve().parents[2] / "bin"
SCRIPT = BIN / "provision_opensearch_cluster.sh"
PLAN_LIB = BIN / "lib" / "opensearch_plan.sh"

NEXT = "datagov-catalog-opensearch-next"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_provision(
    tmp_path, *arguments, space="development", service_exists=False, bound=False
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
  target)
    echo "api endpoint:   https://api.fr.cloud.gov"
    echo "org:            gsa-datagov"
    echo "space:          $CF_SPACE_NAME"
    ;;
  service)
    if [[ "$CF_SERVICE_EXISTS" == "true" ]]; then
      echo "name: $2"
      exit 0
    fi
    echo "not found" >&2
    exit 1
    ;;
  curl)
    if [[ "$CF_BOUND" == "true" ]]; then
      echo '{"pagination":{"total_results":1}}'
    else
      echo '{"pagination":{"total_results":0}}'
    fi
    ;;
  env)
    echo "OPENSEARCH_NEXT_SERVICE_NAME: $CF_EXISTING_ENV"
    ;;
  create-service|bind-service|set-env|restart)
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
        "CF_SPACE_NAME": space,
        "CF_SERVICE_EXISTS": "true" if service_exists else "false",
        "CF_BOUND": "true" if bound else "false",
        "CF_EXISTING_ENV": "",
    }
    result = subprocess.run(
        [str(SCRIPT), *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=30,
    )
    return result, calls_file.read_text()


def _plan_for(space):
    """Ask the shared plan lib what plan a space gets."""
    result = subprocess.run(
        ["sh", "-c", f'. "{PLAN_LIB}"; opensearch_plan_for_space "{space}"'],
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip()


def test_plan_lib_matches_the_documented_plan_per_space():
    assert _plan_for("prod") == "es-large"
    assert _plan_for("staging") == "es-medium-ha"
    assert _plan_for("development") == "es-medium"


def test_plan_lib_gives_an_unrecognized_space_no_plan():
    """So a sandbox space never provisions a multi-node cluster by accident."""
    assert _plan_for("some-sandbox") == ""


def test_provision_creates_the_instance_with_the_space_default_plan(tmp_path):
    result, calls = _run_provision(tmp_path, NEXT)

    assert result.returncode == 0, result.stderr
    assert (
        f"create-service --wait aws-elasticsearch es-medium {NEXT} "
        '-c {"ElasticsearchVersion":"OpenSearch_2.11"}' in calls
    )


def test_provision_honours_an_explicit_plan_override(tmp_path):
    """Resizing is the main reason to migrate, and the broker cannot resize in place."""
    result, calls = _run_provision(tmp_path, NEXT, "es-large")

    assert result.returncode == 0, result.stderr
    assert f"create-service --wait aws-elasticsearch es-large {NEXT}" in calls


def test_provision_refuses_an_unrecognized_space_without_an_explicit_plan(tmp_path):
    result, calls = _run_provision(tmp_path, NEXT, space="some-sandbox")

    assert result.returncode == 1
    assert "No default OpenSearch plan" in result.stderr
    assert "create-service" not in calls


def test_provision_is_idempotent_when_the_instance_exists(tmp_path):
    """A re-dispatched workflow must not pay for provisioning twice."""
    result, calls = _run_provision(tmp_path, NEXT, service_exists=True)

    assert result.returncode == 0, result.stderr
    assert "create-service" not in calls
    assert "already exists" in result.stdout


def test_provision_binds_both_consumers(tmp_path):
    """Catalog must be bound too: after the promotion rename it has to resolve this
    instance under the canonical name."""
    result, calls = _run_provision(tmp_path, NEXT)

    assert result.returncode == 0, result.stderr
    assert f"bind-service datagov-harvest {NEXT}" in calls
    assert f"bind-service datagov-catalog {NEXT}" in calls


def test_provision_skips_binding_apps_that_are_already_bound(tmp_path):
    result, calls = _run_provision(tmp_path, NEXT, bound=True)

    assert result.returncode == 0, result.stderr
    assert "bind-service" not in calls
    assert "already bound" in result.stdout


def test_provision_exposes_the_replacement_to_the_harvester_and_rolls_it(tmp_path):
    """Without this the rebuild cannot resolve the replacement cluster at all."""
    result, calls = _run_provision(tmp_path, NEXT)

    assert result.returncode == 0, result.stderr
    assert f"set-env datagov-harvest OPENSEARCH_NEXT_SERVICE_NAME {NEXT}" in calls
    # Blocking rolling restart: no --no-wait, so a failed start fails the step.
    assert "restart datagov-harvest --strategy rolling" in calls
    assert "--no-wait" not in calls


def test_provision_never_touches_the_live_pointer(tmp_path):
    """Provisioning must leave both apps serving the live cluster."""
    result, calls = _run_provision(tmp_path, NEXT)

    assert result.returncode == 0, result.stderr
    assert "OPENSEARCH_SERVICE_NAME" not in calls.replace(
        "OPENSEARCH_NEXT_SERVICE_NAME", ""
    )
    assert "rename-service" not in calls
    assert "delete-service" not in calls
    assert "restart datagov-catalog" not in calls


def test_provision_requires_a_service_name(tmp_path):
    result, _ = _run_provision(tmp_path)

    assert result.returncode == 1
    assert "Usage:" in result.stderr
