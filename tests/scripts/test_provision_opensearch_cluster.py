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
FAKE_HOST = "vpc-real.us-gov-west-1.es.amazonaws.com"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_provision(
    tmp_path,
    *arguments,
    space="development",
    service_exists=False,
    bound=False,
    empty_host_binds=0,
):
    """Run the script against a fake `cf`.

    ``empty_host_binds`` is how many binding-detail reads return an empty ``host``
    before one returns a real endpoint -- the aws-broker race this script has to
    survive. 0 means the endpoint is ready immediately; a number larger than the
    script's retry budget makes it give up.
    """
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()
    # Counts binding-detail reads across `cf` invocations, which are separate
    # processes, so the fake needs somewhere on disk to keep the tally.
    detail_reads_file = tmp_path / "detail-reads"
    detail_reads_file.write_text("0")

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
    case "$2" in
      */details)
        # The binding's credentials. Return an empty host for the first
        # $CF_EMPTY_HOST_BINDS reads, then a real endpoint -- the broker race.
        reads=$(cat "$CF_DETAIL_READS_FILE")
        echo $((reads + 1)) > "$CF_DETAIL_READS_FILE"
        if [[ $reads -lt $CF_EMPTY_HOST_BINDS ]]; then
          host=""
        else
          host="$FAKE_HOST"
        fi
        printf '{"credentials":{"host":"%s","access_key":"AK"}}\\n' "$host"
        ;;
      *service_credential_bindings*)
        # The binding lookup: used both to decide whether to bind and to find the
        # guid for the details read above.
        if [[ "$CF_BOUND" == "true" ]]; then
          total=1
        else
          total=0
        fi
        printf '{"pagination":{"total_results":%s},' "$total"
        printf '"resources":[{"guid":"binding-guid"}]}\\n'
        ;;
      *)
        echo "Unexpected cf curl path: $2" >&2
        exit 1
        ;;
    esac
    ;;
  env)
    echo "OPENSEARCH_NEXT_SERVICE_NAME: $CF_EXISTING_ENV"
    ;;
  create-service|bind-service|unbind-service|set-env|restart)
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
        "CF_DETAIL_READS_FILE": str(detail_reads_file),
        "CF_EMPTY_HOST_BINDS": str(empty_host_binds),
        "FAKE_HOST": FAKE_HOST,
        # Keep the rebind loop instant; the timing is not what these tests pin.
        "BIND_HOST_RETRY_SECONDS": "0",
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


def test_provision_refuses_an_instance_that_already_exists(tmp_path):
    """Two rebuilds writing into one cluster interleave into a corrupt index.

    The replacement's name is fixed, so an existing `-next` is a migration in
    flight or the wreckage of one -- never something to adopt. Refusing before
    `cf create-service` is the cheapest stop: nothing provisioned, nothing
    billing, live cluster untouched. `start_at=rebuild` is how a genuine
    half-finished migration resumes.
    """
    result, calls = _run_provision(tmp_path, NEXT, service_exists=True)

    assert result.returncode == 1
    assert "already exists" in result.stderr
    assert "start_at=rebuild" in result.stderr
    # Nothing was provisioned, bound, or otherwise touched.
    assert "create-service" not in calls
    assert "bind-service" not in calls


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


def test_provision_needs_no_env_var_and_no_restart(tmp_path):
    """Binding is the entire handoff.

    .profile derives the replacement's name as "<canonical>-next", and the rebuild
    runs via `cf run-task` -- a fresh container that reads current bindings. So no
    `cf set-env` is needed, and no restart: verified in staging 2026-08-10, where a
    task resolved the replacement with the env var unset and no restart performed.

    Not restarting is the point, not an omission. This was the only part of the
    build phase that touched a serving app, and it cost ~70s per run to publish a
    variable nothing read.
    """
    result, calls = _run_provision(tmp_path, NEXT)

    assert result.returncode == 0, result.stderr
    assert "set-env" not in calls
    assert "restart" not in calls


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


def test_provision_rebinds_when_the_binding_has_an_empty_host(tmp_path):
    """The aws-broker race: `cf create-service --wait` returns and the instance
    reports ready before the cluster endpoint exists, so a bind in that window
    stores host="" permanently. Only a rebind re-reads the broker, so the script
    must rebind rather than trust the first bind. Observed in staging 2026-08-10."""
    result, calls = _run_provision(tmp_path, NEXT, empty_host_binds=1)

    assert result.returncode == 0, result.stderr
    assert f"unbind-service datagov-harvest {NEXT}" in calls
    assert "empty host" in result.stdout
    # And it confirms the endpoint it ended up with, rather than assuming.
    assert FAKE_HOST in result.stdout


def test_provision_fails_when_the_host_never_appears(tmp_path):
    """A clean, resumable stop while the live cluster is still untouched beats
    failing later in the rebuild with a confusing credentials error."""
    result, calls = _run_provision(tmp_path, NEXT, empty_host_binds=99)

    assert result.returncode == 1
    assert "no host after" in result.stderr
    # Must not proceed to expose a cluster the harvester cannot reach.
    assert "set-env" not in calls
    assert "restart" not in calls


def test_provision_does_not_rebind_when_the_host_is_present(tmp_path):
    """The happy path must stay a single bind -- no gratuitous unbind/rebind."""
    result, calls = _run_provision(tmp_path, NEXT)

    assert result.returncode == 0, result.stderr
    assert "unbind-service" not in calls


def test_provision_verifies_the_host_even_when_already_bound(tmp_path):
    """A binding left behind by an earlier failed run can itself hold the empty
    host, so the check cannot be skipped just because a binding exists."""
    result, calls = _run_provision(tmp_path, NEXT, bound=True, empty_host_binds=1)

    assert result.returncode == 0, result.stderr
    assert "bind-service" in calls
    assert "empty host" in result.stdout
