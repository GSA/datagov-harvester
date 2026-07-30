"""Tests for bin/promote_opensearch_cluster.sh.

The script's value is entirely in the ORDER of its cf calls -- that order is what
keeps a real, bound instance behind every name an app resolves during the swap. So
these tests assert the exact sequence, not just that each command was issued.
"""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "promote_opensearch_cluster.sh"

# What the script is being asked to do in every test below.
NEXT = "datagov-catalog-opensearch-next"
CANONICAL = "datagov-catalog-opensearch"
RETIRED = f"{CANONICAL}-old"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run_promote(
    tmp_path,
    *arguments,
    existing_services=None,
    bound=True,
    existing_apps=("datagov-harvest", "datagov-catalog"),
    catalog_restart_fails=False,
):
    """Run the script against a stubbed cf and return (result, cf call log).

    ``existing_services`` controls which instance names ``cf service`` reports as
    present; defaults to the replacement and the canonical one, which is the normal
    pre-promotion state. After the rename the stub reports the canonical name as
    present regardless, mirroring the real rename. ``bound`` toggles whether the apps
    are bound to the replacement instance.
    """
    if existing_services is None:
        existing_services = [NEXT, CANONICAL]

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "cf-calls"
    calls_file.touch()

    _write_executable(
        fake_bin / "cf",
        """#!/bin/bash
echo "$*" >> "$CF_CALLS_FILE"

renamed_marker="${CF_CALLS_FILE}.renamed"

case "$1" in
  service)
    # A rename makes the canonical name resolve; mirror that so the script's
    # post-rename check sees what it would really see.
    if [[ -f "$renamed_marker" && "$2" == "$CF_CANONICAL" ]]; then
      echo "name: $2"
      exit 0
    fi
    # $CF_EXISTING_SERVICES is a space-separated list of instance names.
    for existing in $CF_EXISTING_SERVICES; do
      if [[ "$2" == "$existing" ]]; then
        echo "name: $2"
        exit 0
      fi
    done
    echo "Service instance $2 not found" >&2
    exit 1
    ;;
  app)
    for existing in $CF_EXISTING_APPS; do
      if [[ "$2" == "$existing" ]]; then
        exit 0
      fi
    done
    echo "App $2 not found" >&2
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
    echo "OPENSEARCH_SERVICE_NAME: $CF_EXISTING_ENV"
    ;;
  rename-service)
    # Record that the canonical name now exists again.
    if [[ "$3" == "$CF_CANONICAL" ]]; then
      touch "$renamed_marker"
    fi
    exit 0
    ;;
  restart)
    if [[ "$2" == "datagov-catalog" && "$CF_CATALOG_RESTART_FAILS" == "true" ]]; then
      echo "deployment superseded" >&2
      exit 1
    fi
    exit 0
    ;;
  set-env|unset-env)
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
        "CF_EXISTING_SERVICES": " ".join(existing_services),
        "CF_EXISTING_APPS": " ".join(existing_apps),
        "CF_CANONICAL": CANONICAL,
        "CF_EXISTING_ENV": CANONICAL,
        "CF_BOUND": "true" if bound else "false",
        "CF_CATALOG_RESTART_FAILS": "true" if catalog_restart_fails else "false",
        # Skip the real backoff between catalog restart attempts.
        "CATALOG_RESTART_RETRY_SECONDS": "0",
    }
    result = subprocess.run(
        [str(SCRIPT), *arguments],
        capture_output=True,
        env=env,
        text=True,
        timeout=30,
    )
    return result, calls_file.read_text()


def _mutating_calls(calls):
    """The cf calls that actually change state, in order.

    Filters out the read-only pre-flight (`cf service`, `cf curl`, `cf env`) so the
    assertions describe the mutation sequence rather than restating the whole log.
    """
    keep = ("set-env", "unset-env", "rename-service", "restart")
    return [line for line in calls.strip().splitlines() if line.split()[0] in keep]


def test_promote_moves_apps_and_renames_in_the_safe_order(tmp_path):
    result, calls = _run_promote(tmp_path, NEXT, CANONICAL)

    assert result.returncode == 0, result.stderr
    assert _mutating_calls(calls) == [
        # 1. the writer moves first, explicitly, while canonical is still the old one
        f"set-env datagov-harvest OPENSEARCH_SERVICE_NAME {NEXT}",
        "restart datagov-harvest --strategy rolling",
        # 2 & 3. adjacent renames -- the canonical name is unresolvable only between
        # these two commands
        f"rename-service {CANONICAL} {RETIRED}",
        f"rename-service {NEXT} {CANONICAL}",
        # 4. drop the overrides so the harvester resolves canonical via the default
        "unset-env datagov-harvest OPENSEARCH_SERVICE_NAME",
        "unset-env datagov-harvest OPENSEARCH_NEXT_SERVICE_NAME",
        "restart datagov-harvest --strategy rolling",
        # 5. catalog picks up the renamed instance
        "restart datagov-catalog --strategy rolling",
    ]


def test_promote_clears_rather_than_repoints_the_harvester(tmp_path):
    """Step 4 must unset, not set.

    Setting OPENSEARCH_SERVICE_NAME to the replacement's old name would leave the
    harvester pointing at a name that no longer exists after step 3.
    """
    _, calls = _run_promote(tmp_path, NEXT, CANONICAL)

    mutations = _mutating_calls(calls)
    final_env_writes = [
        line for line in mutations if line.startswith(("set-env", "unset-env"))
    ]
    # The only set-env is step 1's temporary pointer; everything after is an unset.
    assert final_env_writes[0].startswith("set-env")
    assert all(line.startswith("unset-env") for line in final_env_writes[1:])
    # And the replacement-cluster pointer is cleared too, so a later
    # `--cluster next` cannot resolve to what is now live.
    assert "unset-env datagov-harvest OPENSEARCH_NEXT_SERVICE_NAME" in mutations


def test_promote_never_deletes_anything(tmp_path):
    """Deletion is a separate, later step so rollback survives the promotion."""
    _, calls = _run_promote(tmp_path, NEXT, CANONICAL)

    assert "delete-service" not in calls
    assert "unbind-service" not in calls


def test_promote_refuses_when_an_app_is_not_bound(tmp_path):
    """Bindings survive renames, so an unbound app stays unbound after step 3 -- and
    then cannot resolve the canonical name at all."""
    result, calls = _run_promote(tmp_path, NEXT, CANONICAL, bound=False)

    assert result.returncode == 1
    assert f"Not bound to {NEXT}" in result.stderr
    assert "cf bind-service" in result.stderr
    # Refused during pre-flight, before touching anything.
    assert _mutating_calls(calls) == []


def test_promote_refuses_when_the_replacement_is_missing(tmp_path):
    result, calls = _run_promote(
        tmp_path, NEXT, CANONICAL, existing_services=[CANONICAL]
    )

    assert result.returncode == 1
    assert f"No service instance named '{NEXT}'" in result.stderr
    assert _mutating_calls(calls) == []


def test_promote_refuses_when_the_retired_name_is_taken(tmp_path):
    """A leftover -old from an earlier migration would make step 2's rename fail
    partway through, after the harvester had already been moved."""
    result, calls = _run_promote(
        tmp_path, NEXT, CANONICAL, existing_services=[NEXT, CANONICAL, RETIRED]
    )

    assert result.returncode == 1
    assert f"'{RETIRED}' already exists" in result.stderr
    assert "delete_opensearch_cluster.sh" in result.stderr
    assert _mutating_calls(calls) == []


def test_promote_refuses_to_promote_a_cluster_onto_itself(tmp_path):
    result, calls = _run_promote(tmp_path, CANONICAL, CANONICAL)

    assert result.returncode == 1
    assert "already been promoted" in result.stderr
    assert _mutating_calls(calls) == []


def test_promote_requires_both_service_names(tmp_path):
    result, _ = _run_promote(tmp_path, NEXT)

    assert result.returncode == 1
    assert "Usage:" in result.stderr


def test_promote_skips_a_catalog_that_is_not_in_the_space(tmp_path):
    """Detected during pre-flight, not at the restart -- which happens after the
    renames, where a failure is far more awkward."""
    result, calls = _run_promote(
        tmp_path, NEXT, CANONICAL, existing_apps=("datagov-harvest",)
    )

    assert result.returncode == 0, result.stderr
    assert "no app named 'datagov-catalog'" in result.stderr
    assert "restart datagov-catalog" not in calls
    # The harvester half still completed.
    assert f"rename-service {NEXT} {CANONICAL}" in calls


def test_promote_refuses_when_the_harvester_is_missing(tmp_path):
    result, calls = _run_promote(
        tmp_path, NEXT, CANONICAL, existing_apps=("datagov-catalog",)
    )

    assert result.returncode == 1
    assert "No app named 'datagov-harvest'" in result.stderr
    assert _mutating_calls(calls) == []


def test_promote_tolerates_a_catalog_restart_losing_to_its_cron(tmp_path):
    """Catalog's own 15-minute restart cron can supersede this deployment, which makes
    cf restart return non-zero even though the migration is complete. The rename has
    already landed, so catalog converges on its own; failing here would report a broken
    migration that is fine."""
    result, calls = _run_promote(tmp_path, NEXT, CANONICAL, catalog_restart_fails=True)

    assert result.returncode == 0, result.stderr
    # Retried, then warned rather than failed.
    assert calls.count("restart datagov-catalog --strategy rolling") == 3
    assert "could not roll datagov-catalog" in result.stderr
    assert "report_opensearch_cluster.sh" in result.stderr
