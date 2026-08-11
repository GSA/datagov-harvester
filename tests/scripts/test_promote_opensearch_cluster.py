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
    harvester_env=CANONICAL,
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
        "CF_EXISTING_ENV": harvester_env,
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
        # 1 & 2. adjacent renames -- the canonical name is unresolvable only between
        # these two commands, and only for new container starts
        f"rename-service {CANONICAL} {RETIRED}",
        f"rename-service {NEXT} {CANONICAL}",
        # 3. the writer re-resolves first, so no write lands on the old cluster
        "restart datagov-harvest --strategy rolling",
        # 4. then the reader
        "restart datagov-catalog --strategy rolling",
    ]
    # No fifth step: the replacement pointer retires itself. .profile derives the
    # name as "<canonical>-next", and step 2 renamed that instance away, so there is
    # no longer anything by that name to resolve.


def test_promote_never_repoints_an_app_with_set_env(tmp_path):
    """Both apps resolve the canonical name, so the rename alone moves them.

    An earlier version set OPENSEARCH_SERVICE_NAME=<next> on the harvester before the
    renames and unset it after. The two cancelled out, and in between the variable
    named an instance the rename had just removed -- so any container start in that
    span failed .profile's empty-host guard. There is no reason to write that variable
    here at all.
    """
    _, calls = _run_promote(tmp_path, NEXT, CANONICAL)

    # Match on whole commands: the substring "set-env" also appears in "unset-env".
    commands = [line.split()[0] for line in calls.strip().splitlines()]
    assert "set-env" not in commands
    # OPENSEARCH_SERVICE_NAME is neither set nor unset -- it is simply left alone.
    assert "OPENSEARCH_SERVICE_NAME" not in calls.replace(
        "OPENSEARCH_NEXT_SERVICE_NAME", ""
    )


def test_promote_restarts_both_apps_after_the_rename(tmp_path):
    """The restarts are mandatory, not cosmetic.

    A rename is metadata-only and .profile resolves the host once at container start,
    so a running app keeps using the endpoint it captured at boot and would stay on the
    old cluster indefinitely without a restart.
    """
    _, calls = _run_promote(tmp_path, NEXT, CANONICAL)

    mutations = _mutating_calls(calls)
    last_rename = max(
        i for i, line in enumerate(mutations) if line.startswith("rename-service")
    )
    restarts = [i for i, line in enumerate(mutations) if line.startswith("restart")]
    assert restarts, "both apps must be restarted"
    assert min(restarts) > last_rename, "restarts must follow the renames"
    # Blocking rolling restarts: a failed start fails the step rather than silently
    # leaving an app on the old cluster.
    assert calls.count("--strategy rolling") >= 2
    assert "--no-wait" not in calls


def test_promote_leaves_no_replacement_pointer_to_clear(tmp_path):
    """The rename retires the replacement pointer by itself.

    This used to `cf unset-env OPENSEARCH_NEXT_SERVICE_NAME`, because that variable
    still named the instance step 2 had just renamed to canonical -- leaving `next`
    and `live` on one host. With the name derived as "<canonical>-next", step 2
    removes the only instance that could match, so `--cluster next` correctly finds
    nothing bound and there is no state left to tidy.
    """
    result, calls = _run_promote(tmp_path, NEXT, CANONICAL)

    assert result.returncode == 0, result.stderr
    # The last mutation is the final restart -- no housekeeping tail.
    assert _mutating_calls(calls)[-1] == "restart datagov-catalog --strategy rolling"
    assert "unset-env" not in calls


def test_promote_refuses_when_the_harvester_is_pinned_elsewhere(tmp_path):
    """A leftover override would survive this script and keep the app pinned to a name
    the rename is about to move."""
    result, calls = _run_promote(
        tmp_path, NEXT, CANONICAL, harvester_env="some-other-cluster"
    )

    assert result.returncode == 1
    assert "pinned to 'some-other-cluster'" in result.stderr
    assert "cf unset-env datagov-harvest OPENSEARCH_SERVICE_NAME" in result.stderr
    assert _mutating_calls(calls) == []


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
