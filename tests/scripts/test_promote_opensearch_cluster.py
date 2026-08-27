import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "promote_opensearch_cluster.sh"

NEXT = "datagov-catalog-opensearch-next"
CANONICAL = "datagov-catalog-opensearch"
OLD = f"{CANONICAL}-old"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(
    tmp_path,
    *,
    existing_services=(NEXT, CANONICAL),
    catalog_exists=True,
    catalog_bound=True,
    fail_second_rename=False,
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
  service)
    [[ " $CF_EXISTING_SERVICES " == *" $2 "* ]]
    ;;
  app)
    [[ "$CF_CATALOG_EXISTS" == "true" ]]
    ;;
  curl)
    if [[ "$CF_CATALOG_BOUND" == "true" ]]; then
      echo '{"resources":[{"guid":"binding-guid"}]}'
    else
      echo '{"resources":[]}'
    fi
    ;;
  rename-service)
    if [[ "$2" == "$CF_NEXT" && "$CF_FAIL_SECOND_RENAME" == "true" ]]; then
      exit 1
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
        [str(SCRIPT), NEXT, CANONICAL],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "CF_CALLS_FILE": str(calls_file),
            "CF_EXISTING_SERVICES": " ".join(existing_services),
            "CF_CATALOG_EXISTS": "true" if catalog_exists else "false",
            "CF_CATALOG_BOUND": "true" if catalog_bound else "false",
            "CF_NEXT": NEXT,
            "CF_FAIL_SECOND_RENAME": "true" if fail_second_rename else "false",
        },
        text=True,
        timeout=10,
    )
    return result, calls_file.read_text()


def _renames(calls):
    return [line for line in calls.splitlines() if line.startswith("rename-service")]


def test_renames_live_then_replacement(tmp_path):
    result, calls = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert _renames(calls) == [
        f"rename-service {CANONICAL} {OLD}",
        f"rename-service {NEXT} {CANONICAL}",
    ]


def test_rolls_back_when_the_replacement_rename_fails(tmp_path):
    result, calls = _run(tmp_path, fail_second_rename=True)

    assert result.returncode == 1
    assert _renames(calls) == [
        f"rename-service {CANONICAL} {OLD}",
        f"rename-service {NEXT} {CANONICAL}",
        f"rename-service {OLD} {CANONICAL}",
    ]
    assert "restoring" in result.stderr


def test_refuses_a_stale_retired_cluster(tmp_path):
    result, calls = _run(tmp_path, existing_services=(NEXT, CANONICAL, OLD))

    assert result.returncode == 1
    assert "refusing a partial" in result.stderr
    assert _renames(calls) == []


def test_refuses_an_unbound_catalog(tmp_path):
    result, calls = _run(tmp_path, catalog_bound=False)

    assert result.returncode == 1
    assert "is not bound" in result.stderr
    assert _renames(calls) == []


def test_allows_a_space_without_catalog(tmp_path):
    result, calls = _run(tmp_path, catalog_exists=False)

    assert result.returncode == 0, result.stderr
    assert len(_renames(calls)) == 2
