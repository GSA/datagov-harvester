import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "cleanup_opensearch_cluster.sh"
OLD = "datagov-catalog-opensearch-old"


def _write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def _run(
    tmp_path,
    *,
    service_exists=True,
    active_harvest_tasks=0,
    active_catalog_deployments=0,
    bound=True,
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
    [[ "$CF_SERVICE_EXISTS" == "true" ]]
    ;;
  app)
    if [[ "${3:-}" == "--guid" ]]; then
      [[ "$2" == "datagov-harvest" ]] && echo harvest-guid || echo catalog-guid
    fi
    ;;
  curl)
    if [[ "$2" == *"/tasks?"* ]]; then
      printf '{"resources":['
      for ((i=0; i<CF_ACTIVE_HARVEST_TASKS; i++)); do
        [[ $i -gt 0 ]] && printf ','
        printf '{"name":"harvest-job-%s","state":"RUNNING"}' "$i"
      done
      echo ']}'
    elif [[ "$2" == *"/deployments?"* ]]; then
      printf '{"resources":['
      for ((i=0; i<CF_ACTIVE_CATALOG_DEPLOYMENTS; i++)); do
        [[ $i -gt 0 ]] && printf ','
        printf '{}'
      done
      echo ']}'
    elif [[ "$CF_BOUND" == "true" ]]; then
      echo '{"resources":[{"guid":"binding-guid"}]}'
    else
      echo '{"resources":[]}'
    fi
    ;;
  unbind-service|delete-service)
    ;;
  *)
    echo "Unexpected cf command: $*" >&2
    exit 1
    ;;
esac
""",
    )

    result = subprocess.run(
        [str(SCRIPT), OLD],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "CF_CALLS_FILE": str(calls_file),
            "CF_SERVICE_EXISTS": "true" if service_exists else "false",
            "CF_ACTIVE_HARVEST_TASKS": str(active_harvest_tasks),
            "CF_ACTIVE_CATALOG_DEPLOYMENTS": str(active_catalog_deployments),
            "CF_BOUND": "true" if bound else "false",
            "GITHUB_WORKSPACE": str(tmp_path),
        },
        text=True,
        timeout=10,
    )
    marker = tmp_path / ".opensearch_cleanup_required"
    return result, calls_file.read_text(), marker


def test_unbinds_consumers_then_deletes_the_retired_cluster(tmp_path):
    result, calls, marker = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (
        f"unbind-service datagov-harvest {OLD}" in calls
        and f"unbind-service datagov-catalog {OLD}" in calls
    )
    assert f"delete-service {OLD} -f --wait" in calls
    assert not marker.exists()


def test_retains_cluster_for_active_harvest_tasks(tmp_path):
    result, calls, marker = _run(tmp_path, active_harvest_tasks=2)

    assert result.returncode == 0
    assert "delete-service" not in calls
    assert "2 harvest task(s)" in marker.read_text()


def test_retains_cluster_for_active_catalog_deployment(tmp_path):
    result, calls, marker = _run(tmp_path, active_catalog_deployments=1)

    assert result.returncode == 0
    assert "delete-service" not in calls
    assert "active deployment" in marker.read_text()


def test_missing_service_is_already_clean(tmp_path):
    result, calls, marker = _run(tmp_path, service_exists=False)

    assert result.returncode == 0
    assert "delete-service" not in calls
    assert not marker.exists()
