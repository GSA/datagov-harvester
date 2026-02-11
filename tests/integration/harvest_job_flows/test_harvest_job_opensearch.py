import os
import time

import pytest

from database.models import Dataset
from harvester.harvest import harvest_job_starter
from harvester.opensearch import OpenSearchInterface


def _wait_for_opensearch(client, timeout_seconds=30):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if client.ping():
                return True
        except Exception:
            time.sleep(1)
    return False


@pytest.fixture()
def opensearch_client(monkeypatch):
    explicit_host = os.getenv("OPENSEARCH_TEST_HOST") or os.getenv("OPENSEARCH_HOST")
    if explicit_host:
        candidates = [explicit_host]
    else:
        candidates = ["opensearch", "localhost"]

    last_error = None
    for host in candidates:
        monkeypatch.setenv("OPENSEARCH_HOST", host)
        try:
            iface = OpenSearchInterface.from_environment()
        except Exception as exc:
            last_error = exc
            continue

        if _wait_for_opensearch(iface.client):
            return iface

        last_error = AssertionError(f"OpenSearch not reachable at {host}:9200")

    raise AssertionError(
        "OpenSearch unavailable. Tried: "
        f"{', '.join(candidates)}. "
        "Set OPENSEARCH_HOST (or OPENSEARCH_TEST_HOST) to a reachable host, "
        "or start the docker service."
    ) from last_error


class TestHarvestJobOpenSearch:
    def test_opensearch_index_on_create_and_delete(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        opensearch_client,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        harvest_job_starter(harvest_job.id, harvest_job.job_type)

        datasets = interface.db.query(Dataset).all()
        assert datasets
        dataset_ids = [str(dataset.id) for dataset in datasets]

        for dataset_id in dataset_ids:
            response = opensearch_client.client.get(
                index=opensearch_client.INDEX_NAME,
                id=dataset_id,
                ignore=[404],
            )
            assert response.get("found") is True

        clear_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
                "job_type": "clear",
            }
        )
        harvest_job_starter(clear_job.id, clear_job.job_type)

        for dataset_id in dataset_ids:
            response = opensearch_client.client.get(
                index=opensearch_client.INDEX_NAME,
                id=dataset_id,
                ignore=[404],
            )
            assert response.get("found") is False
