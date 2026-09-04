from datetime import datetime, timezone
from unittest.mock import patch

import pytest


@pytest.fixture
def dataset_for_source(interface, source, job):
    record = interface.add_harvest_record(
        {
            "identifier": "source-page-test-record",
            "harvest_job_id": job.id,
            "harvest_source_id": source.id,
            "status": "success",
            "action": "create",
            "source_raw": "{}",
        }
    )
    return interface.insert_dataset(
        {
            "slug": "source-page-test-dataset",
            "dcat": {"title": "Source Page Test Dataset"},
            "organization_id": source.organization_id,
            "harvest_source_id": source.id,
            "harvest_record_id": record.id,
            "last_harvested_date": datetime.now(timezone.utc),
        }
    )


class TestViewSource:

    def test_org_name(self, client, source, organization):
        """Organization name is linked."""
        resp = client.get(f"/harvest_source/{source.id}")
        assert organization.name in resp.text
        assert f'href="/organization/{organization.slug}"' in resp.text

    def test_dataset_links_to_catalog_when_configured(
        self, client, source, dataset_for_source
    ):
        with patch(
            "app.main.harvest_sources.CATALOG_BASE_URL", "https://catalog.data.gov"
        ):
            resp = client.get(f"/harvest_source/{source.id}")
        assert resp.status_code == 200
        assert (
            f"https://catalog.data.gov/dataset/{dataset_for_source.slug}" in resp.text
        )

    def test_dataset_omits_catalog_link_when_not_configured(
        self, client, source, dataset_for_source
    ):
        with patch("app.main.harvest_sources.CATALOG_BASE_URL", ""):
            resp = client.get(f"/harvest_source/{source.id}")
        assert resp.status_code == 200
        assert '<th scope="col">Catalog</th>' not in resp.text
