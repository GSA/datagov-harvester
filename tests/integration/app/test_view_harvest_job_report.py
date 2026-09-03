from datetime import datetime, timezone

import pytest


@pytest.fixture
def job_with_field_error(interface, job):
    record = interface.add_harvest_record(
        {
            "identifier": "report-test-record",
            "harvest_job_id": job.id,
            "harvest_source_id": job.source.id,
            "status": "error",
            "action": "create",
            "source_raw": '{"title": "Report Test Dataset"}',
        }
    )
    interface.add_harvest_record_error(
        {
            "type": "ValidationError",
            "message": (
                "<ValidationError: \"$.license, 'center' does not match any "
                "of the acceptable formats: 'uri'\">"
            ),
            "harvest_job_id": job.id,
            "harvest_record_id": record.id,
            "severity": "error",
        }
    )
    return job, record


@pytest.fixture
def dataset_for_job(interface, job, job_with_field_error):
    _, record = job_with_field_error
    return interface.insert_dataset(
        {
            "slug": "report-test-dataset",
            "dcat": {"title": "Report Test Dataset"},
            "organization_id": job.source.organization_id,
            "harvest_source_id": job.source.id,
            "harvest_record_id": record.id,
            "last_harvested_date": datetime.now(timezone.utc),
        }
    )


class TestViewHarvestJobReport:
    def test_not_found_for_invalid_job_id(self, client):
        resp = client.get("/harvest_job/not-a-uuid/report")
        assert resp.status_code == 404

    def test_missing_job_shows_not_found_message(self, client):
        resp = client.get("/harvest_job/6f8b1e8e-2f4d-4c1a-9a3e-2b6a7f9c8d10/report")
        assert resp.status_code == 200
        assert "couldn't find that harvest job" in resp.text

    def test_report_shows_field_level_issue(self, client, job_with_field_error):
        job, _ = job_with_field_error
        resp = client.get(f"/harvest_job/{job.id}/report")
        assert resp.status_code == 200
        assert "license" in resp.text
        assert job.source.name in resp.text
        assert job.source.org.name in resp.text

    def test_report_links_to_dataset(self, client, job, dataset_for_job):
        resp = client.get(f"/harvest_job/{job.id}/report")
        assert resp.status_code == 200
        assert f"/dataset/{dataset_for_job.slug}" in resp.text

    def test_report_no_errors_shows_success_message(self, client, job):
        resp = client.get(f"/harvest_job/{job.id}/report")
        assert resp.status_code == 200
        assert "No record errors or warnings found." in resp.text

    def test_report_reachable_without_login(self, client, job):
        with client.session_transaction() as session:
            session.pop("user", None)
        resp = client.get(f"/harvest_job/{job.id}/report")
        assert resp.status_code == 200


class TestViewHarvestSourceReport:
    def test_redirects_to_latest_job_report(self, client, job):
        resp = client.get(f"/harvest_source/{job.source.id}/report")
        assert resp.status_code == 302
        assert resp.location == f"/harvest_job/{job.id}/report"

    def test_no_jobs_shows_not_found_message(
        self, client, interface, organization_data, source_data_dcatus_2
    ):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus_2)
        resp = client.get(f"/harvest_source/{source.id}/report")
        assert resp.status_code == 200
        assert "couldn't find that harvest job" in resp.text

    def test_invalid_source_id_returns_404(self, client):
        resp = client.get("/harvest_source/not-a-uuid/report")
        assert resp.status_code == 404
