import uuid

import pytest


@pytest.fixture
def record_with_warning(interface, source, job):
    record = interface.add_harvest_record(
        {
            "harvest_job_id": job.id,
            "harvest_source_id": source.id,
            "identifier": "warning-record-001",
            "status": "success",
            "source_raw": '{"title": "Warning Record Title"}',
        }
    )
    interface.add_harvest_record_error(
        {
            "harvest_job_id": job.id,
            "harvest_record_id": record.id,
            "type": "SpatialTransformationException",
            "message": "unable to spatially fix Virginia",
            "severity": "warning",
        }
    )
    return record


@pytest.fixture
def source_with_many_current_issues(interface, source, job):
    # Need at least eleven distinct records to get a second page, since
    # the_for_view query groups issues by harvest_record_id.
    for i in range(11):
        record = interface.add_harvest_record(
            {
                "harvest_job_id": job.id,
                "harvest_source_id": source.id,
                "identifier": f"many-issues-record-{i}",
                "status": "success",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": job.id,
                "harvest_record_id": record.id,
                "type": "TestException",
                "message": f"issue {i}",
                "severity": "error",
            }
        )
    return source


class TestViewHarvestSourceWarnings:
    def test_warnings_page_shows_current_issue(
        self, client, source, record_with_warning
    ):
        resp = client.get(f"/harvest_source/{source.id}/warnings")

        assert resp.status_code == 200
        assert "Outstanding warnings and errors for harvest source" in resp.text
        assert "unable to spatially fix Virginia" in resp.text
        assert "SpatialTransformationException" in resp.text
        assert "warning-record-001" in resp.text
        assert "Warning Record Title" in resp.text

    def test_warnings_page_empty_state(self, client, source):
        resp = client.get(f"/harvest_source/{source.id}/warnings")

        assert resp.status_code == 200
        assert "No record errors or warnings found." in resp.text

    def test_warnings_page_back_link(self, client, source, record_with_warning):
        resp = client.get(f"/harvest_source/{source.id}/warnings")

        assert f'href="/harvest_source/{source.id}"' in resp.text

    def test_warnings_page_unknown_source(self, client):
        unknown_id = str(uuid.uuid4())

        resp = client.get(f"/harvest_source/{unknown_id}/warnings")

        assert resp.status_code == 200
        assert "We couldn't find that harvest source" in resp.text

    def test_warnings_page_invalid_source_id(self, client):
        resp = client.get("/harvest_source/not-a-uuid/warnings")

        assert resp.status_code == 404

    def test_warnings_page_pagination(self, client, source_with_many_current_issues):
        resp = client.get(
            f"/harvest_source/{source_with_many_current_issues.id}/warnings"
        )

        assert (
            f'hx-get="/harvest_source/{source_with_many_current_issues.id}/warnings?page=2"'
            in resp.text
        )

    def test_warnings_page_htmx_partial_omits_page_chrome(
        self, client, source, record_with_warning
    ):
        resp = client.get(
            f"/harvest_source/{source.id}/warnings",
            headers={"HX-Request": "true"},
        )

        assert resp.status_code == 200
        assert "unable to spatially fix Virginia" in resp.text
        assert "Outstanding warnings and errors for harvest source" not in resp.text
        assert "Back to harvest source" not in resp.text
