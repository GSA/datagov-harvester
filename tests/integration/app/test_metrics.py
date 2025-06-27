"""Test our metrics page in Flask."""

from datetime import datetime, timedelta

import pytest


@pytest.fixture()
def new_job(interface, organization_data, source_data_dcatus, job_data_new):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    yield interface.add_harvest_job(job_data_new)

@pytest.fixture()
def failed_job_error(interface, new_job):
    """An error for a failed job."""
    error = interface.add_harvest_job_error(
        {
            "type": "FailedJobCleanup",
            "harvest_job_id": new_job.id,
            "date_created": datetime.now() - timedelta(hours=12),
        }
    )
    yield error

@pytest.fixture()
def old_failed_job_error(interface, new_job):
    """An error for a failed job more than 24 hours old."""
    error = interface.add_harvest_job_error(
        {
            "type": "FailedJobCleanup",
            "harvest_job_id": new_job.id,
            "date_created": datetime.now() - timedelta(hours=25),
        }
    )
    yield error


class TestMetricsPage:

    def test_metrics_failed_table(self, client, failed_job_error):
        """Test that the failed jobs table has rows."""
        resp = client.get("/metrics/")
        assert f'<a href="/harvest_job/{failed_job_error.harvest_job_id}">' in resp.text

    def test_metrics_old_failed_jobs(self, client, old_failed_job_error):
        """The failed jobs table doesn't contain old jobs."""
        resp = client.get("/metrics/")
        assert "No failed jobs" in resp.text
