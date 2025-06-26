"""Test our metrics page in Flask."""

from datetime import datetime, timedelta

import pytest


@pytest.fixture()
def failed_job_error(interface, organization_data, source_data_dcatus, job_data_new):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    job = interface.add_harvest_job(job_data_new)
    error = interface.add_harvest_job_error(
        {
            "type": "FailedJobCleanup",
            "harvest_job_id": job.id,
            "date_created": datetime.now() - timedelta(hours=12),
        }
    )
    yield error


class TestMetricsPage:

    def test_metrics_failed_table(self, client, failed_job_error):
        """Test that the failed jobs table has rows."""
        resp = client.get("/metrics/")
        assert f'<a href="/harvest_job/{failed_job_error.harvest_job_id}">' in resp.text
