from unittest.mock import patch

from harvester.harvest import harvest_job_starter


class TestHarvestJobSync:
    @patch("harvester.harvest.ckan")
    def test_harvest_job_force_update(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
    ):
        """
        Force update exists to update jobs in case
        of code changes when we want to update all datasets,
        not just those that have changed
        """

        CKANMock.action.package_create.return_value = {"id": 1234}
        CKANMock.action.package_update.return_value = "ok"

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)

        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        assert job_type == "harvest"
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)
        job_err = interface.get_harvest_job_errors_by_job(job_id)
        record_err = interface.get_harvest_record_errors_by_job(job_id)

        assert len(job_err) == 0
        assert len(record_err) == 0

        assert CKANMock.action.package_create.call_count == 7

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 7
        assert harvest_job.records_deleted == 0
        assert harvest_job.records_errored == 0
        assert harvest_job.records_ignored == 0
        assert harvest_job.records_total == 7
        assert harvest_job.records_updated == 0
        assert harvest_job.records_validated == 7

        ## create a second force_harvest to pickup sync
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
                "job_type": "force_harvest",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        assert job_type == "force_harvest"
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)

        # assert all records are resynced.
        assert len(job_err) == 0
        assert len(record_err) == 0

        assert CKANMock.action.package_create.call_count == 7

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 0
        assert harvest_job.records_deleted == 0
        assert harvest_job.records_errored == 0
        assert harvest_job.records_ignored == 0
        assert harvest_job.records_total == 7
        assert harvest_job.records_updated == 7
        assert harvest_job.records_validated == 7
