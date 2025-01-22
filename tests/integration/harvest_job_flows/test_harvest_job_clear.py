from unittest.mock import patch

from harvester.harvest import harvest_job_starter


class TestHarvestJobClear:
    @patch("harvester.harvest.ckan")
    def test_harvest_job_clear(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}
        CKANMock.action.dataset_purge.return_value = {"ok"}
        CKANMock.action.package_update = "ok"

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type  # harvest
        harvest_job_starter(job_id, job_type)

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 7

        records_from_db = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )

        assert len(records_from_db) == 7

        # now run a clear
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
                "job_type": "clear",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type  # clear
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_deleted == 7

        records_from_db = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )
        assert len(records_from_db) == 0
