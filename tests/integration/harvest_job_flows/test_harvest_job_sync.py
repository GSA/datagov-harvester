from unittest.mock import patch

from harvester.harvest import harvest_job_starter


class TestHarvestJobSync:
    @patch("harvester.harvest.ckan")
    @patch("harvester.utils.ckan_utils.uuid")
    def test_harvest_job_sync(
        self,
        UUIDMock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_same_title,
    ):
        """Create a new sync job and rerun against a harvest source in case of errors"""
        UUIDMock.uuid4.return_value = 12345
        # ruff: noqa: E501
        CKANMock.action.package_create.side_effect = [
            {"id": "1234"},
            Exception(
                "ValidationError({'name': ['That URL is already in use.'], '__type': 'Validation Error'}"
            ),
            Exception("Some other error occurred"),
            {"id": "5678"},
        ]

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_same_title)

        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_same_title["id"],
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)
        job_err = interface.get_harvest_job_errors_by_job(job_id)
        record_err = interface.get_harvest_record_errors_by_job(job_id)

        assert len(job_err) == 0
        assert len(record_err) == 1
        assert record_err[0][0].type == "SynchronizeException"
        ## assert it's the second record that threw the exception, which validates our package_create mock
        assert record_err[0][0].harvest_record_id == harvest_job.records[1].id

        # pkg create called three times
        assert CKANMock.action.package_create.call_count == 3

        # even though we have record level errors, the job is marked as complete
        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 1
        assert harvest_job.records_ignored == 0
        assert harvest_job.records_errored == 1

        assert harvest_job.records[0].ckan_id == "1234"
        assert harvest_job.records[0].status == "success"
        assert harvest_job.records[1].ckan_id is None
        assert harvest_job.records[1].status == "error"

        ## create a second follow-up job to pickup sync
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_same_title["id"],
                "job_type": "sync",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)

        # assert that we're not calling package_create on records with status:success
        assert CKANMock.action.package_create.call_count == 4

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 1
        assert harvest_job.records_ignored == 0
        assert harvest_job.records_errored == 0

        records = interface.get_latest_harvest_records_by_source(
            source_data_dcatus_same_title["id"]
        )
        assert len(records) == 2
        assert records[0]["ckan_id"] == "1234"
        assert records[0]["status"] == "success"
        assert records[1]["ckan_id"] == "5678"
        assert records[1]["action"] == "create"
        assert records[1]["status"] == "success"
