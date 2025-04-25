import os
from unittest.mock import patch

from harvester.harvest import harvest_job_starter
from harvester.utils.general_utils import download_file

HARVEST_SOURCE_URL = os.getenv("HARVEST_SOURCE_URL")


class TestHarvestJobValidate:
    @patch("harvester.harvest.ckan_sync_tool.ckan")
    def test_validate_single_valid_record(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_single_record,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}
        CKANMock.action.package_update = "ok"
        CKANMock.action.dataset_purge = "ok"

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_single_record["id"],
                "job_type": "validate",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        harvest_job_starter(job_id, job_type)
        records_to_add = download_file(
            source_data_dcatus_single_record["url"], ".json"
        )["dataset"]
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 0
        assert harvest_job.records_validated == 1
        assert harvest_job.records_validated == len(records_to_add) == 1

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    def test_validate_single_invalid_record(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_invalid,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}
        CKANMock.action.package_update = "ok"
        CKANMock.action.dataset_purge = "ok"

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_invalid)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_invalid["id"],
                "job_type": "validate",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        harvest_job_starter(job_id, job_type)
        records_to_add = download_file(source_data_dcatus_invalid["url"], ".json")[
            "dataset"
        ]
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert (
            harvest_job.records_errored == 1
        )  # TODO: this should be 0 after we change count reporter
        assert len(harvest_job.records) == len(records_to_add) == 1
        assert harvest_job.records_validated == 0

    def skip_test_valiate_new_against_existing_source(self):
        # TODO: seed a source with harvest data and validate
        # confirms we are only validating a single record.
        pass

    def skip_test_validate_non_dcat_record(self):
        # TODO: validate a transformed record
        pass
