import json
import os
from unittest.mock import patch

from harvester.harvest import harvest_job_starter
from harvester.utils.general_utils import download_file

HARVEST_SOURCE_URL = os.getenv("HARVEST_SOURCE_URL")


class TestHarvestJobFullFlow:
    @patch("harvester.harvest.ckan")
    def test_harvest_single_record_created(
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
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")
        records_to_add = download_file(
            source_data_dcatus_single_record["url"], ".json"
        )["dataset"]
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_added == len(records_to_add)

    @patch("harvester.harvest.ckan")
    def test_multiple_harvest_jobs(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
    ):

        CKANMock.action.package_create.return_value = {"id": 1234}
        CKANMock.action.dataset_purge.return_value = {"ok"}
        CKANMock.action.package_update.return_value = {"ok"}

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")

        records_from_db = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )

        assert len(records_from_db) == 7

        ## create follow-up job
        interface.update_harvest_source(
            source_data_dcatus["id"],
            {"url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_2.json"},
        )

        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")

        records_from_db = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )

        assert len(records_from_db) == 3

    @patch("harvester.harvest.ckan")
    @patch("harvester.harvest.download_file")
    def test_harvest_record_errors_reported(
        self,
        download_file_mock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        single_internal_record,
    ):
        CKANMock.action.dataset_purge.side_effect = Exception()
        download_file_mock.return_value = dict({"dataset": []})

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        interface.add_harvest_record(single_internal_record)

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")

        interface_errors = interface.get_harvest_record_errors_by_job(job_id)
        harvest_job = interface.get_harvest_job(job_id)
        job_errors = [
            error for record in harvest_job.records for error in record.errors
        ]
        assert harvest_job.status == "complete"
        assert len(interface_errors) == harvest_job.records_errored
        assert len(interface_errors) == len(job_errors)
        assert interface_errors[0][0].harvest_record_id == job_errors[0].harvest_record_id

    @patch("harvester.harvest.ckan")
    @patch("harvester.utils.ckan_utils.uuid")
    def test_validate_same_title(
        self,
        UUIDMock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_same_title,
    ):
        """Validates that a harvest source with two same titled datasets will munge the second title and resolve with a different ckan_id"""
        UUIDMock.uuid4.return_value = 12345
        # ruff: noqa: E501
        CKANMock.action.package_create.side_effect = [
            {"id": "1234"},
            Exception(
                "ValidationError({'name': ['That URL is already in use.'], '__type': 'Validation Error'}"
            ),
            {"id": "5678"},
        ]
        CKANMock.action.package_update.return_value = "ok"
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_same_title)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_same_title["id"],
            }
        )
        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")

        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"

        raw_source_0 = json.loads(harvest_job.records[0].source_raw)
        raw_source_1 = json.loads(harvest_job.records[1].source_raw)

        ## assert title is same
        assert raw_source_0["title"] == raw_source_1["title"]
        ## assert name is unique
        assert harvest_job.records[0].ckan_name != harvest_job.records[1].ckan_name

        ## assert ckan_name persists in db
        for idx, record in enumerate(harvest_job.records):
            db_record = interface.get_harvest_record(record.id)
            assert db_record.ckan_name == harvest_job.records[idx].ckan_name

        # now do a package_update & confirm it uses ckan_name
        ## update harvest source url to simulate harvest source updates
        interface.update_harvest_source(
            source_data_dcatus_same_title["id"],
            {"url": "http://localhost:80/dcatus/dcatus_same_title_step_2.json"},
        )
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_same_title["id"],
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")

        # assert job reports correct metrics
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.records_updated == 1
        assert harvest_job.records_ignored == 1
        assert harvest_job.records_errored == 0

        # assert db record retains correct ckan_id & ckan_name
        db_record = interface.get_harvest_record(harvest_job.records[0].id)
        assert db_record.action == "update"
        assert db_record.ckan_id == "5678"
        assert db_record.ckan_name == "commitment-of-traders-12345"
        assert db_record.identifier == "cftc-dc2"

        ## finally, assert package_update called with proper kwargs
        args, kwargs = CKANMock.action.package_update.call_args_list[0]
        assert kwargs["name"] == "commitment-of-traders-12345"
        assert kwargs["title"] == "Commitment of Traders"
        assert kwargs["id"] == "5678"
        assert kwargs["identifier"] == "cftc-dc2"
