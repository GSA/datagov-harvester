import json
from unittest.mock import patch

from harvester.harvest import HarvestSource
from harvester.utils.general_utils import download_file


# reusable fixture for making db record
def make_db_record_contract(record):
    return {
        "identifier": record.identifier,
        "id": record.id,
        "harvest_job_id": record.harvest_source.job_id,
        "harvest_source_id": record.harvest_source.id,
        "source_hash": record.metadata_hash,
        "source_raw": json.dumps(record.metadata),
        "action": record.action,
    }


class TestHarvestFullFlow:
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
        harvest_source = HarvestSource(job_id)
        harvest_source.extract()
        harvest_source.transform()
        harvest_source.validate()
        harvest_source.load()
        harvest_source.do_report()

        records_to_add = download_file(
            source_data_dcatus_single_record["url"], ".json"
        )["dataset"]
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_added == len(records_to_add)

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

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.transform()
        harvest_source.validate()
        harvest_source.load()
        harvest_source.do_report()

        interface_errors = interface.get_harvest_record_errors_by_record(
            harvest_source.records[0].id
        )

        job_errors = [
            error for record in harvest_job.records for error in record.errors
        ]
        assert harvest_job.status == "complete"
        assert len(interface_errors) == harvest_job.records_errored
        assert len(interface_errors) == len(job_errors)
        assert interface_errors[0].harvest_record_id == job_errors[0].harvest_record_id

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
            {"id": 1234},
            Exception(
                "ValidationError({'name': ['That URL is already in use.'], '__type': 'Validation Error'}"
            ),
            {"id": 5678},
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
        harvest_source = HarvestSource(job_id)
        harvest_source.extract()
        harvest_source.transform()
        harvest_source.validate()
        harvest_source.load()
        harvest_source.do_report()

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

        ## now do a package_update & confirm it uses ckan_name
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_same_title["id"],
            }
        )

        job_id = harvest_job.id
        harvest_source = HarvestSource(job_id)
        harvest_source.url = "http://localhost:80/dcatus/dcatus_same_title_step_2.json"
        harvest_source.extract()
        harvest_source.transform()
        harvest_source.validate()
        harvest_source.load()
        harvest_source.do_report()

        # assert job reports correct metrics
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.records_updated == 1
        assert harvest_job.records_ignored == 1
        assert harvest_job.records_errored == 0

        # assert db record retains correct ckan_id & ckan_name
        db_record = interface.get_harvest_record(harvest_job.records[0].id)
        assert db_record.action == "update"
        assert db_record.ckan_id == 5678
        assert db_record.ckan_name == "commitment-of-traders-12345"
        assert db_record.identifier == "cftc-dc2"

        ## finally, assert package_update called with proper kwargs
        args, kwargs = CKANMock.action.package_update.call_args_list[0]
        assert kwargs["name"] == "commitment-of-traders-12345"
        assert kwargs["title"] == "Commitment of Traders"
        assert kwargs["id"] == 5678
        assert kwargs["identifier"] == "cftc-dc2"

    @patch("harvester.harvest.ckan")
    @patch("harvester.utils.ckan_utils.uuid")
    def test_harvest_restart_job(
        self,
        UUIDMock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_same_title,
    ):
        """Rerun a harvest with same job id in case of errors"""
        UUIDMock.uuid4.return_value = 12345
        # ruff: noqa: E501
        CKANMock.action.package_create.side_effect = [
            {"id": 1234},
            Exception(
                "ValidationError({'name': ['That URL is already in use.'], '__type': 'Validation Error'}"
            ),
            Exception("Some other error occurred"),
            {"id": 5678},
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
        harvest_source = HarvestSource(job_id)
        harvest_source.extract()
        harvest_source.load()
        harvest_source.do_report()

        harvest_records = interface.get_harvest_records_by_job(job_id)
        records_with_errors = [
            record for record in harvest_records if record.status == "error"
        ]
        job_err = interface.get_harvest_job_errors_by_job(job_id)
        record_err = interface.get_harvest_record_errors_by_job(job_id)
        assert len(job_err) == 0
        assert len(record_err) == 1
        assert record_err[0].type == "SynchronizeException"
        assert record_err[0].harvest_record_id == records_with_errors[0].id
        assert (
            harvest_records[1].id
            == records_with_errors[0].id
            == harvest_source.records[1].id
        )  ## assert it's the second record that threw the exception, which validates our package_create mock

        # even though we have record level errors, the job is marked as complete
        assert harvest_source.report["status"] == "complete"
        assert harvest_source.report["records_added"] == 1
        assert harvest_source.report["records_ignored"] == 0
        assert harvest_source.report["records_errored"] == 1

        assert harvest_source.records[0].ckan_id == 1234
        assert harvest_source.records[0].status == "success"
        assert harvest_source.records[1].ckan_id is None
        assert harvest_source.records[1].status == "error"

        job_id = harvest_job.id
        harvest_source = HarvestSource(job_id)
        harvest_source.restart_job_helper()
        harvest_source.load()
        harvest_source.do_report()

        # assert that we're not calling package_create on records with status:success
        assert CKANMock.action.package_create.call_count == 4

        assert harvest_source.report["status"] == "complete"
        assert harvest_source.report["records_added"] == 2
        assert harvest_source.report["records_ignored"] == 0
        assert harvest_source.report["records_errored"] == 0

        assert harvest_source.records[0].ckan_id == 1234
        assert harvest_source.records[0].status == "success"
        assert harvest_source.records[1].ckan_id == 5678
        assert harvest_source.records[1].status == "success"

    @patch("harvester.harvest.ckan")
    @patch("harvester.utils.ckan_utils.uuid")
    def test_harvest_follow_up_job(
        self,
        UUIDMock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_same_title,
    ):
        """Create a new follow-up job and rerun against a harvest source in case of errors"""
        UUIDMock.uuid4.return_value = 12345
        # ruff: noqa: E501
        CKANMock.action.package_create.side_effect = [
            {"id": 1234},
            Exception(
                "ValidationError({'name': ['That URL is already in use.'], '__type': 'Validation Error'}"
            ),
            Exception("Some other error occurred"),
            {"id": 5678},
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
        harvest_source = HarvestSource(job_id)
        harvest_source.extract()
        harvest_source.load()
        harvest_source.do_report()

        harvest_records = interface.get_harvest_records_by_job(job_id)
        records_with_errors = [
            record for record in harvest_records if record.status == "error"
        ]
        job_err = interface.get_harvest_job_errors_by_job(job_id)
        record_err = interface.get_harvest_record_errors_by_job(job_id)
        assert len(job_err) == 0
        assert len(record_err) == 1
        assert record_err[0].type == "SynchronizeException"
        assert record_err[0].harvest_record_id == records_with_errors[0].id
        assert (
            harvest_records[1].id
            == records_with_errors[0].id
            == harvest_source.records[1].id
        )  ## assert it's the second record that threw the exception, which validates our package_create mock

        # even though we have record level errors, the job is marked as complete
        assert harvest_source.report["status"] == "complete"
        assert harvest_source.report["records_added"] == 1
        assert harvest_source.report["records_ignored"] == 0
        assert harvest_source.report["records_errored"] == 1

        assert harvest_source.records[0].ckan_id == 1234
        assert harvest_source.records[0].status == "success"
        assert harvest_source.records[1].ckan_id is None
        assert harvest_source.records[1].status == "error"

        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_same_title["id"],
            }
        )

        job_id = harvest_job.id
        harvest_source = HarvestSource(job_id)
        harvest_source.follow_up_job_helper()
        harvest_source.load()
        harvest_source.do_report()

        # assert that we're not calling package_create on records with status:success
        assert CKANMock.action.package_create.call_count == 4

        assert harvest_source.report["status"] == "complete"
        assert harvest_source.report["records_added"] == 1
        assert harvest_source.report["records_ignored"] == 0
        assert harvest_source.report["records_errored"] == 0

        assert harvest_source.records[0].ckan_id == 1234
        assert harvest_source.records[0].status == "success"
        assert harvest_source.records[1].ckan_id == 5678
        assert harvest_source.records[1].status == "success"
