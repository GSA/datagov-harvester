import json
from unittest.mock import patch

from harvester.harvest import HarvestSource
from harvester.utils.general_utils import dataset_to_hash, download_file, sort_dataset


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
        assert db_record.ckan_id == "5678"
        assert db_record.ckan_name == "commitment-of-traders-12345"
        assert db_record.identifier == "cftc-dc2"

        ## finally, assert package_update called with proper kwargs
        args, kwargs = CKANMock.action.package_update.call_args_list[0]
        assert kwargs["name"] == "commitment-of-traders-12345"
        assert kwargs["title"] == "Commitment of Traders"
        assert kwargs["id"] == "5678"
        assert kwargs["identifier"] == "cftc-dc2"

    @patch("harvester.harvest.ckan")
    def test_pickup_sync_again(
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
        harvest_source.validate()

        assert harvest_job.status == "new"

        new_harvest_source = HarvestSource(job_id)
        new_harvest_source.restart_job()
        new_harvest_source.transform()
        new_harvest_source.validate()
        new_harvest_source.load()
        new_harvest_source.do_report()

        assert harvest_job.status == "complete"

        # assert our records are the same
        assert dataset_to_hash(
            sort_dataset(make_db_record_contract(harvest_source.records[0]))
        ) == dataset_to_hash(
            sort_dataset(make_db_record_contract(new_harvest_source.records[0]))
        )

        # assert against
        assert harvest_source.records[0].ckan_id is None
        assert new_harvest_source.records[0].ckan_id == 1234

        assert harvest_source.records[0].ckan_name is None
        assert new_harvest_source.records[0].ckan_name == "commitment-of-traders"

    @patch("harvester.harvest.ckan")
    def test_pickup_sync_after_partially_complete(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        # this is all to simulate a failed job
        for idx, record in enumerate(record_data_dcatus):
            if idx in range(0, 5):
                record["status"] = "success"
            elif idx in range(5, 10):
                record["status"] = "error"
            interface.add_harvest_record(record)

        job_id = harvest_job.id
        harvest_source = HarvestSource(job_id)
        harvest_source.restart_job()
        harvest_source.do_report()

        # even though we have record level errors, the job is marked as complete
        assert harvest_source.report["status"] == "complete"
        assert harvest_source.report["records_added"] == 5
        assert harvest_source.report["records_ignored"] == 0
        assert harvest_source.report["records_errored"] == 5

        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        harvest_source = HarvestSource(job_id)
        harvest_source.follow_up_job()
        harvest_source.load()
        harvest_source.do_report()

        # assert that we're not calling package_create on records with status:success
        assert CKANMock.action.package_create.call_count == 5

        assert harvest_source.report["status"] == "complete"
        assert harvest_source.report["records_added"] == 10
        assert harvest_source.report["records_ignored"] == 0
        assert harvest_source.report["records_errored"] == 0
