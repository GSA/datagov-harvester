import json
import logging
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from deepdiff import DeepDiff

from harvester.harvest import (
    HarvestSource,
    check_for_more_work,
    ckan_sync_tool,
    harvest_job_starter,
)
from harvester.utils.general_utils import download_file

HARVEST_SOURCE_URL = os.getenv("HARVEST_SOURCE_URL")


class TestHarvestJobFullFlow:
    @patch("harvester.harvest.ckan_sync_tool.ckan")
    @patch("harvester.harvest.HarvestSource.send_notification_emails")
    def test_harvest_single_record_created(
        self,
        send_notification_emails_mock: MagicMock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_single_record,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

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
        # assert that send_notification_emails is not called because email has no errors
        assert send_notification_emails_mock.called is False

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    @patch("harvester.harvest.HarvestSource.send_notification_emails")
    def test_multiple_harvest_jobs(
        self,
        send_notification_emails_mock: MagicMock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

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
        assert send_notification_emails_mock.called

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

        assert send_notification_emails_mock.called
        records_from_db_count = interface.get_latest_harvest_records_by_source_orm(
            source_data_dcatus["id"],
            count=True,
        )

        assert len(records_from_db) == records_from_db_count == 3

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    @patch("harvester.harvest.HarvestSource.send_notification_emails")
    def test_harvest_waf_iso19115_2(
        self,
        send_notification_emails_mock: MagicMock,
        CKANMock,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_waf_iso19115_2["id"],
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")
        harvest_job = interface.get_harvest_job(job_id)

        # assert job rollup
        assert harvest_job.status == "complete"
        assert harvest_job.records_total == 5
        assert len(harvest_job.record_errors) == 3
        assert harvest_job.records_errored == 3

        # assert error insertion order
        errors = interface.get_harvest_record_errors_by_job(job_id)
        # the first doc ("decode_error.xml") throws a decoding error and since we can't fetch the doc
        # there's no record to associate it to so checking to make sure the error
        # exists and the message is what we expect then we remove it so the
        # proceeeding assertions work as intended.
        assert errors[0][0].type == "ExternalRecordToClass"
        assert "UnicodeDecodeError" in errors[0][0].message
        del errors[0]

        # assert harvest_record_id & type match
        for error in errors:
            harvest_record = interface.get_harvest_record(error[0].harvest_record_id)
            assert len(harvest_record.errors) == 1
            assert harvest_record.id == error[0].harvest_record_id
            assert harvest_record.errors[0].type == error[0].type

        ## assert call_args to package_create
        ## TODO this test wil eventually succeed. we can then assert call_args

    def test_harvest_waf_iso19115_2_download_exception(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_waf_iso19115_2["id"],
            }
        )

        job_id = harvest_job.id

        harvest_source = HarvestSource(job_id, "harvest")
        harvest_source.acquire_minimum_external_data()
        harvest_source.external_records[0][
            "identifier"
        ] = "https://localhost:1234/test.xml"

        list(harvest_source.external_records_to_process())

        harvest_job = interface.get_harvest_job(job_id)
        assert len(harvest_job.record_errors) == 1
        # xml record identifier is now used instead of the harvest source url
        assert harvest_job.record_errors[0].message.startswith(
            "Test Source (WAF ISO19115_2) https://localhost:1234/test.xml"
        )

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    @patch("harvester.harvest.HarvestSource.send_notification_emails")
    def test_harvest_iso19115_2_document(
        self,
        send_notification_emails_mock: MagicMock,
        CKANMock,
        interface,
        organization_data,
        source_data_iso19115_2_document,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_iso19115_2_document)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_iso19115_2_document["id"],
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")
        harvest_job = interface.get_harvest_job(job_id)

        # assert job rollup
        assert harvest_job.status == "complete"
        assert harvest_job.records_total == 1
        assert len(harvest_job.record_errors) == 0
        assert harvest_job.records_errored == 0

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    @patch("harvester.harvest.HarvestSource.send_notification_emails")
    def test_harvest_waf_collection(
        self,
        send_notification_emails_mock: MagicMock,
        CKANMock,
        interface,
        organization_data,
        source_data_waf_collection,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_collection)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_waf_collection["id"],
            }
        )

        job_id = harvest_job.id
        harvest_job_starter(job_id, "harvest")
        harvest_job = interface.get_harvest_job(job_id)

        # assert job rollup
        assert harvest_job.status == "complete"
        assert harvest_job.records_total == 6
        assert len(harvest_job.record_errors) == 3
        assert harvest_job.records_errored == 3

        for record in harvest_job.records:
            if record.status == "success":
                assert record.parent_identifier == source_data_waf_collection["collection_parent_url"]

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    @patch("harvester.harvest.download_file")
    @patch("harvester.harvest.HarvestSource.send_notification_emails")
    def test_harvest_record_errors_reported(
        self,
        send_notification_emails_mock: MagicMock,
        download_file_mock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        single_internal_record,
    ):
        CKANMock.action.dataset_purge.side_effect = Exception("test exception")
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
        assert (
            interface_errors[0][0].harvest_record_id == job_errors[0].harvest_record_id
        )
        # assert that send_notification_emails is called because of errors
        assert send_notification_emails_mock.called

    @patch("harvester.harvest.ckan_sync_tool.ckan")
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

    @patch("harvester.harvest.ckan_sync_tool.ckan")
    def test_validate_ckan_export(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_single_record,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}

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

        harvest_job = interface.get_harvest_job(job_id)
        ckan_package_create_args = {
            "name": "commitment-of-traders",
            "owner_org": "d925f84d-955b-4cb7-812f-dcfd6681a18f",
            "identifier": "cftc-dc1",
            "author": None,
            "author_email": None,
            "maintainer": "Harold W. Hild",
            "maintainer_email": "hhild@CFTC.GOV",
            "notes": "COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC",
            "title": "Commitment of Traders",
            "resources": [
                {
                    "url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
                    "mimetype": "text/html",
                    "no_real_name": True,
                    "format": "HTML",
                    "name": "Web Page",
                    "description": "index.htm",
                }
            ],
            "tags": [
                {"name": "commitment-of-traders"},
                {"name": "cot"},
                {"name": "open-interest"},
            ],
            "extras": [
                {"key": "resource-type", "value": "Dataset"},
                {
                    "key": "harvest_object_id",
                    "value": harvest_job.records[0].id,
                },
                {
                    "key": "harvest_source_id",
                    "value": "2f2652de-91df-4c63-8b53-bfced20b276b",
                },
                {"key": "harvest_source_title", "value": "Single Record Test Source"},
                {"key": "identifier", "value": "cftc-dc1"},
                {"key": "source_datajson_identifier", "value": True},
                {"key": "title", "value": "Commitment of Traders"},
                {
                    "key": "description",
                    "value": "COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC",
                },
                {
                    "key": "keyword",
                    "value": '["commitment of traders", "cot", "open interest"]',
                },
                {"key": "modified", "value": "R/P1W"},
                {
                    "key": "publisher_hierarchy",
                    "value": "U.S. Government > U.S. Commodity Futures Trading Commission",
                },
                {
                    "key": "publisher",
                    "value": "U.S. Commodity Futures Trading Commission",
                },
                {
                    "key": "contactPoint",
                    "value": json.dumps(
                        {
                            "fn": "Harold W. Hild",
                            "hasEmail": "mailto:hhild@CFTC.GOV",
                        }
                    ),
                },
                {"key": "identifier", "value": "cftc-dc1"},
                {"key": "accessLevel", "value": "public"},
                {"key": "old-spatial", "value": "United States"},
                {
                    "key": "spatial",
                    "value": '{"type":"MultiPolygon","coordinates":[[[[-124.733253,24.544245],[-124.733253,49.388611],[-66.954811,49.388611],[-66.954811,24.544245],[-124.733253,24.544245]]]]}',
                },
                {"key": "bureauCode", "value": json.dumps(["339:00"])},
                {"key": "programCode", "value": json.dumps(["000:000"])},
            ],
        }

        assert CKANMock.action.package_create.call_count == 1
        assert (
            DeepDiff(
                CKANMock.action.package_create.call_args.kwargs,
                ckan_package_create_args,
            )
            == {}
        )

    def test_harvest_multiple_jobs(
        self,
        interface,
        organization_data,
        source_data_dcatus_single_record,
        caplog,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        harvest_job_doc = {
            "status": "in_progress",
            "harvest_source_id": source_data_dcatus_single_record["id"],
        }
        harvest_job1 = interface.add_harvest_job(harvest_job_doc)

        harvest_job2 = interface.add_harvest_job(harvest_job_doc)

        caplog.set_level(logging.INFO)

        harvest_job_starter(harvest_job1.id, "harvest")
        harvest_job = interface.get_harvest_job(harvest_job1.id)
        assert f"Job {harvest_job2.id} is already in progress for source" in caplog.text
        assert harvest_job.status == "error"

    @patch("requests.get")
    def test_critical_job_error_status_not_overwritten(
        self,
        mock_get,
        interface,
        organization_data,
        source_data_dcatus_single_record,
    ):
        mock_get.return_value.status_code = 500

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_single_record["id"],
            }
        )

        harvest_source = HarvestSource(harvest_job.id, "harvest")
        harvest_source.run_full_harvest()
        harvest_source.report()

        harvest_job = interface.get_harvest_job(harvest_job.id)
        assert harvest_job.status == "error"

        harvest_source.db_interface.close()
        ckan_sync_tool.close_conection()


class TestCheckMoreWork:
    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_check_more_work(
        self, CFCMock, interface, organization_data, source_data_dcatus_single_record
    ):
        """Check more work starts another task."""

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_single_record["id"],
                "date_created": datetime.now() + timedelta(days=-1),
            }
        )

        # no running tasks
        CFCMock.return_value.v3.apps._pagination.return_value = []

        check_for_more_work()
        # one task created
        start_task_mock = CFCMock.return_value.v3.tasks.create
        assert start_task_mock.call_count == 1
        # job in progress
        assert job.status == "in_progress"
