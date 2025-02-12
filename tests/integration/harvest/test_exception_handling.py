# ruff: noqa: F841

import json
from unittest.mock import Mock, patch

import ckanapi
import pytest

from harvester.exceptions import ExtractExternalException, ExtractInternalException
from harvester.harvest import HarvestSource


def download_mock(_, __):
    return dict({"dataset": []})


class TestHarvestJobExceptionHandling:
    def test_bad_harvest_source_url_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus_bad_url,
        job_data_dcatus_bad_url,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_bad_url)
        harvest_job = interface.add_harvest_job(job_data_dcatus_bad_url)

        harvest_source = HarvestSource(harvest_job.id)

        with pytest.raises(ExtractExternalException) as e:
            harvest_source.prepare_external_data()

        assert harvest_job.status == "error"

        harvest_error = interface.get_harvest_job_errors_by_job(harvest_job.id)[0]
        assert harvest_error.type == "ExtractExternalException"

    def test_extract_internal_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)

        harvest_source.internal_records_to_id_hash = Mock()
        harvest_source.internal_records_to_id_hash.side_effect = Exception("Broken")

        with pytest.raises(ExtractInternalException) as e:
            harvest_source.extract()

        assert harvest_job.status == "error"

        harvest_error = interface.get_harvest_job_errors_by_job(harvest_job.id)[0]
        assert harvest_error.type == "ExtractInternalException"

    def test_no_source_info_exception(self, job_data_dcatus):
        with pytest.raises(ExtractInternalException) as e:
            HarvestSource(job_data_dcatus["id"])


class TestHarvestRecordExceptionHandling:
    @patch("harvester.harvest.ckan", ckanapi.RemoteCKAN("mock_address"))
    @patch("harvester.harvest.download_file", download_mock)
    def test_delete_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        single_internal_record,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        interface.add_harvest_record(single_internal_record)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()
        harvest_source.sync()

        interface_record = interface.get_harvest_record(harvest_source.records[0].id)
        interface_errors = interface.get_harvest_record_errors_by_record(
            harvest_source.records[0].id
        )
        assert interface_record.id == harvest_source.records[0].id
        assert interface_record.status == "error"
        assert interface_errors[0].type == "SynchronizeException"

    def test_validation_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus_invalid,
        job_data_dcatus_invalid,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_invalid)
        harvest_job = interface.add_harvest_job(job_data_dcatus_invalid)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()
        harvest_source.validate()
        test_record = [
            x for x in harvest_source.records if x.identifier == "null-spatial"
        ][0]

        interface_record = interface.get_harvest_record(test_record.id)
        interface_errors = interface.get_harvest_record_errors_by_record(test_record.id)
        assert interface_record.id == interface_errors[0].harvest_record_id
        assert interface_record.status == "error"
        assert interface_errors[0].type == "ValidationException"

    @patch("harvester.utils.ckan_utils.ckanify_dcatus", side_effect=Exception("Broken"))
    def test_dcatus_to_ckan_exception(
        self,
        ckanify_dcatus_mock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()
        harvest_source.sync()

        test_record = [x for x in harvest_source.records if x.identifier == "cftc-dc1"][
            0
        ]

        interface_record = interface.get_harvest_record(test_record.id)
        interface_errors = interface.get_harvest_record_errors_by_record(test_record.id)

        assert ckanify_dcatus_mock.call_count == len(harvest_source.records)
        assert interface_record.id == test_record.id
        assert interface_record.status == "error"
        assert interface_errors[0].type == "DCATUSToCKANException"

    # ruff: noqa: F401
    @patch("harvester.harvest.ckan", ckanapi.RemoteCKAN("mock_address"))
    def test_ckan_sync_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()
        harvest_source.sync()

        test_record = [x for x in harvest_source.records if x.identifier == "cftc-dc1"][
            0
        ]

        interface_record = interface.get_harvest_record(test_record.id)

        interface_errors = interface.get_harvest_record_errors_by_record(test_record.id)

        assert interface_record.id == test_record.id
        assert interface_record.status == "error"
        assert interface_errors[0].type == "SynchronizeException"

    @patch("harvester.harvest.ckan")
    @patch("harvester.utils.ckan_utils.uuid")
    def test_validate_nested_exception_handling(
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
            Exception("Some other error occurred"),
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
        harvest_source.compare()
        harvest_source.sync()
        harvest_source.do_report()

        harvest_records = interface.get_harvest_records_by_job(job_id)
        records_with_errors = [
            record for record in harvest_records if record.status == "error"
        ]
        job_err = interface.get_harvest_job_errors_by_job(job_id)
        record_err = interface.get_harvest_record_errors_by_job(job_id)

        record_error, identifier, source_raw = record_err[0]
        title = json.loads(source_raw).get("title", None)
        assert len(job_err) == 0
        assert len(record_err) == 1
        assert record_error.type == "SynchronizeException"
        assert identifier == "cftc-dc2"
        assert title == "Commitment of Traders"
        assert record_error.harvest_record_id == records_with_errors[0].id
        assert (
            harvest_records[1].id == records_with_errors[0].id
        )  ## assert it's the second record that threw the exception, which validates our package_create mock
