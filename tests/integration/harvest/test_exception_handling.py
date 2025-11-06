# ruff: noqa: F841

import smtplib
from unittest.mock import Mock, patch

import pytest
from requests.exceptions import HTTPError
from requests.models import Response

from harvester.exceptions import (
    ExtractExternalException,
    ExtractInternalException,
    SendNotificationException,
)
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
            harvest_source.acquire_minimum_external_data()

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

        harvest_source.store_records_as_internal = Mock()
        harvest_source.store_records_as_internal.side_effect = Exception("Broken")

        with pytest.raises(ExtractInternalException) as e:
            harvest_source.acquire_minimum_internal_data()

        assert harvest_job.status == "error"

        harvest_error = interface.get_harvest_job_errors_by_job(harvest_job.id)[0]
        assert harvest_error.type == "ExtractInternalException"

    def test_no_source_info_exception(self, job_data_dcatus):
        with pytest.raises(ExtractInternalException) as e:
            HarvestSource(job_data_dcatus["id"])

    def test_send_notification_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus_bad_url,
        job_data_dcatus_bad_url,
    ):
        """
        Test that an exception is raised when sending notification emails fails.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_bad_url)
        harvest_job = interface.add_harvest_job(job_data_dcatus_bad_url)

        harvest_source = HarvestSource(harvest_job.id)

        job_results = {
            "records_added": 1,
            "records_updated": 2,
            "records_deleted": 0,
            "records_ignored": 0,
            "records_errored": 0,
            "records_validated": 3,
        }

        harvest_source.notification_emails = ["user@example.com"]

        with patch(
            "harvester.utils.general_utils.smtplib.SMTP",
            side_effect=smtplib.SMTPConnectError(421, "Cannot connect"),
        ):
            with pytest.raises(SendNotificationException) as exc_info:
                harvest_source.send_notification_emails(job_results)

            assert "Error preparing or sending notification emails" in str(
                exc_info.value
            )


def make_http_error(status_code):
    response = Response()
    response.status_code = status_code
    return HTTPError(f"{status_code} Error", response=response)


class TestHarvestRecordExceptionHandling:
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
        harvest_source.acquire_data_sources()

        external_records_to_process = harvest_source.external_records_to_process()

        # there's only 1
        test_record = list(external_records_to_process)[0]
        test_record.compare()
        test_record.validate()

        interface_record = interface.get_harvest_record(test_record.id)
        interface_errors = interface.get_harvest_record_errors_by_record(test_record.id)
        assert interface_record.id == interface_errors[0].harvest_record_id
        assert interface_record.status == "error"
        assert interface_errors[0].type == "ValidationError"
