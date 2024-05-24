# ruff: noqa: F841

from unittest.mock import patch

import ckanapi
import pytest

import harvester
from harvester.exceptions import (
    DCATUSToCKANException,
    ExtractExternalException,
    ExtractInternalException,
    SynchronizeException,
    ValidationException,
)
from harvester.harvest import HarvestSource


def download_mock(_, __):
    return dict({"dataset": []})


class TestExceptionHandling:
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

    def test_no_source_info_exception(self, interface, job_data_dcatus):
        with pytest.raises(ExtractInternalException) as e:
            HarvestSource(job_data_dcatus["id"], interface)

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
        harvest_source.get_record_changes()
        harvest_source.write_compare_to_db()
        harvest_source.synchronize_records()

        interface_record = interface.get_harvest_record(
            harvest_source.internal_records_lookup_table[
                single_internal_record["identifier"]
            ]
        )
        interface_errors = interface.get_harvest_errors_by_record_id(
            harvest_source.internal_records_lookup_table[
                single_internal_record["identifier"]
            ]
        )
        assert (
            interface_record["id"]
            == harvest_source.internal_records_lookup_table[
                single_internal_record["identifier"]
            ]
        )
        assert interface_record["status"] == "error"
        assert interface_errors[0]["type"] == "SynchronizeException"

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
        harvest_source.get_record_changes()
        harvest_source.write_compare_to_db()
        harvest_source.synchronize_records()
        test_record = harvest_source.external_records["null-spatial"]

        interface_record = interface.get_harvest_record(
            harvest_source.internal_records_lookup_table[test_record.identifier]
        )
        interface_errors = interface.get_harvest_errors_by_record_id(
            harvest_source.internal_records_lookup_table[test_record.identifier]
        )
        assert (
            interface_record["id"]
            == harvest_source.internal_records_lookup_table[test_record.identifier]
        )
        assert interface_record["status"] == "error"
        assert interface_errors[0]["type"] == "ValidationException"

    def test_dcatus_to_ckan_exception(
        self,
        interface,
        organization_data,
        source_data_dcatus_invalid,
        job_data_dcatus_invalid,
    ):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus_invalid)
        harvest_job = interface.add_harvest_job(job_data_dcatus_invalid)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        test_record = harvest_source.external_records["null-spatial"]

        with pytest.raises(DCATUSToCKANException) as e:
            test_record.ckanify_dcatus()

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
        source = interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        test_record = harvest_source.external_records["cftc-dc1"]
        test_record.action = "create"

        with pytest.raises(SynchronizeException) as e:
            test_record.sync()
