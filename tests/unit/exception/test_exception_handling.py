from datetime import datetime
from unittest.mock import patch

import ckanapi
import pytest

import harvester
from harvester.exceptions import (
    DCATUSToCKANException,
    ExtractCKANSourceException,
    ExtractHarvestSourceException,
    SynchronizeException,
    ValidationException,
)
from harvester.harvest import HarvestSource

# ruff: noqa: F401
# ruff: noqa: F841


class TestExceptionHandling:
    def test_add_harvest_source(self, db_interface):
        organization = {
            "id": "919bfb9e-89eb-4032-9abf-eee54be5a00c",
            "logo": "url for the logo",
            "name": "GSA",
        }

        harvest_source = {
            "id": "9347a852-2498-4bee-b817-90b8e93c9cec",
            "name": "harvest_source_test",
            "notification_emails": ["admin@example.com"],
            "organization_id": "919bfb9e-89eb-4032-9abf-eee54be5a00c",
            "frequency": "daily",
            "url": "http://example.com",
            "schema_type": "strict",
            "source_type": "json",
        }

        harvest_job = {
            "harvest_source_id": "9347a852-2498-4bee-b817-90b8e93c9cec",
            "id": "1db556ff-fb02-438b-b7d2-ad914e1f2531",
            "status": "in_progress",
            "date_created": datetime.utcnow(),
            "date_finished": datetime.utcnow(),
            "records_added": 0,
            "records_updated": 0,
            "records_deleted": 0,
            "records_errored": 0,
            "records_ignored": 0,
        }
        db_interface.add_organization(organization)
        db_interface.add_harvest_source(
            harvest_source, harvest_source["organization_id"]
        )
        db_interface.add_harvest_job(harvest_job, harvest_job["harvest_source_id"])

    def test_bad_harvest_source_url_exception(self, bad_url_dcatus_config):
        harvest_source = HarvestSource(**bad_url_dcatus_config)

        with pytest.raises(ExtractHarvestSourceException) as e:
            harvest_source.get_harvest_records_as_id_hash()

    @patch("harvester.harvest.ckan", ckanapi.RemoteCKAN("mock_address"))
    def test_get_ckan_records_exception(self, bad_url_dcatus_config):
        # using bad_url_dcatus_config just to populate required fields
        harvest_source = HarvestSource(**bad_url_dcatus_config)

        with pytest.raises(ExtractCKANSourceException) as e:
            harvest_source.get_ckan_records_as_id_hash()

    def test_validation_exception(self, invalid_dcatus_config):
        harvest_source = HarvestSource(**invalid_dcatus_config)
        harvest_source.get_harvest_records_as_id_hash()

        test_record = harvest_source.records["null-spatial"]

        with pytest.raises(ValidationException) as e:
            test_record.validate()

    def test_dcatus_to_ckan_exception(self, invalid_dcatus_config):
        harvest_source = HarvestSource(**invalid_dcatus_config)
        harvest_source.get_harvest_records_as_id_hash()

        test_record = harvest_source.records["null-spatial"]

        with pytest.raises(DCATUSToCKANException) as e:
            test_record.ckanify_dcatus()

    @patch("harvester.harvest.ckan", ckanapi.RemoteCKAN("mock_address"))
    def test_synchronization_exception(self, dcatus_config):
        harvest_source = HarvestSource(**dcatus_config)
        harvest_source.get_harvest_records_as_id_hash()

        test_record = harvest_source.records["cftc-dc1"]
        test_record.operation = "create"

        with pytest.raises(SynchronizeException) as e:
            test_record.sync()
