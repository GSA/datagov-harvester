import pytest
from unittest.mock import patch

import harvester
from harvester.harvest import HarvestSource
from harvester.exceptions import (
    ExtractHarvestSourceException,
    ExtractCKANSourceException,
    ValidationException,
    DCATUSToCKANException,
    SynchronizeException,
)

import ckanapi

# ruff: noqa: F401
# ruff: noqa: F841


class TestExceptionHandling:
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
