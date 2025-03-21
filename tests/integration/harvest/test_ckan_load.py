from itertools import groupby
from unittest.mock import patch

import pytest
from deepdiff import DeepDiff
from jsonschema.exceptions import ValidationError

from harvester.harvest import HarvestSource, Record
from harvester.utils.ckan_utils import create_ckan_resources
from harvester.utils.general_utils import dataset_to_hash, sort_dataset

# ruff: noqa: E501


class TestCKANLoad:
    def delete_mock(self):
        pass

    def update_mock(self):
        pass

    def create_mock(self):
        pass

    @patch.object(Record, "create_record", create_mock)
    @patch.object(Record, "update_record", update_mock)
    @patch.object(Record, "delete_record", delete_mock)
    def test_sync(
        self,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        interface,
        internal_compare_data,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        job_id = job_data_dcatus["id"]
        source_id = source_data_dcatus["id"]

        # prefill with records
        for record in internal_compare_data["records"]:
            data = {
                "identifier": record["identifier"],
                "harvest_job_id": job_id,
                "harvest_source_id": source_id,
                "source_hash": dataset_to_hash(sort_dataset(record)),
                "status": "success",
                "action": "create",
            }
            interface.add_harvest_record(data)

        harvest_source = HarvestSource(job_id)
        harvest_source.extract()
        harvest_source.compare()
        harvest_source.transform()
        harvest_source.validate()
        harvest_source.sync()
        harvest_source.report()

        results = {
            "action": {"create": 0, "update": 0, "delete": 0, None: 0},
            "status": {"success": 0, "error": 0, None: 0},
            "validity": {True: 0, False: 0},
        }
        for key, group in groupby(
            harvest_source.records, lambda x: x.action if x.status != "error" else None
        ):
            results["action"][key] = sum(1 for _ in group)

        for key, group in groupby(harvest_source.records, lambda x: x.status):
            results["status"][key] = sum(1 for _ in group)

        harvest_reporter = harvest_source.reporter.report()
        assert results["action"]["create"] == 6
        assert harvest_reporter["records_added"] == 6

        assert results["action"]["update"] == 1
        assert harvest_reporter["records_updated"] == 1

        assert results["action"]["delete"] == 1
        assert harvest_reporter["records_deleted"] == 1

        assert results["status"]["error"] == 0
        assert harvest_reporter["records_errored"] == 0

        # NOTE: we don't report this status, but it is not in sync b/c deletes aren't counted correctly
        assert results["status"]["success"] == 7
        assert len(harvest_source.records) - harvest_reporter["records_errored"] == 8

    def test_ckanify_dcatus(
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
        harvest_source.prepare_external_data()

        record = {
            "identifier": "cftc-dc1",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_source_id": job_data_dcatus["harvest_source_id"],
        }
        interface.add_harvest_record(record)
        harvest_source.extract()
        harvest_source.compare()
        test_record = [x for x in harvest_source.records if x.identifier == "cftc-dc1"][
            0
        ]

        expected_result = {
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
                {"key": "harvest_object_id", "value": test_record.id},
                {"key": "source_datajson_identifier", "value": True},
                {
                    "key": "harvest_source_id",
                    "value": "2f2652de-91df-4c63-8b53-bfced20b276b",
                },
                {"key": "harvest_source_title", "value": "Test Source"},
                {"key": "accessLevel", "value": "public"},
                {"key": "bureauCode", "value": "339:00"},
                {"key": "identifier", "value": "cftc-dc1"},
                {"key": "modified", "value": "R/P1W"},
                {"key": "programCode", "value": "000:000"},
                {
                    "key": "publisher_hierarchy",
                    "value": "U.S. Government > U.S. Commodity Futures Trading Commission",
                },
                {
                    "key": "publisher",
                    "value": "U.S. Commodity Futures Trading Commission",
                },
                {"key": "old-spatial", "value": "United States"},
                {
                    "key": "spatial",
                    "value": '{"type":"MultiPolygon","coordinates":[[[[-124.733253,24.544245],[-124.733253,49.388611],'
                    "[-66.954811,49.388611],[-66.954811,24.544245],[-124.733253,24.544245]]]]}",
                },
                {"key": "identifier", "value": "cftc-dc1"},
            ],
        }

        test_record.ckanify_dcatus()
        assert DeepDiff(test_record.ckanified_metadata, expected_result) == {}

    def test_create_ckan_resources(self, dol_distribution_json):
        """
        Test that we are accurately parsing information from the
        distribution sections.
        """
        expected_resources = [
            {
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
                "mimetype": "text/csv",
                "no_real_name": True,
                "name": "Web Resource",
            },
            {
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.rdf?accessType=DOWNLOAD",
                "mimetype": "application/rdf+xml",
                "no_real_name": True,
                "name": "Web Resource",
            },
            {
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD",
                "mimetype": "application/json",
                "no_real_name": True,
                "name": "Web Resource",
            },
            {
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.xml?accessType=DOWNLOAD",
                "mimetype": "application/xml",
                "no_real_name": True,
                "name": "Web Resource",
            },
        ]
        resources = create_ckan_resources(dol_distribution_json)
        assert resources == expected_resources

    def test_dcatus1_1_federal_validator_success_spatial_string(
        self,
        interface_with_fixture_json,
        job_data_dcatus,
        internal_compare_data,
    ):
        """
        This test is used to ensure that sources that use
        dcatus1.1: federal as their schema and contains a "spatial"
        attribute as a string that it passes validation.
        """
        # Set up the harvest source
        harvest_source = HarvestSource(job_data_dcatus["id"])
        # Confirm the correct schema type is used in the example
        assert harvest_source.schema_type == "dcatus1.1: federal"
        record = internal_compare_data["records"][0]
        # force in "spatial" attr as string
        record["spatial"] = "United States"
        harvest_source.validator.is_valid(record)
        assert harvest_source.validator.is_valid(record)

    def test_dcatus1_1_federal_validator_success_spatial_object(
        self,
        interface_with_fixture_json,
        job_data_dcatus,
        internal_compare_data,
    ):
        """
        This test is used to ensure that sources that use
        dcatus1.1: federal as their schema and contains a "spatial"
        attribute as a object with a "type" as a string
        and "coordinates" as an array of numbers
        that it passes validation.
        """
        harvest_source = HarvestSource(job_data_dcatus["id"])
        assert harvest_source.schema_type == "dcatus1.1: federal"
        record = internal_compare_data["records"][0]
        # force in "spatial" attr as json object
        record["spatial"] = {
            "coordinates": [[-81.0563, 34.9991], [-80.6033, 35.4024]],
            "type": "envelope",
        }
        harvest_source.validator.validate(record)
        assert harvest_source.validator.is_valid(record)

    def test_dcatus1_1_federal_validator_fails(
        self,
        interface_with_fixture_json,
        job_data_dcatus,
        internal_compare_data,
    ):
        """
        This test is used to ensure that sources that use
        dcatus1.1: federal as their schema and contains a "spatial"
        attribute that isn't a string or json object that meets
        the defined criteria, fails validation.
        """
        harvest_source = HarvestSource(job_data_dcatus["id"])
        assert harvest_source.schema_type == "dcatus1.1: federal"

        record = internal_compare_data["records"][0]
        # force in "spatial" attr as array of strings
        record["spatial"] = ["United States"]
        assert harvest_source.validator.is_valid(record) is False
        with pytest.raises(ValidationError):
            harvest_source.validator.validate(record)
