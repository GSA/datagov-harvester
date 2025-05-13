from itertools import groupby
from unittest.mock import patch

import pytest
from deepdiff import DeepDiff
from jsonschema.exceptions import ValidationError

from harvester.harvest import HarvestSource, ckan_sync_tool
from harvester.utils.ckan_utils import create_ckan_resources
from harvester.utils.general_utils import dataset_to_hash, sort_dataset

# ruff: noqa: E501


class TestCKANLoad:
    @patch("harvester.harvest.ckan_sync_tool.ckan")
    def test_sync(
        self,
        CKANMock,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        interface,
        internal_compare_data,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}
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
            "validity": {True: 0, None: 0},
        }
        for key, group in groupby(
            harvest_source.records, lambda x: x.action if x.status != "error" else None
        ):
            results["action"][key] = sum(1 for _ in group)

        for key, group in groupby(harvest_source.records, lambda x: x.status):
            results["status"][key] = sum(1 for _ in group)

        for key, group in groupby(harvest_source.records, lambda x: x.valid):
            results["validity"][key] = sum(1 for _ in group)

        harvest_reporter = harvest_source.reporter.report()
        assert results["action"]["create"] == 6
        assert harvest_reporter["records_added"] == 6

        assert results["action"]["update"] == harvest_reporter["records_updated"] == 1

        assert results["action"]["delete"] == harvest_reporter["records_deleted"] == 1

        assert results["status"]["error"] == harvest_reporter["records_errored"] == 0

        assert (
            results["status"]["success"]
            == len(harvest_source.records) - harvest_reporter["records_errored"]
            == 8
        )

        assert results["validity"][True] == harvest_reporter["records_validated"] == 7

        assert (
            results["validity"][None]
            == len(harvest_source.records) - harvest_reporter["records_validated"]
            == 1
        )

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
                {
                    "key": "harvest_source_id",
                    "value": "2f2652de-91df-4c63-8b53-bfced20b276b",
                },
                {"key": "harvest_source_title", "value": "Test Source"},
                {"key": "identifier", "value": "cftc-dc1"},
                {"key": "source_datajson_identifier", "value": True},
                {"key": "title", "value": "Commitment of Traders"},
                {
                    "key": "description",
                    "value": "COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC",
                },
                {"key": "keyword", "value": "commitment of traders"},
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
                    "value": {
                        "fn": "Harold W. Hild",
                        "hasEmail": "mailto:hhild@CFTC.GOV",
                    },
                },
                {"key": "identifier", "value": "cftc-dc1"},
                {"key": "accessLevel", "value": "public"},
                {"key": "old-spatial", "value": "United States"},
                {
                    "key": "spatial",
                    "value": '{"type":"MultiPolygon","coordinates":[[[[-124.733253,24.544245],[-124.733253,49.388611],[-66.954811,49.388611],[-66.954811,24.544245],[-124.733253,24.544245]]]]}',
                },
                {"key": "isPartOf", "value": "some-collection-id"},
                {"key": "bureauCode", "value": "339:00"},
                {"key": "programCode", "value": "000:000"},
            ],
        }

        ckan_sync_tool.ckanify_record(test_record)
        assert DeepDiff(test_record.ckanified_metadata, expected_result) == {}

    def test_create_ckan_resources(self, dol_distribution_json):
        """
        Test that we are accurately parsing information from the
        distribution sections.
        """
        expected_resources = [
            {
                "description": "CSV resource for battery electric vehicles in WA",
                "name": "Battery Electric Vehicle CSV File",
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
                "mimetype": "text/csv",
            },
            {
                "description": "RDF resource for plug-in hybrid electric vehicles in WA",
                "name": "Plug-in Hybrid Electric Vehicles RDF file",
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.rdf?accessType=DOWNLOAD",
                "mimetype": "application/rdf+xml",
            },
            {
                "description": "JSON resource for BEVs & PHEVs in WA",
                "name": "BEVs & PHEVs JSON file",
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD",
                "mimetype": "application/json",
            },
            {
                "description": "XML resource for electric and hybrid vehicles in WA",
                "name": "Electric and Hybrid Vehicles XML file",
                "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.xml?accessType=DOWNLOAD",
                "mimetype": "application/xml",
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

    def test_duplicate_identifier_handled_as_error_record(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        duplicated_identifier_records,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(job.id)

        # Should not raise an exception
        harvest_source.external_records_to_id_hash(duplicated_identifier_records)

        # Only one unique record should be added to external_records
        assert len(harvest_source.external_records) == 1

        # Assert that errors was logged (the duplicate identifier)
        record_err = interface.get_harvest_record_errors_by_job(job.id)
        assert len(record_err) == 2

        # Check that each error message includes "Duplicate identifier"
        for error_entry in record_err:
            error_obj, identifier, _ = error_entry
            assert "Duplicate identifier" in error_obj.message