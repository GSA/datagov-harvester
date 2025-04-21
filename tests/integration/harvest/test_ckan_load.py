import pytest
from deepdiff import DeepDiff
from jsonschema.exceptions import ValidationError

from harvester.exceptions import ExtractExternalException
from harvester.harvest import HarvestSource
from harvester.utils.ckan_utils import create_ckan_resources

# ruff: noqa: E501


class TestCKANLoad:
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

    def test_duplicate_identifier_raises_extract_exception(
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

        with pytest.raises(ExtractExternalException) as exc_info:
            harvest_source.external_records_to_id_hash(duplicated_identifier_records)

        # Assert that the raised exception contains a duplicate identifier message
        assert "Duplicate identifier" in str(exc_info.value)
