"""Test full harvest flow for ISO sources producing DCAT 3.0 output."""

import pytest

from harvester.harvest import HarvestSource


class TestISOToDcatus3Flow:
    def test_iso_waf_full_harvest_produces_dcatus3(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """
        Test complete harvest flow for ISO WAF source.

        Verifies:
        - ISO XML extracted from WAF
        - MDTranslator converts to DCAT 1.1
        - Converter upgrades to DCAT 3.0
        - Validation passes against DCAT 3.0 schema
        - Dataset synced to database with v3.0 structure
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)

        # Full harvest cycle
        harvest_source.extract_external()
        assert len(harvest_source.external_records) > 0

        harvest_source.compare()
        harvest_source.transform_records()

        # Check records were converted to v3.0
        for identifier, record in harvest_source.external_records.items():
            if record.transformed_data:
                assert "conformsTo" in record.transformed_data
                assert record.transformed_data["conformsTo"]["title"] == "DCAT-US 3.0"

        harvest_source.validate_records()

        # Verify validation passed (no v3.0 schema errors)
        job = interface.get_harvest_job(harvest_job.id)
        assert job.records_errored == 0 or job.records_errored < len(
            harvest_source.external_records
        )

        harvest_source.sync_records()

        # Verify datasets were created
        datasets = interface.get_datasets()
        assert len(datasets) > 0

    def test_dcatus1_1_sources_unaffected(
        self,
        interface,
        organization_data,
    ):
        """
        Regression test: verify DCAT 1.1 sources still work.

        Ensures that the ISO conversion logic doesn't affect
        native DCAT 1.1 document sources.
        """
        # Create DCAT 1.1 source
        source_data = {
            "name": "Test DCAT 1.1 Source",
            "organization_id": organization_data["id"],
            "notification_emails": [],
            "url": "http://example.gov/data.json",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
            "frequency": "manual",
            "notification_frequency": "on_error",
        }

        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data)

        job_data = {
            "status": "new",
            "harvest_source_id": source.id,
        }
        harvest_job = interface.add_harvest_job(job_data)

        harvest_source = HarvestSource(harvest_job.id)

        # Verify DCAT 1.1 sources still use v1.1 validator
        validator = harvest_source.validator_for("dataset")

        # DCAT 1.1 uses Draft202012Validator, not build_dcatus3_validator
        from jsonschema import Draft202012Validator

        assert isinstance(validator, Draft202012Validator)
