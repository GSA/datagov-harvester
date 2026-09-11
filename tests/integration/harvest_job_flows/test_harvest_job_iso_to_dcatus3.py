"""Test full harvest flow for ISO sources producing DCAT 3.0 output."""

from harvester.harvest import HarvestSource


class TestISOToDcatus3Flow:
    def test_iso_waf_harvest_uses_dcatus3_validator(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """Test that ISO WAF sources use DCAT 3.0 validator."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)

        validator = harvest_source.validator_for("dataset")
        assert hasattr(validator, "schema")

        harvest_source.acquire_minimum_external_data()
        external_records = harvest_source.external_records_to_process()
        records_list = list(external_records)

        assert len(records_list) > 0

        found_valid = False
        for record in records_list:
            if record.identifier.endswith("/iso_2_waf/valid_iso2.xml"):
                record.transform()
                assert record.transformed_data is not None
                assert "@context" not in record.transformed_data
                found_valid = True
                break

        assert found_valid, "Should find and transform valid_iso2.xml"

    def test_dcatus1_1_sources_unaffected(
        self,
        interface,
        organization_data,
    ):
        """Regression test: verify DCAT 1.1 sources still work."""
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

        validator = harvest_source.validator_for("dataset")

        from jsonschema import Draft202012Validator

        assert isinstance(validator, Draft202012Validator)
