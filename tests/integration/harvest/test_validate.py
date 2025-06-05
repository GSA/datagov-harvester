import pytest

from harvester.exceptions import ValidationException
from harvester.harvest import HarvestSource


class TestValidateDataset:
    def test_validate_dcatus_federal(
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

        test_record = harvest_source.external_records["cftc-dc1"]
        test_record.validate()

        assert test_record.valid is True

    def test_validate_dcatus_non_federal(
        self,
        interface,
        organization_data,
        source_data_dcatus_single_record_non_federal,
        job_data_dcatus_non_federal,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record_non_federal)
        harvest_job = interface.add_harvest_job(job_data_dcatus_non_federal)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        test_record = harvest_source.external_records["cftc-dc1"]
        test_record.validate()

        assert test_record.valid is True

    def test_invalid_license_uri_dcatus_non_federal(
        self,
        interface,
        organization_data,
        source_data_dcatus_bad_license_uri,
        job_data_dcatus_non_federal,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_bad_license_uri)
        harvest_job = interface.add_harvest_job(job_data_dcatus_non_federal)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        test_id = (
            "https://www.arcgis.com/home/item.html?id=99731bb0369848169d98f31ce83fb0e2"
        )
        test_record = harvest_source.external_records[test_id]

        with pytest.raises(ValidationException) as e:
            test_record.validate()

        # omitting the entire message for brevity
        # see the fixture for more details
        assert e.value.msg.startswith(
            "<ValidationError: '\"<p align=\\'center\\'"
        ) and e.value.msg.endswith("is not valid under any of the given schemas'>")
        assert test_record.valid is False

    def test_valid_transformed_iso(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        iso2_name = "http://localhost:80/iso_2_waf/valid_iso2.xml"
        iso2_test_record = harvest_source.external_records[iso2_name]
        iso2_test_record.transform()
        iso2_test_record.validate()

        assert iso2_test_record.valid is True

    def test_invalid_transformed_iso(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        iso2_name = "http://localhost:80/iso_2_waf/valid_iso1.xml"
        iso2_test_record = harvest_source.external_records[iso2_name]
        iso2_test_record.transform()

        # we increased our contactPoint options in mdtranslator
        # so this actually gets pulled so deleting it here
        del iso2_test_record.transformed_data["contactPoint"]

        # validator throws an exception when the dataset is invalid
        with pytest.raises(ValidationException) as e:
            iso2_test_record.validate()
        assert (
            e.value.msg
            == "<ValidationError: \"'contactPoint' is a required property\">"
        )
