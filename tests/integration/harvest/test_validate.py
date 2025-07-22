import pytest

from harvester.harvest import HarvestSource


@pytest.fixture
def valid_iso_2_record(
    interface, organization_data, source_data_waf_iso19115_2, job_data_waf_iso19115_2
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_waf_iso19115_2)
    harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

    harvest_source = HarvestSource(harvest_job.id)
    harvest_source.acquire_minimum_external_data()
    external_records_to_process = harvest_source.external_records_to_process()

    # "valid_iso2.xml" is always the last one
    yield list(external_records_to_process)[-1]


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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)
        assert test_record.validate()

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)
        assert test_record.validate()

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)

        assert not test_record.validate()

        # omitting the entire message for brevity
        # see the fixture for more details
        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]
        assert errors[0].message.startswith("<ValidationError: '\"<p align=\\'center\\'")
        assert errors[0].message.endswith(
            "is not valid under any of the given schemas'>"
        )

    def test_multiple_invalid(
        self,
        interface,
        organization_data,
        source_data_dcatus_multiple_invalid,
        job_data_dcatus_non_federal,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_multiple_invalid)
        harvest_job = interface.add_harvest_job(job_data_dcatus_non_federal)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)

        assert not test_record.validate()

        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]
        assert len(errors) == 3
        assert errors[-1].message.startswith("<ValidationError: '\"<p align=\\'center\\'")
        assert errors[-1].message.endswith(
            "is not valid under any of the given schemas'>"
        )

    def test_valid_transformed_iso(
        self,
        valid_iso_2_record,
    ):
        valid_iso_2_record.transform()
        assert valid_iso_2_record.validate()

    def test_invalid_transformed_iso(
        self,
        interface,
        valid_iso_2_record,
    ):
        valid_iso_2_record.transform()

        # we increased our contactPoint options in mdtranslator
        # so this actually gets pulled so deleting it here
        del valid_iso_2_record.transformed_data["contactPoint"]

        # validator logs exceptions when the dataset is invalid
        valid_iso_2_record.validate()
        errors = [
            e[0]  # returns a tuple, first is the error
            for e in interface.get_harvest_record_errors_by_job(
                valid_iso_2_record.harvest_source.job_id
            )
        ]
        assert (
            errors[0].message
            == "<ValidationError: \"'contactPoint' is a required property\">"
        )

    def test_transformed_iso_contact_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["contactPoint"]

        # now fill in the missing contactPoint
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()
        assert (
            "@gsa.gov"
            in valid_iso_2_record.transformed_data["contactPoint"]["hasEmail"]
        )
        assert valid_iso_2_record.transformed_data["contactPoint"][
            "hasEmail"
        ].startswith("mailto:")

    def test_transformed_iso_description_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["description"]

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert "No description" in valid_iso_2_record.transformed_data["description"]

    def test_transformed_iso_keyword_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["keyword"]

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert len(valid_iso_2_record.transformed_data["keyword"]) == 1
        assert valid_iso_2_record.transformed_data["keyword"][0] == "__"

    def test_transformed_iso_publisher_placeholder(
        self, organization_data, valid_iso_2_record
    ):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["publisher"]

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert valid_iso_2_record.transformed_data["publisher"] == {
            "name": organization_data["name"]
        }
