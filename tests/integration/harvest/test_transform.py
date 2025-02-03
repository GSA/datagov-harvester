import pytest

from harvester.exceptions import TransformationException
from harvester.harvest import HarvestSource


class TestTransform:
    def test_invalid_transform_iso19115_2(
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
        harvest_source.extract()
        harvest_source.compare()

        for record in harvest_source.records:
            if record.identifier == "http://localhost:80/iso_2_waf/invalid_47662.xml":
                test_record = record

        # ruff: noqa: F841
        with pytest.raises(TransformationException) as e:
            test_record.transform()

        assert test_record.transformed_data is None

        expected = (
            "structure messages:  \nvalidation messages: WARNING: "
            "ISO19115-2 reader: element 'role' is missing valid nil reason within "
            "'CI_ResponsibleParty'"
        )

        assert test_record.mdt_msgs == expected

        expected_error_msg = (
            "record failed to transform: structure messages:  \n"
            "validation messages: WARNING: ISO19115-2 reader: element "
            "'role' is missing valid nil reason within 'CI_ResponsibleParty'"
        )

        job_errors = interface.get_harvest_record_errors_by_job(harvest_job.id)
        assert len(job_errors) == 1
        assert job_errors[0][0].message == expected_error_msg

        record = interface.get_harvest_record(job_errors[0][0].record.id)
        assert record.status == "error"

    def test_valid_transform_iso19115_2(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
        iso19115_2_transform,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        name = "http://localhost:80/iso_2_waf/valid_47598.xml"
        test_record = harvest_source.external_records[name]
        test_record.transform()

        assert test_record.mdt_msgs == ""
        assert test_record.transformed_data == iso19115_2_transform
