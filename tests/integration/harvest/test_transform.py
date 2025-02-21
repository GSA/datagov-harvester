import pytest

from harvester.exceptions import TransformationException
from harvester.harvest import HarvestSource

from deepdiff import DeepDiff


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

    def test_valid_transform_iso19115_docs(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
        iso19115_2_transform,
        iso19115_1_transform,
    ):
        # this test transforms ISO19115-1 & ISO19115-2 docs into DCATUS
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        iso2_name = "http://localhost:80/iso_2_waf/valid_iso2.xml"
        iso2_test_record = harvest_source.external_records[iso2_name]
        iso2_test_record.transform()

        assert iso2_test_record.mdt_msgs == ""
        assert DeepDiff(iso2_test_record.transformed_data, iso19115_2_transform) == {}

        iso1_name = "http://localhost:80/iso_2_waf/valid_iso1.xml"
        iso1_test_record = harvest_source.external_records[iso1_name]
        iso1_test_record.transform()

        assert iso1_test_record.mdt_msgs == ""
        assert DeepDiff(iso1_test_record.transformed_data, iso19115_1_transform) == {}
