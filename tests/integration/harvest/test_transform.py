from unittest.mock import Mock, patch

import pytest
import requests
from deepdiff import DeepDiff

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
            if record.identifier == "http://localhost:80/iso_2_waf/invalid_iso2.xml":
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

    def test_mdtranslator_down(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """
        Test that the transformation fails when mdtranslator is down and
        the domain is resolving to a 404 page.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()

        for record in harvest_source.records:
            if record.identifier == "http://localhost:80/iso_2_waf/invalid_iso2.xml":
                test_record = record

        with patch("requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "404 Client Error: Not Found for url: http://mdtranslator:5000/transform"
            )
            mock_post.return_value = mock_response
            with pytest.raises(TransformationException) as e:
                test_record.transform()

            assert (
                str(e.value.msg)
                == "record failed to transform because of unexpected status code: 404"
            )

    def test_mdtranslator_bad_request(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """
        Test that the transformation fails when mdtranslator returns a 422
        status code with validation errors.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()

        for record in harvest_source.records:
            if record.identifier == "http://localhost:80/iso_2_waf/invalid_iso2.xml":
                test_record = record

        with patch("requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = 422
            mock_response.json.return_value = {
                "readerStructureMessages": [
                    "WARNING: Some warnings in the structure of the record",
                    "ERROR: Invalid spatial representation type",
                ],
                "readerValidationMessages": [
                    "WARNING: Some validation warnings in the record",
                    "ERROR: Invalid spatial representation type",
                ],
            }
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "422 Client Error: Unprocessable record for url: http://mdtranslator:5000/transform"
            )
            mock_post.return_value = mock_response
            with pytest.raises(TransformationException) as e:
                test_record.transform()

            error_messages = (
                "record failed to transform: structure messages: WARNING: "
                "Some warnings in the structure of the record,"
                " ERROR: Invalid spatial representation type \nvalidation "
                "messages: WARNING: Some validation warnings"
                " in the record, ERROR: Invalid spatial representation type"
            )
            assert str(e.value.msg) == error_messages

    def test_mdtranslator_down_timeout(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """
        Test that the transformation fails when mdtranslator is down and
        the request times out."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()

        for record in harvest_source.records:
            if record.identifier == "http://localhost:80/iso_2_waf/invalid_iso2.xml":
                test_record = record

        with patch("requests.post") as mock_post:
            mock_post.side_effect = requests.Timeout()

            with pytest.raises(TransformationException) as e:
                test_record.transform()

            assert (
                str(e.value.msg) == "record failed to transform due to request timeout"
            )

    def test_mdtranslator_unexpected_error(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """
        Test to see if an unexpected error arises during transformation,
        it raises a TransformationException with the error message."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.extract()
        harvest_source.compare()

        for record in harvest_source.records:
            if record.identifier == "http://localhost:80/iso_2_waf/invalid_iso2.xml":
                test_record = record

        with patch("requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = 1
            mock_post.return_value = mock_response
            with pytest.raises(TransformationException) as e:
                test_record.transform()

            assert str(e.value.msg) == (
                "record failed to transform with error: 'int' object is not "
                "subscriptable"
            )
