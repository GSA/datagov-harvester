from unittest.mock import Mock, patch

import pytest
import requests

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "invalid_iso2.xml" is always the first one
        test_record = next(external_records_to_process)
        test_record.compare()

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

        # 'ExternalRecordToClass' caused by decoding error. not needed for this test.
        del job_errors[0]

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
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        iso_records = list(external_records_to_process)

        test_iso_2_record = next(
            (
                record
                for record in iso_records
                if record.identifier.endswith("/iso_2_waf/valid_iso2.xml")
            ),
            None,
        )

        if test_iso_2_record is None:
            raise ValueError("Could not find record with 'valid_iso2' in identifier")

        test_iso_2_record.transform()

        assert test_iso_2_record.mdt_msgs == ""
        assert test_iso_2_record.transformed_data is not None
        assert "@context" not in test_iso_2_record.transformed_data
        assert "title" in test_iso_2_record.transformed_data
        assert "identifier" in test_iso_2_record.transformed_data

        test_iso_1_record = next(
            (
                record
                for record in iso_records
                if record.identifier.endswith("/iso_2_waf/valid_iso1.xml")
            ),
            None,
        )

        test_iso_1_record.transform()

        assert test_iso_1_record.mdt_msgs == ""
        assert test_iso_1_record.transformed_data is not None
        assert "@context" not in test_iso_1_record.transformed_data
        assert "title" in test_iso_1_record.transformed_data
        assert "identifier" in test_iso_1_record.transformed_data

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "invalid_iso2.xml" is always the first one
        test_record = next(external_records_to_process)

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "invalid_iso2.xml" is always the first one
        test_record = next(external_records_to_process)

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "invalid_iso2.xml" is always the first one
        test_record = next(external_records_to_process)

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
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "invalid_iso2.xml" is always the first one
        test_record = next(external_records_to_process)

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

    def test_iso19115_to_dcatus3_conversion(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
        iso19115_2_transform,
    ):
        """Test ISO records converted from DCAT 1.1 to 3.0 after MDTranslator."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        iso_records = list(external_records_to_process)
        test_record = next(
            (
                record
                for record in iso_records
                if record.identifier.endswith("/iso_2_waf/valid_iso2.xml")
            ),
            None,
        )

        if test_record is None:
            pytest.fail("No valid_iso2.xml record found in harvest")

        test_record.transform()

        transformed = test_record.transformed_data

        assert "@context" not in transformed
        assert "describedBy" not in transformed

        if "accessLevel" in transformed:
            assert "accessRights" in transformed

        assert transformed is not None
        assert "title" in transformed
        assert "description" in transformed

    def test_iso19115_validates_against_dcatus3_schema(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """Test that converted ISO records validate against DCAT 3.0 schema."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)

        validator = harvest_source.validator_for("dataset")
        assert hasattr(validator, "schema")

        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        iso_records = list(external_records_to_process)
        test_record = next(
            (
                record
                for record in iso_records
                if record.identifier.endswith("/iso_2_waf/valid_iso2.xml")
            ),
            None,
        )

        if test_record is None:
            pytest.fail("No valid_iso2.xml record found in harvest")

        test_record.transform()

        result = test_record.validate()

        assert result is not None

    def test_iso19115_field_transformations(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """Test specific field transformations from DCAT 1.1 to 3.0."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        iso_records = list(external_records_to_process)
        test_record = next(
            (
                record
                for record in iso_records
                if record.identifier.endswith("/iso_2_waf/valid_iso2.xml")
            ),
            None,
        )

        if test_record is None:
            pytest.fail("No valid_iso2.xml record found in harvest")

        test_record.transform()
        transformed = test_record.transformed_data

        if "license" in transformed and "distribution" in transformed:
            for dist in transformed["distribution"]:
                if isinstance(dist, dict):
                    assert "license" in dist

        if "temporal" in transformed:
            temporal = transformed["temporal"]
            if isinstance(temporal, dict):
                if "startDate" in temporal:
                    assert "T" in temporal["startDate"]
                if "endDate" in temporal:
                    assert "T" in temporal["endDate"]
