import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests
from deepdiff import DeepDiff

from database.models import HarvestRecordCurrentIssue
from harvester.exceptions import TransformationException
from harvester.harvest import HarvestSource, Record
from harvester.utils.general_utils import dataset_to_hash

INVALID_ISO2 = Path(__file__).parents[3] / "example_data/iso_2_waf/invalid_iso2.xml"


def _mdtranslator_422():
    response = Mock()
    response.status_code = 422
    response.json.return_value = {
        "readerStructureMessages": [],
        "readerValidationMessages": ["ERROR: XML file is not well formed"],
    }
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "422 Client Error: Unprocessable Entity"
    )
    return response


def _harvest_untransformable_record(interface, job_data, identifier):
    """Run one harvest job's worth of work for a single ISO record that
    mdTranslator rejects."""
    job = interface.add_harvest_job({**job_data, "id": str(uuid.uuid4())})
    harvest_source = HarvestSource(job.id)
    source_raw = INVALID_ISO2.read_text()
    record = Record(harvest_source, identifier, source_raw, dataset_to_hash(source_raw))
    with patch("requests.post", return_value=_mdtranslator_422()):
        record.harvest()
    return record


def _outstanding_issues(interface, source_id, identifier):
    return (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(harvest_source_id=source_id, identifier=identifier)
        .all()
    )


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
        # this test transforms ISO19115-1 & ISO19115-2 docs into DCATUS
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        iso_records = list(external_records_to_process)

        # Filter for the record with 'valid_iso2' in the identifier
        test_iso_2_record = next(
            (
                record
                for record in iso_records
                if "http://localhost:80/iso_2_waf/valid_iso2.xml" == record.identifier
            ),
            None,
        )

        if test_iso_2_record is None:
            raise ValueError("Could not find record with 'valid_iso2' in identifier")

        test_iso_2_record.transform()

        assert test_iso_2_record.mdt_msgs == ""
        assert DeepDiff(test_iso_2_record.transformed_data, iso19115_2_transform) == {}

        # "valid_iso1.xml" is always the second one
        test_iso_1_record = next(
            (
                record
                for record in iso_records
                if "http://localhost:80/iso_2_waf/valid_iso1.xml" == record.identifier
            ),
            None,
        )

        test_iso_1_record.transform()

        assert test_iso_1_record.mdt_msgs == ""
        assert DeepDiff(test_iso_1_record.transformed_data, iso19115_1_transform) == {}

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

    def test_failed_transform_does_not_pile_up_outstanding_issues(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """A file that keeps failing to transform is one outstanding issue,
        replaced on every run, not one more per run."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        source_id = source_data_waf_iso19115_2["id"]
        identifier = "http://localhost:80/iso_2_waf/invalid_iso2.xml"

        for _ in range(3):
            record = _harvest_untransformable_record(
                interface, job_data_waf_iso19115_2, identifier
            )
            assert record.status == "error"

        issues = _outstanding_issues(interface, source_id, identifier)
        assert [issue.type for issue in issues] == ["TransformationException"]
        assert issues[0].harvest_record_id == record.id

    def test_failed_transform_replaces_earlier_outstanding_issues(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """Issues left by an earlier attempt are replaced by the transform
        failure, like any other reprocessed record."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        source_id = source_data_waf_iso19115_2["id"]
        identifier = "http://localhost:80/iso_2_waf/invalid_iso2.xml"

        earlier_job = interface.add_harvest_job(
            {**job_data_waf_iso19115_2, "id": str(uuid.uuid4())}
        )
        earlier = interface.add_harvest_record(
            {
                "identifier": identifier,
                "harvest_job_id": earlier_job.id,
                "harvest_source_id": source_id,
                "status": "error",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": earlier_job.id,
                "harvest_record_id": earlier.id,
                "type": "ValidationException",
                "message": "$, 'description' is a required property",
            }
        )
        assert len(_outstanding_issues(interface, source_id, identifier)) == 1

        record = _harvest_untransformable_record(
            interface, job_data_waf_iso19115_2, identifier
        )

        issues = _outstanding_issues(interface, source_id, identifier)
        assert [issue.type for issue in issues] == ["TransformationException"]
        assert issues[0].harvest_record_id == record.id

    def test_failed_transform_clears_outstanding_issues_piled_up_by_earlier_runs(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        """Sources already carry one transform error per earlier run for the
        same file. The next run leaves just its own."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        source_id = source_data_waf_iso19115_2["id"]
        identifier = "http://localhost:80/iso_2_waf/invalid_iso2.xml"

        for _ in range(3):
            earlier_job = interface.add_harvest_job(
                {**job_data_waf_iso19115_2, "id": str(uuid.uuid4())}
            )
            earlier = interface.add_harvest_record(
                {
                    "identifier": identifier,
                    "harvest_job_id": earlier_job.id,
                    "harvest_source_id": source_id,
                    "status": "error",
                }
            )
            interface.add_harvest_record_error(
                {
                    "harvest_job_id": earlier_job.id,
                    "harvest_record_id": earlier.id,
                    "type": "TransformationException",
                    "message": "record failed to transform",
                }
            )
        assert len(_outstanding_issues(interface, source_id, identifier)) == 3

        record = _harvest_untransformable_record(
            interface, job_data_waf_iso19115_2, identifier
        )

        issues = _outstanding_issues(interface, source_id, identifier)
        assert [issue.type for issue in issues] == ["TransformationException"]
        assert issues[0].harvest_record_id == record.id
