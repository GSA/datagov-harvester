import csv
import io
import json
from datetime import datetime

import pytest


@pytest.fixture
def job_with_many_errors(interface_with_fixture_json):
    # Pagination is by distinct record, so spread errors across many
    # records (one page's worth of page-1 + page-2) rather than piling
    # them onto a single record.
    job = interface_with_fixture_json.get_first_harvest_job_by_filter({})
    for r in range(15):
        record = interface_with_fixture_json.add_harvest_record(
            {
                "harvest_job_id": job.id,
                "harvest_source_id": job.source.id,
                "identifier": f"many-errors-record-{r}",
                "status": "error",
            }
        )
        for i in range(5):
            interface_with_fixture_json.add_harvest_record_error(
                {
                    "type": "testing",
                    "message": f"Error {r}-{i}",
                    "harvest_job_id": job.id,
                    "harvest_record_id": record.id,
                }
            )
    return job


@pytest.fixture
def job_with_transformation_error(interface_with_fixture_json):
    job = interface_with_fixture_json.get_first_harvest_job_by_filter({})
    record_id = job.records[0].id
    # add 1 ISO transformation error
    interface_with_fixture_json.add_harvest_record_error(
        {
            "type": "TransformationException",
            "message": "failed to transform",
            "harvest_job_id": job.id,
            "harvest_record_id": record_id,
        }
    )

    return job


class TestViewHarvestJob:
    def test_harvest_source_name(self, client, job):
        """The harvest source's name appear on the harvest job page."""
        resp = client.get(f"/harvest_job/{job.id}")
        assert job.source.name in resp.text
        assert f'a href="/harvest_source/{job.source.id}"' in resp.text

    def test_download_large_record_errors_csv(self, client, interface, job):
        """Test downloading CSV for a job with over 5000 harvest record errors."""
        # Use the existing job fixture (which already has organization, source,
        # and job created)
        harvest_job = job
        harvest_source = job.source

        # Create a sample harvest record to associate errors with
        record_data = {
            "harvest_job_id": harvest_job.id,
            "harvest_source_id": harvest_source.id,
            "identifier": "test-record-001",
            "source_raw": json.dumps(
                {
                    "title": "Test Dataset Title",
                    "description": "A test dataset for error testing",
                }
            ),
            "status": "error",
        }
        harvest_record = interface.add_harvest_record(record_data)

        # Create over 5000 harvest record errors to test memory optimization
        num_errors = 5200  # Back to original large number for real test
        error_messages = [
            "Validation error: Missing required field",
            "Transformation error: Invalid date format",
            "Schema error: Field exceeds maximum length",
            "Network error: Connection timeout",
            "Parse error: Invalid JSON structure",
        ]

        # Create errors in batches to avoid potential database issues
        batch_size = 500
        for batch_start in range(0, num_errors, batch_size):
            batch_end = min(batch_start + batch_size, num_errors)

            for i in range(batch_start, batch_end):
                error_data = {
                    "harvest_job_id": harvest_job.id,
                    "harvest_record_id": harvest_record.id,
                    "type": "ValidationException",
                    "message": f"{error_messages[i % 5]} (Error #{i+1})",
                    "date_created": datetime.now(),
                }
                interface.add_harvest_record_error(error_data)

        # Test the CSV download endpoint
        resp = client.get(f"/api/v1/harvest_job/{harvest_job.id}/errors/record")

        # Verify response is successful
        assert resp.status_code == 200

        # Verify content type and headers
        assert resp.content_type == "text/csv; charset=utf-8"
        assert "attachment" in resp.headers.get("Content-Disposition", "")
        assert f"{harvest_job.id}_record_errors.csv" in resp.headers.get(
            "Content-Disposition", ""
        )

        # Parse the CSV response to verify content
        csv_content = resp.data.decode("utf-8")
        csv_reader = csv.reader(io.StringIO(csv_content))

        # Check header row
        header = next(csv_reader)
        expected_header = [
            "record_error_id",
            "identifier",
            "title",
            "harvest_record_id",
            "record_error_type",
            "severity",
            "message",
            "date_created",
        ]
        assert header == expected_header

        # Count data rows and verify we get all errors
        data_rows = list(csv_reader)
        assert len(data_rows) == num_errors

        # Verify every error was streamed without relying on database row order
        expected_messages = {
            f"{error_messages[i % 5]} (Error #{i + 1})" for i in range(num_errors)
        }
        assert {row[6] for row in data_rows} == expected_messages

        # All errors belong to the same harvest record and have the same type
        for row in data_rows:
            assert row[1] == "test-record-001"  # identifier
            assert row[2] == "Test Dataset Title"  # title
            assert row[3] == str(harvest_record.id)  # harvest_record_id
            assert row[4] == "ValidationException"  # record_error_type
            assert row[5] == "error"  # severity

        # Test that memory wasn't overwhelmed by checking response was properly streamed
        # (The fact that we got a complete response with 5200+ rows indicates streaming
        # worked)
        assert len(csv_content) > 100000  # Should be a substantial amount of data

    def test_download_job_errors_csv(self, client, interface, job):
        """Test downloading CSV for job errors (smaller dataset test)."""
        job_error = interface.add_harvest_job_error(
            {
                "harvest_job_id": job.id,
                "type": "JobError",
                "message": "Test job error message",
            }
        )

        # Test the CSV download endpoint for job errors
        resp = client.get(f"/api/v1/harvest_job/{job.id}/errors/job")

        # Verify response is successful
        assert resp.status_code == 200

        # Verify content type and headers
        assert resp.content_type == "text/csv; charset=utf-8"
        assert "attachment" in resp.headers.get("Content-Disposition", "")
        assert f"{job.id}_job_errors.csv" in resp.headers.get("Content-Disposition", "")

        # Parse the CSV response to verify structure
        csv_content = resp.data.decode("utf-8")
        csv_reader = csv.reader(io.StringIO(csv_content))

        # Check header row
        header = next(csv_reader)
        expected_header = [
            "harvest_job_id",
            "date_created",
            "job_error_type",
            "message",
            "harvest_job_error_id",
        ]
        assert header == expected_header

        data_rows = list(csv_reader)
        assert len(data_rows) == 1
        assert data_rows[0][0] == str(job.id)
        assert data_rows[0][2] == "JobError"
        assert data_rows[0][3] == "Test job error message"
        assert data_rows[0][4] == str(job_error.id)

    def test_download_errors_invalid_type(self, client, job):
        """Test error handling for invalid error type."""
        resp = client.get(f"/api/v1/harvest_job/{job.id}/errors/invalid_type")

        assert resp.status_code == 400
        assert "Invalid error type" in resp.data.decode("utf-8")

    def test_job_error_pagination(self, client, job_with_many_errors):
        resp = client.get(f"/harvest_job/{job_with_many_errors.id}")
        # page=2 should be in the "next" button
        assert f'hx-get="/harvest_job/{job_with_many_errors.id}?page=2"' in resp.text

        # page=1 should be in the "previous" button
        resp = client.get(f"/harvest_job/{job_with_many_errors.id}?page=2")
        assert f'hx-get="/harvest_job/{job_with_many_errors.id}?page=1"' in resp.text

    def test_record_error_summary(self, client, job_with_many_errors):
        resp = client.get(f"/harvest_job/{job_with_many_errors.id}")
        # fixture errors are of type "testing"
        assert "Record issue type summary" in resp.text
        assert "Severity" in resp.text
        assert "Number of issues" in resp.text
        assert "testing" in resp.text

    def test_record_warnings_display_on_job_page(self, client, interface, job):
        harvest_record = interface.add_harvest_record(
            {
                "harvest_job_id": job.id,
                "harvest_source_id": job.source.id,
                "identifier": "warning-record-001",
                "status": "success",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": job.id,
                "harvest_record_id": harvest_record.id,
                "type": "SpatialTransformationException",
                "message": "unable to spatially fix Virginia",
                "severity": "warning",
            }
        )
        interface.update_harvest_job(job.id, {"records_warned": 1})

        resp = client.get(f"/harvest_job/{job.id}")

        assert "Records warned" in resp.text
        assert "<td>1</td>" in resp.text
        assert "unable to spatially fix Virginia" in resp.text
        assert "SpatialTransformationException" in resp.text
        assert "warning" in resp.text

    def test_record_with_error_and_warning_renders_one_card_two_sections(
        self, client, interface, job
    ):
        """A record with one error + one warning of different types is one
        card with an Error section and a Warning section, per #6476."""
        harvest_record = interface.add_harvest_record(
            {
                "harvest_job_id": job.id,
                "harvest_source_id": job.source.id,
                "identifier": "mixed-issue-record-001",
                "status": "error",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": job.id,
                "harvest_record_id": harvest_record.id,
                "type": "ValidationException",
                "message": "record is invalid",
                "severity": "error",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": job.id,
                "harvest_record_id": harvest_record.id,
                "type": "duplicate_keyword",
                "message": "keyword appears twice",
                "severity": "warning",
            }
        )

        resp = client.get(f"/harvest_job/{job.id}")

        assert resp.text.count("mixed-issue-record-001") == 1
        assert resp.text.count('class="issue-section"') == 2
        assert "usa-tag bg-error-dark text-white" in resp.text
        assert "usa-tag bg-warning" in resp.text
        assert "record is invalid" in resp.text
        assert "keyword appears twice" in resp.text

    def test_record_with_two_same_type_warnings_renders_one_section(
        self, client, interface, job
    ):
        """A record with two warnings of the identical type is one card
        with one section containing both messages, per #6476."""
        harvest_record = interface.add_harvest_record(
            {
                "harvest_job_id": job.id,
                "harvest_source_id": job.source.id,
                "identifier": "same-type-warning-record-001",
                "status": "error",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": job.id,
                "harvest_record_id": harvest_record.id,
                "type": "duplicate_keyword",
                "message": "first duplicate keyword",
                "severity": "warning",
            }
        )
        interface.add_harvest_record_error(
            {
                "harvest_job_id": job.id,
                "harvest_record_id": harvest_record.id,
                "type": "duplicate_keyword",
                "message": "second duplicate keyword",
                "severity": "warning",
            }
        )

        resp = client.get(f"/harvest_job/{job.id}")

        assert resp.text.count("same-type-warning-record-001") == 1
        assert resp.text.count('class="issue-section"') == 1
        assert "first duplicate keyword" in resp.text
        assert "second duplicate keyword" in resp.text

    def test_job_accept_json(self, client, job_with_many_errors):
        resp = client.get(
            f"/api/v1/harvest_job/{job_with_many_errors.id}",
            headers={"Accept": "application/json"},
        )
        assert resp.is_json
        assert "id" in resp.json

    def test_iso_transformation_no_exception(
        self, client, caplog, job_with_transformation_error
    ):
        # fixture job has 8 distinct error records, all of which fit on
        # page 1 now that pagination is by record, not row.
        client.get(
            f"/harvest_job/{job_with_transformation_error.id}",
        )
        assert (
            "transformation error produced no source_transform. "
            "can't json load an xml doc" in caplog.messages
        )
