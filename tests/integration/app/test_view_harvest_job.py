import csv
import io
import json
from datetime import datetime

import pytest


@pytest.fixture
def job_with_many_errors(interface_with_fixture_json):
    # Need at lest seven pages of errors to get the next and previous
    # arrows, so let's make 100 of them.
    job = interface_with_fixture_json.get_first_harvest_job_by_filter({})
    record_id = job.records[0].id
    for i in range(100):
        interface_with_fixture_json.add_harvest_record_error(
            {
                "type": "testing",
                "message": f"Error {i}",
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
        resp = client.get(f"/harvest_job/{harvest_job.id}/errors/record")

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
            "message",
            "date_created",
        ]
        assert header == expected_header

        # Count data rows and verify we get all errors
        data_rows = list(csv_reader)
        assert len(data_rows) == num_errors

        # Verify a few sample rows have expected data
        first_row = data_rows[0]
        assert first_row[1] == "test-record-001"  # identifier
        assert first_row[2] == "Test Dataset Title"  # title
        assert first_row[3] == str(harvest_record.id)  # harvest_record_id
        assert first_row[4] == "ValidationException"  # record_error_type
        assert "Error #1" in first_row[5]  # message contains error number

        # Verify last row to ensure all data was streamed
        last_row = data_rows[-1]
        assert last_row[1] == "test-record-001"  # identifier
        assert last_row[2] == "Test Dataset Title"  # title
        assert f"Error #{num_errors}" in last_row[5]  # message contains last error num

        # Test that memory wasn't overwhelmed by checking response was properly streamed
        # (The fact that we got a complete response with 5200+ rows indicates streaming
        # worked)
        assert len(csv_content) > 100000  # Should be a substantial amount of data

    def test_download_job_errors_csv(self, client, interface, job):
        """Test downloading CSV for job errors (smaller dataset test)."""
        # Test the CSV download endpoint for job errors
        resp = client.get(f"/harvest_job/{job.id}/errors/job")

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

    def test_download_errors_invalid_type(self, client, job):
        """Test error handling for invalid error type."""
        resp = client.get(f"/harvest_job/{job.id}/errors/invalid_type")

        assert resp.status_code == 400
        assert "Invalid error type" in resp.data.decode("utf-8")

    def test_job_error_pagination(self, client, job_with_many_errors):
        resp = client.get(f"/harvest_job/{job_with_many_errors.id}")
        # page=2 should be in the "next" button
        assert f'hx-get="/harvest_job/{job_with_many_errors.id}?page=2"' in resp.text

        # page=1 should be in the "previous" button
        resp = client.get(f"/harvest_job/{job_with_many_errors.id}?page=2")
        assert f'hx-get="/harvest_job/{job_with_many_errors.id}?page=1"' in resp.text
