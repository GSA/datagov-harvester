#!/usr/bin/env python3
"""
Generate test fixtures with dynamic dates for the last 7 days.
This keeps test data fresh while preserving IDs and relationships.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict
from uuid import UUID


def generate_dynamic_fixtures() -> Dict[str, Any]:
    """Generate fixtures with dates from the last 7 days."""

    # Base fixture template - preserves all IDs and relationships
    base_fixtures = {
        "organization": [
            {
                "name": "Test Org",
                "logo": "https://raw.githubusercontent.com/GSA/datagov-harvester/refs/heads/main/app/static/assets/img/placeholder-organization.png",
                "organization_type": "Federal Government",
                "id": "d925f84d-955b-4cb7-812f-dcfd6681a18f",
            }
        ],
        "source": [
            {
                "id": "2f2652de-91df-4c63-8b53-bfced20b276b",
                "name": "Test Source",
                "notification_emails": ["email@example.com"],
                "organization_id": "d925f84d-955b-4cb7-812f-dcfd6681a18f",
                "frequency": "daily",
                "url": "http://localhost:80/dcatus/dcatus.json",
                "schema_type": "dcatus1.1: federal",
                "source_type": "document",
                "notification_frequency": "always",
            }
        ],
        "job": [],
        "job_error": [
            {
                "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
                "message": "error reading records from harvest database",
                "type": "ExtractInternalException",
            }
        ],
        "record": [],
        "record_error": [],
    }

    # Generate jobs with dates from the last 7 days
    now = datetime.now()
    job_templates = [
        {
            "id": "6bce761c-7a39-41c1-ac73-94234c139c76",
            "status": "error",
            "days_ago": 4,
            "duration_minutes": 0,
            "records_total": 10,
            "records_added": 2,
            "records_updated": 0,
            "records_deleted": 0,
            "records_errored": 8,
            "records_ignored": 0,
            "records_validated": 0,
        },
        {
            "status": "new",
            "days_ago": 0,
            "duration_minutes": None,  # No finish date for new jobs
        },
        {
            "id": "8def432a-9b21-4d56-87e4-1c3a5b7f8901",
            "status": "complete",
            "days_ago": 3,
            "duration_minutes": 1,
            "records_total": 85,
            "records_added": 23,
            "records_updated": 12,
            "records_deleted": 3,
            "records_errored": 5,
            "records_ignored": 42,
            "records_validated": 80,
        },
        {
            "id": "1a2b3c4d-5e6f-7890-abcd-ef1234567890",
            "status": "complete",
            "days_ago": 2,
            "duration_minutes": 0,
            "records_total": 45,
            "records_added": 0,
            "records_updated": 0,
            "records_deleted": 0,
            "records_errored": 45,
            "records_ignored": 0,
            "records_validated": 0,
        },
        {
            "id": "9f8e7d6c-5b4a-3928-1765-fedcba098765",
            "status": "complete",
            "days_ago": 6,
            "duration_minutes": 2,
            "records_total": 67,
            "records_added": 15,
            "records_updated": 28,
            "records_deleted": 7,
            "records_errored": 2,
            "records_ignored": 15,
            "records_validated": 65,
        },
        {
            "id": "4e5f6a7b-8c9d-0123-4567-890abcdef123",
            "status": "in_progress",
            "days_ago": 0,
            "duration_minutes": 4,
            "records_total": 0,
            "records_added": 0,
            "records_updated": 0,
            "records_deleted": 0,
            "records_errored": 0,
            "records_ignored": 0,
            "records_validated": 0,
        },
    ]

    # Generate additional jobs to fill out the week
    additional_job_ids = [str(UUID(_, version=4)) for _ in [
        "7c8d9e0f-1a2b-3c4d-5e6f-789012345678",
        "2b3c4d5e-6f7a-8b9c-0d1e-2f3456789abc",
        "5d6e7f8a-9b0c-1d2e-3f4a-5b6789cdef01",
        "0a1b2c3d-4e5f-6789-abcd-ef0123456789",
        "3f4a5b6c-7d8e-9f01-2345-6789abcdef12",
        "6b7c8d9e-0f1a-2b3c-4d5e-6f789012345a",
        "9e0f1a2b-3c4d-5e6f-7890-abcdef123456",
        "c2d3e4f5-6a7b-8c9d-0e1f-23456789abcd",
        "f5a6b7c8-9d0e-1f2a-3b4c-56789def0123",
        "56789def-9d0e-1f2a-3b4c-56789def0123",
    ]]

    for i, job_id in enumerate(additional_job_ids):
        job_templates.append(
            {
                "id": job_id,
                "status": "complete",
                "days_ago": (i % 7) + 1,  # Spread across the week
                "duration_minutes": (i % 5) + 1,
                "records_total": 30 + (i * 5),
                "records_added": 10 + (i % 10),
                "records_updated": 5 + (i % 15),
                "records_deleted": i % 8,
                "records_errored": i % 10,
                "records_ignored": 5 + (i % 5),
                "records_validated": 25 + (i * 4),
            }
        )

    # Convert templates to actual job records
    for template in job_templates:
        created_date = now - timedelta(days=template["days_ago"])

        job = {
            "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
            "status": template["status"],
            "date_created": created_date.strftime("%Y-%m-%d %H:%M:%S.%f"),
        }

        # Add ID if specified
        if "id" in template:
            job["id"] = template["id"]

        # Add finish date if job is completed or has duration
        if template.get("duration_minutes") is not None:
            finished_date = created_date + timedelta(
                minutes=template["duration_minutes"]
            )
            job["date_finished"] = finished_date.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Add record counts for jobs that have them
        for field in [
            "records_total",
            "records_added",
            "records_updated",
            "records_deleted",
            "records_errored",
            "records_ignored",
            "records_validated",
        ]:
            if field in template:
                job[field] = template[field]

        base_fixtures["job"].append(job)

    # Generate corresponding records for the main test job
    test_job_id = "6bce761c-7a39-41c1-ac73-94234c139c76"
    source_id = "2f2652de-91df-4c63-8b53-bfced20b276b"

    # Add successful records
    for i in range(1, 3):  # 2 successful records
        base_fixtures["record"].append(
            {
                "id": f"09f073b3-00e3-4147-ba69-a5d0fd7ce02{i}",
                "ckan_id": str(1234 + i - 1),
                "identifier": f"test_identifier-{8 + i}",
                "harvest_job_id": test_job_id,
                "harvest_source_id": source_id,
                "action": "create",
                "status": "success",
                "source_raw": (
                    f'{{"title": "test-{7 + i}", ' f'"identifier": "test-{7 + i}"}}'
                ),
            }
        )

    # Add error records
    error_record_ids = [str(UUID(_, version=4)) for _ in [
        "0779c855-df20-49c8-9108-66359d82b77c",
        "c218c965-3670-45c8-bfcd-f852d71ed917",
        "e1f603cc-8b6b-483f-beb4-86bda5462b79",
        "1c004473-0802-4f22-a16d-7a2d7559719e",
        "deb12fa0-d812-4d6e-98f4-d4f7d776c6b3",
        "27b5d5d6-808b-4a8c-ae4a-99f118e282dd",
        "c232a2ca-6344-4692-adc2-29f618a2eff3",
        "95021355-bad0-442b-98e9-475ecd849033",
    ]]

    for i, record_id in enumerate(error_record_ids):
        base_fixtures["record"].append(
            {
                "id": record_id,
                "identifier": f"test_identifier-{i + 1}",
                "harvest_job_id": test_job_id,
                "harvest_source_id": source_id,
                "action": "create",
                "status": "error",
                "source_raw": f'{{"title": "test-{i}", "identifier": "test-{i}"}}',
            }
        )

        # Add corresponding record error
        base_fixtures["record_error"].append(
            {
                "id": f"3ccb48db-21fc-427a-9ec7-36b0d0f621d{i}",
                "harvest_record_id": record_id,
                "harvest_job_id": test_job_id,
                "message": "record is invalid",
                "type": "ValidationException",
            }
        )

    return base_fixtures


def main():
    """Generate and save dynamic fixtures."""
    fixtures = generate_dynamic_fixtures()

    # Save to fixtures.json
    fixtures_path = os.path.join(os.path.dirname(__file__), "fixtures.json")
    with open(fixtures_path, "w") as f:
        json.dump(fixtures, f, indent=2)

    print("Generated fresh fixtures with dates from the last 7 days")
    print(f"Saved to: {fixtures_path}")


if __name__ == "__main__":
    main()
