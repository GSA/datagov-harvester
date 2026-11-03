"""The public validator must report what a real harvest of the same catalog
reports beyond schema errors: duplicate identifiers and warnings
(GSA/data.gov#6380). Both sides use the test database's place lookup here, so
this covers the record pipeline; the CSV place lookup is tested separately."""

import json
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from dcatus_validation.catalog_issues import find_catalog_issues
from harvester.harvest import harvest_job_starter
from tests.conftest import HARVEST_SOURCE_URL

EXAMPLE_DIR = Path(__file__).resolve().parents[3] / "example_data" / "dcatus"

CASES = [
    ("dcatus3_0_validator_parity.json", "dcatus3.0", "dcatus3.0 catalog"),
    (
        "dcatus_validator_parity.json",
        "dcatus1.1: federal",
        "dcatus1.1: federal dataset",
    ),
    ("dcatus3_0_warning.json", "dcatus3.0", "dcatus3.0 catalog"),
    (
        "dcatus_spatial_no_translate.json",
        "dcatus1.1: federal",
        "dcatus1.1: federal dataset",
    ),
]


@pytest.mark.parametrize("filename, source_schema, validator_schema", CASES)
@patch("harvester.harvest.HarvestSource.send_notification_emails")
def test_validator_reports_what_harvest_reports(
    _send_notification_emails,
    interface,
    organization_data,
    filename,
    source_schema,
    validator_schema,
):
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(
        {
            "id": str(uuid.uuid4()),
            "name": f"Parity {filename}",
            "notification_emails": ["email@example.com"],
            "organization_id": organization_data["id"],
            "frequency": "daily",
            "url": f"{HARVEST_SOURCE_URL}/dcatus/{filename}",
            "schema_type": source_schema,
            "source_type": "document",
            "notification_frequency": "always",
        }
    )
    job = interface.add_harvest_job({"status": "new", "harvest_source_id": source.id})

    harvest_job_starter(job.id, "harvest")

    harvested = interface.get_harvest_record_errors_by_job(job.id, paginate=False)
    harvest_duplicates = sorted(
        identifier
        for error, identifier, _ in harvested
        if error.type == "DuplicateIdentifierException"
    )
    harvest_warnings = sorted(
        (identifier, error.type, error.message)
        for error, identifier, _ in harvested
        if error.severity == "warning"
    )

    # every case is authored to produce issues, so equality below isn't vacuous
    assert harvest_duplicates or harvest_warnings

    catalog = json.loads((EXAMPLE_DIR / filename).read_text())
    duplicate_errors, warnings, incomplete = find_catalog_issues(
        catalog, validator_schema, place_lookup=interface.get_geo_from_string
    )

    assert not incomplete
    assert sorted(identifier for identifier, _ in duplicate_errors) == (
        harvest_duplicates
    )
    assert (
        sorted((w.identifier, w.warning_type, w.message) for w in warnings)
        == harvest_warnings
    )
