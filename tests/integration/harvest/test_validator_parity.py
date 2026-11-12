"""The public validator must report what a real harvest of the same catalog
reports beyond schema errors: duplicate identifiers and warnings
(GSA/data.gov#6380). Both sides use the test database's place lookup here, so
this covers the record pipeline; the place-name lookup is tested separately."""

import json
from unittest.mock import patch

import pytest

from dcatus_validation.catalog_issues import (
    DUPLICATE_IDENTIFIER_TYPE,
    find_catalog_issues,
)
from harvester.harvest import VALIDATOR_SCHEMA_NAMES, harvest_job_starter
from tests.conftest import EXAMPLE_DATA


@pytest.mark.parametrize(
    "source_fixture",
    [
        "source_data_dcatus3_0_validator_parity",
        "source_data_dcatus_validator_parity",
        "source_data_dcatus3_0_warning",
        "source_data_dcatus_cant_translate_spatial",
    ],
)
@patch("harvester.harvest.HarvestSource.send_notification_emails")
def test_validator_reports_what_harvest_reports(
    _send_notification_emails, request, interface, organization_data, source_fixture
):
    source_data = request.getfixturevalue(source_fixture)
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data)
    job = interface.add_harvest_job({"status": "new", "harvest_source_id": source.id})

    harvest_job_starter(job.id, "harvest")

    harvested = interface.get_harvest_record_errors_by_job(job.id, paginate=False)
    harvest_duplicates = sorted(
        identifier
        for error, identifier, _ in harvested
        if error.type == DUPLICATE_IDENTIFIER_TYPE
    )
    harvest_warnings = sorted(
        (identifier, error.type, error.message)
        for error, identifier, _ in harvested
        if error.severity == "warning"
    )

    # every case is authored to produce issues, so equality below isn't vacuous
    assert harvest_duplicates or harvest_warnings

    filename = source_data["url"].rsplit("/", 1)[-1]
    catalog = json.loads((EXAMPLE_DATA / "dcatus" / filename).read_text())
    duplicate_errors, warnings, incomplete = find_catalog_issues(
        catalog,
        VALIDATOR_SCHEMA_NAMES[source_data["schema_type"]],
        place_lookup=interface.get_geo_from_string,
    )

    assert not incomplete
    assert sorted(identifier for identifier, _ in duplicate_errors) == (
        harvest_duplicates
    )
    assert (
        sorted((w.identifier, w.warning_type, w.message) for w in warnings)
        == harvest_warnings
    )
