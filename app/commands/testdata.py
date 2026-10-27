import json

import click
from flask import Blueprint

from database.interface import HarvesterDBInterface
from database.models import db

testdata = Blueprint("testdata", __name__)

db_interface = HarvesterDBInterface()


## Load Test Data
# TODO move this into its own file when you break up routes
@testdata.cli.command("load_test_data")
@click.option(
    "--reset",
    is_flag=True,
    default=False,
    help="Drop and recreate the database before loading fixtures.",
)
def fixtures(reset: bool) -> None:
    """
    Load database fixtures from JSON.

    Use --reset to wipe the database and recreate all tables before loading.
    """
    from tests.generate_fixtures import generate_dynamic_fixtures, iso_fixture_data_ui

    if reset:
        db.drop_all()
        db.create_all()
        click.echo("Database reset: all tables dropped and recreated.")

    fixture = generate_dynamic_fixtures()
    fixture = iso_fixture_data_ui(fixture)

    for item in fixture["organization"]:
        db_interface.add_organization(item)
    for item in fixture["source"]:
        db_interface.add_harvest_source(item)
    for item in fixture["job"]:
        db_interface.add_harvest_job(item)
    for item in fixture["job_error"]:
        db_interface.add_harvest_job_error(item)
    for item in fixture["record"]:
        db_interface.add_harvest_record(item)
    for item in fixture["record_error"]:
        db_interface.add_harvest_record_error(item)
    for item in fixture["dataset"]:
        db_interface.insert_dataset(item)

    click.echo("Done.")


@testdata.cli.command("load_demo_issues")
def load_demo_issues() -> None:
    """
    Load a small, separate demo harvest source/job with realistic record
    identifiers and messages covering error-only, warning-only, and
    combined error+warning records. Useful for eyeballing the "Errors"/
    "Warnings" card sections (#6476) locally without hand-writing records.

    Kept out of `load_test_data` on purpose: that command's output is
    asserted against byte-for-byte by the playwright suite, so adding
    records there would change source/job counts those tests hardcode.
    Run this separately, after `load_test_data`, when you want to look at
    the issue cards in a browser.
    """
    organization = db_interface.pget_organizations(paginate=False)[0]

    url = "http://localhost:80/dcatus/demo-issues.json"
    existing = db_interface.get_harvest_source_by_url(url)
    if existing:
        db_interface.delete_harvest_source(existing.id)

    source = db_interface.add_harvest_source(
        {
            "name": "Demo Issue Examples",
            "notification_emails": ["email@example.com"],
            "organization_id": organization.id,
            "frequency": "manual",
            "url": url,
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
            "notification_frequency": "always",
        }
    )
    job = db_interface.add_harvest_job(
        {
            "harvest_source_id": source.id,
            "status": "complete",
        }
    )

    demo_records = [
        {
            "identifier": "usda-crop-yield-estimates-2024",
            "title": "USDA Crop Yield Estimates 2024",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'description'.",
                ),
            ],
        },
        {
            "identifier": "noaa-coastal-flood-risk-zones",
            "title": "NOAA Coastal Flood Risk Zones",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'modified'.",
                ),
                (
                    "warning",
                    "SpatialTransformationException",
                    "Unable to reproject spatial bounding box for "
                    "Virginia Beach, VA.",
                ),
            ],
        },
        {
            "identifier": "epa-air-quality-monitoring-stations",
            "title": "EPA Air Quality Monitoring Stations",
            "issues": [
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'air-quality' appears more than once.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'monitoring' appears more than once.",
                ),
                (
                    "warning",
                    "invalid_media_type",
                    "Distribution media type 'application/xls' is not a "
                    "valid IANA media type.",
                ),
            ],
        },
        {
            "identifier": "doi-national-parks-boundaries-2024",
            "title": "DOI National Parks Boundaries 2024",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'bureauCode'.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'parks' appears more than once.",
                ),
            ],
        },
        {
            "identifier": "dot-bridge-inspection-records",
            "title": "DOT Bridge Inspection Records",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset identifier does not match the expected "
                    "pattern for this publisher.",
                ),
            ],
        },
        {
            "identifier": "hud-fair-market-rents-2025",
            "title": "HUD Fair Market Rents 2025",
            "issues": [
                (
                    "warning",
                    "invalid_media_type",
                    "Distribution media type 'text/plain' is not a valid "
                    "IANA media type for format 'CSV'.",
                ),
            ],
        },
        {
            "identifier": "usgs-earthquake-event-catalog",
            "title": "USGS Earthquake Event Catalog",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'temporal'.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'seismic' appears more than once.",
                ),
            ],
        },
        {
            "identifier": "nps-visitor-use-statistics",
            "title": "NPS Visitor Use Statistics",
            "issues": [
                (
                    "warning",
                    "deprecated_license",
                    "License URL uses an outdated Creative Commons "
                    "version; consider CC BY 4.0.",
                ),
            ],
        },
        {
            "identifier": "cdc-wastewater-surveillance-data",
            "title": "CDC Wastewater Surveillance Data",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'bureauCode'.",
                ),
            ],
        },
        {
            "identifier": "faa-airport-noise-complaints",
            "title": "FAA Airport Noise Complaints",
            "issues": [
                (
                    "warning",
                    "invalid_media_type",
                    "Distribution media type 'application/vnd.ms-excel' "
                    "is not a valid IANA media type.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'noise' appears more than once.",
                ),
            ],
        },
        {
            "identifier": "treasury-federal-spending-by-agency",
            "title": "Treasury Federal Spending by Agency",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Distribution downloadURL returned HTTP 404.",
                ),
            ],
        },
        {
            "identifier": "doe-renewable-energy-capacity-2024",
            "title": "DOE Renewable Energy Capacity 2024",
            "issues": [
                (
                    "warning",
                    "SpatialTransformationException",
                    "Unable to reproject spatial bounding box for the "
                    "Alaska panhandle region.",
                ),
            ],
        },
        {
            "identifier": "fda-food-recall-notices",
            "title": "FDA Food Recall Notices",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'contactPoint'.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'recall' appears more than once.",
                ),
                (
                    "warning",
                    "invalid_media_type",
                    "Distribution media type 'application/xml' is not a "
                    "valid IANA media type for format 'JSON'.",
                ),
            ],
        },
        {
            "identifier": "ssa-disability-determination-rates",
            "title": "SSA Disability Determination Rates",
            "issues": [
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'disability' appears more than once.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'ssa' appears more than once.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'benefits' appears more than once.",
                ),
            ],
        },
    ]

    for demo in demo_records:
        record = db_interface.add_harvest_record(
            {
                "identifier": demo["identifier"],
                "harvest_job_id": job.id,
                "harvest_source_id": source.id,
                "action": "create",
                "status": "error",
                "source_raw": json.dumps(
                    {"title": demo["title"], "identifier": demo["identifier"]}
                ),
            }
        )
        for severity, issue_type, message in demo["issues"]:
            db_interface.add_harvest_record_error(
                {
                    "harvest_record_id": record.id,
                    "harvest_job_id": job.id,
                    "type": issue_type,
                    "message": message,
                    "severity": severity,
                }
            )

    dcatus3_job = _load_demo_dcatus3_records(organization)

    click.echo(f"Done. View at /harvest_job/{job.id} and /harvest_job/{dcatus3_job.id}")


def _load_demo_dcatus3_records(organization) -> None:
    """A DCAT-US 3.0 demo source -- the "Record Type" card field only shows
    on dcatus3.0 sources, since only that schema distinguishes "dataset"
    from "data_service"/"catalog_record"/"data_series" records (#6476).
    """
    url = "http://localhost:80/dcatus/demo-dcatus3-issues.json"
    existing = db_interface.get_harvest_source_by_url(url)
    if existing:
        db_interface.delete_harvest_source(existing.id)

    source = db_interface.add_harvest_source(
        {
            "name": "Demo DCAT-US 3.0 Examples",
            "notification_emails": ["email@example.com"],
            "organization_id": organization.id,
            "frequency": "manual",
            "url": url,
            "schema_type": "dcatus3.0",
            "source_type": "document",
            "notification_frequency": "always",
        }
    )
    job = db_interface.add_harvest_job(
        {
            "harvest_source_id": source.id,
            "status": "complete",
        }
    )

    demo_records = [
        {
            "identifier": "noaa-buoy-observations-2025",
            "title": "NOAA Buoy Observations 2025",
            "record_type": "dataset",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "Dataset is missing required field 'description'.",
                ),
            ],
        },
        {
            "identifier": "noaa-buoy-observations-api",
            "title": "NOAA Buoy Observations API",
            "record_type": "data_service",
            "issues": [
                (
                    "warning",
                    "invalid_media_type",
                    "servesDataset media type 'application/xls' is not a "
                    "valid IANA media type.",
                ),
            ],
        },
        {
            "identifier": "noaa-marine-weather-catalog",
            "title": "NOAA Marine Weather Catalog",
            "record_type": "catalog_record",
            "issues": [
                (
                    "error",
                    "ValidationException",
                    "CatalogRecord is missing required field 'modified'.",
                ),
                (
                    "warning",
                    "duplicate_keyword",
                    "Keyword 'marine' appears more than once.",
                ),
            ],
        },
        {
            "identifier": "noaa-buoy-observations-series",
            "title": "NOAA Buoy Observations Series",
            "record_type": "data_series",
            "issues": [
                (
                    "warning",
                    "SpatialTransformationException",
                    "Unable to reproject spatial bounding box for "
                    "member dataset 'noaa-buoy-observations-2025'.",
                ),
            ],
        },
    ]

    for demo in demo_records:
        record = db_interface.add_harvest_record(
            {
                "identifier": demo["identifier"],
                "harvest_job_id": job.id,
                "harvest_source_id": source.id,
                "record_type": demo["record_type"],
                "action": "create",
                "status": "error",
                "source_raw": json.dumps(
                    {"title": demo["title"], "identifier": demo["identifier"]}
                ),
            }
        )
        for severity, issue_type, message in demo["issues"]:
            db_interface.add_harvest_record_error(
                {
                    "harvest_record_id": record.id,
                    "harvest_job_id": job.id,
                    "type": issue_type,
                    "message": message,
                    "severity": severity,
                }
            )

    return job
