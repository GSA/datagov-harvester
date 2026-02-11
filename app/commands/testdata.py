import click
from flask import Blueprint

from database.interface import HarvesterDBInterface

testdata = Blueprint("testdata", __name__)

db = HarvesterDBInterface()


## Load Test Data
# TODO move this into its own file when you break up routes
@testdata.cli.command("load_test_data")
def fixtures():
    """Load database fixtures from JSON."""

    from tests.generate_fixtures import generate_dynamic_fixtures

    fixture = generate_dynamic_fixtures()

    for item in fixture["organization"]:
        db.add_organization(item)
    for item in fixture["source"]:
        db.add_harvest_source(item)
    for item in fixture["job"]:
        db.add_harvest_job(item)
    for item in fixture["job_error"]:
        db.add_harvest_job_error(item)
    for item in fixture["record"]:
        db.add_harvest_record(item)
    for item in fixture["record_error"]:
        db.add_harvest_record_error(item)

    click.echo("Done.")
