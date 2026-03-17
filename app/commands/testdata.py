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
    from tests.generate_fixtures import generate_dynamic_fixtures

    if reset:
        db.drop_all()
        db.create_all()
        click.echo("Database reset: all tables dropped and recreated.")

    fixture = generate_dynamic_fixtures()

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
