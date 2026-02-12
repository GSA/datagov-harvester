import click
from flask import Blueprint

from database.interface import HarvesterDBInterface

job = Blueprint("harvest_job", __name__)

db = HarvesterDBInterface()


## Harvet Job Management
@job.cli.command("delete")
@click.argument("id")
def cli_remove_harvest_job(id):
    """Remove a harvest job with a given id."""
    result = db.delete_harvest_job(id)
    if result:
        print(f"Triggered delete of harvest job with ID: {id}")
    else:
        print("Failed to delete harvest job")
