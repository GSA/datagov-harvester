import click
from flask import Blueprint

from app import deps

harvest = Blueprint("harvest", __name__)


@harvest.cli.command("start")
def cli_start_harvest_scheduler():
    """Queue and start due harvest jobs."""
    deps.load_manager.start()
    click.echo("Harvest scheduler run completed.")
