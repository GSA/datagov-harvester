import click
from flask import Blueprint

from database.interface import HarvesterDBInterface
from harvester.lib.load_manager import LoadManager

job = Blueprint("harvest_job", __name__)

db = HarvesterDBInterface()


@job.cli.command("pause-scheduling")
def pause_harvest_scheduling():
    """Prevent new harvest CF tasks from starting."""
    db.set_harvest_scheduling_paused(True)
    click.echo("Harvest task scheduling is paused.")


@job.cli.command("resume-scheduling")
@click.option(
    "--start-jobs",
    is_flag=True,
    help="Immediately start queued jobs after enabling scheduling.",
)
def resume_harvest_scheduling(start_jobs: bool):
    """Allow new harvest CF tasks to start."""
    db.set_harvest_scheduling_paused(False)
    click.echo("Harvest task scheduling is enabled.")
    if start_jobs:
        LoadManager()._start_new_jobs()
        click.echo("Checked for queued harvest jobs.")


@job.cli.command("scheduling-status")
def harvest_scheduling_status():
    """Show whether new harvest CF tasks may start."""
    status = "paused" if db.is_harvest_scheduling_paused() else "enabled"
    click.echo(f"Harvest task scheduling is {status}.")


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
