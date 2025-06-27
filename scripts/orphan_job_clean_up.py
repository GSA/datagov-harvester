import os
import sys
from typing import Optional

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface

DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)


@click.command()
@click.option("--job-id", type=str, help="ID of the job to delete.", required=False)
@click.option(
    "--dry-run",
    default=True,
    type=bool,
    help="Perform a dry run without deleting jobs.",
)
def delete_jobs(job_id: Optional[str], dry_run: bool) -> None:
    """
    Delete harvest jobs by ID or all potentially orphaned jobs.
    Dry run mode is enabled by default to prevent accidental deletions.
    """
    jobs = []
    interface: HarvesterDBInterface = HarvesterDBInterface(session=session)
    if job_id:
        job = interface.get_harvest_job(job_id=job_id)
        if job:
            jobs.append(job)
    else:
        jobs = interface.get_orphaned_harvest_jobs()

    if jobs:
        if dry_run:
            click.echo(
                "Dry run: The following jobs would be deleted: "
                f"{', '.join([job.id for job in jobs])}"
            )
        else:
            for job in jobs:
                interface.delete_harvest_job(job_id=job.id)
                click.echo(f"Deleted job with ID: {job.id}")
    else:
        click.echo("No jobs found to delete.")
    interface.close()


if __name__ == "__main__":
    delete_jobs()
