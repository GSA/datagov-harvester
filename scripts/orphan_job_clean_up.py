import os
import sys
from typing import Optional, Tuple

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

# because this runs outside of the context the flask app
# we need this line to help resolve the python path
# to find the harvester modules
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface
from harvester.utils.general_utils import get_datetime

DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)


@click.command()
@click.option(
    "--job-id",
    type=str,
    help="ID of the job to delete. This flag can be provided"
    " multiple times to delete multiple jobs.",
    required=False,
    multiple=True,
)
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    type=bool,
    help="Perform a dry run without deleting jobs.",
)
def delete_jobs(job_id: Optional[Tuple[str]], dry_run: bool) -> None:
    """
    Delete harvest jobs by ID or all potentially orphaned jobs.
    Dry run mode is enabled by default to prevent accidental deletions.
    can be run as:
    `python scripts/orphan_job_clean_up.py --job-id <job_id1> --job-id <job_id2>`
    or to delete all orphaned jobs:
    `python scripts/orphan_job_clean_up.py`
    include `--no-dry-run` to delete the jobs.
    """
    jobs = []
    interface: HarvesterDBInterface = HarvesterDBInterface(session=session)
    if job_id:
        for id in job_id:
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
            click.echo("Use --no-dry-run to delete these jobs.")
        else:
            for job in jobs:
                interface.update_harvest_job(
                    job.id,
                    {
                        "status": "error",
                        "date_finished": get_datetime(),
                    },
                )
                interface.add_harvest_job_error(
                    {
                        "date_created": get_datetime(),
                        "type": "FailedJobCleanup",
                        "message": "In-progress job stopped running "
                        "for an unknown reason.",
                        "harvest_job_id": job.id,
                    }
                )
                click.echo(f"Deleted job with ID: {job.id}")
    else:
        click.echo("No jobs found to delete.")
    interface.close()


if __name__ == "__main__":
    delete_jobs()
