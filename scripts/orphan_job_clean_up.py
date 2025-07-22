import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

# because this runs outside of the context the flask app
# we need this line to help resolve the python path
# to find the harvester modules
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface
from harvester.lib.cf_handler import CFHandler
from harvester.utils.general_utils import get_datetime

DATABASE_URI = os.getenv("DATABASE_URI")
CF_API_URL = os.getenv("CF_API_URL")
CF_SERVICE_USER = os.getenv("CF_SERVICE_USER")
CF_SERVICE_AUTH = os.getenv("CF_SERVICE_AUTH")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)


def is_task_stale(task: Dict[str, Any], hours_threshold: int = 24) -> bool:
    """
    Check if a task hasn't been updated within the specified time threshold.
    """
    try:
        # Parse the updated_at timestamp (ISO 8601 format with Z suffix)
        updated_at_str = task.get("updated_at")
        if not updated_at_str:
            # If no updated_at, consider it stale
            return True

        # Parse the ISO timestamp - handle 'Z' suffix for UTC
        if updated_at_str.endswith("Z"):
            updated_at_str = updated_at_str[:-1] + "+00:00"

        updated_at = datetime.fromisoformat(updated_at_str)

        # Ensure it's timezone-aware (should be UTC)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        # Get current time in UTC
        now = datetime.now(timezone.utc)

        # Calculate time difference
        time_diff = now - updated_at
        threshold = timedelta(hours=hours_threshold)

        return time_diff > threshold

    except (ValueError, TypeError) as e:
        click.echo(f"Error parsing timestamp: {e}")
        # If we can't parse, assume it's stale
        return True


@click.command()
@click.option(
    "--job-id",
    type=str,
    help="ID of the job to delete. This flag can be provided"
    " multiple times to delete multiple jobs. MUST be the ID used in the Flask DB "
    "(GUID)",
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
    interface: HarvesterDBInterface = HarvesterDBInterface(session=session)
    handler = CFHandler(CF_API_URL, CF_SERVICE_USER, CF_SERVICE_AUTH)
    jobs = []

    # get RUNNING tasks from CF
    try:
        tasks = handler.get_running_app_tasks()
    except Exception as e:
        click.echo(f"Error fetching tasks: {e}")
        return

    # short circuit everything if no tasks are found
    if tasks:
        # loop through the tasks
        for task in tasks:
            # for clarity the cf task id is in sequence
            task_seq_id = task.get("id")
            # parse out guid from the task command (task guid != harvest job guid)
            uuid_pattern = (
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
                "[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
            )
            match = re.search(uuid_pattern, task.get("command", ""))
            harvest_task_guid = match.group(0) if match else None
            # if job_id is provided, and the harvest_task_guid is in the job_id (tuple)
            # and the task is stale, we stop the task.
            # if no job_id is provided, we stop the task if it is stale.
            if ((job_id and harvest_task_guid in job_id) and is_task_stale(task)) or (
                len(job_id) == 0 and is_task_stale(task)
            ):
                # update the db with the stopped job info
                job = interface.get_harvest_job(job_id=harvest_task_guid)
                # only stop and make db edits if not a dry run
                if dry_run is False:
                    # task sequence id is used to stop the task
                    handler.stop_task(task_seq_id)
                    if job:
                        jobs.append(job)
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
        if jobs:
            if dry_run:
                click.echo(
                    f"Dry run: would stop {len(jobs)} harvest job(s) with IDs: "
                    f"{', '.join([job.id for job in jobs])}"
                )
                click.echo("Use --no-dry-run to actually stop the jobs.")
            else:
                click.echo(
                    f"Stopped {len(jobs)} harvest job(s) with IDs: "
                    f"{', '.join([job.id for job in jobs])}"
                )
        else:
            click.echo("No tasks to be stopped")
            click.echo("Either bad Job ID(s) provided or no stale tasks found.")
    else:
        click.echo("No running tasks found.")
    interface.close()


if __name__ == "__main__":
    delete_jobs()
