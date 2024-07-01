import logging
import os
from datetime import datetime

from database.interface import HarvesterDBInterface
from harvester.lib.cf_handler import CFHandler
from harvester.utils.general_utils import create_future_date

CF_API_URL = os.getenv("CF_API_URL")
CF_SERVICE_USER = os.getenv("CF_SERVICE_USER")
CF_SERVICE_AUTH = os.getenv("CF_SERVICE_AUTH")
HARVEST_RUNNER_APP_GUID = os.getenv("HARVEST_RUNNER_APP_GUID")
CF_INSTANCE_INDEX = os.getenv("CF_INSTANCE_INDEX")

MAX_TASKS_COUNT = 3

interface = HarvesterDBInterface()

logger = logging.getLogger("harvest_admin")


def create_cf_handler():
    # check for correct env vars to init CFHandler
    if not CF_API_URL or not CF_SERVICE_USER or not CF_SERVICE_AUTH:
        logger.info("CFHandler is not configured correctly. Check your env vars.")
        return
    return CFHandler(CF_API_URL, CF_SERVICE_USER, CF_SERVICE_AUTH)


def create_task(job_id, cf_handler=None):
    task_contract = {
        "app_guuid": HARVEST_RUNNER_APP_GUID,
        "command": f"python harvester/harvest.py {job_id}",
        "task_id": f"harvest-job-{job_id}",
    }
    if cf_handler is None:
        cf_handler = create_cf_handler()

    cf_handler.start_task(**task_contract)
    updated_job = interface.update_harvest_job(job_id, {"status": "in_progress"})
    message = f"Updated job {updated_job.id} to in_progress"
    logger.info(message)
    return message


def trigger_manual_job(source_id):
    source = interface.get_harvest_source(source_id)
    jobs_in_progress = interface.get_all_harvest_jobs_by_filter(
        {"harvest_source_id": source.id, "status": "in_progress"}
    )
    if len(jobs_in_progress):
        return (
            f"Can't trigger harvest. Job {jobs_in_progress[0].id} already in progress."
        )
    job_data = interface.add_harvest_job(
        {
            "harvest_source_id": source.id,
            "status": "new",
            "date_created": datetime.now(),
        }
    )
    if job_data:
        logger.info(
            f"Created new manual harvest job: for {job_data.harvest_source_id}."
        )
        return create_task(job_data.id)


def schedule_first_job(source_id):
    future_jobs = interface.get_new_harvest_jobs_by_source_in_future(source_id)
    # delete any future scheduled jobs
    for job in future_jobs:
        interface.delete_harvest_job(job.id)
        logger.info(f"Deleted harvest job: {job.id} for source {source_id}.")
    # then schedule next job
    return schedule_next_job(source_id)


def schedule_next_job(source_id):
    source = interface.get_harvest_source(source_id)
    if source.frequency != "manual":
        # schedule new future job
        job_data = interface.add_harvest_job(
            {
                "harvest_source_id": source.id,
                "status": "new",
                "date_created": create_future_date(source.frequency),
            }
        )
        message = f"Scheduled new harvest job: for {job_data.harvest_source_id} at {job_data.date_created}."  # noqa E501
        logger.info(message)
        return message
    else:
        return "No job scheduled for manual source."


def load_manager():
    # confirm CF_INSTANCE_INDEX == 0. we don't want multiple instances starting jobs
    if os.getenv("CF_INSTANCE_INDEX") != "0":
        logger.info("CF_INSTANCE_INDEX is not set or not equal to zero")
        return

    cf_handler = create_cf_handler()

    # get new jobs older than now
    jobs = interface.get_new_harvest_jobs_in_past()

    # get list of running tasks
    running_tasks = cf_handler.get_all_running_app_tasks(HARVEST_RUNNER_APP_GUID)

    # confirm tasks < MAX_JOBS_COUNT or bail
    if running_tasks >= MAX_TASKS_COUNT:
        logger.info(
            f"{running_tasks} running_tasks >= max tasks count ({MAX_TASKS_COUNT})."
        )
        return
    else:
        slots = MAX_TASKS_COUNT - running_tasks

    # invoke cf_task with next job(s)
    # then mark that job(s) as running in the DB
    logger.info("Load Manager :: Updated Harvest Jobs")
    for job in jobs[:slots]:
        create_task(job.id, cf_handler)
        schedule_next_job(job.harvest_source_id)
