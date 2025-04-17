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

MAX_TASKS_COUNT = 3

interface = HarvesterDBInterface()

logger = logging.getLogger("harvest_admin")


class LoadManager:
    def __init__(self):
        try:
            self.handler = CFHandler(CF_API_URL, CF_SERVICE_USER, CF_SERVICE_AUTH)
        except Exception as e:
            logger.info(
                f"err {e} :: CFHandler is not configured correctly. \
                Check your env vars."
            )
        self.jobs = []
        self.running_tasks = []

    def start(self):
        """Runs on Flask Admin start, roughly every 15min"""
        if os.getenv("CF_INSTANCE_INDEX") != "0":
            logger.info("CF_INSTANCE_INDEX is not set or not equal to zero")
            return
        self.jobs = interface.get_new_harvest_jobs_in_past()
        self.running_tasks = self.handler.get_all_running_app_tasks(
            HARVEST_RUNNER_APP_GUID
        )
        if self.running_tasks >= MAX_TASKS_COUNT:
            logger.info(
                f"{self.running_tasks} running_tasks >= max tasks count ({MAX_TASKS_COUNT})."  # noqa E501
            )
            return
        else:
            slots = MAX_TASKS_COUNT - self.running_tasks

        # invoke cf_task with next job(s)
        # then mark that job(s) as running in the DB
        for job in self.jobs[:slots]:
            self.start_job(job.id)
            self.schedule_next_job(job.harvest_source_id)

    def start_job(self, job_id, job_type="harvest"):
        """task manager start interface,
        takes a job_id"""
        task_contract = {
            "app_guuid": HARVEST_RUNNER_APP_GUID,
            "command": f"python harvester/harvest.py {job_id} {job_type}",
            "task_id": f"harvest-job-{job_id}-{job_type}",
        }

        self.handler.start_task(**task_contract)
        updated_job = interface.update_harvest_job(job_id, {"status": "in_progress"})
        message = f"Updated job {updated_job.id} to in_progress"
        logger.info(message)
        return message

    def stop_job(self, job_id, job_type="harvest"):
        """task manager stop interface,
        takes a job_id"""
        tasks = self.handler.get_all_app_tasks(HARVEST_RUNNER_APP_GUID)
        job_task = [
            (t["guid"], t["state"])
            for t in tasks
            if t["name"] == f"harvest-job-{job_id}-{job_type}"
        ]

        if len(job_task) == 0:
            return f"No task with job_id: {job_id}"

        if job_task[0][1] != "RUNNING":
            updated_job = interface.update_harvest_job(job_id, {"status": "complete"})
            return f"Task for job {job_id} is not running. Job marked as complete."

        self.handler.stop_task(job_task[0][0])

        updated_job = interface.update_harvest_job(job_id, {"status": "complete"})
        message = f"Updated job {updated_job.id} to complete"
        logger.info(message)
        return message

    def schedule_first_job(self, source_id):
        """schedule first job on harvest source registration or frequency change,
        takes a source_id"""
        future_jobs = interface.get_new_harvest_jobs_by_source_in_future(source_id)
        # delete any future scheduled jobs
        for job in future_jobs:
            interface.delete_harvest_job(job.id)
            logger.info(f"Deleted harvest job: {job.id} for source {source_id}.")
        # then schedule next job
        return self.schedule_next_job(source_id)

    def schedule_next_job(self, source_id):
        """immediately schedule next job to emulate cron,
        takes a source_id"""
        source = interface.get_harvest_source(source_id)
        if source.frequency == "manual":
            message = "No job scheduled for manual source."
        else:
            # schedule new future job
            job_data = interface.add_harvest_job(
                {
                    "harvest_source_id": source.id,
                    "status": "new",
                    "date_created": create_future_date(source.frequency),
                }
            )
            message = f"Scheduled new harvest job: for {job_data.harvest_source_id} \
            at {job_data.date_created}."

        logger.info(message)
        return message

    def trigger_manual_job(self, source_id, job_type="harvest"):
        """manual trigger harvest job,
        takes a source_id"""
        try:
            source = interface.get_harvest_source(source_id)
            jobs_in_progress = interface.pget_harvest_jobs(
                facets=f"harvest_source_id = '{source.id}', status = 'in_progress'",
                paginate=False,
            )
            if len(jobs_in_progress):
                return f"Can't trigger harvest. Job {jobs_in_progress[0].id} already in progress."  # noqa E501
            job_data = interface.add_harvest_job(
                {
                    "harvest_source_id": source.id,
                    "status": "new",
                    "job_type": job_type,
                    "date_created": datetime.now(),
                }
            )
            if job_data:
                logger.info(
                    f"Created new manual harvest job: for {job_data.harvest_source_id}."
                )
                return self.start_job(job_data.id, job_type)
        except Exception as e:
            message = f"LoadManager: trigger_manual_job failed :: {repr(e)}"
            logger.error(message)
            return message
