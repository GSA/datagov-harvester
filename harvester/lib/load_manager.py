import logging
import os
from datetime import datetime

from harvester import SMTP_CONFIG
from harvester.lib.cf_handler import CFHandler
from harvester.utils.general_utils import (
    create_future_date,
    get_datetime,
    send_email_to_recipients,
)

# use the session scoped interface already made for the harvester
from .. import db_interface as interface

CF_API_URL = os.getenv("CF_API_URL")
CF_SERVICE_USER = os.getenv("CF_SERVICE_USER")
CF_SERVICE_AUTH = os.getenv("CF_SERVICE_AUTH")

MAX_TASKS_COUNT = int(os.getenv("HARVEST_RUNNER_MAX_TASKS", 5))


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

    def _handle_failed_job(self, job):
        """Handle a HarvestJob that failed.

        A failed job has status in_progress in the database but there isn't a
        task running for it. Something happened to its task but we likely
        don't know what. Minimally, set the state to `error` and record a job
        error that we saw this.

        The next job is scheduled on job start so we shouldn't need to schedule
        anything else and we are choosing not to retry these.
        """
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
                "message": "In-progress job stopped running for an unknown reason.",
                "harvest_job_id": job.id,
            }
        )
        job_url = f"{SMTP_CONFIG['base_url']}/harvest_job/{job.id}"
        send_email_to_recipients(
            [SMTP_CONFIG.get("recipient")],
            f"Failed job cleaned up for {job.source.name}",
            (
                f"The harvest job ({job.id}) for harvest source {job.source.name}\n"
                "was found to have failed.\n\n"
                f"You can view the details here: {job_url}\n"
            ),
        )

    def _clean_old_jobs(self):
        """Check for in_progress jobs in the database that aren't running."""
        in_progress_jobs = interface.get_in_progress_jobs()
        running_tasks = self.handler.get_running_app_tasks()
        running_harvest_ids = set(self.handler.job_ids_from_tasks(running_tasks))

        failed_jobs = [
            job for job in in_progress_jobs if job.id not in running_harvest_ids
        ]
        for job in failed_jobs:
            self._handle_failed_job(job)

    def _start_new_jobs(self, check_from_task=False):
        """Start new jobs to be done, up to the max tasks count.

        If check_from_task is True, then this is being called from a running
        task before it stops so we adjust the running_tasks calculation and
        only schedule at most one new job.
        """
        running_tasks = self.handler.num_running_app_tasks()
        if running_tasks is None:
            # None here indicates that tasks couldn't be listed with the API
            # so be safe by not doing anything.
            logger.warning("Not starting new jobs because tasks could not be listed")
            return

        if check_from_task:
            running_tasks -= 1

        if running_tasks >= MAX_TASKS_COUNT:
            logger.info(
                f"{running_tasks} running tasks >= max tasks count ({MAX_TASKS_COUNT})."  # noqa E501
            )
            return
        else:
            slots = MAX_TASKS_COUNT - running_tasks

        if check_from_task:
            # from a task only do 1 at most
            slots = 1 if slots > 0 else 0

        # invoke cf_task with next jobs
        # then mark the job as running in the DB
        jobs = interface.get_new_harvest_jobs_in_past(limit=slots)
        for job in jobs:
            self.start_job(job.id)
            self.schedule_next_job(job.harvest_source_id)

    def start(self):
        """Runs on Flask Admin start, roughly every 15min"""
        if os.getenv("CF_INSTANCE_INDEX") != "0":
            logger.debug("CF_INSTANCE_INDEX is not set or not equal to zero")
            return

        self._clean_old_jobs()
        self._start_new_jobs()

    def start_job(self, job_id, job_type="harvest"):
        """
        Start a harvest job if no other job is currently in progress for the same source

        This method checks if a job with status 'in_progress' already exists for the
        given harvest source. If not, it updates the job status to 'in_progress',
        creates a task contract, and starts the task using the handler. If an error
        occurs during this process, the job status is reset to 'new'.

        Returns:
            str: A message indicating the result of the operation.
        """

        try:
            """Check if a job is already running for this source."""
            harvest_job = interface.get_harvest_job(job_id)
            jobs_in_progress = interface.pget_harvest_jobs(
                facets=f"harvest_source_id eq {harvest_job.harvest_source_id},status eq in_progress",  # noqa E501
                per_page=1,  # Only need 1 job to know we should not start a new one
                page=0,
            )
            if len(jobs_in_progress):
                return f"Can't trigger harvest. Job {jobs_in_progress[0].id} already in progress."  # noqa E501

            """task manager start interface, takes a job_id"""
            task_contract = {
                "command": f"python harvester/harvest.py {job_id} {job_type}",
                "task_id": f"harvest-job-{job_id}-{job_type}",
            }

            updated_job = interface.update_harvest_job(
                job_id, {"status": "in_progress", "date_created": get_datetime()}
            )
            self.handler.start_task(**task_contract)
            message = f"Updated job {updated_job.id} to in_progress"
            logger.info(message)
            return message
        except Exception as e:
            message = f"LoadManager: start_job failed :: {repr(e)}"
            logger.error(message)
            try:
                updated_job = interface.update_harvest_job(
                    job_id, {"status": "new", "date_created": get_datetime()}
                )
            except Exception as e:
                logger.error(f"Failed to reset job {job_id} status: {repr(e)}")
                pass
            return message

    def stop_job(self, job_id, job_type="harvest"):
        """task manager stop interface, takes a job_id"""
        tasks = self.handler.get_all_app_tasks()
        if tasks is None:
            # couldn't list tasks, nothing to do
            return f"Could not stop job {job_id}, can't list tasks"
        job_task = [
            (t["guid"], t["state"])
            for t in tasks
            if t["name"] == f"harvest-job-{job_id}-{job_type}"
        ]

        if len(job_task) == 0:
            return f"No task with job_id: {job_id}"

        # Task options from https://v3-apidocs.cloudfoundry.org/version/3.202.0/index.html#tasks
        # Should be nothing to do, but make sure the job is marked error if needed
        if job_task[0][1] in ["SUCCEEDED", "CANCELING", "FAILED"]:
            current_job = interface.get_harvest_job(job_id)
            if current_job.status not in ["complete", "error"]:
                updated_job = interface.update_harvest_job(
                    job_id, {"status": "error", "date_finished": get_datetime()}
                )
                message = f"Task for job {updated_job.id} is not running, but marked job as error."  # noqa E501
            else:
                message = f"Task for job {job_id} is not running, job status is {current_job.status}."  # noqa E501

            logger.info(message)
            return message

        self.handler.stop_task(job_task[0][0])

        updated_job = interface.update_harvest_job(
            job_id, {"status": "error", "date_finished": get_datetime()}
        )
        interface.add_harvest_job_error(
            {
                "date_created": get_datetime(),
                "type": "CancelledJob",
                "message": "Job was manually cancelled.",
                "harvest_job_id": updated_job.id,
            }
        )
        message = f"Updated job {updated_job.id} to error and stopped the job."
        logger.info(message)
        return message

    def schedule_first_job(self, source_id):
        """schedule first job on harvest source registration or frequency change,
        takes a source_id
        """
        future_jobs = interface.get_new_harvest_jobs_by_source_in_future(source_id)
        # delete any future scheduled jobs
        for job in future_jobs:
            interface.delete_harvest_job(job.id)
            logger.info(f"Deleted harvest job: {job.id} for source {source_id}.")
        # then schedule next job
        return self.schedule_next_job(source_id)

    def schedule_next_job(self, source_id):
        """immediately schedule next job to emulate cron, takes a source_id"""
        source = interface.get_harvest_source(source_id)
        if source.frequency == "manual":
            logger.info("No job scheduled for manual source.")
            return "No job scheduled for manual source."

        # check if there is a job already scheduled in the future
        future_jobs = interface.get_new_harvest_jobs_by_source_in_future(source_id)
        if len(future_jobs) > 0:
            message = f"Job already scheduled for source {source_id} at \
            {future_jobs[0].date_created}."
            logger.info(message)
            return message

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
        """manual trigger harvest job, takes a source_id"""
        try:
            source = interface.get_harvest_source(source_id)
            jobs_in_progress = interface.pget_harvest_jobs(
                facets=f"harvest_source_id eq {source.id},status eq in_progress",
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
