import logging
import os
from datetime import datetime

from harvester import SMTP_CONFIG
from harvester.lib.task_handler import create_task_handler
from harvester.utils.general_utils import (
    create_future_date,
    get_datetime,
    send_email_to_recipients,
)

# use the session scoped interface already made for the harvester
from .. import db_interface as interface

MAX_TASKS_COUNT = int(os.getenv("HARVEST_RUNNER_MAX_TASKS", 5))


logger = logging.getLogger("harvest_admin")


class LoadManager:
    def __init__(self):
        self.handler = create_task_handler()

    def _handle_failed_job(self, job):
        """Handle a HarvestJob that failed.

        A failed job has status in_progress in the database but there isn't a
        task running for it. Something happened to its task but we likely
        don't know what. Minimally, set the state to `error` and record a job
        error that we saw this.

        The next run is already stored on the harvest source as date_next_run
        (bumped when the job was enqueued), so we are choosing not to retry
        these.
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

    def _drop_queued_jobs(self, source_id):
        """Delete waiting status=new jobs for a source (e.g. frequency change)."""
        queued_jobs = interface.get_queued_harvest_jobs_for_source(source_id)
        for job in queued_jobs:
            interface.delete_harvest_job(job.id)
            logger.info(f"Deleted harvest job: {job.id} for source {source_id}.")
        return queued_jobs

    def _stamp_missing_next_runs(self):
        """Give non-manual sources without date_next_run a future next run."""
        for source in interface.get_unstamped_harvest_sources():
            next_run = create_future_date(source.frequency)
            interface.update_harvest_source(source.id, {"date_next_run": next_run})
            logger.info(f"Set next harvest run for source {source.id} at {next_run}.")

    def _enqueue_due_sources(self):
        """Create a job for each due source and bump date_next_run."""
        self._stamp_missing_next_runs()
        for source in interface.get_due_harvest_sources():
            job_data = interface.add_harvest_job(
                {
                    "harvest_source_id": source.id,
                    "status": "new",
                    "date_created": datetime.now(),
                }
            )
            if not job_data:
                logger.error(f"Failed to queue harvest job for source {source.id}.")
                continue
            next_run = create_future_date(source.frequency)
            interface.update_harvest_source(source.id, {"date_next_run": next_run})
            logger.info(
                f"Queued harvest job {job_data.id} for source {source.id}; "
                f"next run at {next_run}."
            )

    def _start_new_jobs(self, check_from_task=False):
        """Start new jobs to be done, up to the max tasks count.

        If check_from_task is True, then this is being called from a running
        task before it stops so we adjust the running_tasks calculation and
        only schedule at most one new job.
        """
        try:
            running_tasks = self.handler.num_running_app_tasks()
            if running_tasks is None:
                # None here indicates that tasks couldn't be listed with the API
                # so be safe by not doing anything.
                logger.warning(
                    "Not starting new jobs because tasks could not be listed"
                )
                return

            self._enqueue_due_sources()

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
                self.start_job(job.id, job.job_type)
        finally:
            # closes the scoped_session object
            interface.close()

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
                job_id, {"status": "in_progress", "date_started": get_datetime()}
            )
            self.handler.start_task(**task_contract)
            message = f"Updated job {updated_job.id} to in_progress"
            logger.info(message)
            return message
        except Exception as e:
            message = f"LoadManager: start_job failed :: {repr(e)}"
            logger.error(message)
            try:
                interface.update_harvest_job(
                    job_id, {"status": "new", "date_started": None}
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
        """Set date_next_run on harvest source registration or frequency change.

        Drops any waiting new job for the source. Does not create a job row.
        """
        self._drop_queued_jobs(source_id)
        source = interface.get_harvest_source(source_id)
        if source.frequency == "manual":
            interface.update_harvest_source(source_id, {"date_next_run": None})
            logger.info("No next run scheduled for manual source.")
            return "No next run scheduled for manual source."

        next_run = create_future_date(source.frequency)
        interface.update_harvest_source(source_id, {"date_next_run": next_run})
        message = f"Set next harvest run for {source_id} at {next_run}."
        logger.info(message)
        return message

    def schedule_next_job(self, source_id):
        """Stamp date_next_run if the source does not already have a future run."""
        source = interface.get_harvest_source(source_id)
        if source.frequency == "manual":
            logger.info("No next run scheduled for manual source.")
            return "No next run scheduled for manual source."

        if source.date_next_run and source.date_next_run > datetime.now():
            message = (
                f"Next harvest already scheduled for source {source_id} at "
                f"{source.date_next_run}."
            )
            logger.info(message)
            return message

        next_run = create_future_date(source.frequency)
        interface.update_harvest_source(source_id, {"date_next_run": next_run})
        message = f"Set next harvest run for {source_id} at {next_run}."
        logger.info(message)
        return message

    def trigger_manual_job(self, source_id, job_type="harvest"):
        """manual trigger harvest job, takes a source_id"""
        try:
            source = interface.get_harvest_source(source_id)
            active_job = interface.get_active_harvest_job_for_source(source.id)
            if active_job:
                if active_job.status == "in_progress":
                    return f"Can't trigger harvest. Job {active_job.id} already in progress."  # noqa E501
                return f"Can't trigger harvest. Job {active_job.id} already queued."
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
