"""Enqueue harvest-source archives as CF/local tasks (avoid HTTP timeouts)."""

import logging

from harvester.lib.local_task_handler import LocalTaskHandler
from harvester.lib.task_handler import create_task_handler
from harvester.utils.env_utils import is_running_on_cloud_foundry

logger = logging.getLogger("harvest_admin")

ARCHIVE_IN_PROGRESS_MESSAGE = "This harvest source may take some time to archive."


def _task_handler_for_archive():
    """Return the handler that will archive against this process's database.

    Off Cloud Foundry, CI/local often still have CF credentials (e.g. from
    ``.env.sample`` + workflow secrets). ``create_task_handler()`` would then
    schedule the archive on a remote CF app whose DATABASE_URI is not the Docker
    DB the UI just prechecked — so the source never gets archived locally.
    """
    if is_running_on_cloud_foundry():
        return create_task_handler()
    return LocalTaskHandler()


def enqueue_harvest_source_archive(source_id, user_email, db_interface):
    """Precheck then schedule a background task to archive a harvest source.

    The clear-first / not-found checks run synchronously so the UI can show a
    409 immediately. The archive operation itself runs in a CF or local task so
    the web request can return before the gateway times out.

    The task repeats the precheck to protect records created after this request
    returns. If that second check fails, the task logs the failure and leaves
    the source in the list; there is no durable task-status UI for archives.

    Args:
        source_id: ID of the harvest source to archive
        user_email: Email of the user performing the archive
        db_interface: Database interface instance

    Returns:
        tuple[str, int]: ``(message, status)`` — 202 when scheduled, otherwise
        the precheck failure status (404/409).
    """
    ok, message, status = db_interface.can_archive_harvest_source(source_id)
    if not ok:
        return message, status

    task_id = f"archive-harvest-source-{source_id}"
    command = f"python -m harvester.archive_source {source_id} {user_email}"
    try:
        _task_handler_for_archive().start_task(command=command, task_id=task_id)
    except Exception as e:
        err = f"Failed to schedule harvest source archive :: {repr(e)}"
        logger.error(err)
        return err, 500

    logger.info("Scheduled harvest source archive task %s", task_id)
    return ARCHIVE_IN_PROGRESS_MESSAGE, 202


enqueue_harvest_source_delete = enqueue_harvest_source_archive
