"""Enqueue harvest-source deletes as CF/local tasks (avoid HTTP timeouts)."""

import logging

from harvester.lib.local_task_handler import LocalTaskHandler
from harvester.lib.task_handler import create_task_handler
from harvester.utils.env_utils import is_running_on_cloud_foundry

logger = logging.getLogger("harvest_admin")

DELETE_IN_PROGRESS_MESSAGE = "This harvest source may take some time to delete."


def _task_handler_for_delete():
    """Return the handler that will delete against this process's database.

    Off Cloud Foundry, CI/local often still have CF credentials (e.g. from
    ``.env.sample`` + workflow secrets). ``create_task_handler()`` would then
    schedule the delete on a remote CF app whose DATABASE_URI is not the Docker
    DB the UI just prechecked — so the source never disappears locally.
    """
    if is_running_on_cloud_foundry():
        return create_task_handler()
    return LocalTaskHandler()


def enqueue_harvest_source_delete(source_id, db_interface):
    """Precheck then schedule a background task to delete a harvest source.

    The clear-first / not-found checks run synchronously so the UI can show a
    409 immediately. The CASCADE delete itself runs in a CF or local task so
    the web request can return before the gateway times out.

    Returns:
        tuple[str, int]: ``(message, status)`` — 202 when scheduled, otherwise
        the precheck failure status (404/409).
    """
    ok, message, status = db_interface.can_delete_harvest_source(source_id)
    if not ok:
        return message, status

    task_id = f"delete-harvest-source-{source_id}"
    command = f"python harvester/delete_source.py {source_id}"
    try:
        _task_handler_for_delete().start_task(command=command, task_id=task_id)
    except Exception as e:
        err = f"Failed to schedule harvest source delete :: {repr(e)}"
        logger.error(err)
        return err, 500

    logger.info("Scheduled harvest source delete task %s", task_id)
    return DELETE_IN_PROGRESS_MESSAGE, 202
