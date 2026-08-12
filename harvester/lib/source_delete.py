"""Enqueue harvest-source deletes as CF/local tasks (avoid HTTP timeouts)."""

import logging

from harvester.lib.task_handler import create_task_handler

logger = logging.getLogger("harvest_admin")

DELETE_IN_PROGRESS_MESSAGE = "This harvest source may take some time to delete."


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
        create_task_handler().start_task(command=command, task_id=task_id)
    except Exception as e:
        err = f"Failed to schedule harvest source delete :: {repr(e)}"
        logger.error(err)
        return err, 500

    logger.info("Scheduled harvest source delete task %s", task_id)
    return DELETE_IN_PROGRESS_MESSAGE, 202
