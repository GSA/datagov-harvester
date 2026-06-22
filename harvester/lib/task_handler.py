"""Create the task handler used to start and monitor harvest jobs."""

import logging
import os

from harvester.lib.cf_handler import CFHandler
from harvester.lib.local_task_handler import LocalTaskHandler

logger = logging.getLogger("harvest_admin")


def is_running_on_cloud_foundry() -> bool:
    return "VCAP_APPLICATION" in os.environ


def create_task_handler():
    """Return Cloud Foundry task handler in deployed envs, local runner otherwise."""
    cf_api_url = os.getenv("CF_API_URL")
    cf_service_user = os.getenv("CF_SERVICE_USER")
    cf_service_auth = os.getenv("CF_SERVICE_AUTH")
    on_cloud_foundry = is_running_on_cloud_foundry()

    if on_cloud_foundry or cf_api_url:
        try:
            return CFHandler(cf_api_url, cf_service_user, cf_service_auth)
        except Exception as e:
            if on_cloud_foundry:
                raise
            logger.warning(
                "CFHandler init failed (%s); using LocalTaskHandler.", e
            )

    logger.warning(
        "Cloud Foundry task API is not configured; using LocalTaskHandler."
    )
    return LocalTaskHandler()
