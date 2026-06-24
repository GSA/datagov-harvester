"""Create the task handler used to start and monitor harvest jobs.

In deployed (Cloud Foundry) environments we use CFHandler, which runs harvest
jobs as CF tasks. Locally there is no CF task API, so we fall back to
LocalTaskHandler, which runs the same command as a child subprocess.
"""

import logging
import os

from harvester.lib.cf_handler import CFHandler
from harvester.lib.local_task_handler import LocalTaskHandler

logger = logging.getLogger("harvest_admin")


def is_running_on_cloud_foundry() -> bool:
    return "VCAP_APPLICATION" in os.environ


def create_task_handler():
    """Return Cloud Foundry task handler in deployed envs, local runner otherwise.

    We only attempt CFHandler when it can actually authenticate: either we are
    running on Cloud Foundry, or all three CF_* credentials are configured.
    This keeps local development from making a doomed OAuth round-trip (and the
    resulting 401) on every LoadManager construction when only CF_API_URL is
    set but the service credentials are not.
    """
    cf_api_url = os.getenv("CF_API_URL")
    cf_service_user = os.getenv("CF_SERVICE_USER")
    cf_service_auth = os.getenv("CF_SERVICE_AUTH")
    on_cloud_foundry = is_running_on_cloud_foundry()

    has_cf_credentials = all((cf_api_url, cf_service_user, cf_service_auth))

    if on_cloud_foundry or has_cf_credentials:
        try:
            return CFHandler(cf_api_url, cf_service_user, cf_service_auth)
        except Exception as e:
            if on_cloud_foundry:
                # in a deployed env we can't run subprocesses meaningfully, so
                # surface the failure instead of silently degrading
                raise
            logger.warning("CFHandler init failed (%s); using LocalTaskHandler.", e)

    logger.info("Cloud Foundry task API is not configured; using LocalTaskHandler.")
    return LocalTaskHandler()
