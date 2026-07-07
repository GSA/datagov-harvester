"""Create the task handler used to start and monitor harvest jobs.

In deployed (Cloud Foundry) environments we use CFHandler, which runs harvest
jobs as CF tasks. Locally there is no CF task API, so we fall back to
LocalTaskHandler, which runs the same command as a child subprocess.
"""

import logging
import os

from harvester.lib.cf_handler import CFHandler
from harvester.lib.local_task_handler import LocalTaskHandler
from harvester.utils.env_utils import is_running_on_cloud_foundry

logger = logging.getLogger("harvest_admin")

_CF_TASK_API_NOT_CONFIGURED = (
    "Cloud Foundry task API is not configured; "
    "CF_API_URL, CF_SERVICE_USER, and CF_SERVICE_AUTH must be set."
)


def create_task_handler():
    """Return Cloud Foundry task handler in deployed envs, local runner otherwise.

    We only attempt CFHandler when all three CF_* credentials are configured.
    This keeps local development from making a doomed OAuth round-trip (and the
    resulting 401) on every LoadManager construction when only CF_API_URL is
    set but the service credentials are not.

    LocalTaskHandler is never used on Cloud Foundry; missing or invalid CF
    configuration fails fast in deployed environments.
    """
    cf_api_url = os.getenv("CF_API_URL")
    cf_service_user = os.getenv("CF_SERVICE_USER")
    cf_service_auth = os.getenv("CF_SERVICE_AUTH")
    on_cloud_foundry = is_running_on_cloud_foundry()
    has_cf_credentials = all((cf_api_url, cf_service_user, cf_service_auth))

    if on_cloud_foundry:
        if not has_cf_credentials:
            raise RuntimeError(_CF_TASK_API_NOT_CONFIGURED)
        return CFHandler(cf_api_url, cf_service_user, cf_service_auth)

    if has_cf_credentials:
        try:
            return CFHandler(cf_api_url, cf_service_user, cf_service_auth)
        except Exception as e:
            logger.warning("CFHandler init failed (%s); using LocalTaskHandler.", e)

    return LocalTaskHandler()
