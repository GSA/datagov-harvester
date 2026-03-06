import os
from time import sleep
from unittest.mock import patch

import pytest
from cloudfoundry_client.errors import InvalidStatusCode

from harvester.lib.cf_handler import CFHandler

CF_API_URL = os.getenv("CF_API_URL")
CF_SERVICE_USER = os.getenv("CF_SERVICE_USER")
CF_SERVICE_AUTH = os.getenv("CF_SERVICE_AUTH")
RUNNING_IN_GITHUB = os.getenv("GITHUB_ACTIONS", "").lower() == "true"
HAS_CF_CREDS = all([CF_API_URL, CF_SERVICE_USER, CF_SERVICE_AUTH])

pytestmark = pytest.mark.skipif(
    not RUNNING_IN_GITHUB and not HAS_CF_CREDS,
    reason="Cloud Foundry credentials are not configured for functional tests",
)

if HAS_CF_CREDS:
    cf_handler = CFHandler(CF_API_URL, CF_SERVICE_USER, CF_SERVICE_AUTH)
else:
    if RUNNING_IN_GITHUB:
        raise RuntimeError(
            "CF_API_URL, CF_SERVICE_USER, and CF_SERVICE_AUTH must be set in GitHub Actions"
        )
    cf_handler = None

dhl_cf_task_data = {
    "task_id": "cf_task_func_spec",
    "command": "/usr/bin/sleep 60",
}


class TestCFTasking:
    def test_crud_task(self):
        # start a new task
        new_task = cf_handler.start_task(**dhl_cf_task_data)
        sleep(2)
        # retrieve that task via task guid
        task = cf_handler.get_task(new_task["guid"])

        # read the recent logs of the task
        logs = cf_handler.read_recent_app_logs(task_id=task["guid"])
        assert logs is not None

        # cancel the task
        cancelled_task = cf_handler.stop_task(task["guid"])
        assert cancelled_task is not None

    def test_get_all_app_tasks(self):
        cf_handler.setup()

        tasks = cf_handler.get_all_app_tasks()
        assert tasks is not None

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def tests_get_all_app_tasks_api_error(self, CFCMock, caplog):
        cf_handler.setup()

        CFCMock.return_value.v3.apps.get.side_effect = InvalidStatusCode(500, "")

        tasks = cf_handler.get_all_app_tasks()
        assert CFCMock.return_value.v3.apps.get.call_count == 1
        assert tasks is None
        assert "Failed to get app tasks" in caplog.text

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def tests_get_running_app_tasks_api_error(self, CFCMock, caplog):
        cf_handler.setup()

        CFCMock.return_value.v3.apps.get.side_effect = InvalidStatusCode(500, "")

        tasks = cf_handler.get_running_app_tasks()
        assert tasks is None
        assert "Failed to get app tasks" in caplog.text

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def tests_num_running_app_tasks_api_error(self, CFCMock, caplog):
        cf_handler.setup()

        CFCMock.return_value.v3.apps.get.side_effect = InvalidStatusCode(500, "")

        num_tasks = cf_handler.num_running_app_tasks()
        assert num_tasks is None
        assert "Failed to get app tasks" in caplog.text
