import logging
from unittest.mock import patch

from cloudfoundry_client.errors import InvalidStatusCode

from harvester.harvest import harvest_job_starter
from harvester.lib.cf_handler import CFHandler


@patch("harvester.lib.cf_handler.CloudFoundryClient")
class TestCFTasking:
    def test_add_task(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        assert CFUtil.start_task(**dhl_cf_task_data) is not None

    def test_get_task(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        task = CFUtil.get_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_get_all_app_tasks(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        # ruff: noqa: E501
        CFClientMock.return_value.v3.apps._pagination.return_value = [1]
        tasks = CFUtil.get_all_app_tasks()
        assert len(tasks) > 0

    def test_get_running_app_tasks(self, CFClientMock):
        CFUtil = CFHandler("url", "user", "password")
        CFClientMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING", "name": "harvest-job-"},
            {"state": "RUNNING", "name": "cf_task_func_spec"},
            {"state": "SUCCEEDED", "name": "harvest-job-"},
        ]
        running_tasks = CFUtil.get_running_app_tasks()
        assert len(running_tasks) == 1

    def test_get_active_harvest_tasks(self, CFClientMock):
        CFUtil = CFHandler("url", "user", "password")
        CFClientMock.return_value.v3.apps._pagination.return_value = [
            {"state": "PENDING", "name": "harvest-job-pending"},
            {"state": "RUNNING", "name": "harvest-job-running"},
            {"state": "CANCELING", "name": "harvest-job-canceling"},
            {"state": "SUCCEEDED", "name": "harvest-job-complete"},
            {"state": "RUNNING", "name": "other-task"},
        ]

        active_tasks = CFUtil.get_active_harvest_tasks()

        assert [task["state"] for task in active_tasks] == [
            "PENDING",
            "RUNNING",
            "CANCELING",
        ]

    def test_get_active_harvest_tasks_distinguishes_empty_from_api_error(
        self, CFClientMock
    ):
        CFUtil = CFHandler("url", "user", "password")
        CFClientMock.return_value.v3.apps._pagination.return_value = []

        assert CFUtil.get_active_harvest_tasks() == []

        CFClientMock.return_value.v3.apps.get.side_effect = InvalidStatusCode(500, "")

        assert CFUtil.get_active_harvest_tasks() is None

    def test_num_running_app_tasks(self, CFClientMock):
        CFUtil = CFHandler("url", "user", "password")
        CFClientMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING", "name": "harvest-job-"},
            {"state": "SUCCEEDED", "name": "harvest-job-"},
        ]
        running_tasks = CFUtil.num_running_app_tasks()
        assert running_tasks == 1

    def test_job_ids_from_tasks(self, CFClientMock):
        CFUtil = CFHandler("url", "user", "password")
        job_ids = CFUtil.job_ids_from_tasks(
            [
                {"name": "harvest-job-this_id-harvest"},
                {"name": "not-our-format-of-name-so-no-id"},
            ]
        )
        assert len(job_ids) == 1
        assert job_ids[0] == "this_id"

    def test_cancel_task(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        task = CFUtil.stop_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_read_recent_task_logs(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        logs = CFUtil.read_recent_app_logs(task_id=dhl_cf_task_data["task_id"])
        assert logs is not None

    def test_harvest_multiple_tasks(
        self,
        CFClientMock,  # Class-level patch parameter comes first
        interface,
        organization_data,
        source_data_dcatus_single_record,
        caplog,
        monkeypatch,
    ):
        # this test exercises CF-backed duplicate detection, so make the task
        # handler factory choose CFHandler (which is mocked above) by providing
        # full CF credentials.
        monkeypatch.setenv("CF_API_URL", "https://api.example.com")
        monkeypatch.setenv("CF_SERVICE_USER", "user")
        monkeypatch.setenv("CF_SERVICE_AUTH", "pass")

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        harvest_job = interface.add_harvest_job(
            {
                "status": "in_progress",
                "harvest_source_id": source_data_dcatus_single_record["id"],
            }
        )

        CFClientMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING", "name": f"harvest-job-{harvest_job.id}-harvest"},
            {"state": "RUNNING", "name": f"harvest-job-{harvest_job.id}-harvest"},
            {
                "state": "RUNNING",
                "name": "harvest-job-1c3d686c-6156-429d-b27b-5ab163750e76-harvest",
            },
        ]

        caplog.set_level(logging.INFO)
        harvest_job_starter(harvest_job.id, "harvest")
        assert (
            f"Job {harvest_job.id} is already running in another task. Exiting."
            in caplog.text
        )
