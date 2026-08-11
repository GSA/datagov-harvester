import logging
from unittest.mock import patch

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
        # Save the id before running the job to avoid detached-instance issues
        # after the DB/session lifecycle changes during harvest_job_starter().
        job_id = harvest_job.id

        CFClientMock.return_value.v3.apps._pagination.return_value = [
            {
                "guid": "task-a",
                "state": "RUNNING",
                "name": f"harvest-job-{job_id}-harvest",
            },
            {
                "guid": "task-b",
                "state": "RUNNING",
                "name": f"harvest-job-{job_id}-harvest",
            },
            {
                "guid": "task-other-job",
                "state": "RUNNING",
                "name": "harvest-job-1c3d686c-6156-429d-b27b-5ab163750e76-harvest",
            },
        ]

        caplog.set_level(logging.WARNING)

        harvest_job_starter(job_id, "harvest")

        assert f"Detected 2 running tasks for job {job_id}." in caplog.text
        assert "continuing without exiting" in caplog.text

        assert CFClientMock.return_value.v3.tasks.cancel.call_count == 1
        cancelled_task_id = CFClientMock.return_value.v3.tasks.cancel.call_args[0][0]
        assert cancelled_task_id in {"task-a", "task-b"}
        assert cancelled_task_id != "task-other-job"

        updated_job = interface.get_harvest_job(job_id)
        assert updated_job.status != "error"

    def test_harvest_duplicate_task_stop_failure_logs_warning_and_continues(
        self,
        CFClientMock,
        interface,
        organization_data,
        source_data_dcatus_single_record,
        caplog,
        monkeypatch,
    ):
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
        job_id = harvest_job.id

        CFClientMock.return_value.v3.apps._pagination.return_value = [
            {
                "guid": "task-a",
                "state": "RUNNING",
                "name": f"harvest-job-{job_id}-harvest",
            },
            {
                "guid": "task-b",
                "state": "RUNNING",
                "name": f"harvest-job-{job_id}-harvest",
            },
        ]
        CFClientMock.return_value.v3.tasks.cancel.side_effect = Exception(
            "cancel failed"
        )

        caplog.set_level(logging.WARNING)

        harvest_job_starter(job_id, "harvest")

        assert f"Detected 2 running tasks for job {job_id}." in caplog.text
        assert (
            f"Failed to stop duplicate task task-b for job {job_id}: cancel failed"
            in caplog.text
        )

        updated_job = interface.get_harvest_job(job_id)
        assert updated_job.status != "error"
