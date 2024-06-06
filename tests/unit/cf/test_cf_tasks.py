from unittest.mock import patch

from harvester.utils import CFHandler


@patch("harvester.utils.CloudFoundryClient")
@patch("harvester.utils.TaskManager")
class TestCFTasking:
    def test_add_task(self, TMMock, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        assert CFUtil.start_task(**dhl_cf_task_data) is not None

    def test_get_task(self, TMMock, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        task = CFUtil.get_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_get_all_app_tasks(self, TMMock, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        # ruff: noqa: E501
        CFClientMock.return_value.v3.apps.__getitem__.return_value.tasks.return_value = [
            1
        ]
        tasks = CFUtil.get_all_app_tasks(dhl_cf_task_data["app_guuid"])
        assert len(tasks) > 0

    def test_get_all_running_app_tasks(self, CFClientMock, TMMock):
        CFUtil = CFHandler("url", "user", "password")
        tasks = [{"state": "RUNNING"}, {"state": "SUCCEEDED"}]
        running_tasks = CFUtil.get_all_running_tasks(tasks)
        assert running_tasks == 1

    def test_cancel_task(self, TMMock, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        task = CFUtil.stop_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_read_recent_task_logs(self, TMMock, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        logs = CFUtil.read_recent_app_logs(
            dhl_cf_task_data["app_guuid"], dhl_cf_task_data["task_id"]
        )
        assert logs is not None
