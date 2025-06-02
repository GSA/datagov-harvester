from unittest.mock import patch

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
        CFClientMock.return_value.v3.apps.get.return_value = [1]
        tasks = CFUtil.get_all_app_tasks()
        assert len(tasks) > 0

    def test_get_all_running_app_tasks(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        CFClientMock.return_value.v3.apps.get.return_value = [
            {"state": "RUNNING"},
            {"state": "SUCCEEDED"},
        ]
        running_tasks = CFUtil.num_running_app_tasks()
        assert running_tasks == 1

    def test_cancel_task(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        task = CFUtil.stop_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_read_recent_task_logs(self, CFClientMock, dhl_cf_task_data):
        CFUtil = CFHandler("url", "user", "password")
        logs = CFUtil.read_recent_app_logs(task_id=dhl_cf_task_data["task_id"])
        assert logs is not None
