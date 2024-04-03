from unittest.mock import patch
from harvester.utils import CFHandler


class TestCFTasking:
    @patch.object(CFHandler, "start_task")
    def test_add_task(self, start_mock, cf_handler, dhl_cf_task_data):
        start_mock.return_value = {"guuid": "test"}

        assert cf_handler.start_task(**dhl_cf_task_data) is not None

    @patch.object(CFHandler, "get_task")
    def test_get_task(self, get_mock, cf_handler, dhl_cf_task_data):
        get_mock.return_value = {"guuid": "test"}

        task = cf_handler.get_task(dhl_cf_task_data["task_id"])
        assert task is not None

    @patch.object(CFHandler, "get_all_app_tasks")
    def test_get_all_app_tasks(self, tasks_mock, cf_handler, dhl_cf_task_data):
        tasks_mock.return_value = [{"guuid": "test"}]

        tasks = cf_handler.get_all_app_tasks(dhl_cf_task_data["app_guuid"])
        assert len(tasks) > 0

    @patch.object(CFHandler, "stop_task")
    def test_cancel_task(self, stop_mock, cf_handler, dhl_cf_task_data):
        stop_mock.return_value = {"guuid": "test"}

        task = cf_handler.stop_task(dhl_cf_task_data["task_id"])
        assert task is not None

    @patch.object(CFHandler, "read_recent_app_logs")
    def test_read_recent_task_logs(self, read_mock, cf_handler, dhl_cf_task_data):
        read_mock.return_value = "recent information on the task"

        logs = cf_handler.read_recent_app_logs(
            dhl_cf_task_data["app_guuid"], dhl_cf_task_data["task_id"]
        )

        assert logs is not None
