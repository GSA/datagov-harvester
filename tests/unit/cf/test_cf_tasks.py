import collections
from unittest.mock import patch

from harvester.utils import CFHandler


class AppMock:
    def __init__(self, x):
        self.tasks = self.tasksMock
        self.recent_logs = self.logsMock

    def tasksMock(x):
        return [1, 2, 3]

    def logsMock(x):
        return [{"cf_task_integration": 1}, 2, 3]

    def __getitem__(self, x):
        return self


class V2Mock:
    def __init__(self, x):
        self.apps = AppMock(x)


class V3Mock:
    def __init__(self, x):
        self.apps = AppMock(x)


class CFClientMock:
    def __init__(self, x):
        self.v2 = V2Mock(x)
        self.v3 = V3Mock(x)
        pass

    def init_with_user_credentials(self, a, b):
        pass


class TaskManagerMock:
    def __init__(self, x, y):
        pass

    def create(self, a, b, c):
        return collections.defaultdict(list)

    def get(self, a):
        return collections.defaultdict(list)

    def cancel(self, a):
        return "cancelled"


with patch("harvester.utils.CloudFoundryClient", CFClientMock), patch(
    "harvester.utils.TaskManager", TaskManagerMock
):
    CFUtil = CFHandler("url", "user", "password")


class TestCFTasking:
    def test_add_task(self, dhl_cf_task_data):
        assert CFUtil.start_task(**dhl_cf_task_data) is not None

    def test_get_task(self, dhl_cf_task_data):
        task = CFUtil.get_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_get_all_app_tasks(self, dhl_cf_task_data):
        tasks = CFUtil.get_all_app_tasks(dhl_cf_task_data["app_guuid"])
        assert len(tasks) > 0

    def test_get_all_running_app_tasks(self):
        tasks = [{"state": "RUNNING"}, {"state": "SUCCEEDED"}]
        running_tasks = CFUtil.get_all_running_tasks(tasks)
        assert running_tasks == 1

    def test_cancel_task(self, dhl_cf_task_data):
        task = CFUtil.stop_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_read_recent_task_logs(self, dhl_cf_task_data):
        logs = CFUtil.read_recent_app_logs(
            dhl_cf_task_data["app_guuid"], dhl_cf_task_data["task_id"]
        )

        assert logs is not None
