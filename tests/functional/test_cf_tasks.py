import os
from time import sleep

from harvester.lib.cf_handler import CFHandler

cf_handler = CFHandler(
    os.getenv("CF_API_URL"), os.getenv("CF_SERVICE_USER"), os.getenv("CF_SERVICE_AUTH")
)

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
