import logging
import os

from cloudfoundry_client.client import CloudFoundryClient

logger = logging.getLogger("harvest_admin")


class CFHandler:
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.user = user
        self.password = password
        self.setup()

    def setup(self):
        logger.debug("Running CFHandler setup")
        self.client = CloudFoundryClient(self.url)
        self.client.init_with_user_credentials(self.user, self.password)
        self.task_mgr = self.client.v3.tasks

    def start_task(self, app_guuid, command, task_id):
        self.setup()
        TASK_MEMORY = os.getenv("HARVEST_RUNNER_TASK_MEM", "4096")
        TASK_DISK = os.getenv("HARVEST_RUNNER_TASK_DISK", "1536")
        return self.task_mgr.create(app_guuid, command, task_id, TASK_MEMORY, TASK_DISK)

    def stop_task(self, task_id):
        self.setup()
        return self.task_mgr.cancel(task_id)

    def get_task(self, task_id):
        self.setup()
        return self.task_mgr.get(task_id)

    def get_all_app_tasks(self, app_guuid):
        self.setup()
        return [task for task in self.client.v3.apps[app_guuid].tasks()]

    def get_all_running_app_tasks(self, app_guuid):
        tasks = self.get_all_app_tasks(app_guuid)
        return sum(1 for _ in filter(lambda task: task["state"] == "RUNNING", tasks))

    def read_recent_app_logs(self, app_guuid, task_id=None):
        self.setup()
        app = self.client.v2.apps[app_guuid]
        logs = filter(lambda lg: task_id in lg, [str(log) for log in app.recent_logs()])
        return "\n".join(logs)
