from cloudfoundry_client.client import CloudFoundryClient
from cloudfoundry_client.v3.tasks import TaskManager


class CFHandler:
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.user = user
        self.password = password
        self.setup()

    def setup(self):
        self.client = CloudFoundryClient(self.url)
        self.client.init_with_user_credentials(self.user, self.password)
        self.task_mgr = TaskManager(self.url, self.client)

    def start_task(self, app_guuid, command, task_id):
        return self.task_mgr.create(app_guuid, command, task_id)

    def stop_task(self, task_id):
        return self.task_mgr.cancel(task_id)

    def get_task(self, task_id):
        return self.task_mgr.get(task_id)

    def get_all_app_tasks(self, app_guuid):
        return [task for task in self.client.v3.apps[app_guuid].tasks()]

    def get_all_running_app_tasks(self, app_guuid):
        tasks = self.get_all_app_tasks(app_guuid)
        return sum(1 for _ in filter(lambda task: task["state"] == "RUNNING", tasks))

    def read_recent_app_logs(self, app_guuid, task_id=None):
        app = self.client.v2.apps[app_guuid]
        logs = filter(lambda lg: task_id in lg, [str(log) for log in app.recent_logs()])
        return "\n".join(logs)
