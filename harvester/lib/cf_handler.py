import json
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

    def _app_guuid(self):
        """Get the GUUID of the running app."""
        try:
            return json.loads(os.getenv("VCAP_APPLICATION", "{}"))["application_id"]
        except KeyError:
            # No application id, return an empty string
            return ""

    def setup(self):
        logger.debug("Running CFHandler setup")
        self.client = CloudFoundryClient(self.url)
        self.client.init_with_user_credentials(self.user, self.password)
        self.task_mgr = self.client.v3.tasks

    def start_task(self, command, task_id):
        """Start a task on the currently running app's GUUID."""
        self.setup()
        TASK_MEMORY = os.getenv("HARVEST_RUNNER_TASK_MEM", "1536")
        TASK_DISK = os.getenv("HARVEST_RUNNER_TASK_DISK", "4096")
        return self.task_mgr.create(
            self._app_guuid(),
            command=command,
            name=task_id,
            memory_in_mb=TASK_MEMORY,
            disk_in_mb=TASK_DISK,
        )

    def stop_task(self, task_id):
        self.setup()
        return self.task_mgr.cancel(task_id)

    def get_task(self, task_id):
        self.setup()
        return self.task_mgr.get(task_id)

    def get_all_app_tasks(self, app_guuid=None):
        """Get a list of all tasks for a specified app.

        If the GUUID is not given, then use the `VCAP_APPLICATION` environment
        variable to look up the current running app's guuid.
        """
        if app_guuid is None:
            app_guuid = self._app_guuid()
        self.setup()
        return [task for task in self.client.v3.apps[app_guuid].tasks()]

    def num_running_app_tasks(self, app_guuid=None):
        """Count how many tasks are in the running state.

        If app_guid is not given, it will use the current app's GUUID.
        """
        tasks = self.get_all_app_tasks(app_guuid)
        return sum(1 for _ in filter(lambda task: task["state"] == "RUNNING", tasks))

    def read_recent_app_logs(self, app_guuid=None, task_id=None):
        """Get a string of recent logs.

        If app_guid is not given, it will use the current app's GUUID.
        """
        if app_guuid is None:
            app_guuid = self._app_guuid()
        self.setup()
        app = self.client.v3.apps[app_guuid]
        logs = filter(lambda lg: task_id in lg, [str(log) for log in app.recent_logs()])
        return "\n".join(logs)
