import functools
import json
import logging
import os
import re

from cloudfoundry_client.client import CloudFoundryClient
from cloudfoundry_client.errors import InvalidStatusCode

logger = logging.getLogger("harvest_admin")


class CFHandler:
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.user = user
        self.password = password
        self.setup()

    @functools.cache
    def _app_guuid(self):
        """Get the GUUID of the running app."""
        try:
            return json.loads(os.getenv("VCAP_APPLICATION", "{}"))["application_id"]
        except KeyError:
            # No application id, not running under Cloud Foundry
            # go looking for an app with the right name.
            possible_apps = list(
                self.client.v3.apps.list(names="datagov-harvest-admin,datagov-harvest")
            )
            try:
                return possible_apps[0]["guid"]
            except (IndexError, KeyError):
                # no apps that work, just return an empty string
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

        If the API call fails, return None. Callers should handle this
        possibility.
        """
        if app_guuid is None:
            app_guuid = self._app_guuid()
        self.setup()
        # get tasks returns a single "page" and we need to manually wrap it into a
        # pagination object
        try:
            result_iter = self.client.v3.apps._pagination(
                self.client.v3.apps.get(app_guuid, "tasks")
            )
        except InvalidStatusCode:
            logger.warning(
                "Failed to get app tasks from CF, task information is not accurate"
            )
            return None
        return list(result_iter)

    @staticmethod
    def job_ids_from_tasks(task_list):
        """Convert a list of task dicts into a list of their job ids.

        An argument of None can occur when tasks can't be listed. Return
        an empty list in that case.
        """

        def _id_from_task(task):
            name = task["name"]
            try:
                return re.match(r"harvest-job-(.*)-\w+", name)[1]
            except TypeError:  # no match
                return None

        if task_list is None:
            return []

        id_list = [_id_from_task(task) for task in task_list]
        return [id_ for id_ in id_list if id_ is not None]

    def get_running_app_tasks(self, app_guuid=None):
        """Get a list of running tasks for a specified app.

        There are other tasks in the list that aren't our harvest jobs
        so we filter those out here.

        If the tasks cannot be listed, this returns None. Callers should
        handle this possibility.
        """
        tasks = self.get_all_app_tasks(app_guuid)
        if tasks is None:
            return None

        return [
            task
            for task in tasks
            if task.get("state", "") == "RUNNING"
            and task.get("name", "").startswith("harvest-job-")
        ]

    def num_running_app_tasks(self, app_guuid=None):
        """Count how many tasks are in the running state.

        If app_guid is not given, it will use the current app's GUUID.

        If tasks can't be listed, return None. Callers should handle this
        possibility.
        """
        tasks = self.get_running_app_tasks(app_guuid)
        if tasks is None:
            return None
        return len(tasks)

    def read_recent_app_logs(self, app_guuid=None, task_id=None):
        """Get a string of recent logs.

        If app_guid is not given, it will use the current app's GUUID.
        """
        if app_guuid is None:
            app_guuid = self._app_guuid()
        self.setup()
        app = self.client.v2.apps[app_guuid]
        logs = filter(lambda lg: task_id in lg, [str(log) for log in app.recent_logs()])
        return "\n".join(logs)
