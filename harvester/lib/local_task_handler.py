"""Run harvest jobs as local subprocesses when Cloud Foundry is unavailable.

This is the local-dev counterpart to CFHandler. Instead of asking the Cloud
Foundry task API to run ``python harvester/harvest.py <job_id> <job_type>``, it
spawns that same command as a child process of the running app. It exposes the
same surface that LoadManager and harvest_job_starter rely on, so the two
handlers are interchangeable.
"""

import logging
import shlex
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

from harvester.lib.cf_handler import ACTIVE_HARVEST_TASK_STATES, CFHandler

logger = logging.getLogger("harvest_admin")

# repo root: harvester/lib/local_task_handler.py -> repo root is two parents up
APP_ROOT = Path(__file__).resolve().parents[2]


class LocalTaskHandler:
    """Minimal task runner for local development.

    Task state lives in a class-level registry so that it survives the
    short-lived handler instances that LoadManager creates. Note that harvest
    jobs run as separate subprocesses, so a handler created inside a harvest
    subprocess starts with an empty registry; that is fine because the
    duplicate-job guard in LoadManager.start_job is enforced at the database
    level, not via this registry.
    """

    _tasks: dict[str, dict] = {}
    _lock = threading.Lock()

    def start_task(self, command, task_id):
        with self._lock:
            existing = self._tasks.get(task_id)
            if existing and existing["process"].poll() is None:
                # already running; don't start a duplicate
                return existing

            logger.info("Starting local harvest task: %s", command)
            process = subprocess.Popen(
                shlex.split(command),
                cwd=APP_ROOT,
            )
            task = {
                "guid": task_id,
                "name": task_id,
                "command": command,
                "state": "RUNNING",
                "process": process,
                "updated_at": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
            self._tasks[task_id] = task
            return task

    def stop_task(self, task_id):
        with self._lock:
            task = self._tasks.get(task_id)
            if task and task["state"] == "RUNNING":
                task["process"].terminate()
                task["state"] = "CANCELING"
                return task
        return None

    def get_all_app_tasks(self, app_guuid=None):
        self._refresh_states()
        with self._lock:
            return [self._task_view(task) for task in self._tasks.values()]

    def get_running_app_tasks(self, app_guuid=None):
        tasks = self.get_all_app_tasks(app_guuid)
        return [
            task
            for task in tasks
            if task.get("state") == "RUNNING"
            and task.get("name", "").startswith("harvest-job-")
        ]

    def get_active_harvest_tasks(self, app_guuid=None):
        tasks = self.get_all_app_tasks(app_guuid)
        return [
            task
            for task in tasks
            if task.get("state") in ACTIVE_HARVEST_TASK_STATES
            and task.get("name", "").startswith("harvest-job-")
        ]

    def num_running_app_tasks(self, app_guuid=None):
        return len(self.get_running_app_tasks(app_guuid))

    # the job-id parsing is identical to CF, so reuse it. wrap in staticmethod:
    # CFHandler.job_ids_from_tasks resolves to a plain function here, and without
    # re-wrapping it would bind `self` as the first positional arg when called.
    job_ids_from_tasks = staticmethod(CFHandler.job_ids_from_tasks)

    def _refresh_states(self):
        with self._lock:
            for task in self._tasks.values():
                if task["state"] != "RUNNING":
                    continue
                return_code = task["process"].poll()
                if return_code is None:
                    continue
                task["state"] = "SUCCEEDED" if return_code == 0 else "FAILED"

    @staticmethod
    def _task_view(task):
        # hide the Popen handle; callers only expect json-serializable fields
        return {key: value for key, value in task.items() if key != "process"}
