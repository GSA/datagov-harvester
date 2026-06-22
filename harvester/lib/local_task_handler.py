"""Run harvest jobs as local subprocesses when Cloud Foundry is unavailable."""

import logging
import shlex
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from harvester.lib.cf_handler import CFHandler

logger = logging.getLogger("harvest_admin")

APP_ROOT = Path(__file__).resolve().parents[2]


class LocalTaskHandler:
    """Minimal task runner for local development."""

    _tasks: dict[str, dict] = {}
    _lock = threading.Lock()

    def start_task(self, command, task_id):
        with self._lock:
            existing = self._tasks.get(task_id)
            if existing and existing["process"].poll() is None:
                return existing

            logger.info("Starting local harvest task: %s", command)
            process = subprocess.Popen(
                shlex.split(command),
                cwd=APP_ROOT,
            )
            task = {
                "guid": str(uuid.uuid4()),
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
            for task in self._tasks.values():
                if task["guid"] == task_id and task["state"] == "RUNNING":
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

    def num_running_app_tasks(self, app_guuid=None):
        return len(self.get_running_app_tasks(app_guuid))

    job_ids_from_tasks = CFHandler.job_ids_from_tasks

    def _refresh_states(self):
        with self._lock:
            for task in self._tasks.values():
                return_code = task["process"].poll()
                if return_code is None:
                    continue
                task["state"] = "SUCCEEDED" if return_code == 0 else "FAILED"

    @staticmethod
    def _task_view(task):
        return {key: value for key, value in task.items() if key != "process"}
