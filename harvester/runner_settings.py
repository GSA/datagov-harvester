import os

HARVEST_RUNNER_MAX_TASKS_ENV = "HARVEST_RUNNER_MAX_TASKS"
DEFAULT_HARVEST_RUNNER_MAX_TASKS = 5


def harvest_runner_max_tasks() -> int:
    return int(
        os.getenv(
            HARVEST_RUNNER_MAX_TASKS_ENV,
            str(DEFAULT_HARVEST_RUNNER_MAX_TASKS),
        )
    )


def harvest_scheduling_is_disabled() -> bool:
    return harvest_runner_max_tasks() <= 0
