import os

HARVEST_RUNNER_MAX_TASKS_ENV = "HARVEST_RUNNER_MAX_TASKS"
HARVEST_RUNNER_ENABLED_ENV = "HARVEST_RUNNER_ENABLED"
DEFAULT_HARVEST_RUNNER_MAX_TASKS = 3
DEFAULT_HARVEST_RUNNER_ENABLED = True


def configured_harvest_runner_max_tasks() -> int:
    return int(
        os.getenv(
            HARVEST_RUNNER_MAX_TASKS_ENV,
            str(DEFAULT_HARVEST_RUNNER_MAX_TASKS),
        )
    )


def harvest_runner_enabled() -> bool:
    return (
        os.getenv(
            HARVEST_RUNNER_ENABLED_ENV,
            str(DEFAULT_HARVEST_RUNNER_ENABLED),
        ).lower()
        == "true"
    )


def harvest_runner_max_tasks() -> int:
    if not harvest_runner_enabled():
        return 0
    return configured_harvest_runner_max_tasks()
