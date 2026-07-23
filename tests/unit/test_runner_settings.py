from harvester.runner_settings import (
    DEFAULT_HARVEST_RUNNER_MAX_TASKS,
    harvest_runner_max_tasks,
)


def test_harvest_runner_max_tasks_defaults_to_three(monkeypatch):
    monkeypatch.delenv("HARVEST_RUNNER_MAX_TASKS", raising=False)

    assert DEFAULT_HARVEST_RUNNER_MAX_TASKS == 3
    assert harvest_runner_max_tasks() == 3
