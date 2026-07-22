import pytest

from harvester.runner_settings import harvest_runner_max_tasks


def test_harvest_runner_max_tasks_defaults_to_three(monkeypatch):
    monkeypatch.delenv("HARVEST_RUNNER_MAX_TASKS", raising=False)

    assert harvest_runner_max_tasks() == 3


@pytest.mark.parametrize(("value", "expected"), [("0", 0), ("3", 3)])
def test_harvest_runner_max_tasks_uses_environment(value, expected, monkeypatch):
    monkeypatch.setenv("HARVEST_RUNNER_MAX_TASKS", value)

    assert harvest_runner_max_tasks() == expected
