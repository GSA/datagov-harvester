import pytest

from harvester.runner_settings import (
    configured_harvest_runner_max_tasks,
    harvest_runner_enabled,
    harvest_runner_max_tasks,
)


def test_harvest_runner_defaults_to_enabled_with_three_tasks(monkeypatch):
    monkeypatch.delenv("HARVEST_RUNNER_MAX_TASKS", raising=False)
    monkeypatch.delenv("HARVEST_RUNNER_ENABLED", raising=False)

    assert configured_harvest_runner_max_tasks() == 3
    assert harvest_runner_enabled() is True
    assert harvest_runner_max_tasks() == 3


def test_harvest_runner_uses_configured_max_tasks_when_enabled(monkeypatch):
    monkeypatch.setenv("HARVEST_RUNNER_MAX_TASKS", "5")
    monkeypatch.setenv("HARVEST_RUNNER_ENABLED", "true")

    assert configured_harvest_runner_max_tasks() == 5
    assert harvest_runner_max_tasks() == 5


@pytest.mark.parametrize("value", ["false", "FALSE"])
def test_disabled_harvest_runner_has_zero_effective_tasks(value, monkeypatch):
    monkeypatch.setenv("HARVEST_RUNNER_MAX_TASKS", "5")
    monkeypatch.setenv("HARVEST_RUNNER_ENABLED", value)

    assert configured_harvest_runner_max_tasks() == 5
    assert harvest_runner_enabled() is False
    assert harvest_runner_max_tasks() == 0
