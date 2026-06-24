from unittest.mock import patch

import pytest

from harvester.lib.load_manager import LoadManager
from harvester.lib.local_task_handler import LocalTaskHandler
from harvester.lib.task_handler import create_task_handler


class TestCreateTaskHandler:
    def test_uses_local_handler_without_cf_credentials(self, monkeypatch):
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        for var in ("CF_API_URL", "CF_SERVICE_USER", "CF_SERVICE_AUTH"):
            monkeypatch.delenv(var, raising=False)

        handler = create_task_handler()

        assert isinstance(handler, LocalTaskHandler)

    def test_uses_local_handler_with_only_cf_api_url(self, monkeypatch):
        # the common local-dev case: CF_API_URL is set (it ships in .env.sample)
        # but the service credentials are not. we should not attempt a CF login.
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        monkeypatch.setenv("CF_API_URL", "https://api.fr.cloud.gov")
        monkeypatch.delenv("CF_SERVICE_USER", raising=False)
        monkeypatch.delenv("CF_SERVICE_AUTH", raising=False)

        with patch("harvester.lib.task_handler.CFHandler") as cf_handler_mock:
            handler = create_task_handler()

        cf_handler_mock.assert_not_called()
        assert isinstance(handler, LocalTaskHandler)

    def test_raises_on_cloud_foundry_without_cf_credentials(self, monkeypatch):
        monkeypatch.setenv("VCAP_APPLICATION", '{"application_id":"abc"}')
        for var in ("CF_API_URL", "CF_SERVICE_USER", "CF_SERVICE_AUTH"):
            monkeypatch.delenv(var, raising=False)

        with patch("harvester.lib.task_handler.LocalTaskHandler") as local_handler_mock:
            with pytest.raises(
                RuntimeError, match="Cloud Foundry task API is not configured"
            ):
                create_task_handler()

        local_handler_mock.assert_not_called()

    @patch("harvester.lib.task_handler.CFHandler")
    def test_uses_cf_handler_on_cloud_foundry(self, cf_handler_mock, monkeypatch):
        monkeypatch.setenv("VCAP_APPLICATION", '{"application_id":"abc"}')
        monkeypatch.setenv("CF_API_URL", "https://api.example.com")
        monkeypatch.setenv("CF_SERVICE_USER", "user")
        monkeypatch.setenv("CF_SERVICE_AUTH", "pass")
        cf_handler_mock.side_effect = None

        create_task_handler()

        cf_handler_mock.assert_called_once_with(
            "https://api.example.com",
            "user",
            "pass",
        )

    @patch("harvester.lib.task_handler.LocalTaskHandler")
    @patch("harvester.lib.task_handler.CFHandler")
    def test_does_not_fallback_when_cf_handler_fails_on_cloud_foundry(
        self, cf_handler_mock, local_handler_mock, monkeypatch
    ):
        monkeypatch.setenv("VCAP_APPLICATION", '{"application_id":"abc"}')
        monkeypatch.setenv("CF_API_URL", "https://api.example.com")
        monkeypatch.setenv("CF_SERVICE_USER", "user")
        monkeypatch.setenv("CF_SERVICE_AUTH", "pass")
        cf_handler_mock.side_effect = RuntimeError("cf auth failed")

        with pytest.raises(RuntimeError, match="cf auth failed"):
            create_task_handler()

        local_handler_mock.assert_not_called()

    @patch("harvester.lib.task_handler.CFHandler")
    def test_uses_cf_handler_with_full_credentials_off_cf(
        self, cf_handler_mock, monkeypatch
    ):
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        monkeypatch.setenv("CF_API_URL", "https://api.example.com")
        monkeypatch.setenv("CF_SERVICE_USER", "user")
        monkeypatch.setenv("CF_SERVICE_AUTH", "pass")

        create_task_handler()

        cf_handler_mock.assert_called_once_with(
            "https://api.example.com",
            "user",
            "pass",
        )

    @patch("harvester.lib.task_handler.CFHandler")
    def test_falls_back_to_local_when_cf_init_fails_off_cf(
        self, cf_handler_mock, monkeypatch
    ):
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        monkeypatch.setenv("CF_API_URL", "https://api.example.com")
        monkeypatch.setenv("CF_SERVICE_USER", "user")
        monkeypatch.setenv("CF_SERVICE_AUTH", "pass")
        cf_handler_mock.side_effect = Exception("401 no password")

        handler = create_task_handler()

        assert isinstance(handler, LocalTaskHandler)


class TestLoadManagerLocalHandler:
    def test_load_manager_has_handler_without_cf_credentials(self, monkeypatch):
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        for var in ("CF_API_URL", "CF_SERVICE_USER", "CF_SERVICE_AUTH"):
            monkeypatch.delenv(var, raising=False)

        load_manager = LoadManager()

        assert hasattr(load_manager, "handler")
        assert isinstance(load_manager.handler, LocalTaskHandler)


SLEEP_CMD = 'python -c "import time; time.sleep(5)"'
NOOP_CMD = 'python -c "pass"'


class TestLocalTaskHandler:
    def setup_method(self):
        # isolate the class-level registry between tests
        LocalTaskHandler._tasks = {}

    def test_start_task_spawns_process_and_tracks_it(self):
        handler = LocalTaskHandler()
        task = handler.start_task(SLEEP_CMD, "harvest-job-abc-harvest")

        try:
            assert task["state"] == "RUNNING"
            assert task["name"] == "harvest-job-abc-harvest"
            running = handler.get_running_app_tasks()
            assert len(running) == 1
            assert running[0]["name"] == "harvest-job-abc-harvest"
            # the Popen handle must not leak into the task view
            assert "process" not in running[0]
        finally:
            handler.stop_task(task["guid"])

    def test_start_task_is_idempotent_for_same_task_id(self):
        handler = LocalTaskHandler()
        first = handler.start_task(SLEEP_CMD, "harvest-job-dup-harvest")
        second = handler.start_task(SLEEP_CMD, "harvest-job-dup-harvest")

        try:
            assert first["guid"] == second["guid"]
            assert handler.num_running_app_tasks() == 1
        finally:
            handler.stop_task(first["guid"])

    def test_completed_task_is_not_running(self):
        handler = LocalTaskHandler()
        task = handler.start_task(NOOP_CMD, "harvest-job-done-harvest")
        task["process"].wait(timeout=10)

        assert handler.num_running_app_tasks() == 0
        all_tasks = handler.get_all_app_tasks()
        assert all_tasks[0]["state"] == "SUCCEEDED"
