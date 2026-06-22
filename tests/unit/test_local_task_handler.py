from unittest.mock import patch

from harvester.lib.local_task_handler import LocalTaskHandler
from harvester.lib.load_manager import LoadManager
from harvester.lib.task_handler import create_task_handler


class TestCreateTaskHandler:
    def test_uses_local_handler_without_cf_credentials(self, monkeypatch):
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        for var in ("CF_API_URL", "CF_SERVICE_USER", "CF_SERVICE_AUTH"):
            monkeypatch.delenv(var, raising=False)

        handler = create_task_handler()

        assert isinstance(handler, LocalTaskHandler)

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


class TestLoadManagerLocalHandler:
    def test_load_manager_has_handler_without_cf_credentials(self, monkeypatch):
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
        for var in ("CF_API_URL", "CF_SERVICE_USER", "CF_SERVICE_AUTH"):
            monkeypatch.delenv(var, raising=False)

        load_manager = LoadManager()

        assert hasattr(load_manager, "handler")
        assert isinstance(load_manager.handler, LocalTaskHandler)
