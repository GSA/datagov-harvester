import os
from unittest.mock import patch

import pytest

from app import create_app
from app.local_dev_auth import LOCAL_DEV_SESSION_EMAIL


@pytest.fixture
def app():
    with patch("app.deps.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True, "WTF_CSRF_ENABLED": False})
    return app


@pytest.fixture
def dev_login_env():
    with patch.dict(
        os.environ,
        {"ENABLE_LOCAL_DEV_LOGIN": "true"},
        clear=False,
    ):
        os.environ.pop("VCAP_APPLICATION", None)
        yield


@pytest.fixture
def dev_client(dev_login_env):
    with patch("app.deps.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True, "WTF_CSRF_ENABLED": False})
    return app.test_client()


class TestAuthLogging:
    class TestLocalDevLoginSuccess:
        def test_logs_event_tag(self, dev_client):
            with patch("app.main.auth.logger") as mock_logger:
                dev_client.post(
                    "/login",
                    data={"username": "admin", "password": "admin"},
                )

                mock_logger.info.assert_called_once()
                log_message = mock_logger.info.call_args[0][0]
                assert "event=user_login" in log_message

        def test_logs_method_tag(self, dev_client):
            with patch("app.main.auth.logger") as mock_logger:
                dev_client.post(
                    "/login",
                    data={"username": "admin", "password": "admin"},
                )

                mock_logger.info.assert_called_once()
                log_message = mock_logger.info.call_args[0][0]
                assert "method=local_dev" in log_message

        def test_logs_user_email(self, dev_client):
            with patch("app.main.auth.logger") as mock_logger:
                dev_client.post(
                    "/login",
                    data={"username": "admin", "password": "admin"},
                )

                mock_logger.info.assert_called_once()
                call_args = mock_logger.info.call_args[0]
                assert LOCAL_DEV_SESSION_EMAIL in call_args

        def test_logs_ip_from_x_forwarded_for(self, dev_client):
            with patch("app.main.auth.logger") as mock_logger:
                dev_client.post(
                    "/login",
                    data={"username": "admin", "password": "admin"},
                    headers={"X-Forwarded-For": "203.0.113.42"},
                )

                mock_logger.info.assert_called_once()
                call_args = mock_logger.info.call_args[0]
                assert "203.0.113.42" in str(call_args)

        def test_logs_ip_from_remote_addr_when_no_forwarded(self, dev_client):
            with patch("app.main.auth.logger") as mock_logger:
                dev_client.post(
                    "/login",
                    data={"username": "admin", "password": "admin"},
                )

                mock_logger.info.assert_called_once()
                call_args = mock_logger.info.call_args[0]
                assert "127.0.0.1" in str(call_args)
