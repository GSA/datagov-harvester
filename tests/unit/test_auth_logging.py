import os
import time
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

    class TestOIDCLoginSuccess:
        @pytest.fixture
        def oidc_client(self, app):
            return app.test_client()

        @pytest.fixture
        def mock_oidc_flow(self):
            """Mock the complete OIDC authentication flow"""
            with patch("app.main.auth.requests.post") as mock_post, patch(
                "app.main.auth.jwt.decode"
            ) as mock_jwt_decode, patch("app.deps.db.verify_user") as mock_verify_user:
                # Mock token endpoint response
                mock_post.return_value.status_code = 200
                mock_post.return_value.json.return_value = {
                    "id_token": "mock_id_token",
                    "access_token": "mock_access_token",
                }

                # Mock JWT decode to return user claims
                mock_jwt_decode.return_value = {
                    "email": "test@gsa.gov",
                    "sub": "mock-sso-id-12345",
                }

                # Mock user verification to return valid user
                mock_verify_user.return_value = True

                yield {
                    "post": mock_post,
                    "jwt_decode": mock_jwt_decode,
                    "verify_user": mock_verify_user,
                }

        def test_logs_event_tag(self, oidc_client, mock_oidc_flow):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                # Find the info log call about login success (has event=user_login)
                info_calls = [
                    call for call in mock_logger.info.call_args_list if len(call[0]) > 1
                ]
                login_success_call = [
                    call for call in info_calls if "event=user_login" in call[0][0]
                ][0]

                log_message = login_success_call[0][0]
                assert "event=user_login" in log_message

        def test_logs_method_tag(self, oidc_client, mock_oidc_flow):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                info_calls = [
                    call for call in mock_logger.info.call_args_list if len(call[0]) > 1
                ]
                login_success_call = [
                    call for call in info_calls if "event=user_login" in call[0][0]
                ][0]

                log_message = login_success_call[0][0]
                assert "method=oidc" in log_message

        def test_logs_user_email(self, oidc_client, mock_oidc_flow):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                info_calls = [
                    call for call in mock_logger.info.call_args_list if len(call[0]) > 1
                ]
                login_success_call = [
                    call for call in info_calls if "event=user_login" in call[0][0]
                ][0]

                call_args = login_success_call[0]
                assert "test@gsa.gov" in str(call_args)

        def test_logs_ssoid(self, oidc_client, mock_oidc_flow):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                info_calls = [
                    call for call in mock_logger.info.call_args_list if len(call[0]) > 1
                ]
                login_success_call = [
                    call for call in info_calls if "event=user_login" in call[0][0]
                ][0]

                call_args = login_success_call[0]
                assert "mock-sso-id-12345" in str(call_args)

        def test_logs_ip_address(self, oidc_client, mock_oidc_flow):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                    headers={"X-Forwarded-For": "198.51.100.23"},
                )

                info_calls = [
                    call for call in mock_logger.info.call_args_list if len(call[0]) > 1
                ]
                login_success_call = [
                    call for call in info_calls if "event=user_login" in call[0][0]
                ][0]

                call_args = login_success_call[0]
                assert "198.51.100.23" in str(call_args)

    class TestOIDCLoginRejection:
        @pytest.fixture
        def oidc_client(self, app):
            return app.test_client()

        @pytest.fixture
        def mock_oidc_flow_unregistered_user(self):
            """Mock OIDC flow where user authenticates but is not registered"""
            with patch("app.main.auth.requests.post") as mock_post, patch(
                "app.main.auth.jwt.decode"
            ) as mock_jwt_decode, patch("app.deps.db.verify_user") as mock_verify_user:
                # Mock token endpoint response
                mock_post.return_value.status_code = 200
                mock_post.return_value.json.return_value = {
                    "id_token": "mock_id_token",
                    "access_token": "mock_access_token",
                }

                # Mock JWT decode to return user claims
                mock_jwt_decode.return_value = {
                    "email": "unregistered@example.com",
                    "sub": "mock-sso-id-99999",
                }

                # Mock user verification to return None (user not registered)
                mock_verify_user.return_value = None

                yield {
                    "post": mock_post,
                    "jwt_decode": mock_jwt_decode,
                    "verify_user": mock_verify_user,
                }

        def test_logs_event_tag(self, oidc_client, mock_oidc_flow_unregistered_user):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                # Find the warning log call about login rejection
                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_call = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ][0]

                log_message = rejection_call[0][0]
                assert "event=login_rejected" in log_message

        def test_logs_reason_tag(self, oidc_client, mock_oidc_flow_unregistered_user):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_call = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ][0]

                log_message = rejection_call[0][0]
                assert "reason=unregistered_user" in log_message

        def test_logs_method_tag(self, oidc_client, mock_oidc_flow_unregistered_user):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_call = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ][0]

                log_message = rejection_call[0][0]
                assert "method=oidc" in log_message

        def test_logs_user_email(self, oidc_client, mock_oidc_flow_unregistered_user):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_call = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ][0]

                call_args = rejection_call[0]
                assert "unregistered@example.com" in str(call_args)

        def test_logs_ssoid(self, oidc_client, mock_oidc_flow_unregistered_user):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_call = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ][0]

                call_args = rejection_call[0]
                assert "mock-sso-id-99999" in str(call_args)

        def test_logs_ip_address(self, oidc_client, mock_oidc_flow_unregistered_user):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                    headers={"X-Forwarded-For": "192.0.2.100"},
                )

                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_call = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ][0]

                call_args = rejection_call[0]
                assert "192.0.2.100" in str(call_args)

        def test_uses_warning_log_level(
            self, oidc_client, mock_oidc_flow_unregistered_user
        ):
            with oidc_client.session_transaction() as sess:
                sess["state"] = "test_state"
                sess["nonce"] = "test_nonce"

            with patch("app.main.auth.logger") as mock_logger:
                oidc_client.get(
                    "/callback?code=test_code&state=test_state",
                )

                # Verify warning was called and has our structured message
                assert mock_logger.warning.called
                warning_calls = [
                    call
                    for call in mock_logger.warning.call_args_list
                    if len(call[0]) > 1
                ]
                rejection_calls = [
                    call
                    for call in warning_calls
                    if "event=login_rejected" in call[0][0]
                ]
                assert len(rejection_calls) > 0

    class TestLogout:
        @pytest.fixture
        def client_with_session(self, app):
            client = app.test_client()
            # Set up a logged-in session
            with client.session_transaction() as sess:
                sess["user"] = "test@gsa.gov"
                sess["last_activity"] = int(time.time())
            return client

        def test_logs_event_tag(self, client_with_session):
            with patch("app.main.auth.logger") as mock_logger:
                client_with_session.get("/logout")

                mock_logger.info.assert_called_once()
                log_message = mock_logger.info.call_args[0][0]
                assert "event=user_logout" in log_message

        def test_logs_user_email_when_logged_in(self, client_with_session):
            with patch("app.main.auth.logger") as mock_logger:
                client_with_session.get("/logout")

                mock_logger.info.assert_called_once()
                call_args = mock_logger.info.call_args[0]
                assert "test@gsa.gov" in str(call_args)

        def test_logs_anonymous_when_no_user(self, app):
            client = app.test_client()
            with patch("app.main.auth.logger") as mock_logger:
                client.get("/logout")

                mock_logger.info.assert_called_once()
                call_args = mock_logger.info.call_args[0]
                assert "<anonymous>" in str(call_args)

        def test_logs_ip_address(self, client_with_session):
            with patch("app.main.auth.logger") as mock_logger:
                client_with_session.get(
                    "/logout",
                    headers={"X-Forwarded-For": "203.0.113.99"},
                )

                mock_logger.info.assert_called_once()
                call_args = mock_logger.info.call_args[0]
                assert "203.0.113.99" in str(call_args)

    class TestAPITokenLogin:
        @pytest.fixture
        def api_client(self, app):
            app.config["API_TOKEN"] = "test-api-token-12345"
            return app.test_client()

        def test_logs_event_tag(self, api_client):
            # Import here to get the actual logger instance
            import app.auth as auth_module

            with patch.object(auth_module, "logger") as mock_logger:
                # Use an authenticated endpoint (POST requires auth)
                api_client.post(
                    "/api/organization/add",
                    json={"name": "Test Org"},
                    headers={"X-API-Key": "test-api-token-12345"},
                )

                # Find the info log call with event=user_login
                info_calls = mock_logger.info.call_args_list
                login_calls = [
                    call for call in info_calls if "event=user_login" in str(call)
                ]
                assert len(login_calls) > 0
                log_message = login_calls[0][0][0]
                assert "event=user_login" in log_message

        def test_logs_method_tag(self, api_client):
            import app.auth as auth_module

            with patch.object(auth_module, "logger") as mock_logger:
                api_client.post(
                    "/api/organization/add",
                    json={"name": "Test Org"},
                    headers={"X-API-Key": "test-api-token-12345"},
                )

                info_calls = mock_logger.info.call_args_list
                login_calls = [
                    call for call in info_calls if "event=user_login" in str(call)
                ]
                assert len(login_calls) > 0
                log_message = login_calls[0][0][0]
                assert "method=api_token" in log_message

        def test_logs_user_as_api_token(self, api_client):
            import app.auth as auth_module

            with patch.object(auth_module, "logger") as mock_logger:
                api_client.post(
                    "/api/organization/add",
                    json={"name": "Test Org"},
                    headers={"X-API-Key": "test-api-token-12345"},
                )

                info_calls = mock_logger.info.call_args_list
                login_calls = [
                    call for call in info_calls if "event=user_login" in str(call)
                ]
                assert len(login_calls) > 0
                call_args = login_calls[0][0]
                assert "<api_token>" in str(call_args)

        def test_logs_ip_address(self, api_client):
            import app.auth as auth_module

            with patch.object(auth_module, "logger") as mock_logger:
                api_client.post(
                    "/api/organization/add",
                    json={"name": "Test Org"},
                    headers={
                        "X-API-Key": "test-api-token-12345",
                        "X-Forwarded-For": "198.51.100.50",
                    },
                )

                info_calls = mock_logger.info.call_args_list
                login_calls = [
                    call for call in info_calls if "event=user_login" in str(call)
                ]
                assert len(login_calls) > 0
                call_args = login_calls[0][0]
                assert "198.51.100.50" in str(call_args)

        def test_logs_request_path(self, api_client):
            import app.auth as auth_module

            with patch.object(auth_module, "logger") as mock_logger:
                api_client.post(
                    "/api/organization/add",
                    json={"name": "Test Org"},
                    headers={"X-API-Key": "test-api-token-12345"},
                )

                info_calls = mock_logger.info.call_args_list
                login_calls = [
                    call for call in info_calls if "event=user_login" in str(call)
                ]
                assert len(login_calls) > 0
                call_args = login_calls[0][0]
                assert "/api/organization/add" in str(call_args)
