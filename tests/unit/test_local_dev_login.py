import os
from unittest.mock import patch

import pytest

from app import create_app
from app.local_dev_auth import (
    LOCAL_DEV_SESSION_EMAIL,
    is_local_dev_login_enabled,
    is_running_on_cloud_foundry,
)


@pytest.fixture
def app():
    with patch("app.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True, "WTF_CSRF_ENABLED": False})
    return app


@pytest.fixture
def client(app):
    return app.test_client()


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
    with patch("app.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True, "WTF_CSRF_ENABLED": False})
    return app.test_client()


class TestLocalDevLoginGuards:
    def test_session_cookie_secure_false_when_not_on_cloud_foundry(self, app):
        assert app.config["SESSION_COOKIE_SECURE"] is False

    def test_session_cookie_secure_true_when_vcap_application_is_present(self):
        with patch.dict(
            os.environ,
            {"VCAP_APPLICATION": '{"application_name": "harvester"}'},
            clear=False,
        ):
            with patch("app.load_manager.start", lambda: True):
                cf_app = create_app()
        assert cf_app.config["SESSION_COOKIE_SECURE"] is True

    def test_is_local_dev_login_disabled_on_cloud_foundry(self):
        with patch.dict(
            os.environ,
            {
                "ENABLE_LOCAL_DEV_LOGIN": "true",
                "VCAP_APPLICATION": '{"application_name": "harvester"}',
            },
            clear=False,
        ):
            assert is_running_on_cloud_foundry() is True
            assert is_local_dev_login_enabled() is False

    def test_login_redirects_to_login_gov_when_disabled(self, client):
        with patch.dict(os.environ, {"ENABLE_LOCAL_DEV_LOGIN": ""}, clear=False):
            response = client.get("/login")
        assert response.status_code == 302
        assert "openid_connect/authorize" in response.location

    def test_login_post_returns_404_when_disabled(self, client):
        response = client.post(
            "/login",
            data={"username": "admin", "password": "admin"},
        )
        assert response.status_code == 404

    def test_login_post_returns_404_by_default(self, client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_LOCAL_DEV_LOGIN", None)
            response = client.post(
                "/login",
                data={"username": "admin", "password": "admin"},
            )
        assert response.status_code == 404

    def test_login_post_returns_404_when_vcap_application_is_present(self, client):
        with patch.dict(
            os.environ,
            {
                "ENABLE_LOCAL_DEV_LOGIN": "true",
                "VCAP_APPLICATION": '{"application_name": "harvester"}',
            },
            clear=False,
        ):
            with patch("app.load_manager.start", lambda: True):
                cf_app = create_app()
            cf_app.config.update({"TESTING": True, "WTF_CSRF_ENABLED": False})
            response = cf_app.test_client().post(
                "/login",
                data={"username": "admin", "password": "admin"},
            )
        assert response.status_code == 404


class TestLocalDevLogin:
    def test_get_login_shows_form(self, dev_client):
        response = dev_client.get("/login")
        assert response.status_code == 200
        assert b"Local development login" in response.data
        assert b'name="username"' in response.data

    def test_wrong_password_shows_error(self, dev_client):
        response = dev_client.post(
            "/login",
            data={"username": "admin", "password": "wrong"},
        )
        assert response.status_code == 200
        assert b"Invalid username or password." in response.data

    def test_successful_login_sets_session(self, dev_client):
        response = dev_client.post(
            "/login",
            data={"username": "admin", "password": "admin"},
        )
        assert response.status_code == 302
        assert response.location.endswith("/")

        with dev_client.session_transaction() as sess:
            assert sess["user"] == LOCAL_DEV_SESSION_EMAIL
            assert "last_activity" in sess

    def test_authenticated_user_can_access_protected_route(self, dev_client):
        dev_client.post(
            "/login",
            data={"username": "admin", "password": "admin"},
        )
        response = dev_client.get("/organization/add")
        assert response.status_code == 200

    def test_login_oidc_redirects_to_login_gov(self, dev_client):
        response = dev_client.get("/login/oidc")
        assert response.status_code == 302
        assert "openid_connect/authorize" in response.location
