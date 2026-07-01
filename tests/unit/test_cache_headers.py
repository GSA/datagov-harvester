from pathlib import Path
from unittest.mock import patch

import pytest
from flask import request, session

from app import HSTS_HEADER, create_app


@pytest.fixture
def app():
    with patch("app.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True})

    @app.route("/_cache_test")
    def cache_test():
        return "ok"

    @app.route("/_set_session")
    def set_session():
        for key, value in request.args.items():
            session[key] = value
        return "ok"

    return app


@pytest.fixture
def dbapp(app):
    yield app


@pytest.fixture
def default_function_fixture():
    yield


@pytest.fixture
def client(app):
    return app.test_client()


def test_anonymous_page_is_publicly_cacheable(client):
    response = client.get("/_cache_test")

    assert response.headers["Cache-Control"] == "public, max-age=60, s-maxage=60"
    assert "Pragma" not in response.headers
    assert "Expires" not in response.headers


def test_https_responses_set_preload_ready_hsts_header(client):
    response = client.get("/_cache_test", headers={"X-Forwarded-Proto": "https"})

    assert response.headers["Strict-Transport-Security"] == HSTS_HEADER


def test_nginx_sets_hsts_header_for_public_proxy():
    nginx_header = f'add_header Strict-Transport-Security "{HSTS_HEADER}" always;'

    nginx_config = Path("proxy/nginx-common.conf").read_text()

    assert nginx_header in nginx_config
    assert "proxy_hide_header Strict-Transport-Security;" in nginx_config


def test_logged_in_page_is_private_no_store(client):
    client.get(
        "/_set_session?user=test.user@gsa.gov&last_activity=1000",
        base_url="https://localhost",
    )

    with patch("app.current_unix_timestamp", return_value=1_100):
        response = client.get("/_cache_test", base_url="https://localhost")

    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["Expires"] == "0"
    assert any(
        header.startswith("harvest_auth=1;")
        for header in response.headers.getlist("Set-Cookie")
    )

    with client.session_transaction() as sess:
        assert sess["user"] == "test.user@gsa.gov"
        assert sess["last_activity"] == 1_100


def test_logged_in_session_without_last_activity_is_initialized(client):
    client.get(
        "/_set_session?user=test.user@gsa.gov",
        base_url="https://localhost",
    )

    with patch("app.current_unix_timestamp", return_value=1_100):
        response = client.get("/_cache_test", base_url="https://localhost")

    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"

    with client.session_transaction() as sess:
        assert sess["user"] == "test.user@gsa.gov"
        assert sess["last_activity"] == 1_100


def test_login_route_is_private_no_store(client):
    response = client.get("/login")

    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["Expires"] == "0"
    assert any(
        header.startswith("harvest_session=")
        for header in response.headers.getlist("Set-Cookie")
    )


def test_assets_are_cached_for_one_hour(client):
    response = client.get("/assets/img/icon-glossary.svg")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=3600, s-maxage=3600"
    assert "Pragma" not in response.headers
    assert "Expires" not in response.headers


def test_versioned_static_assets_are_cached_for_one_hour(app):
    app.config["ASSET_VERSION"] = "8eb9d3e"

    response = app.test_client().get("/js/datetime.8eb9d3e.js")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=3600, s-maxage=3600"
    assert "Pragma" not in response.headers
    assert "Expires" not in response.headers


def test_logout_clears_full_session(client):
    with client.session_transaction() as sess:
        sess["user"] = "test.user@gsa.gov"
        sess["state"] = "expected-state"
        sess["nonce"] = "expected-nonce"
        sess["last_activity"] = 1_000
        sess["next"] = "main.view_metrics"

    response = client.get("/logout")

    assert response.status_code == 302
    assert any(
        header.startswith("harvest_auth=;")
        for header in response.headers.getlist("Set-Cookie")
    )

    with client.session_transaction() as sess:
        assert dict(sess) == {}


def test_stale_logged_in_session_is_cleared_and_cookie_deleted(client):
    client.get(
        "/_set_session?user=test.user@gsa.gov&last_activity=1000",
        base_url="https://localhost",
    )

    with patch("app.current_unix_timestamp", return_value=1_901):
        response = client.get("/_cache_test", base_url="https://localhost")

    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"
    assert any(
        header.startswith("harvest_session=;")
        for header in response.headers.getlist("Set-Cookie")
    )
    assert any(
        header.startswith("harvest_auth=;")
        for header in response.headers.getlist("Set-Cookie")
    )

    with client.session_transaction() as sess:
        assert dict(sess) == {}
