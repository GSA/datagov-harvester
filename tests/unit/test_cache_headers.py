from unittest.mock import patch

import pytest

from app import create_app


@pytest.fixture
def app():
    with patch("app.load_manager.start", lambda: True):
        app = create_app()
    app.config.update({"TESTING": True})

    @app.route("/_cache_test")
    def cache_test():
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


def test_logged_in_page_is_private_no_store(client):
    with client.session_transaction() as sess:
        sess["user"] = "test.user@gsa.gov"

    response = client.get("/_cache_test")

    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["Expires"] == "0"


def test_login_route_is_private_no_store(client):
    response = client.get("/login")

    assert response.headers["Cache-Control"] == "private, no-store, max-age=0"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["Expires"] == "0"


def test_assets_are_cached_for_one_hour(client):
    response = client.get("/assets/img/icon-glossary.svg")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=3600, s-maxage=3600"
    assert "Pragma" not in response.headers
    assert "Expires" not in response.headers


def test_logout_clears_full_session(client):
    with client.session_transaction() as sess:
        sess["user"] = "test.user@gsa.gov"
        sess["state"] = "expected-state"
        sess["nonce"] = "expected-nonce"
        sess["next"] = "main.view_metrics"

    response = client.get("/logout")

    assert response.status_code == 302

    with client.session_transaction() as sess:
        assert dict(sess) == {}
