"""CORS tests for the public validator API."""

import pytest

from validator_api import create_app


@pytest.fixture
def validator_client(monkeypatch):
    monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", True)
    app = create_app()
    app.config.update({"TESTING": True})
    return app.test_client()


VALIDATE_ROUTES = (
    "/api/v1/validate",
    "/api/v1/validate/document",
)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
}


def assert_cors_headers(response):
    for header, expected in CORS_HEADERS.items():
        assert response.headers.get(header) == expected

    assert "Access-Control-Allow-Credentials" not in response.headers


@pytest.mark.parametrize("path", VALIDATE_ROUTES)
def test_cors_preflight(validator_client, path):
    response = validator_client.options(
        path,
        headers={
            "Origin": "https://inventory.example.org",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert 200 <= response.status_code < 300
    assert_cors_headers(response)


@pytest.mark.parametrize("path", VALIDATE_ROUTES)
def test_cors_post_validation_error(validator_client, path):
    response = validator_client.post(
        path,
        json={},
        headers={"Origin": "https://inventory.example.org"},
    )

    assert response.status_code == 422
    assert_cors_headers(response)


@pytest.mark.parametrize(
    "path",
    (
        "/health",
        "/validator/health",
        "/validator/docs",
        "/validator/openapi.json",
    ),
)
def test_cors_not_added_to_other_routes(validator_client, path):
    response = validator_client.get(
        path,
        headers={"Origin": "https://inventory.example.org"},
    )

    assert response.status_code == 200

    for header in CORS_HEADERS:
        assert header not in response.headers

    assert "Access-Control-Allow-Credentials" not in response.headers


def test_cors_not_added_to_unversioned_redirect(validator_client):
    response = validator_client.post(
        "/api/validate",
        json={},
        headers={"Origin": "https://inventory.example.org"},
    )

    assert response.status_code == 308
    assert "Access-Control-Allow-Origin" not in response.headers
