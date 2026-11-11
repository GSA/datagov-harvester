import pytest

from validator_api import create_app


@pytest.fixture(autouse=True)
def allow_private_addresses(monkeypatch):
    """
    Tests mock `requests.get`, so nothing is actually fetched, but the
    private-address check still resolves the hostname. Allow by default so the
    suite doesn't depend on DNS; tests of the check itself turn it back off.
    """
    monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", True)


@pytest.fixture(autouse=True)
def isolated_large_document_lock(monkeypatch, tmp_path):
    """The real lock file is machine-wide, so parallel (xdist) workers would
    get each other's 503 "busy" response on large catalogs."""
    monkeypatch.setattr(
        "validator_api.api.LARGE_DOCUMENT_LOCK_PATH", str(tmp_path / "large.lock")
    )


# Named apart from the root conftest's `app`/`client`, which are the admin app.
@pytest.fixture
def validator_app():
    app = create_app()
    app.config.update({"TESTING": True})
    return app


@pytest.fixture
def validator_client(validator_app):
    return validator_app.test_client()
