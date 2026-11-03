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


# Named apart from the root conftest's `app`/`client`, which are the admin app.
@pytest.fixture
def validator_app():
    app = create_app()
    app.config.update({"TESTING": True})
    return app


@pytest.fixture
def validator_client(validator_app):
    return validator_app.test_client()
