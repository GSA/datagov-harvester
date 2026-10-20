import pytest


@pytest.fixture(autouse=True)
def allow_private_addresses(monkeypatch):
    """
    Tests mock `requests.get`, so nothing is actually fetched, but the
    private-address check still resolves the hostname. Allow by default so the
    suite doesn't depend on DNS; tests of the check itself turn it back off.
    """
    monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", True)
