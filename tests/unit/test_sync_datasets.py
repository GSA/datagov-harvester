from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from flask import Flask

from scripts.sync_datasets import (
    _datasets_missing_records,
    _datasets_with_unexpected_records,
    _records_missing_datasets,
)


@pytest.fixture(scope="session", autouse=True)
def app():
    """Lightweight Flask app that avoids touching the real DB."""

    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


@pytest.fixture(autouse=True)
def dbapp():
    """Override the global dbapp fixture to avoid touching the real DB."""

    yield


@pytest.fixture(autouse=True)
def default_function_fixture():
    """Bypass the default patching fixture which depends on the database."""

    yield


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def interface():
    return MagicMock()


def test_records_missing_datasets_returns_query_results():
    session = MagicMock()
    expected = [SimpleNamespace(id="rec-1"), SimpleNamespace(id="rec-2")]

    (
        session.query.return_value
        .outerjoin.return_value
        .filter.return_value
        .all.return_value
    ) = expected

    assert _records_missing_datasets(session) == expected

    session.query.assert_called_once()
    session.query.return_value.outerjoin.assert_called_once()


def test_datasets_missing_records_returns_query_results():
    session = MagicMock()
    expected = [SimpleNamespace(slug="ds-1")]

    (
        session.query.return_value
        .outerjoin.return_value
        .filter.return_value
        .all.return_value
    ) = expected

    assert _datasets_missing_records(session) == expected
    session.query.assert_called_once()


def test_datasets_with_unexpected_records_returns_query_results():
    session = MagicMock()
    expected = [SimpleNamespace(slug="bad-ds")]

    (
        session.query.return_value
        .join.return_value
        .filter.return_value
        .all.return_value
    ) = expected

    assert _datasets_with_unexpected_records(session) == expected
    session.query.assert_called_once()
