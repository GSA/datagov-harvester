from unittest.mock import MagicMock

import pytest
from flask import Flask

from scripts.sync_datasets import (
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

def test_records_missing_datasets_returns_query(session):
    query = (
        session.query.return_value
        .join.return_value
        .outerjoin.return_value
        .filter.return_value
    )

    result = _records_missing_datasets(session)

    assert result is query
    session.query.assert_called_once()
    session.query.return_value.join.assert_called_once()
    session.query.return_value.join.return_value.outerjoin.assert_called_once()


def test_records_missing_datasets_excludes_failed_ids(session):
    query = (
        session.query.return_value
        .join.return_value
        .outerjoin.return_value
    )

    _records_missing_datasets(session, exclude_ids=[1, 2])

    assert query.filter.call_count == 2


def test_datasets_with_unexpected_records_returns_query(session):
    query = session.query.return_value.join.return_value.filter.return_value

    result = _datasets_with_unexpected_records(session)

    assert result is query
    session.query.assert_called_once()
    session.query.return_value.join.assert_called_once()


def test_datasets_with_unexpected_records_excludes_failed_ids(session):
    query = session.query.return_value.join.return_value

    _datasets_with_unexpected_records(session, exclude_ids=[3])

    assert query.filter.call_count == 2
