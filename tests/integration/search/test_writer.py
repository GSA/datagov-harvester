import pytest

from search.client import OpenSearchClient
from search.config import DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS
from search.writer import (
    ConnectionTimeout,
    OpenSearchWriter,
)
from search.writer import helpers as oswriter_helpers
from search.writer import time as oswriter_time
from tests.conftest import FakeClient


def test_run_with_timeout_retry(monkeypatch):
    client = OpenSearchClient.from_environment()
    iface = OpenSearchWriter(client)
    calls = {"count": 0}
    sleeps = []

    def action():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionTimeout("timeout")
        return "ok"

    def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(oswriter_time, "sleep", fake_sleep)

    result = iface._run_with_timeout_retry(
        action,
        action_name="test",
        timeout_retries=3,
        timeout_backoff_base=2.0,
    )

    assert result == "ok"
    assert calls["count"] == 3
    assert sleeps == [2.0, 4.0]


def test_index_datasets_counts_errors(monkeypatch, sample_dataset):
    client = OpenSearchClient.from_environment()
    iface = OpenSearchWriter(client)

    def fake_streaming_bulk(client, documents, raise_on_error, max_retries):
        assert client is iface.client
        assert len(documents) == 1
        yield True, {"index": {"result": "created", "_id": "dataset-1"}}
        yield (
            False,
            {
                "index": {
                    "status": 400,
                    "error": {
                        "type": "mapper_parsing_exception",
                        "reason": "bad",
                        "caused_by": {"type": "illegal_argument_exception"},
                    },
                }
            },
        )

    monkeypatch.setattr(oswriter_helpers, "streaming_bulk", fake_streaming_bulk)

    succeeded, failed, errors = iface.index_datasets(
        [sample_dataset],
        refresh_after=False,
    )

    assert succeeded == 1
    assert failed == 1
    assert len(errors) == 2


def test_delete_dataset_by_id_calls_client(monkeypatch):
    fake_client = FakeClient(exists=False)
    monkeypatch.setattr(
        OpenSearchClient,
        "_create_test_opensearch_client",
        staticmethod(lambda host: fake_client),
    )

    iface = OpenSearchClient(test_host="localhost")
    iface = OpenSearchWriter(iface)

    result = iface.delete_dataset_by_id("dataset-1", refresh_after=False)

    assert result is True
    assert iface.client.deleted == [
        (
            iface.INDEX_NAME,
            "dataset-1",
            [404],
            DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
        )
    ]


def test_delete_dataset_by_id_no_id(monkeypatch):
    fake_client = FakeClient(exists=False)
    monkeypatch.setattr(
        OpenSearchClient,
        "_create_test_opensearch_client",
        staticmethod(lambda host: fake_client),
    )

    iface = OpenSearchClient(test_host="localhost")
    iface = OpenSearchWriter(iface)

    result = iface.delete_dataset_by_id("", refresh_after=False)

    assert result is False
    assert iface.client.deleted == []


def test_run_with_timeout_retry_eventual_success(monkeypatch, opensearch_writer):
    monkeypatch.setattr(oswriter_time, "sleep", lambda _: None)

    attempts = {"count": 0}

    def _action():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionTimeout("TIMEOUT")
        return "done"

    result = opensearch_writer._run_with_timeout_retry(
        _action,
        action_name="test action",
        timeout_retries=3,
        timeout_backoff_base=2.0,
    )

    assert result == "done"
    assert attempts["count"] == 3


def test_run_with_timeout_retry_exhausted(monkeypatch, opensearch_writer):
    monkeypatch.setattr(oswriter_time, "sleep", lambda _: None)

    def _action():
        raise ConnectionTimeout("TIMEOUT")

    with pytest.raises(ConnectionTimeout):
        opensearch_writer._run_with_timeout_retry(
            _action,
            action_name="test action",
            timeout_retries=2,
            timeout_backoff_base=2.0,
        )
