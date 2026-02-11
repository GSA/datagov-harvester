from datetime import date, datetime
from types import SimpleNamespace

import pytest

import harvester.opensearch as opensearch
from harvester.opensearch import (
    DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
    ConnectionTimeout,
    OpenSearchInterface,
)


class DummyOrg:
    def to_dict(self):
        return {"id": "org-1", "name": "Test Org"}


class FakeIndices:
    def __init__(self, exists=True):
        self._exists = exists
        self.created = None
        self.refreshed = []

    def exists(self, index):
        return self._exists

    def create(self, index, body):
        self.created = {"index": index, "body": body}
        return {"acknowledged": True}

    def refresh(self, index, request_timeout):
        self.refreshed.append((index, request_timeout))
        return {"result": "refreshed"}


class FakeClient:
    def __init__(self, exists=True):
        self.indices = FakeIndices(exists=exists)
        self.deleted = []

    def delete(self, index, id, ignore, request_timeout):
        self.deleted.append((index, id, ignore, request_timeout))
        return {"result": "deleted"}


@pytest.fixture()
def sample_dataset():
    return SimpleNamespace(
        id="dataset-1",
        slug="dataset-1",
        dcat={
            "title": "Dataset Title",
            "description": "Dataset description",
            "publisher": {"name": "Publisher"},
            "keyword": ["kw-1"],
            "theme": ["theme-1"],
            "identifier": "id-1",
            "spatial": "POINT(1 2)",
            "modified": date(2024, 1, 2),
        },
        last_harvested_date=datetime(2024, 1, 3, 4, 5, 6),
        translated_spatial={"type": "Point", "coordinates": [1, 2]},
        organization=DummyOrg(),
        popularity=7,
        harvest_record_id="hr-1",
    )


def test_normalize_dcat_dates():
    dcat = {
        "modified": date(2024, 1, 2),
        "issued": datetime(2024, 1, 3, 4, 5, 6),
        "temporal": 123,
    }
    normalized = OpenSearchInterface._normalize_dcat_dates(dcat)

    assert normalized["modified"] == "2024-01-02"
    assert normalized["issued"].startswith("2024-01-03T04:05:06")
    assert normalized["temporal"] == "123"


def test_geometry_centroid_returns_average():
    geometry = {"type": "MultiPoint", "coordinates": [[0, 0], [2, 2]]}

    centroid = OpenSearchInterface._geometry_centroid(geometry)

    assert centroid == {"lat": 1.0, "lon": 1.0}


def test_dataset_to_document(sample_dataset):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)

    document = iface.dataset_to_document(sample_dataset)

    assert document["_index"] == OpenSearchInterface.INDEX_NAME
    assert document["_id"] == sample_dataset.id
    assert document["title"] == "Dataset Title"
    assert document["publisher"] == "Publisher"
    assert document["has_spatial"] is True
    assert document["harvest_record"] == "/harvest_record/hr-1"
    assert document["spatial_centroid"] == {"lat": 2.0, "lon": 1.0}


def test_init_creates_index_when_missing(monkeypatch):
    fake_client = FakeClient(exists=False)
    monkeypatch.setattr(
        OpenSearchInterface,
        "_create_test_opensearch_client",
        staticmethod(lambda host: fake_client),
    )

    OpenSearchInterface(test_host="localhost")

    created = fake_client.indices.created
    assert created is not None
    assert created["index"] == OpenSearchInterface.INDEX_NAME
    assert created["body"]["mappings"] == OpenSearchInterface.MAPPINGS
    assert created["body"]["settings"] == OpenSearchInterface.SETTINGS


def test_from_environment_uses_aws_client(monkeypatch):
    fake_client = FakeClient(exists=True)
    captured = {}

    def fake_aws_client(host):
        captured["host"] = host
        return fake_client

    monkeypatch.setenv("OPENSEARCH_HOST", "search.example.es.amazonaws.com")
    monkeypatch.setattr(
        OpenSearchInterface,
        "_create_aws_opensearch_client",
        staticmethod(fake_aws_client),
    )
    monkeypatch.setattr(
        OpenSearchInterface,
        "_create_test_opensearch_client",
        staticmethod(lambda host: FakeClient(exists=True)),
    )

    iface = OpenSearchInterface.from_environment()

    assert iface.client is fake_client
    assert captured["host"] == "search.example.es.amazonaws.com"


def test_from_environment_requires_host(monkeypatch):
    monkeypatch.delenv("OPENSEARCH_HOST", raising=False)

    with pytest.raises(ValueError):
        OpenSearchInterface.from_environment()


def test_run_with_timeout_retry(monkeypatch):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    calls = {"count": 0}
    sleeps = []

    def action():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionTimeout("timeout")
        return "ok"

    def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(opensearch.time, "sleep", fake_sleep)

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
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    iface.client = FakeClient(exists=True)

    def fake_streaming_bulk(client, documents, raise_on_error, max_retries):
        assert client is iface.client
        assert len(documents) == 1
        yield True, {"index": {"result": "created", "_id": "dataset-1"}}
        yield False, {
            "index": {
                "status": 400,
                "error": {
                    "type": "mapper_parsing_exception",
                    "reason": "bad",
                    "caused_by": {"type": "illegal_argument_exception"},
                },
            }
        }

    monkeypatch.setattr(opensearch.helpers, "streaming_bulk", fake_streaming_bulk)

    succeeded, failed, errors = iface.index_datasets(
        [sample_dataset],
        refresh_after=False,
    )

    assert succeeded == 1
    assert failed == 1
    assert len(errors) == 2


def test_delete_dataset_by_id_calls_client():
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    iface.client = FakeClient(exists=True)

    result = iface.delete_dataset_by_id("dataset-1", refresh_after=False)

    assert result is True
    assert iface.client.deleted == [
        (
            OpenSearchInterface.INDEX_NAME,
            "dataset-1",
            [404],
            DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
        )
    ]


def test_delete_dataset_by_id_no_id():
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    iface.client = FakeClient(exists=True)

    result = iface.delete_dataset_by_id("", refresh_after=False)

    assert result is False
    assert iface.client.deleted == []
