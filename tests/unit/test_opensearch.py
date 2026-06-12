from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import harvester.opensearch as opensearch
from harvester.opensearch import (
    DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
    ConnectionTimeout,
    OpenSearchInterface,
)


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
            "isPartOf": "collection-1",
            "distribution": [
                {"title": "CSV download"},
                {"title": "API endpoint"},
                {"accessURL": "https://example.com/no-title"},
            ],
        },
        last_harvested_date=datetime(2024, 1, 3, 4, 5, 6),
        translated_spatial={"type": "Point", "coordinates": [1, 2]},
        organization=DummyOrg(),
        popularity=7,
        harvest_record_id="hr-1",
        harvest_record=SimpleNamespace(source_transform={"title": "Transformed"}),
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


class TestDcatDateNormalization:
    """Test suite for _normalize_dcat_dates method."""

    def test_normalize_datetime_modified_field(self):
        """Test that datetime objects in modified field are converted to ISO strings."""
        dcat = {
            "title": "Test Dataset",
            "modified": datetime(2023, 6, 22, 20, 25, 39, 652070),
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert isinstance(result["modified"], str)
        assert result["modified"] == "2023-06-22T20:25:39.652070"
        assert result["title"] == "Test Dataset"
        assert result["description"] == "Test description"

    def test_normalize_date_modified_field(self):
        """Test that date objects in modified field are converted to ISO strings."""
        dcat = {
            "title": "Test Dataset",
            "modified": date(2023, 6, 22),
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert isinstance(result["modified"], str)
        assert result["modified"] == "2023-06-22"

    def test_normalize_datetime_issued_field(self):
        """Test that datetime objects in issued field are converted to ISO strings."""
        dcat = {
            "title": "Test Dataset",
            "issued": datetime(2006, 5, 31, 0, 0, 0),
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert isinstance(result["issued"], str)
        assert result["issued"] == "2006-05-31T00:00:00"

    def test_normalize_multiple_date_fields(self):
        """Test that multiple date fields are all normalized."""
        dcat = {
            "title": "Test Dataset",
            "modified": datetime(2023, 6, 22, 20, 25, 39),
            "issued": date(2006, 5, 31),
            "temporal": "2004/2005",
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert isinstance(result["modified"], str)
        assert result["modified"] == "2023-06-22T20:25:39"
        assert isinstance(result["issued"], str)
        assert result["issued"] == "2006-05-31"
        assert result["temporal"] == "2004/2005"  # Already string

    def test_normalize_leaves_string_dates_unchanged(self):
        """Test that date fields that are already strings are not modified."""
        dcat = {
            "title": "Test Dataset",
            "modified": "2023-06-22T20:25:39.652070",
            "issued": "2006-05-31",
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert result["modified"] == "2023-06-22T20:25:39.652070"
        assert result["issued"] == "2006-05-31"

    def test_normalize_with_missing_date_fields(self):
        """Test that missing date fields don't cause errors."""
        dcat = {
            "title": "Test Dataset",
            "description": "Test description",
            "keyword": ["health", "education"],
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert "modified" not in result
        assert "issued" not in result
        assert result["title"] == "Test Dataset"
        assert result["keyword"] == ["health", "education"]

    def test_normalize_with_none_date_fields(self):
        """Test that None values in date fields are preserved."""
        dcat = {
            "title": "Test Dataset",
            "modified": None,
            "issued": None,
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert result["modified"] is None
        assert result["issued"] is None

    def test_normalize_with_integer_date_field(self):
        """Test that non-standard types (like integers) are converted to strings."""
        dcat = {
            "title": "Test Dataset",
            "modified": 20230622,  # Non-standard format
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert isinstance(result["modified"], str)
        assert result["modified"] == "20230622"

    def test_normalize_does_not_mutate_original(self):
        """Test that the original dcat dict is not modified."""
        modified_datetime = datetime(2023, 6, 22, 20, 25, 39)
        dcat = {
            "title": "Test Dataset",
            "modified": modified_datetime,
            "description": "Test description",
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        # Original should still have datetime object
        assert isinstance(dcat["modified"], datetime)
        assert dcat["modified"] is modified_datetime
        # Result should have string
        assert isinstance(result["modified"], str)

    def test_normalize_preserves_nested_structures(self):
        """Test that nested structures in DCAT are preserved."""
        dcat = {
            "title": "Test Dataset",
            "modified": datetime(2023, 6, 22, 20, 25, 39),
            "publisher": {
                "name": "Department of Education",
                "subOrganizationOf": {"name": "U.S. Government"},
            },
            "distribution": [
                {"title": "Data File", "downloadURL": "https://example.com/data.csv"}
            ],
        }

        result = OpenSearchInterface._normalize_dcat_dates(dcat)

        assert isinstance(result["modified"], str)
        assert result["publisher"]["name"] == "Department of Education"
        assert result["publisher"]["subOrganizationOf"]["name"] == "U.S. Government"
        assert len(result["distribution"]) == 1
        assert result["distribution"][0]["title"] == "Data File"


class TestDatasetToDocument:
    """Test suite for dataset_to_document with date normalization."""

    def test_dataset_to_document_normalizes_modified_datetime(
        self, opensearch_client, mock_dataset_with_datetime
    ):
        """Test that dataset_to_document normalizes datetime in modified field."""
        # Convert to document
        document = opensearch_client.dataset_to_document(mock_dataset_with_datetime)

        # Verify modified is a string
        assert isinstance(document["dcat"]["modified"], str)
        assert document["dcat"]["modified"] == "2023-06-22T20:25:39.652070"
        assert document["title"] == "Test Dataset"
        assert document["slug"] == "test-dataset"

    def test_dataset_to_document_normalizes_issued_date(
        self, opensearch_client, mock_dataset_with_date
    ):
        """Test that dataset_to_document normalizes date in issued field."""
        document = opensearch_client.dataset_to_document(mock_dataset_with_date)

        assert isinstance(document["dcat"]["issued"], str)
        assert document["dcat"]["issued"] == "2006-05-31"

    def test_dataset_to_document_preserves_string_dates(
        self, opensearch_client, mock_dataset_with_string_dates
    ):
        """Test that string dates in DCAT are preserved as-is."""
        document = opensearch_client.dataset_to_document(mock_dataset_with_string_dates)

        assert document["dcat"]["modified"] == "2023-06-22T20:25:39.652070"
        assert document["dcat"]["issued"] == "2006-05-31"

    def test_dataset_to_document_with_spatial_data(
        self, opensearch_client, mock_dataset_with_spatial
    ):
        """Test dataset_to_document with spatial data and date normalization."""
        document = opensearch_client.dataset_to_document(mock_dataset_with_spatial)

        assert document["has_spatial"] is True
        assert isinstance(document["dcat"]["modified"], str)
        assert document["dcat"]["modified"] == "2023-01-15T10:30:00"

    def test_dataset_to_document_does_not_modify_original_dcat(
        self, opensearch_client, mock_dataset_with_datetime
    ):
        """Test that the original dataset.dcat is not mutated."""
        modified_datetime = mock_dataset_with_datetime.dcat["modified"]

        # Convert to document
        document = opensearch_client.dataset_to_document(mock_dataset_with_datetime)

        # Original dcat should still have datetime object
        assert isinstance(mock_dataset_with_datetime.dcat["modified"], datetime)
        assert mock_dataset_with_datetime.dcat["modified"] is modified_datetime

        # Document should have string
        assert isinstance(document["dcat"]["modified"], str)

    def test_dataset_to_document_includes_harvest_record_raw_url(
        self, dbapp, opensearch_client, mock_dataset_with_datetime
    ):
        """Test that harvest_record_raw is included as a URL."""
        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"
            mock_dataset_with_datetime.harvest_record_id = (
                "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
            )

            document = opensearch_client.dataset_to_document(mock_dataset_with_datetime)

            assert (
                document["harvest_record_raw"]
                == "https://catalog.data.gov/harvest_record/c9b367ca-3dd4-407e-b170-6d9688f3b79e/raw"
            )

    def test_dataset_to_document_includes_harvest_record_transformed_url(
        self, dbapp, opensearch_client, mock_dataset_with_datetime
    ):
        """Test that harvest_record_transformed is included as a URL."""
        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"
            mock_dataset_with_datetime.harvest_record_id = (
                "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
            )
            mock_dataset_with_datetime.harvest_record = type(
                "Record", (), {"source_transform": {"title": "x"}}
            )()

            document = opensearch_client.dataset_to_document(mock_dataset_with_datetime)

            assert (
                document["harvest_record_transformed"]
                == "https://catalog.data.gov/harvest_record/c9b367ca-3dd4-407e-b170-6d9688f3b79e/transformed"
            )

    def test_dataset_to_document_omits_harvest_record_transformed_without_payload(
        self, dbapp, opensearch_client, mock_dataset_with_datetime
    ):
        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"
            mock_dataset_with_datetime.harvest_record_id = (
                "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
            )
            mock_dataset_with_datetime.harvest_record = type(
                "Record", (), {"source_transform": None}
            )()

            document = opensearch_client.dataset_to_document(mock_dataset_with_datetime)

            assert "harvest_record_transformed" not in document

    def test_dataset_to_document(self, sample_dataset):
        iface = OpenSearchInterface.__new__(OpenSearchInterface)

        document = iface.dataset_to_document(sample_dataset)

        assert document["_index"] == OpenSearchInterface.INDEX_NAME
        assert document["_id"] == sample_dataset.id
        assert document["title"] == "Dataset Title"
        assert document["publisher"] == "Publisher"
        assert document["dcat"]["isPartOf"] == "collection-1"
        assert document["distribution_titles"] == ["CSV download", "API endpoint"]
        assert document["has_spatial"] is True
        assert (
            document["harvest_record"] == "https://catalog.data.gov/harvest_record/hr-1"
        )
        assert (
            document["harvest_record_raw"]
            == "https://catalog.data.gov/harvest_record/hr-1/raw"
        )
        assert (
            document["harvest_record_transformed"]
            == "https://catalog.data.gov/harvest_record/hr-1/transformed"
        )
        assert document["spatial_centroid"] == {"lat": 2.0, "lon": 1.0}


class TestGeometryCentroid:
    def test_geometry_centroid_from_polygon(self):
        geometry = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]],
        }
        centroid = OpenSearchInterface._geometry_centroid(geometry)
        assert centroid is not None
        assert centroid["lon"] == pytest.approx(0.8)
        assert centroid["lat"] == pytest.approx(0.8)

    def test_geometry_centroid_returns_average(self):
        geometry = {"type": "MultiPoint", "coordinates": [[0, 0], [2, 2]]}

        centroid = OpenSearchInterface._geometry_centroid(geometry)

        assert centroid == {"lat": 1.0, "lon": 1.0}


class TestCreateHarvestRecordUrl:
    """Test for _create_harvest_record_url method."""

    def test_create_harvest_record_url_with_local_server(
        self, dbapp, mock_dataset_with_datetime, opensearch_client
    ):
        """Test that _create_harvest_record_url generates correct URL for
        local server."""
        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"

            # Set a specific harvest_record_id
            mock_dataset_with_datetime.harvest_record_id = (
                "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
            )

            url = opensearch_client._create_harvest_record_url(
                mock_dataset_with_datetime
            )

            assert (
                url
                == "https://catalog.data.gov/harvest_record/c9b367ca-3dd4-407e-b170-6d9688f3b79e"
            )


def test_dataset_to_document_has_dcat_spatial(sample_dataset):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    sample_dataset.translated_spatial = None

    document = iface.dataset_to_document(sample_dataset)

    assert document["has_spatial"] is True


def test_dataset_to_document_has_translated_spatial(sample_dataset):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    sample_dataset.dcat.pop("spatial", None)

    document = iface.dataset_to_document(sample_dataset)

    assert document["has_spatial"] is True


@pytest.mark.parametrize(
    "theme",
    [
        ["Geospatial"],
        ["GEOSPATIAL"],
        ["Health", " geospatial "],
        "Geospatial",
    ],
)
def test_dataset_to_document_has_spatial_theme(sample_dataset, theme):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    sample_dataset.dcat.pop("spatial", None)
    sample_dataset.dcat["theme"] = theme
    sample_dataset.translated_spatial = None

    document = iface.dataset_to_document(sample_dataset)

    assert document["has_spatial"] is True


@pytest.mark.parametrize(
    "theme",
    [None, [], ["Health"], "Environment", "Spatial"],
)
def test_dataset_to_document_without_spatial_data_or_theme(sample_dataset, theme):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    sample_dataset.dcat.pop("spatial", None)
    sample_dataset.dcat["theme"] = theme
    sample_dataset.translated_spatial = None

    document = iface.dataset_to_document(sample_dataset)

    assert document["has_spatial"] is False


def test_dataset_to_document_omits_transformed_url_without_payload(sample_dataset):
    iface = OpenSearchInterface.__new__(OpenSearchInterface)
    sample_dataset.harvest_record = SimpleNamespace(source_transform=None)

    document = iface.dataset_to_document(sample_dataset)

    assert "harvest_record_transformed" not in document


class TestDistributionTitles:
    """Validate the `or []` fallback in dataset_to_document's distribution_titles."""

    def _make_dataset(self, dcat: dict, mock_organization: Mock) -> Mock:
        """Return a minimal mock dataset whose dcat is the supplied dict."""
        dataset = Mock()
        dataset.id = "dist-test-id"
        dataset.slug = "dist-test-dataset"
        dataset.last_harvested_date = Mock()
        dataset.last_harvested_date.isoformat.return_value = "2024-01-01"
        dataset.translated_spatial = None
        dataset.harvest_record_id = "harvest-rec-id"
        dataset.harvest_record = None
        dataset.popularity = 0
        dataset.organization = mock_organization
        dataset.dcat = dcat
        return dataset

    def test_distribution_key_absent_yields_empty_list(
        self, dbapp, opensearch_client, mock_organization
    ):
        """When 'distribution' is not present in dcat at all, distribution_titles
        must be an empty list (the `or []` prevents a TypeError on None)."""
        dcat = {
            "title": "No Distribution Dataset",
            "description": "DCAT with no distribution key whatsoever.",
            "publisher": {"name": "Test Agency"},
            # intentionally omitting "distribution"
        }
        dataset = self._make_dataset(dcat, mock_organization)

        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"
            document = opensearch_client.dataset_to_document(dataset)

        assert document["distribution_titles"] == []

    def test_distribution_none_yields_empty_list(
        self, dbapp, opensearch_client, mock_organization
    ):
        """When 'distribution' is explicitly None, the `or []` guard kicks in
        and distribution_titles must still be an empty list."""
        dcat = {
            "title": "Null Distribution Dataset",
            "description": "DCAT where distribution is None.",
            "publisher": {"name": "Test Agency"},
            "distribution": None,
        }
        dataset = self._make_dataset(dcat, mock_organization)

        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"
            document = opensearch_client.dataset_to_document(dataset)

        assert document["distribution_titles"] == []

    def test_distribution_empty_list_yields_empty_list(
        self, dbapp, opensearch_client, mock_organization
    ):
        """When 'distribution' is an empty list the comprehension iterates
        zero times, so distribution_titles must be an empty list."""
        dcat = {
            "title": "Empty Distribution Dataset",
            "description": "DCAT where distribution is [].",
            "publisher": {"name": "Test Agency"},
            "distribution": [],
        }
        dataset = self._make_dataset(dcat, mock_organization)

        with dbapp.app_context():
            dbapp.config["SERVER_NAME"] = "0.0.0.0:8080"
            dbapp.config["PREFERRED_URL_SCHEME"] = "http"
            document = opensearch_client.dataset_to_document(dataset)

        assert document["distribution_titles"] == []


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


def test_mappings_include_catalog_compatible_fields():
    mappings = OpenSearchInterface.MAPPINGS["properties"]
    normalizer = OpenSearchInterface.SETTINGS["analysis"]["normalizer"][
        OpenSearchInterface.KEYWORD_NORMALIZER
    ]

    assert mappings["dcat"]["properties"]["isPartOf"] == {"type": "keyword"}
    assert mappings["distribution_titles"]["type"] == "text"
    assert mappings["publisher"]["fields"]["raw"] == {"type": "keyword"}
    assert mappings["publisher"]["fields"]["normalized"] == {
        "type": "keyword",
        "normalizer": OpenSearchInterface.KEYWORD_NORMALIZER,
    }
    assert mappings["keyword"]["fields"]["normalized"] == {
        "type": "keyword",
        "normalizer": OpenSearchInterface.KEYWORD_NORMALIZER,
    }
    assert normalizer == {
        "type": "custom",
        "filter": ["lowercase"],
    }


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
