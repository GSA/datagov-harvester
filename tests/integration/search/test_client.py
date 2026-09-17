import pytest

from search.client import OpenSearchClient
from tests.conftest import FakeClient


def test_init_creates_index_when_missing(monkeypatch):
    fake_client = FakeClient(exists=False)
    monkeypatch.setattr(
        OpenSearchClient,
        "_create_test_opensearch_client",
        staticmethod(lambda host: fake_client),
    )

    OpenSearchClient(test_host="localhost")

    created = fake_client.indices.created
    assert created is not None
    assert created["index"] == OpenSearchClient.INDEX_NAME
    assert created["body"]["mappings"] == OpenSearchClient.MAPPINGS
    assert created["body"]["settings"] == OpenSearchClient.SETTINGS


def test_mappings_include_catalog_compatible_fields():

    client = OpenSearchClient.from_environment()
    mappings = client.MAPPINGS["properties"]
    normalizer = client.SETTINGS["analysis"]["normalizer"][client.KEYWORD_NORMALIZER]

    assert mappings["dcat"]["properties"]["isPartOf"] == {"type": "keyword"}
    assert mappings["parent_identifier"] == {"type": "keyword"}
    assert mappings["theme"] == {
        "type": "text",
        "analyzer": client.TEXT_ANALYZER,
        "search_analyzer": client.TEXT_ANALYZER,
    }
    assert mappings["distribution_titles"]["type"] == "text"
    assert mappings["publisher"]["fields"]["raw"] == {"type": "keyword"}
    assert mappings["publisher"]["fields"]["normalized"] == {
        "type": "keyword",
        "normalizer": client.KEYWORD_NORMALIZER,
    }
    assert mappings["keyword"]["fields"]["normalized"] == {
        "type": "keyword",
        "normalizer": client.KEYWORD_NORMALIZER,
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
        OpenSearchClient,
        "_create_aws_opensearch_client",
        staticmethod(fake_aws_client),
    )
    monkeypatch.setattr(
        OpenSearchClient,
        "_create_test_opensearch_client",
        staticmethod(lambda host: FakeClient(exists=True)),
    )

    iface = OpenSearchClient.from_environment()

    assert iface.client is fake_client
    assert captured["host"] == "search.example.es.amazonaws.com"


def test_from_environment_requires_host(monkeypatch):
    monkeypatch.delenv("OPENSEARCH_HOST", raising=False)

    with pytest.raises(ValueError):
        OpenSearchClient.from_environment()

    with pytest.raises(ValueError):
        # both hostnames
        OpenSearchClient(test_host="not-empty", aws_host="also-not-empty")


def test_dcat_modified_field_mapping(opensearch_client):
    """Test that DCAT modified field is mapped as keyword type."""
    mappings = opensearch_client.MAPPINGS

    assert "dcat" in mappings["properties"]
    assert mappings["properties"]["dcat"]["type"] == "nested"
    assert "properties" in mappings["properties"]["dcat"]

    dcat_properties = mappings["properties"]["dcat"]["properties"]
    assert "modified" in dcat_properties
    assert dcat_properties["modified"]["type"] == "keyword"


def test_dcat_issued_field_mapping(opensearch_client):
    """Test that DCAT issued field is mapped as keyword type."""
    mappings = opensearch_client.MAPPINGS
    dcat_properties = mappings["properties"]["dcat"]["properties"]

    assert "issued" in dcat_properties
    assert dcat_properties["issued"]["type"] == "keyword"


def test_other_mappings_unchanged(opensearch_client):
    """Test that other field mappings are preserved."""
    mappings = opensearch_client.MAPPINGS

    # Verify other fields are still present
    assert mappings["properties"]["title"]["type"] == "text"
    assert mappings["properties"]["slug"]["type"] == "keyword"
    assert mappings["properties"]["keyword"]["type"] == "text"
    assert mappings["properties"]["keyword"]["fields"]["raw"]["type"] == "keyword"
    assert mappings["properties"]["organization"]["type"] == "nested"


def test_keyword_normalized_sub_field_exists(opensearch_client):
    """
    keyword.normalized sub-field must be present for case-insensitive search.
    """
    keyword_fields = opensearch_client.MAPPINGS["properties"]["keyword"]["fields"]

    assert "normalized" in keyword_fields
    assert keyword_fields["normalized"]["type"] == "keyword"
    assert keyword_fields["normalized"]["normalizer"] == (
        opensearch_client.KEYWORD_NORMALIZER
    )


def test_lowercase_normalizer_defined_in_settings(opensearch_client):
    """
    The lowercase_normalizer must be declared in SETTINGS so OpenSearch
    can apply it when doing index.
    """
    normalizers = opensearch_client.SETTINGS.get("analysis", {}).get("normalizer", {})

    assert opensearch_client.KEYWORD_NORMALIZER in normalizers
    normalizer_cfg = normalizers[opensearch_client.KEYWORD_NORMALIZER]
    assert normalizer_cfg["type"] == "custom"
    assert "lowercase" in normalizer_cfg["filter"]


def test_spatial_centroid_mapping(opensearch_client):
    """Test that spatial centroid field is mapped as geo_point."""
    mappings = opensearch_client.MAPPINGS
    assert mappings["properties"]["spatial_centroid"]["type"] == "geo_point"
