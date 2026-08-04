from datetime import date, datetime
from types import SimpleNamespace

import pytest

from search.documents import DatasetDocument
from tests.conftest import make_dataset_by_dcat


def test_normalize_dcat_dates():
    dcat = {
        "modified": date(2024, 1, 2),
        "issued": datetime(2024, 1, 3, 4, 5, 6),
        "temporal": 123,
    }
    normalized = DatasetDocument._normalize_dcat_dates(dcat)

    assert normalized["modified"] == "2024-01-02"
    assert normalized["issued"].startswith("2024-01-03T04:05:06")
    assert normalized["temporal"] == "123"


def test_normalize_dcat_spatial_object():
    dcat = {
        "spatial": {
            "@type": "Location",
            "prefLabel": "United States",
        },
    }

    normalized = DatasetDocument._normalize_dcat_dates(dcat)

    assert (
        normalized["spatial"] == '{"@type": "Location", "prefLabel": "United States"}'
    )


def test_normalize_dcat_distribution_structured_field():
    dcat = {
        "distribution": [
            {
                "title": "CSV download",
                "conformsTo": {
                    "@type": "Standard",
                    "identifier": "https://www.w3.org/TR/tabular-data-primer/",
                    "title": "CSV on the Web",
                },
            }
        ]
    }

    normalized = DatasetDocument._normalize_dcat_dates(dcat)

    assert normalized["distribution"][0]["title"] == "CSV download"
    assert normalized["distribution"][0]["conformsTo"] == (
        '{"@type": "Standard", '
        '"identifier": "https://www.w3.org/TR/tabular-data-primer/", '
        '"title": "CSV on the Web"}'
    )


def test_normalize_dcat_serializes_nested_metadata_objects():
    dcat = {
        "contactPoint": {
            "fn": "Data contact",
            "hasEmail": {"@id": "mailto:data@example.gov"},
        }
    }

    normalized = DatasetDocument._normalize_dcat_dates(dcat)

    expected_email = '{"@id": "mailto:data@example.gov"}'
    assert normalized["contactPoint"]["fn"] == "Data contact"
    assert normalized["contactPoint"]["hasEmail"] == expected_email


def test_normalize_dcat_preserves_publisher_suborganization_object():
    dcat = {
        "publisher": {
            "name": "U.S. Commodity Futures Trading Commission",
            "subOrganizationOf": {"name": "U.S. Government"},
        }
    }

    normalized = DatasetDocument._normalize_dcat_dates(dcat)

    assert normalized["publisher"]["subOrganizationOf"] == {"name": "U.S. Government"}


def test_dataset_to_document(sample_dataset, monkeypatch):
    monkeypatch.delenv("CATALOG_BASE_URL", raising=False)

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["_index"] == dataset_doc.INDEX_NAME
    assert document["_id"] == sample_dataset.id
    assert document["title"] == "Dataset Title"
    assert document["publisher"] == "Publisher"
    assert document["dcat"]["isPartOf"] == "collection-1"
    assert document["distribution_titles"] == ["CSV download", "API endpoint"]
    assert document["has_spatial"] is True
    assert document["harvest_record"] == "https://catalog.data.gov/harvest_record/hr-1"
    assert (
        document["harvest_record_raw"]
        == "https://catalog.data.gov/harvest_record/hr-1/raw"
    )
    assert (
        document["harvest_record_transformed"]
        == "https://catalog.data.gov/harvest_record/hr-1/transformed"
    )
    assert document["spatial_centroid"] == {"lat": 2.0, "lon": 1.0}


def test_dataset_to_document_handles_missing_date_and_organization(sample_dataset):
    sample_dataset.last_harvested_date = None
    sample_dataset.organization = None

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["last_harvested_date"] is None
    assert document["organization"] == {}


def test_dataset_to_document_uses_configured_catalog_base_url(
    sample_dataset, monkeypatch
):
    monkeypatch.setenv("CATALOG_BASE_URL", "https://example.gov/")

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["harvest_record"] == "https://example.gov/harvest_record/hr-1"
    assert (
        document["harvest_record_raw"] == "https://example.gov/harvest_record/hr-1/raw"
    )
    assert (
        document["harvest_record_transformed"]
        == "https://example.gov/harvest_record/hr-1/transformed"
    )


def test_dataset_to_document_has_dcat_spatial(sample_dataset):
    sample_dataset.translated_spatial = None

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is True


def test_dataset_to_document_has_translated_spatial(sample_dataset):
    sample_dataset.dcat.pop("spatial", None)

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is True


@pytest.mark.parametrize(
    "theme",
    [
        ["Geospatial"],
        ["GEOSPATIAL"],
        ["Health", " geospatial "],
        "Geospatial",
        [{"prefLabel": "geospatial"}],
        [{"@type": "Concept", "prefLabel": "Geospatial"}],
        ["Health", {"prefLabel": " geospatial "}],
    ],
)
def test_dataset_to_document_has_spatial_theme(sample_dataset, theme):
    sample_dataset.dcat.pop("spatial", None)
    sample_dataset.dcat["theme"] = theme
    sample_dataset.translated_spatial = None

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is True


@pytest.mark.parametrize(
    "theme",
    [
        None,
        [],
        ["Health"],
        "Environment",
        "Spatial",
        [{"prefLabel": "Environment"}],
        [{"@type": "Concept", "prefLabel": "Health"}],
    ],
)
def test_dataset_to_document_without_spatial_data_or_theme(sample_dataset, theme):
    sample_dataset.dcat.pop("spatial", None)
    sample_dataset.dcat["theme"] = theme
    sample_dataset.translated_spatial = None

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is False


def test_dataset_to_document_omits_transformed_url_without_payload(sample_dataset):
    sample_dataset.harvest_record = SimpleNamespace(source_transform=None)

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert "harvest_record_transformed" not in document


def test_normalize_datetime_modified_field():
    """Test that datetime objects in modified field are converted to ISO strings."""
    dcat = {
        "title": "Test Dataset",
        "modified": datetime(2023, 6, 22, 20, 25, 39, 652070),
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert isinstance(result["modified"], str)
    assert result["modified"] == "2023-06-22T20:25:39.652070"
    assert result["title"] == "Test Dataset"
    assert result["description"] == "Test description"


def test_normalize_date_modified_field():
    """Test that date objects in modified field are converted to ISO strings."""
    dcat = {
        "title": "Test Dataset",
        "modified": date(2023, 6, 22),
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert isinstance(result["modified"], str)
    assert result["modified"] == "2023-06-22"


def test_normalize_datetime_issued_field():
    """Test that datetime objects in issued field are converted to ISO strings."""
    dcat = {
        "title": "Test Dataset",
        "issued": datetime(2006, 5, 31, 0, 0, 0),
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert isinstance(result["issued"], str)
    assert result["issued"] == "2006-05-31T00:00:00"


def test_normalize_multiple_date_fields():
    """Test that multiple date fields are all normalized."""
    dcat = {
        "title": "Test Dataset",
        "modified": datetime(2023, 6, 22, 20, 25, 39),
        "issued": date(2006, 5, 31),
        "temporal": "2004/2005",
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert isinstance(result["modified"], str)
    assert result["modified"] == "2023-06-22T20:25:39"
    assert isinstance(result["issued"], str)
    assert result["issued"] == "2006-05-31"
    assert result["temporal"] == "2004/2005"  # Already string


def test_normalize_leaves_string_dates_unchanged():
    """Test that date fields that are already strings are not modified."""
    dcat = {
        "title": "Test Dataset",
        "modified": "2023-06-22T20:25:39.652070",
        "issued": "2006-05-31",
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert result["modified"] == "2023-06-22T20:25:39.652070"
    assert result["issued"] == "2006-05-31"


def test_normalize_with_missing_date_fields():
    """Test that missing date fields don't cause errors."""
    dcat = {
        "title": "Test Dataset",
        "description": "Test description",
        "keyword": ["health", "education"],
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert "modified" not in result
    assert "issued" not in result
    assert result["title"] == "Test Dataset"
    assert result["keyword"] == ["health", "education"]


def test_normalize_with_none_date_fields():
    """Test that None values in date fields are preserved."""
    dcat = {
        "title": "Test Dataset",
        "modified": None,
        "issued": None,
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert result["modified"] is None
    assert result["issued"] is None


def test_normalize_with_integer_date_field():
    """Test that non-standard types (like integers) are converted to strings."""
    dcat = {
        "title": "Test Dataset",
        "modified": 20230622,  # Non-standard format
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert isinstance(result["modified"], str)
    assert result["modified"] == "20230622"


def test_normalize_does_not_mutate_original():
    """Test that the original dcat dict is not modified."""
    modified_datetime = datetime(2023, 6, 22, 20, 25, 39)
    dcat = {
        "title": "Test Dataset",
        "modified": modified_datetime,
        "description": "Test description",
    }

    result = DatasetDocument._normalize_dcat_dates(dcat)

    # Original should still have datetime object
    assert isinstance(dcat["modified"], datetime)
    assert dcat["modified"] is modified_datetime
    # Result should have string
    assert isinstance(result["modified"], str)


def test_normalize_preserves_nested_structures():
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

    result = DatasetDocument._normalize_dcat_dates(dcat)

    assert isinstance(result["modified"], str)
    assert result["publisher"]["name"] == "Department of Education"
    assert result["publisher"]["subOrganizationOf"]["name"] == "U.S. Government"
    assert len(result["distribution"]) == 1
    assert result["distribution"][0]["title"] == "Data File"


def test_dataset_to_document_normalizes_modified_datetime(
    mock_dataset_with_datetime,
):
    """Test that dataset_to_document normalizes datetime in modified field."""
    # Convert to document

    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    # Verify modified is a string
    assert isinstance(document["dcat"]["modified"], str)
    assert document["dcat"]["modified"] == "2023-06-22T20:25:39.652070"
    assert document["title"] == "Test Dataset"
    assert document["slug"] == "test-dataset"


def test_dataset_to_document_normalizes_issued_date(
    mock_dataset_with_date,
):
    """Test that dataset_to_document normalizes date in issued field."""

    dataset_doc = DatasetDocument(mock_dataset_with_date)
    document = dataset_doc.dataset_to_document()

    assert isinstance(document["dcat"]["issued"], str)
    assert document["dcat"]["issued"] == "2006-05-31"


def test_dataset_to_document_preserves_string_dates(
    mock_dataset_with_string_dates,
):
    """Test that string dates in DCAT are preserved as-is."""
    dataset_doc = DatasetDocument(mock_dataset_with_string_dates)
    document = dataset_doc.dataset_to_document()

    assert document["dcat"]["modified"] == "2023-06-22T20:25:39.652070"
    assert document["dcat"]["issued"] == "2006-05-31"


def test_dataset_to_document_with_spatial_data(
    mock_dataset_with_spatial,
):
    """Test dataset_to_document with spatial data and date normalization."""
    dataset_doc = DatasetDocument(mock_dataset_with_spatial)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is True
    assert isinstance(document["dcat"]["modified"], str)
    assert document["dcat"]["modified"] == "2023-01-15T10:30:00"


@pytest.mark.parametrize(
    "theme",
    [
        ["Geospatial"],
        ["GEOSPATIAL"],
        ["Health", " geospatial "],
        "Geospatial",
    ],
)
def test_dataset_to_document_with_geospatial_theme(mock_dataset_with_datetime, theme):
    mock_dataset_with_datetime.dcat["theme"] = theme
    mock_dataset_with_datetime.dcat.pop("spatial", None)
    mock_dataset_with_datetime.translated_spatial = None

    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is True


@pytest.mark.parametrize("theme", [None, [], ["Health"], "Environment", "Spatial"])
def test_dataset_to_document_without_spatial_values_or_geospatial_theme(
    mock_dataset_with_datetime, theme
):
    mock_dataset_with_datetime.dcat["theme"] = theme
    mock_dataset_with_datetime.dcat.pop("spatial", None)
    mock_dataset_with_datetime.translated_spatial = None

    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    assert document["has_spatial"] is False


def test_dataset_to_document_does_not_modify_original_dcat(
    mock_dataset_with_datetime,
):
    """Test that the original dataset.dcat is not mutated."""
    modified_datetime = mock_dataset_with_datetime.dcat["modified"]

    # Convert to document
    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    # Original dcat should still have datetime object
    assert isinstance(mock_dataset_with_datetime.dcat["modified"], datetime)
    assert mock_dataset_with_datetime.dcat["modified"] is modified_datetime

    # Document should have string
    assert isinstance(document["dcat"]["modified"], str)


def test_dataset_to_document_includes_harvest_record_raw_url(
    mock_dataset_with_datetime, monkeypatch
):
    """Test that harvest_record_raw is included as a URL."""
    monkeypatch.delenv("CATALOG_BASE_URL", raising=False)
    mock_dataset_with_datetime.harvest_record_id = (
        "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
    )

    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    assert (
        document["harvest_record_raw"]
        == "https://catalog.data.gov/harvest_record/c9b367ca-3dd4-407e-b170-6d9688f3b79e/raw"
    )


def test_dataset_to_document_includes_harvest_record_transformed_url(
    mock_dataset_with_datetime, monkeypatch
):
    """Test that harvest_record_transformed is included as a URL."""
    monkeypatch.delenv("CATALOG_BASE_URL", raising=False)

    mock_dataset_with_datetime.harvest_record_id = (
        "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
    )
    mock_dataset_with_datetime.harvest_record = type(
        "Record", (), {"source_transform": {"title": "x"}}
    )()

    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    assert (
        document["harvest_record_transformed"]
        == "https://catalog.data.gov/harvest_record/c9b367ca-3dd4-407e-b170-6d9688f3b79e/transformed"
    )


def test_dataset_to_document_omits_harvest_record_transformed_without_payload(
    mock_dataset_with_datetime,
):
    mock_dataset_with_datetime.harvest_record_id = (
        "c9b367ca-3dd4-407e-b170-6d9688f3b79e"
    )
    mock_dataset_with_datetime.harvest_record = type(
        "Record", (), {"source_transform": None}
    )()

    dataset_doc = DatasetDocument(mock_dataset_with_datetime)
    document = dataset_doc.dataset_to_document()

    assert "harvest_record_transformed" not in document


def test_distribution_key_absent_yields_empty_list(mock_organization):
    """When 'distribution' is not present in dcat at all, distribution_titles
    must be an empty list (the `or []` prevents a TypeError on None)."""
    dcat = {
        "title": "No Distribution Dataset",
        "description": "DCAT with no distribution key whatsoever.",
        "publisher": {"name": "Test Agency"},
        # intentionally omitting "distribution"
    }
    dataset = make_dataset_by_dcat(dcat, mock_organization)

    dataset_doc = DatasetDocument(dataset)
    document = dataset_doc.dataset_to_document()

    assert document["distribution_titles"] == []


def test_distribution_none_yields_empty_list(mock_organization):
    """When 'distribution' is explicitly None, the `or []` guard kicks in
    and distribution_titles must still be an empty list."""
    dcat = {
        "title": "Null Distribution Dataset",
        "description": "DCAT where distribution is None.",
        "publisher": {"name": "Test Agency"},
        "distribution": None,
    }
    dataset = make_dataset_by_dcat(dcat, mock_organization)

    dataset_doc = DatasetDocument(dataset)
    document = dataset_doc.dataset_to_document()

    assert document["distribution_titles"] == []


def test_distribution_empty_list_yields_empty_list(mock_organization):
    """When 'distribution' is an empty list the comprehension iterates
    zero times, so distribution_titles must be an empty list."""
    dcat = {
        "title": "Empty Distribution Dataset",
        "description": "DCAT where distribution is [].",
        "publisher": {"name": "Test Agency"},
        "distribution": [],
    }
    dataset = make_dataset_by_dcat(dcat, mock_organization)

    dataset_doc = DatasetDocument(dataset)
    document = dataset_doc.dataset_to_document()

    assert document["distribution_titles"] == []


def test_dataset_to_document_flattens_dcat3_theme_and_identifier(sample_dataset):
    sample_dataset.dcat["theme"] = [
        {
            "@id": "https://example.gov/concepts/climate-science",
            "@type": "Concept",
            "prefLabel": "Climate Science",
        },
        "weather",
    ]
    sample_dataset.dcat["identifier"] = {
        "@type": "Identifier",
        "@id": "https://example.gov/identifiers/dataset-1",
    }
    sample_dataset.dcat["inSeries"] = [
        {
            "@id": "https://example.gov/series/annual",
            "@type": "DatasetSeries",
            "title": "Annual Series",
        }
    ]

    dataset_doc = DatasetDocument(sample_dataset)
    document = dataset_doc.dataset_to_document()

    assert document["theme"] == ["Climate Science", "weather"]
    assert document["identifier"] == "https://example.gov/identifiers/dataset-1"
    # Nested dcat keeps original DCAT shapes; only top-level fields are flattened.
    assert document["dcat"]["theme"] == sample_dataset.dcat["theme"]
    assert document["dcat"]["identifier"] == sample_dataset.dcat["identifier"]
    # inSeries is not aliased onto isPartOf.
    assert document["dcat"]["isPartOf"] == "collection-1"
