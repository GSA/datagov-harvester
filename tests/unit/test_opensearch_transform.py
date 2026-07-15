"""Unit tests for the DCAT -> OpenSearch index field transformer.

These tests pin down how DCAT-US values are flattened into the existing
(legacy) OpenSearch field shapes so DCAT-US 1.1 and 3.0 share one mapping.
"""

import json
from pathlib import Path

import pytest

from harvester.opensearch_transform import (
    INDEX_FIELDS,
    DcatIndexTransformer,
    coerce_identifier,
    coerce_keywords,
    coerce_publisher_name,
    coerce_text,
    coerce_theme_labels,
    distribution_titles,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DCAT3_COMPLETE_EXAMPLE = (
    REPO_ROOT
    / "schemas"
    / "dcatus3.0"
    / "examples"
    / "Dataset"
    / "good"
    / "complete_example.json"
)


# ---------------------------------------------------------------------------
# identifier: string search field
# ---------------------------------------------------------------------------


def test_coerce_identifier_keeps_dcat1_string():
    assert coerce_identifier("cftc-dc1") == "cftc-dc1"


def test_coerce_identifier_extracts_dcat3_object_id():
    value = {
        "@type": "Identifier",
        "@id": "https://example.gov/identifiers/dataset-1",
        "notation": "DATASET-1",
    }

    assert coerce_identifier(value) == "https://example.gov/identifiers/dataset-1"


def test_coerce_identifier_returns_empty_for_dcat3_object_without_id():
    value = {
        "@type": "Identifier",
        "schemaAgency": "National Climate Data Center",
        "notation": "NCDC-CLIMATE-OBS-2024",
        "version": "1.0",
    }

    assert coerce_identifier(value) == ""


def test_coerce_identifier_ignores_extra_dcat3_object_fields():
    value = {
        "@id": "https://example.gov/identifiers/dataset-1",
        "creator": {"@type": "Organization", "name": "NCDC"},
        "issued": {"@type": "date", "value": "2024-01-01"},
    }

    assert coerce_identifier(value) == "https://example.gov/identifiers/dataset-1"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_coerce_identifier_returns_empty_for_empty(value):
    assert coerce_identifier(value) == ""


def test_coerce_identifier_returns_empty_for_object_without_scalars():
    assert coerce_identifier({"@type": "Identifier"}) == ""


# ---------------------------------------------------------------------------
# theme: strings or Concept objects -> list[str]
# ---------------------------------------------------------------------------


def test_coerce_theme_labels_keeps_dcat1_string_list():
    assert coerce_theme_labels(["climate", "weather"]) == ["climate", "weather"]


def test_coerce_theme_labels_wraps_single_string():
    assert coerce_theme_labels("geospatial") == ["geospatial"]


def test_coerce_theme_labels_extracts_dcat3_preflabel():
    value = [
        {
            "@id": "https://example.gov/concepts/climate-science",
            "@type": "Concept",
            "prefLabel": "Climate Science",
            "altLabel": "Climatology",
            "inScheme": {
                "@id": "https://example.gov/concept-schemes/science-domains",
                "@type": "ConceptScheme",
                "title": "Science Domains",
            },
        }
    ]

    assert coerce_theme_labels(value) == ["Climate Science"]


def test_coerce_theme_labels_handles_single_concept_object():
    assert coerce_theme_labels({"@type": "Concept", "prefLabel": "Published"}) == [
        "Published"
    ]


def test_coerce_theme_labels_mixes_strings_and_objects():
    assert coerce_theme_labels(["health", {"prefLabel": "Environment"}]) == [
        "health",
        "Environment",
    ]


@pytest.mark.parametrize("value", [None, [], "", "   "])
def test_coerce_theme_labels_returns_empty(value):
    assert coerce_theme_labels(value) == []


def test_coerce_theme_labels_skips_empty_members():
    assert coerce_theme_labels(["", None, "keep", {"prefLabel": "  "}]) == ["keep"]


# ---------------------------------------------------------------------------
# keyword: list[str] in both versions
# ---------------------------------------------------------------------------


def test_coerce_keywords_passes_through_list():
    assert coerce_keywords(["a", "b"]) == ["a", "b"]


def test_coerce_keywords_wraps_single_string():
    assert coerce_keywords("single") == ["single"]


@pytest.mark.parametrize("value", [None, []])
def test_coerce_keywords_empty(value):
    assert coerce_keywords(value) == []


def test_coerce_keywords_drops_empty_entries():
    assert coerce_keywords(["a", "", None, "b"]) == ["a", "b"]


# ---------------------------------------------------------------------------
# publisher: DCAT-US 1.1 {name} / DCAT-US 3.0 Organization -> display name
# ---------------------------------------------------------------------------


def test_coerce_publisher_name_reads_name():
    assert coerce_publisher_name({"name": "NOAA"}) == "NOAA"


def test_coerce_publisher_name_prefers_name_over_preflabel():
    value = {
        "@type": "Organization",
        "name": "NCDC",
        "prefLabel": "National Climate Data Center",
        "altLabel": "NCDC",
    }
    assert coerce_publisher_name(value) == "NCDC"


def test_coerce_publisher_name_falls_back_to_preflabel():
    assert coerce_publisher_name({"prefLabel": "United States Census Bureau"}) == (
        "United States Census Bureau"
    )


@pytest.mark.parametrize("value", [None, {}, {"name": ""}])
def test_coerce_publisher_name_empty(value):
    assert coerce_publisher_name(value) == ""


# ---------------------------------------------------------------------------
# title / description: string in both versions
# ---------------------------------------------------------------------------


def test_coerce_text_passthrough():
    assert coerce_text("Daily Climate Observations") == "Daily Climate Observations"


@pytest.mark.parametrize("value", [None, 123, {"x": 1}, []])
def test_coerce_text_non_string_becomes_empty(value):
    assert coerce_text(value) == ""


# ---------------------------------------------------------------------------
# distribution_titles: distribution[].title (compatible across versions)
# ---------------------------------------------------------------------------


def test_distribution_titles_extracts_titles():
    distributions = [
        {"title": "CSV download", "downloadURL": "https://example.gov/a.csv"},
        {"@type": "Distribution", "title": "API endpoint", "byteSize": "10"},
        {"accessURL": "https://example.gov/no-title"},
        "not-a-dict",
    ]
    assert distribution_titles(distributions) == ["CSV download", "API endpoint"]


@pytest.mark.parametrize("value", [None, [], "nope"])
def test_distribution_titles_empty(value):
    assert distribution_titles(value) == []


# ---------------------------------------------------------------------------
# INDEX_FIELDS registry + DcatIndexTransformer.transform
# ---------------------------------------------------------------------------


def test_index_fields_registry_covers_expected_destinations():
    assert set(INDEX_FIELDS) == {
        "title",
        "description",
        "publisher",
        "keyword",
        "theme",
        "identifier",
        "distribution_titles",
    }


def test_transform_dcat1_dataset():
    dcat = {
        "title": "Commitment of Traders",
        "description": "COT reports.",
        "publisher": {
            "name": "U.S. Commodity Futures Trading Commission",
            "subOrganizationOf": {"name": "U.S. Government"},
        },
        "keyword": ["commitment of traders", "cot"],
        "theme": ["geospatial"],
        "identifier": "cftc-dc1",
        "isPartOf": "collection-1",
        "distribution": [
            {"accessURL": "https://www.cftc.gov/index.htm"},
            {"title": "Report CSV"},
        ],
    }

    result = DcatIndexTransformer().transform(dcat)

    assert result == {
        "title": "Commitment of Traders",
        "description": "COT reports.",
        "publisher": "U.S. Commodity Futures Trading Commission",
        "keyword": ["commitment of traders", "cot"],
        "theme": ["geospatial"],
        "identifier": "cftc-dc1",
        "distribution_titles": ["Report CSV"],
    }


def test_transform_dcat3_dataset():
    dcat = {
        "title": "National Climate Observations 2024",
        "description": "Comprehensive daily climate observations.",
        "publisher": {
            "@type": "Organization",
            "name": "National Climate Data Center",
            "altLabel": "NCDC",
            "prefLabel": "National Climate Data Center",
        },
        "keyword": ["climate", "weather"],
        "theme": [
            {
                "@id": "https://example.gov/concepts/climate-science",
                "@type": "Concept",
                "prefLabel": "Climate Science",
            }
        ],
        "identifier": {
            "@type": "Identifier",
            "@id": "https://example.gov/identifiers/ncdc-climate-obs-2024",
            "schemaAgency": "National Climate Data Center",
            "notation": "NCDC-CLIMATE-OBS-2024",
            "version": "1.0",
        },
        "inSeries": [
            {
                "@id": "https://example.gov/series/annual-climate-observations",
                "@type": "DatasetSeries",
                "title": "Annual Climate Observations Series",
            }
        ],
        "distribution": [
            {"@type": "Distribution", "title": "Climate Observations CSV"},
            {"@type": "Distribution", "title": "Climate Observations JSON"},
        ],
    }

    result = DcatIndexTransformer().transform(dcat)

    assert result == {
        "title": "National Climate Observations 2024",
        "description": "Comprehensive daily climate observations.",
        "publisher": "National Climate Data Center",
        "keyword": ["climate", "weather"],
        "theme": ["Climate Science"],
        "identifier": "https://example.gov/identifiers/ncdc-climate-obs-2024",
        "distribution_titles": [
            "Climate Observations CSV",
            "Climate Observations JSON",
        ],
    }
    # inSeries is intentionally not mapped into isPartOf / index fields.
    assert "inSeries" not in result
    assert "isPartOf" not in result


def test_transform_real_dcat3_complete_example():
    dcat = json.loads(DCAT3_COMPLETE_EXAMPLE.read_text())

    result = DcatIndexTransformer().transform(dcat)

    assert result["title"] == "National Climate Observations 2024"
    assert result["publisher"] == "National Climate Data Center"
    assert result["identifier"] == ""
    assert result["theme"] == ["Climate Science", "Environmental Monitoring"]
    assert result["distribution_titles"] == [
        "Climate Observations CSV",
        "Climate Observations JSON",
    ]
    assert "inSeries" not in result
    assert "isPartOf" not in result


def test_transform_handles_empty_dcat():
    assert DcatIndexTransformer().transform({}) == {
        "title": "",
        "description": "",
        "publisher": "",
        "keyword": [],
        "theme": [],
        "identifier": "",
        "distribution_titles": [],
    }


def test_transform_handles_none_dcat():
    assert DcatIndexTransformer().transform(None)["identifier"] == ""
