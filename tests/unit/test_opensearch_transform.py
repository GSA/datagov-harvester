"""Unit tests for the DCAT -> OpenSearch index field transformer.

These tests pin down, field by field, how DCAT-US 1.1 values are mapped *up*
into the DCAT-US 3.0 structure so that every OpenSearch field has a single,
stable shape regardless of the source schema version.
"""

import json
from pathlib import Path

import pytest

from harvester.opensearch_transform import (
    DEFAULT_FIELD_SPECS,
    DcatIndexTransformer,
    FieldSpec,
    coerce_concepts,
    coerce_identifier,
    coerce_in_series,
    coerce_keywords,
    coerce_publisher_name,
    coerce_text,
    concept_labels,
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
# identifier: DCAT-US 1.1 string  ->  DCAT-US 3.0 Identifier object
# ---------------------------------------------------------------------------


def test_coerce_identifier_wraps_dcat1_string_as_object():
    assert coerce_identifier("cftc-dc1") == {"@id": "cftc-dc1"}


def test_coerce_identifier_keeps_dcat3_object_with_id():
    value = {
        "@type": "Identifier",
        "@id": "https://example.gov/identifiers/dataset-1",
        "notation": "DATASET-1",
    }

    assert coerce_identifier(value) == {
        "@id": "https://example.gov/identifiers/dataset-1",
        "notation": "DATASET-1",
    }


def test_coerce_identifier_keeps_dcat3_object_without_id():
    value = {
        "@type": "Identifier",
        "schemaAgency": "National Climate Data Center",
        "notation": "NCDC-CLIMATE-OBS-2024",
        "version": "1.0",
    }

    assert coerce_identifier(value) == {
        "schemaAgency": "National Climate Data Center",
        "notation": "NCDC-CLIMATE-OBS-2024",
        "version": "1.0",
    }


def test_coerce_identifier_drops_nested_non_scalar_fields():
    value = {
        "@id": "https://example.gov/identifiers/dataset-1",
        "creator": {"@type": "Organization", "name": "NCDC"},
        "issued": {"@type": "date", "value": "2024-01-01"},
    }

    # Only searchable scalar sub-fields survive so the OpenSearch object mapping
    # stays flat and predictable.
    assert coerce_identifier(value) == {
        "@id": "https://example.gov/identifiers/dataset-1"
    }


@pytest.mark.parametrize("value", [None, "", "   "])
def test_coerce_identifier_returns_none_for_empty(value):
    assert coerce_identifier(value) is None


def test_coerce_identifier_returns_none_for_object_without_scalars():
    assert coerce_identifier({"@type": "Identifier"}) is None


# ---------------------------------------------------------------------------
# theme: DCAT-US 1.1 list[str]  ->  DCAT-US 3.0 list[Concept]
# ---------------------------------------------------------------------------


def test_coerce_concepts_wraps_dcat1_string_list():
    assert coerce_concepts(["climate", "weather"]) == [
        {"prefLabel": "climate"},
        {"prefLabel": "weather"},
    ]


def test_coerce_concepts_wraps_single_string():
    assert coerce_concepts("geospatial") == [{"prefLabel": "geospatial"}]


def test_coerce_concepts_keeps_dcat3_concept_fields():
    value = [
        {
            "@id": "https://example.gov/concepts/climate-science",
            "@type": "Concept",
            "prefLabel": "Climate Science",
            "altLabel": "Climatology",
            "notation": ["CLIM-SCI"],
            "definition": "The study of climate.",
            "inScheme": {
                "@id": "https://example.gov/concept-schemes/science-domains",
                "@type": "ConceptScheme",
                "title": "Science Domains",
            },
        }
    ]

    # inScheme (a nested object) is dropped; scalar/list search fields are kept.
    assert coerce_concepts(value) == [
        {
            "@id": "https://example.gov/concepts/climate-science",
            "prefLabel": "Climate Science",
            "altLabel": "Climatology",
            "notation": ["CLIM-SCI"],
            "definition": "The study of climate.",
        }
    ]


def test_coerce_concepts_handles_single_concept_object():
    assert coerce_concepts({"@type": "Concept", "prefLabel": "Published"}) == [
        {"prefLabel": "Published"}
    ]


def test_coerce_concepts_mixes_strings_and_objects():
    assert coerce_concepts(["health", {"prefLabel": "Environment"}]) == [
        {"prefLabel": "health"},
        {"prefLabel": "Environment"},
    ]


@pytest.mark.parametrize("value", [None, [], "", "   "])
def test_coerce_concepts_returns_empty(value):
    assert coerce_concepts(value) == []


def test_coerce_concepts_skips_empty_members():
    assert coerce_concepts(["", None, "keep"]) == [{"prefLabel": "keep"}]


# ---------------------------------------------------------------------------
# concept_labels: extract searchable label strings from canonical concepts
# ---------------------------------------------------------------------------


def test_concept_labels_extracts_preflabels():
    concepts = [{"prefLabel": "Geospatial"}, {"prefLabel": "Health"}]
    assert concept_labels(concepts) == ["Geospatial", "Health"]


def test_concept_labels_tolerates_plain_strings():
    assert concept_labels(["Geospatial", {"prefLabel": "Health"}]) == [
        "Geospatial",
        "Health",
    ]


def test_concept_labels_empty():
    assert concept_labels([]) == []
    assert concept_labels(None) == []


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
# inSeries: DCAT-US 1.1 isPartOf -> DCAT-US 3.0 list[DatasetSeries]
# ---------------------------------------------------------------------------


def test_coerce_in_series_maps_dcat1_ispartof_to_dataset_series():
    assert coerce_in_series({"isPartOf": "collection-1"}) == [{"@id": "collection-1"}]


def test_coerce_in_series_keeps_dcat3_inseries_object():
    dcat = {
        "inSeries": [
            {
                "@id": "https://example.gov/series/annual-climate",
                "@type": "DatasetSeries",
                "title": "Annual Climate Observations Series",
                "description": "An annual series of climate datasets.",
                "seriesMember": [{"@id": "https://example.gov/datasets/2024"}],
            }
        ]
    }
    assert coerce_in_series(dcat) == [
        {
            "@id": "https://example.gov/series/annual-climate",
            "title": "Annual Climate Observations Series",
            "description": "An annual series of climate datasets.",
        }
    ]


def test_coerce_in_series_keeps_title_without_id():
    dcat = {"inSeries": [{"@type": "DatasetSeries", "title": "Annual Series"}]}
    assert coerce_in_series(dcat) == [{"title": "Annual Series"}]


def test_coerce_in_series_prefers_inseries_over_ispartof():
    dcat = {
        "isPartOf": "collection-1",
        "inSeries": [{"@id": "https://example.gov/series/annual"}],
    }
    assert coerce_in_series(dcat) == [{"@id": "https://example.gov/series/annual"}]


def test_coerce_in_series_falls_back_to_ispartof_when_inseries_empty():
    dcat = {"isPartOf": "collection-1", "inSeries": []}
    assert coerce_in_series(dcat) == [{"@id": "collection-1"}]


@pytest.mark.parametrize(
    "dcat",
    [{}, {"isPartOf": ""}, {"isPartOf": "   "}, {"inSeries": []}, {"inSeries": [{}]}],
)
def test_coerce_in_series_empty(dcat):
    assert coerce_in_series(dcat) == []


# ---------------------------------------------------------------------------
# DcatIndexTransformer.transform: full aggregation
# ---------------------------------------------------------------------------


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
        "theme": [{"prefLabel": "geospatial"}],
        "identifier": {"@id": "cftc-dc1"},
        "distribution_titles": ["Report CSV"],
        "inSeries": [{"@id": "collection-1"}],
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
        "theme": [
            {
                "@id": "https://example.gov/concepts/climate-science",
                "prefLabel": "Climate Science",
            }
        ],
        "identifier": {
            "schemaAgency": "National Climate Data Center",
            "notation": "NCDC-CLIMATE-OBS-2024",
            "version": "1.0",
        },
        "distribution_titles": [
            "Climate Observations CSV",
            "Climate Observations JSON",
        ],
        "inSeries": [
            {
                "@id": "https://example.gov/series/annual-climate-observations",
                "title": "Annual Climate Observations Series",
            }
        ],
    }


def test_transform_real_dcat3_complete_example():
    dcat = json.loads(DCAT3_COMPLETE_EXAMPLE.read_text())

    result = DcatIndexTransformer().transform(dcat)

    assert result["title"] == "National Climate Observations 2024"
    assert result["publisher"] == "National Climate Data Center"
    assert result["identifier"] == {
        "schemaAgency": "National Climate Data Center",
        "notation": "NCDC-CLIMATE-OBS-2024",
        "version": "1.0",
    }
    assert result["theme"] == [
        {
            "@id": "https://example.gov/concepts/climate-science",
            "prefLabel": "Climate Science",
            "altLabel": "Climatology",
            "definition": (
                "The scientific study of climate, including patterns, "
                "variability, and change over time."
            ),
            "notation": ["CLIM-SCI"],
        },
        {
            "@id": "https://example.gov/concepts/environmental-monitoring",
            "prefLabel": "Environmental Monitoring",
            "definition": (
                "Systematic collection of environmental data to assess "
                "conditions and detect changes."
            ),
        },
    ]
    assert result["distribution_titles"] == [
        "Climate Observations CSV",
        "Climate Observations JSON",
    ]
    assert result["inSeries"] == [
        {
            "@id": "https://example.gov/series/annual-climate-observations",
            "title": "Annual Climate Observations Series",
            "description": (
                "An annual series of national climate observations datasets."
            ),
        }
    ]


def test_transform_handles_empty_dcat():
    assert DcatIndexTransformer().transform({}) == {
        "title": "",
        "description": "",
        "publisher": "",
        "keyword": [],
        "theme": [],
        "identifier": None,
        "distribution_titles": [],
        "inSeries": [],
    }


def test_transform_handles_none_dcat():
    # A dataset with no dcat payload should not raise.
    assert DcatIndexTransformer().transform(None)["identifier"] is None


# ---------------------------------------------------------------------------
# Configurability: the field spec table is the public knob
# ---------------------------------------------------------------------------


def test_transformer_uses_default_field_specs():
    names = {spec.name for spec in DEFAULT_FIELD_SPECS}
    assert names == {
        "title",
        "description",
        "publisher",
        "keyword",
        "theme",
        "identifier",
        "distribution_titles",
        "inSeries",
    }


def test_transformer_accepts_custom_field_specs():
    specs = [
        FieldSpec(name="identifier", source="identifier", coerce=coerce_identifier),
        FieldSpec(name="title", source="title", coerce=coerce_text),
    ]

    result = DcatIndexTransformer(field_specs=specs).transform(
        {"identifier": "abc", "title": "Hi", "theme": ["ignored"]}
    )

    assert result == {"identifier": {"@id": "abc"}, "title": "Hi"}


def test_field_spec_can_override_source_key():
    spec = FieldSpec(
        name="identifier", source="otherIdentifier", coerce=coerce_identifier
    )

    result = DcatIndexTransformer(field_specs=[spec]).transform(
        {"otherIdentifier": "doi:123", "identifier": "primary"}
    )

    assert result == {"identifier": {"@id": "doi:123"}}


def test_field_spec_from_document_receives_full_dcat():
    spec = FieldSpec(
        name="inSeries",
        coerce=coerce_in_series,
        from_document=True,
    )

    result = DcatIndexTransformer(field_specs=[spec]).transform(
        {"inSeries": [{"@id": "series-1"}]}
    )

    assert result == {"inSeries": [{"@id": "series-1"}]}
