import pytest

# swapping werkzeug.datastructures.MultiDict for builtin dict
# data access shouldn't be coupled to any specific WSGI implementation
from search.queries import (
    API_CONTEXT,
    FILTERS,
    MAIN_CONTEXT,
    FilterParseError,
    SearchCriteria,
    build_aggregation_specs,
    build_filter_clauses,
    build_filter_sections,
    parse_filter_aggregations,
    visible_filter_query_params,
)


def test_search_criteria_parses_registered_filters():

    criteria = SearchCriteria.from_request_args(
        {
            "q": "water",
            "keyword": ["health", "education"],
            "org_type": "City Government",
            "publisher": "City Publisher",
            "org_slug": "city-example",
            "spatial_filter": "geospatial",
            "collection": "collection-id",
            "has_download": "true",
            "sort": "popularity",
        },
        route_context=MAIN_CONTEXT,
    )

    assert criteria.query == "water"
    assert criteria.get_filter("keyword") == ["health", "education"]
    assert criteria.get_filter("org_type") == ["City Government"]
    assert criteria.get_filter("publisher") == "City Publisher"
    assert criteria.get_filter("organization") == "city-example"
    assert criteria.get_filter("spatial_data") == "geospatial"
    assert criteria.get_filter("collection") == "collection-id"
    assert criteria.get_filter("has_download") is True
    assert criteria.sort_by == "popularity"


def test_search_criteria_rejects_malformed_spatial_geometry():
    with pytest.raises(FilterParseError) as excinfo:
        SearchCriteria.from_request_args(
            {"spatial_geometry": "{bad-json"},
            route_context=MAIN_CONTEXT,
        )

    assert excinfo.value.parameter == "spatial_geometry"
    assert excinfo.value.message == "spatial_geometry parameter is malformed"


def test_has_download_filter_defaults_to_inactive():
    criteria = SearchCriteria.from_request_args({}, route_context=MAIN_CONTEXT)

    assert criteria.get_filter("has_download") is None
    assert build_filter_clauses(criteria) == []


def test_query_params_are_generated_from_registered_filters():

    criteria = SearchCriteria.from_request_args(
        {
            "q": "parks",
            "keyword": ["trees", "transit"],
            "spatial_geometry": '{"type":"Point","coordinates":[-75,40]}',
            "spatial_within": "false",
            "geography_label": "Example Place",
        },
        route_context=MAIN_CONTEXT,
    )

    params = criteria.to_query_dict(include_query=True, include_sort=True)

    assert params["q"] == "parks"
    assert params["sort"] == "relevance"
    assert params["keyword"] == ["trees", "transit"]
    assert params["spatial_within"] == "false"
    assert params["geography_label"] == "Example Place"
    assert params["spatial_geometry"] == '{"type":"Point","coordinates":[-75,40]}'


def test_filter_clauses_are_built_from_registered_filters():
    criteria = SearchCriteria.from_request_args(
        {
            "keyword": "health",
            "org_slug": "city-example",
            "org_type": "City Government",
            "publisher": "City Publisher",
            "spatial_filter": "non-geospatial",
            "collection": "collection-id",
            "has_download": "true",
        },
        route_context=MAIN_CONTEXT,
    )

    criteria.set_resolved_filter("organization", "org-id")

    clauses = build_filter_clauses(criteria)

    assert {"term": {"keyword.normalized": "health"}} in clauses
    assert {"term": {"publisher.normalized": "city publisher"}} in clauses
    assert {"term": {"has_spatial": False}} in clauses
    assert {"term": {"has_download": True}} in clauses
    assert {
        "nested": {
            "path": "organization",
            "query": {"term": {"organization.id": "org-id"}},
        }
    } in clauses
    assert {
        "nested": {
            "path": "organization",
            "query": {"terms": {"organization.organization_type": ["City Government"]}},
        }
    } in clauses
    assert {
        "nested": {
            "path": "dcat",
            "query": {"term": {"dcat.isPartOf": "collection-id"}},
        }
    } in clauses


def test_aggregation_specs_and_parsing_are_registry_owned():
    criteria = SearchCriteria(include_aggregations=True, keyword_size=5)

    specs = build_aggregation_specs(criteria)

    assert set(specs) == {"unique_keywords", "organizations", "unique_publishers"}
    assert specs["unique_keywords"]["terms"]["field"] == "keyword.normalized"
    assert specs["unique_keywords"]["terms"]["size"] == 5

    parsed = parse_filter_aggregations(
        {
            "unique_keywords": {"buckets": [{"key": "health", "doc_count": 3}]},
            "organizations": {
                "by_slug": {"buckets": [{"key": "org-slug", "doc_count": 2}]}
            },
            "unique_publishers": {"buckets": [{"key": "Publisher", "doc_count": 1}]},
        }
    )

    assert parsed == {
        "keywords": [{"keyword": "health", "count": 3}],
        "organizations": [{"slug": "org-slug", "count": 2}],
        "publishers": [{"name": "Publisher", "count": 1}],
    }


def test_fixed_option_sections_are_built_by_filter_definitions():
    criteria = SearchCriteria.from_values(
        filters={
            "org_type": ["City Government"],
            "spatial_data": "geospatial",
            "has_download": True,
        }
    )

    sections = build_filter_sections(criteria, route_context=MAIN_CONTEXT)
    by_name = {section["name"]: section for section in sections}

    assert by_name["org_type"]["field_name"] == "org_type"
    assert by_name["org_type"]["values"] == ["City Government"]
    assert by_name["org_type"]["active_summary"] == "City Government"
    assert by_name["spatial_data"]["field_name"] == "spatial_filter"
    assert by_name["spatial_data"]["value"] == "geospatial"
    assert by_name["spatial_data"]["active_summary"] == "Geospatial only"
    assert by_name["has_download"]["field_name"] == "has_download"
    assert by_name["has_download"]["values"] == ["true"]
    assert by_name["has_download"]["active_summary"] is not None


def test_geography_section_exposes_template_fields_only():
    geometry = {"type": "Point", "coordinates": [-75, 40]}
    criteria = SearchCriteria.from_values(
        filters={
            "geography": {
                "geometry": geometry,
                "within": False,
                "label": "Example Place",
            }
        }
    )

    sections = build_filter_sections(
        criteria,
        route_context=MAIN_CONTEXT,
        search_result_geometries=[geometry],
    )
    geography_section = next(
        section for section in sections if section["name"] == "geography"
    )

    assert geography_section["spatial_geometry"] == geometry
    assert geography_section["geography_label"] == "Example Place"
    assert geography_section["search_result_geometries"] == [geometry]
    assert "spatial_within" not in geography_section
    # within is still preserved for query round-trips even though the section
    # does not expose it to the template.
    assert criteria.get_geography().get("within", True) is False
    assert criteria.to_query_dict()["spatial_within"] == "false"


def test_visible_filter_query_params_come_from_registered_sections():
    assert visible_filter_query_params(MAIN_CONTEXT) >= {
        "keyword",
        "org_slug",
        "org_type",
        "publisher",
        "spatial_filter",
        "spatial_geometry",
        "spatial_within",
        "geography_label",
        "has_download",
    }
    assert "collection" not in visible_filter_query_params(MAIN_CONTEXT)


def test_api_filter_schema_metadata_matches_registered_query_params():
    for definition in FILTERS:
        if API_CONTEXT not in definition.parse_contexts:
            continue

        documented_params = {param.name for param in definition.api_query_params}

        assert documented_params == set(definition.query_params), definition.name


def test_from_values_accepts_generic_filter_map():
    criteria = SearchCriteria.from_values(
        query="water",
        filters={
            "keyword": ["health"],
            "collection": "collection-id",
            "geography": {
                "geometry": {"type": "Point", "coordinates": [-75, 40]},
                "within": False,
                "label": "Example",
            },
        },
    )

    assert criteria.query == "water"
    assert criteria.get_filter("keyword") == ["health"]
    assert criteria.get_filter("collection") == "collection-id"
    geography = criteria.get_geography()
    assert geography.get("geometry") == {"type": "Point", "coordinates": [-75, 40]}
    assert geography.get("within", True) is False


def test_from_request_args_parses_access_level():
    args = {"access_level": "restricted public"}
    criteria = SearchCriteria.from_request_args(args, route_context=API_CONTEXT)
    assert criteria.get_filter("access_level") == "restricted public"


def test_access_level_filter_parses_and_builds_clause():
    criteria = SearchCriteria.from_request_args(
        {"access_level": "non-public"},
        route_context=API_CONTEXT,
    )

    assert criteria.get_filter("access_level") == "non-public"

    clauses = build_filter_clauses(criteria)
    assert clauses == [{"term": {"access_level": "non-public"}}]
