from __future__ import annotations

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ORGANIZATION_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    FilterOption,
    parse_string,
)

SPATIAL_DATA_OPTIONS = (
    FilterOption("", "All datasets", "filter-spatial-all"),
    FilterOption("geospatial", "Geospatial only", "filter-spatial-geo"),
    FilterOption("non-geospatial", "Non-geospatial only", "filter-spatial-non-geo"),
)


def _clause(criteria, value: str) -> dict | None:
    if value == "geospatial":
        return {"term": {"has_spatial": True}}
    if value == "non-geospatial":
        return {"term": {"has_spatial": False}}
    return None


def _section(criteria, context) -> dict:
    value = criteria.get_filter("spatial_data") or ""
    labels = {
        option.value: option.label for option in SPATIAL_DATA_OPTIONS if option.value
    }
    return {
        "field_name": "spatial_filter",
        "value": value,
        "section_id": "filter-spatial",
        "button_id": "spatial-data-label",
        "active_summary": labels.get(value),
    }


SPATIAL_DATA_FILTER = FilterDefinition(
    name="spatial_data",
    query_params=("spatial_filter",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT, ORGANIZATION_CONTEXT),
    ui_contexts=(MAIN_CONTEXT, ORGANIZATION_CONTEXT),
    label="Spatial Data",
    renderer="radio_group",
    options=SPATIAL_DATA_OPTIONS,
    api_query_params=(
        ApiQueryParam("spatial_filter", enum_values=("geospatial", "non-geospatial")),
    ),
    parse=lambda args: parse_string(args, "spatial_filter"),
    to_query_pairs=lambda value: [("spatial_filter", value)],
    clause_builder=_clause,
    section_builder=_section,
)
