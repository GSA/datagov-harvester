from __future__ import annotations

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ORGANIZATION_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    FilterOption,
    get_value,
    parse_bool_param,
)

HAS_DOWNLOAD_OPTIONS = (
    FilterOption(
        value="true",
        label="Only show datasets with a downloadable file",
        input_id="filter-has-download",
    ),
)


def _clause(criteria, value: bool) -> dict | None:
    if value:
        return {"term": {"has_download": True}}
    return None


def _section(criteria, context) -> dict:
    value = criteria.get_filter("has_download") or False
    return {
        "field_name": "has_download",
        "values": ["true"] if value else [],
        "subtitle": "Limit results to datasets with a direct data download",
        "section_id": "filter-has-download",
        "button_id": "has-download-label",
        "active_summary": HAS_DOWNLOAD_OPTIONS[0].label if value else None,
    }


HAS_DOWNLOAD_FILTER = FilterDefinition(
    name="has_download",
    query_params=("has_download",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT, ORGANIZATION_CONTEXT),
    ui_contexts=(MAIN_CONTEXT, ORGANIZATION_CONTEXT),
    label="Data Access",
    renderer="checkbox_group",
    options=HAS_DOWNLOAD_OPTIONS,
    api_query_params=(ApiQueryParam("has_download", field_type="boolean"),),
    parse=lambda args: parse_bool_param(get_value(args, "has_download"), False),
    to_query_pairs=lambda value: [("has_download", "true")] if value else [],
    is_active=lambda value: value is True,
    clause_builder=_clause,
    section_builder=_section,
)
