from __future__ import annotations

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    FilterOption,
    get_list,
    selection_summary,
)
from shared.constants import ORGANIZATION_TYPE_VALUES

ORG_TYPE_INPUT_IDS = {
    "Federal Government": "filter-federal",
    "City Government": "filter-city",
    "State Government": "filter-state",
    "County Government": "filter-county",
    "University": "filter-university",
    "Tribal": "filter-tribal",
    "Non-Profit": "filter-nonprofit",
}

ORG_TYPE_OPTIONS = tuple(
    FilterOption(
        value=value,
        label=value,
        input_id=ORG_TYPE_INPUT_IDS.get(value, f"filter-org-type-{index}"),
    )
    for index, value in enumerate(ORGANIZATION_TYPE_VALUES)
)


def _clause(criteria, values: list[str]) -> dict:
    return {
        "nested": {
            "path": "organization",
            "query": {"terms": {"organization.organization_type": values}},
        }
    }


def _section(criteria, context) -> dict:
    values = criteria.get_filter("org_type", [])
    return {
        "field_name": "org_type",
        "values": values,
        "subtitle": "Federal, state, city, and other org categories",
        "section_id": "filter-organization",
        "button_id": "organization-type-label",
        "active_summary": selection_summary(values),
    }


ORGANIZATION_TYPE_FILTER = FilterDefinition(
    name="org_type",
    query_params=("org_type",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT),
    ui_contexts=(MAIN_CONTEXT,),
    label="Organization Type",
    renderer="checkbox_group",
    options=ORG_TYPE_OPTIONS,
    api_query_params=(
        ApiQueryParam(
            "org_type",
            repeated=True,
            enum_values=tuple(ORGANIZATION_TYPE_VALUES),
        ),
    ),
    parse=lambda args: get_list(args, "org_type"),
    to_query_pairs=lambda values: [("org_type", value) for value in values],
    clause_builder=_clause,
    section_builder=_section,
)
