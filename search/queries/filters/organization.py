from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    parse_string,
)


def _clause(criteria, value: str) -> dict:
    return {
        "nested": {
            "path": "organization",
            "query": {"term": {"organization.id": value}},
        }
    }


def _aggregation(criteria) -> dict:
    return {
        "nested": {"path": "organization"},
        "aggs": {
            "by_slug": {
                "terms": {
                    "field": "organization.slug",
                    "size": criteria.org_size,
                    "min_doc_count": 1,
                    "order": {"_count": "desc"},
                }
            }
        },
    }


def _parse_aggregation(raw_aggs: Mapping[str, Any]) -> list[dict]:
    buckets = raw_aggs.get("organizations", {}).get("by_slug", {}).get("buckets", [])
    return [{"slug": bucket["key"], "count": bucket["doc_count"]} for bucket in buckets]


def _section(criteria, context) -> dict:
    return {
        "selected_organization": context.get("selected_organization"),
        "org_slug": criteria.get_filter("organization"),
        "suggested_organizations": context.get("suggested_organizations") or [],
        "contextual_org_counts": context.get("contextual_org_counts") or {},
    }


ORGANIZATION_FILTER = FilterDefinition(
    name="organization",
    query_params=("org_slug",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT),
    ui_contexts=(MAIN_CONTEXT,),
    label="Organization",
    renderer="organization",
    api_query_params=(ApiQueryParam("org_slug"),),
    parse=lambda args: parse_string(args, "org_slug"),
    to_query_pairs=lambda value: [("org_slug", value)],
    clause_builder=_clause,
    aggregation_name="organizations",
    aggregation_result_key="organizations",
    aggregation_builder=_aggregation,
    aggregation_parser=_parse_aggregation,
    section_builder=_section,
)
