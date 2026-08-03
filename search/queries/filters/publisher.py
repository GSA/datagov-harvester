from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ORGANIZATION_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    parse_string,
)


def _clause(criteria, value: str) -> dict:
    return {"term": {"publisher.normalized": value.lower()}}


def _aggregation(criteria) -> dict:
    return {
        "terms": {
            "field": "publisher.raw",
            "size": criteria.publisher_size,
            "min_doc_count": 1,
            "order": {"_count": "desc"},
        }
    }


def _parse_aggregation(raw_aggs: Mapping[str, Any]) -> list[dict]:
    buckets = raw_aggs.get("unique_publishers", {}).get("buckets", [])
    return [{"name": bucket["key"], "count": bucket["doc_count"]} for bucket in buckets]


def _section(criteria, context) -> dict:
    return {
        "publisher": criteria.get_filter("publisher"),
        "suggested_publishers": context.get("suggested_publishers") or [],
        "contextual_publisher_counts": context.get("contextual_publisher_counts") or {},
    }


PUBLISHER_FILTER = FilterDefinition(
    name="publisher",
    query_params=("publisher",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT, ORGANIZATION_CONTEXT),
    ui_contexts=(MAIN_CONTEXT, ORGANIZATION_CONTEXT),
    label="Publisher",
    renderer="publisher",
    api_query_params=(ApiQueryParam("publisher"),),
    parse=lambda args: parse_string(args, "publisher"),
    to_query_pairs=lambda value: [("publisher", value)],
    clause_builder=_clause,
    aggregation_name="unique_publishers",
    aggregation_result_key="publishers",
    aggregation_builder=_aggregation,
    aggregation_parser=_parse_aggregation,
    section_builder=_section,
)
