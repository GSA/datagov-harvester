from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ORGANIZATION_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    get_list,
)


def _clause(criteria, values: list[str]) -> list[dict]:
    return [{"term": {"keyword.normalized": keyword.lower()}} for keyword in values]


def _aggregation(criteria) -> dict:
    return {
        "terms": {
            "field": "keyword.normalized",
            "size": criteria.keyword_size,
            "min_doc_count": 1,
            "order": {"_count": "desc"},
        }
    }


def _parse_aggregation(raw_aggs: Mapping[str, Any]) -> list[dict]:
    buckets = raw_aggs.get("unique_keywords", {}).get("buckets", [])
    return [
        {"keyword": bucket["key"], "count": bucket["doc_count"]} for bucket in buckets
    ]


def _section(criteria, context) -> dict:
    return {
        "keywords": criteria.get_filter("keyword", []),
        "suggested_keywords": context.get("suggested_keywords") or [],
        "contextual_keyword_counts": context.get("contextual_keyword_counts") or {},
    }


KEYWORD_FILTER = FilterDefinition(
    name="keyword",
    query_params=("keyword",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT, ORGANIZATION_CONTEXT),
    ui_contexts=(MAIN_CONTEXT, ORGANIZATION_CONTEXT),
    label="Keywords",
    renderer="keyword",
    api_query_params=(ApiQueryParam("keyword", repeated=True),),
    parse=lambda args: get_list(args, "keyword"),
    to_query_pairs=lambda values: [("keyword", value) for value in values],
    clause_builder=_clause,
    aggregation_name="unique_keywords",
    aggregation_result_key="keywords",
    aggregation_builder=_aggregation,
    aggregation_parser=_parse_aggregation,
    section_builder=_section,
)
