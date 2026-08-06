from __future__ import annotations

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
            "path": "dcat",
            "query": {"term": {"dcat.isPartOf": value}},
        }
    }


COLLECTION_FILTER = FilterDefinition(
    name="collection",
    query_params=("collection",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT),
    api_query_params=(ApiQueryParam("collection"),),
    parse=lambda args: parse_string(args, "collection"),
    to_query_pairs=lambda value: [("collection", value)],
    clause_builder=_clause,
)
