from __future__ import annotations

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    parse_string,
)


def _clause(criteria, value: str) -> dict:
    normalized_value = (value or "").strip().lower()
    return {"term": {"access_level": normalized_value}}


ACCESS_LEVEL_FILTER = FilterDefinition(
    name="access_level",
    query_params=("access_level",),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT),
    api_query_params=(ApiQueryParam("access_level"),),
    parse=lambda args: parse_string(args, "access_level"),
    to_query_pairs=lambda value: [("access_level", value)],
    clause_builder=_clause,
)
