from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from search.queries.criteria import SearchCriteria
from search.queries.filters import FILTERS


def build_filter_clauses(criteria: SearchCriteria) -> list[dict]:
    clauses: list[dict] = []
    for definition in FILTERS:
        if definition.clause_builder is None:
            continue
        value = criteria.get_resolved_filter(definition.name)
        if not definition.is_active(value):
            continue
        clause = definition.clause_builder(criteria, value)
        if clause is None:
            continue
        if isinstance(clause, list):
            clauses.extend(clause)
        else:
            clauses.append(clause)
    return clauses


def build_aggregation_specs(criteria: SearchCriteria) -> dict[str, dict]:
    specs = {}
    for definition in FILTERS:
        if definition.aggregation_name and definition.aggregation_builder:
            aggregation = definition.aggregation_builder(criteria)
            if aggregation is not None:
                specs[definition.aggregation_name] = aggregation
    return specs


def parse_filter_aggregations(raw_aggs: Mapping[str, Any] | None) -> dict | None:
    if raw_aggs is None:
        return None
    parsed = {}
    for definition in FILTERS:
        if definition.aggregation_parser is not None:
            parsed_name = definition.aggregation_result_key or definition.name
            parsed[parsed_name] = definition.aggregation_parser(raw_aggs)
    return parsed


def build_filter_sections(
    criteria: SearchCriteria,
    *,
    route_context: str,
    selected_organization=None,
    suggested_keywords=None,
    suggested_organizations=None,
    suggested_publishers=None,
    contextual_keyword_counts=None,
    contextual_org_counts=None,
    contextual_publisher_counts=None,
    search_result_geometries=None,
) -> list[dict]:
    context = {
        "selected_organization": selected_organization,
        "suggested_keywords": suggested_keywords,
        "suggested_organizations": suggested_organizations,
        "suggested_publishers": suggested_publishers,
        "contextual_keyword_counts": contextual_keyword_counts,
        "contextual_org_counts": contextual_org_counts,
        "contextual_publisher_counts": contextual_publisher_counts,
        "search_result_geometries": search_result_geometries,
    }
    sections = []
    for definition in FILTERS:
        if route_context not in definition.ui_contexts:
            continue
        if definition.section_builder is None:
            continue
        value = criteria.get_filter(definition.name)
        section = {
            "name": definition.name,
            "label": definition.label,
            "renderer": definition.renderer,
            "value": value,
            "is_active": definition.is_active(value),
            "options": definition.options,
        }
        section.update(definition.section_builder(criteria, context))
        sections.append(section)
    return sections


def visible_filter_query_params(route_context: str) -> set[str]:
    params: set[str] = set()
    for definition in FILTERS:
        if route_context in definition.ui_contexts:
            params.update(definition.query_params)
    return params


def build_document_by_slug_query(slug_or_id: str) -> dict[str, Any]:
    return {
        "size": 1,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {"term": {"slug": slug_or_id}},
                                {"term": {"_id": slug_or_id}},
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                ]
            }
        },
    }


def build_ispartof_query() -> dict[str, Any]:
    return {
        "query": {
            "nested": {
                "path": "dcat",
                "query": {"exists": {"field": "dcat.isPartOf"}},
            }
        }
    }


def build_phrase_query(phrase_text: str) -> dict[str, Any]:
    """
    Build a bool query with match_phrase across multiple fields for
    exact phrase matching.

    Args:
        phrase_text: The phrase to search for (without quotes)

    Returns:
        OpenSearch bool query with match_phrase for each field
    """
    return {
        "bool": {
            "should": [
                {"match_phrase": {"title": {"query": phrase_text, "boost": 5}}},
                {"match_phrase": {"description": {"query": phrase_text, "boost": 3}}},
                {"match_phrase": {"publisher": {"query": phrase_text, "boost": 3}}},
                {"match_phrase": {"keyword": {"query": phrase_text, "boost": 2}}},
                {"match_phrase": {"theme": {"query": phrase_text}}},
                {"match_phrase": {"identifier": {"query": phrase_text}}},
                {
                    "match_phrase": {
                        "distribution_titles": {"query": phrase_text, "boost": 2}
                    }
                },
            ],
            "minimum_should_match": 1,
        }
    }


def build_multi_match_query(query_text: str) -> dict[str, Any]:
    """
    Build a multi_match query for a single term or phrase (no quotes).
    """
    return {
        "multi_match": {
            "query": query_text,
            "type": "most_fields",
            "fields": [
                "title^5",
                "description^3",
                "publisher^3",
                "keyword^2",
                "theme",
                "identifier",
                "distribution_titles^2",
            ],
            "operator": "AND",
            "zero_terms_query": "all",
        }
    }


def build_query_for_parsed_term(term_dict: dict) -> dict[str, Any]:
    """
    Build an appropriate OpenSearch query for a parsed term or phrase.

    Args:
        term_dict: Dictionary with 'text' and 'type' keys

    Returns:
        OpenSearch query dict
    """
    text = term_dict["text"]
    term_type = term_dict["type"]

    if term_type == "phrase":
        return build_phrase_query(text)
    else:
        return build_multi_match_query(text)


def parse_search_query(query: str) -> dict[str, Any] | None:
    """
    Parse a query string to identify phrases (quoted text) and OR operators.

    Returns:
        Dict with:
        - 'has_or': bool indicating if OR operators are present
        - 'terms': list of dicts with 'text' and 'type' ('phrase' or 'term')
        Returns None if query is a simple term search (no quotes, no OR)

    Examples:
        '"poor food"' -> {'has_or': False, 'terms': [{'text': 'poor food',
            'type': 'phrase'}]}
        'food OR health' -> {'has_or': True, 'terms': [{'text': 'food', 'type':
            'term'}, {'text': 'health', 'type': 'term'}]}
        '"poor food" OR health' -> {'has_or': True, 'terms': [{'text': 'poor food',
            'type': 'phrase'}, {'text': 'health', 'type': 'term'}]}
        '"poor food" OR "electric vehicle"' -> {'has_or': True, 'terms':
            [{'text': 'poor food', 'type': 'phrase'}, {'text': 'electric vehicle',
            'type': 'phrase'}]}
        'simple search' -> None (regular multi_match query)
    """
    if not query or not isinstance(query, str):
        return None

    query = query.strip()
    if not query:
        return None

    # Check if query contains OR (case insensitive)
    has_or = bool(re.search(r"\s+OR\s+", query, re.IGNORECASE))

    # Check if query has any quoted phrases
    has_quotes = '"' in query

    # If no OR and no quotes, return None for regular search
    if not has_or and not has_quotes:
        return None

    # Pattern to match: quoted strings, OR keyword, or
    # non-whitespace/non-quote sequences
    pattern = r'"([^"]+)"|(\bOR\b)|(\S+)'
    matches = re.finditer(pattern, query, re.IGNORECASE)

    terms = []
    for match in matches:
        if match.group(1):  # Quoted phrase
            phrase_text = match.group(1).strip()
            if phrase_text:
                terms.append({"text": phrase_text, "type": "phrase"})
        elif match.group(2):  # OR keyword - skip
            continue
        elif match.group(3):  # Regular term
            term = match.group(3).strip()
            if term.upper() != "OR":
                terms.append({"text": term, "type": "term"})

    if not terms:
        return None

    return {"has_or": has_or, "terms": terms}


def build_sort_clause(sort_by: str, sort_point: Any = None) -> list[dict]:
    """Return the OpenSearch sort clause for the requested key."""
    sort_key = (sort_by or "relevance").lower()

    if sort_key == "distance":
        if sort_point:
            return [
                {
                    "_geo_distance": {
                        "spatial_centroid": sort_point,
                        "order": "asc",
                        "unit": "km",
                        "distance_type": "arc",
                        "mode": "min",
                        "ignore_unmapped": True,
                    }
                },
                {"_score": {"order": "desc"}},
                {"popularity": {"order": "desc", "missing": "_last"}},
                {"_id": {"order": "desc"}},
            ]
        sort_key = "relevance"

    if sort_key == "popularity":
        return [
            {"popularity": {"order": "desc", "missing": "_last"}},
            {"_score": {"order": "desc"}},
            {"_id": {"order": "desc"}},
        ]

    if sort_key == "last_harvested_date":
        return [
            {"last_harvested_date": {"order": "desc", "missing": "_last"}},
            {"_score": {"order": "desc"}},
            {"popularity": {"order": "desc", "missing": "_last"}},
            {"_id": {"order": "desc"}},
        ]

    # Default to relevance sorting with popularity as a tie-breaker
    return [
        {"_score": {"order": "desc"}},
        {"popularity": {"order": "desc", "missing": "_last"}},
        {"_id": {"order": "desc"}},
    ]


def build_base_query(query: str) -> dict[str, Any]:
    parsed_query = parse_search_query(query) if query else None

    if parsed_query:
        # Build query based on parsed terms
        if parsed_query["has_or"]:
            # Build a bool query with should clauses (OR logic)
            base_query: dict[str, Any] = {
                "bool": {
                    "should": [
                        build_query_for_parsed_term(term)
                        for term in parsed_query["terms"]
                    ],
                    "minimum_should_match": 1,
                }
            }
        else:
            # Single phrase query without OR
            # Should only have one term since has_or is False
            if len(parsed_query["terms"]) == 1:
                base_query = build_query_for_parsed_term(parsed_query["terms"][0])
            else:
                # Shouldn't happen, but fall back to match_all
                base_query = {"match_all": {}}
    elif query and query.strip():
        # Standard AND query (no phrases, no OR)
        base_query: dict[str, Any] = build_multi_match_query(query)
    else:
        # No query, match all
        base_query = {"match_all": {}}

    return base_query


def build_unique_keywords_query(
    size=100, min_doc_count=1, search=None
) -> dict[str, Any]:
    terms_clause = {
        "field": "keyword.normalized",
        "size": size,
        "min_doc_count": min_doc_count,
        "order": {"_count": "desc"},
    }

    if search:
        escaped = re.escape(search.lower())
        terms_clause["include"] = f".*{escaped}.*"

    return {
        "size": 0,  # Don't return documents, just aggregations
        "aggs": {
            "unique_keywords": {
                "terms": terms_clause,
            }
        },
    }


def build_organization_counts_query(size=100, min_doc_count=1) -> dict[str, Any]:
    return {
        "size": 0,
        "aggs": {
            "organizations": {
                "nested": {"path": "organization"},
                "aggs": {
                    "by_slug": {
                        "terms": {
                            "field": "organization.slug",
                            "size": size,
                            "min_doc_count": min_doc_count,
                            "order": {"_count": "desc"},
                        }
                    }
                },
            }
        },
    }


def build_publisher_counts_query(size=100, min_doc_count=1) -> dict[str, Any]:
    return {
        "size": 0,
        "aggs": {
            "unique_publishers": {
                "terms": {
                    "field": "publisher.raw",
                    "size": size,
                    "min_doc_count": min_doc_count,
                    "order": {"_count": "desc"},
                }
            }
        },
    }


def build_last_harvested_stats_query() -> dict[str, Any]:
    return {
        "size": 0,
        "aggs": {
            "age_bins": {
                "filters": {
                    "filters": {
                        "last_week": {
                            "range": {"last_harvested_date": {"gte": "now-7d/d"}}
                        },
                        "last_month": {
                            "range": {
                                "last_harvested_date": {
                                    "gte": "now-30d/d",
                                    "lt": "now-7d/d",
                                }
                            }
                        },
                        "last_year": {
                            "range": {
                                "last_harvested_date": {
                                    "gte": "now-365d/d",
                                    "lt": "now-30d/d",
                                }
                            }
                        },
                        "older": {
                            "range": {"last_harvested_date": {"lt": "now-365d/d"}}
                        },
                    }
                }
            },
        },
    }


def build_search_body_query(
    base_query: dict, sort_by: str, sort_point: Any, per_page: int
):
    return {
        "query": base_query,
        "sort": build_sort_clause(sort_by, sort_point=sort_point),
        # ask for one more to help with pagination, see
        # from_opensearch_result above.
        # When per_page is 0 the caller only wants aggregations; set size
        # to 0 so OpenSearch skips hits entirely.
        "size": 0 if per_page == 0 else per_page + 1,
    }


def build_search_filter_body_query(base_query: dict, filters: list):
    return {
        "bool": {
            "filter": filters,
            "must": [
                # use the previous query in here
                base_query,
            ],
        }
    }
