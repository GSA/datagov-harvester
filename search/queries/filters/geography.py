from __future__ import annotations

import json
from typing import Any
from urllib.parse import unquote

from search.queries.filters.base import (
    API_CONTEXT,
    MAIN_CONTEXT,
    ORGANIZATION_CONTEXT,
    ApiQueryParam,
    FilterDefinition,
    FilterParseError,
    get_value,
    parse_bool_param,
)


def _parse_geography(args) -> dict | None:
    raw_geometry = get_value(args, "spatial_geometry")
    if raw_geometry is None:
        return None
    try:
        geometry = json.loads(unquote(raw_geometry))
    except json.JSONDecodeError as exc:
        raise FilterParseError(
            "spatial_geometry", "spatial_geometry parameter is malformed"
        ) from exc

    label = (get_value(args, "geography_label", None) or "").strip() or None
    return {
        "geometry": geometry,
        "within": parse_bool_param(get_value(args, "spatial_within"), True),
        "label": label,
    }


def _json_query_value(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _to_query_pairs(value: dict) -> list[tuple[str, str]]:
    return [
        ("spatial_geometry", _json_query_value(value["geometry"])),
        ("spatial_within", "true" if value.get("within", True) else "false"),
        *([("geography_label", value["label"])] if value.get("label") else []),
    ]


def _clause(criteria, value: dict) -> dict | None:
    geometry = value.get("geometry")
    if geometry is None:
        return None
    return {
        "geo_shape": {
            "spatial_shape": {
                "shape": geometry,
                "relation": "WITHIN" if value.get("within", True) else "INTERSECTS",
            }
        }
    }


def _section(criteria, context) -> dict:
    geography = criteria.get_geography()
    return {
        "spatial_geometry": geography.get("geometry"),
        "geography_label": geography.get("label"),
        "search_result_geometries": context.get("search_result_geometries"),
    }


GEOGRAPHY_FILTER = FilterDefinition(
    name="geography",
    query_params=("spatial_geometry", "spatial_within", "geography_label"),
    parse_contexts=(MAIN_CONTEXT, API_CONTEXT, ORGANIZATION_CONTEXT),
    ui_contexts=(MAIN_CONTEXT, ORGANIZATION_CONTEXT),
    label="Geographic Area",
    renderer="geography",
    api_query_params=(
        ApiQueryParam("spatial_geometry", field_type="json_string"),
        ApiQueryParam("spatial_within", field_type="boolean"),
        ApiQueryParam("geography_label"),
    ),
    parse=_parse_geography,
    to_query_pairs=_to_query_pairs,
    clause_builder=_clause,
    section_builder=_section,
)
