from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from search.queries.criteria import SearchCriteria


DEFAULT_SEARCH_PER_PAGE = 20
# TODO: data_access should have no knowledge of what's consuming it.
# these route prefixes should be supplied by the flask app but currently
# filter defintions rely on these values. maybe this is an exception.
MAIN_CONTEXT = "main"
API_CONTEXT = "api"
ORGANIZATION_CONTEXT = "organization"


class FilterParseError(ValueError):
    def __init__(self, parameter: str, message: str):
        self.parameter = parameter
        self.message = message
        super().__init__(message)


@dataclass(frozen=True)
class FilterOption:
    value: str
    label: str
    input_id: str


@dataclass(frozen=True)
class ApiQueryParam:
    name: str
    repeated: bool = False
    field_type: str = "string"
    enum_values: tuple[str, ...] = ()


ClauseBuilder = Callable[
    ["SearchCriteria", Any],
    dict | list[dict] | None,
]
Parser = Callable[[Any], Any]
AggregationBuilder = Callable[["SearchCriteria"], dict | None]
AggregationParser = Callable[[Mapping[str, Any]], list[dict]]
SectionBuilder = Callable[["SearchCriteria", Mapping[str, Any]], dict]


@dataclass(frozen=True)
class FilterDefinition:
    name: str
    query_params: tuple[str, ...]
    parse_contexts: tuple[str, ...]
    ui_contexts: tuple[str, ...] = ()
    label: str | None = None
    renderer: str | None = None
    options: tuple[FilterOption, ...] = ()
    api_query_params: tuple[ApiQueryParam, ...] = ()
    parse: Parser = lambda args: None
    to_query_pairs: Callable[[Any], list[tuple[str, str]]] = lambda value: []
    is_active: Callable[[Any], bool] = lambda value: value not in (None, "", [])
    clause_builder: ClauseBuilder | None = None
    aggregation_name: str | None = None
    aggregation_result_key: str | None = None
    aggregation_builder: AggregationBuilder | None = None
    aggregation_parser: AggregationParser | None = None
    section_builder: SectionBuilder | None = None


def get_value(args, name: str, default: Any = None) -> Any:
    if hasattr(args, "get"):
        return args.get(name, default)
    return default


def get_list(args, name: str) -> list[str]:
    if hasattr(args, "getlist"):
        return [value for value in args.getlist(name) if value not in (None, "")]
    value = get_value(args, name)
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [item for item in value if item not in (None, "")]
    return [value]


def get_int(args, name: str, default: int) -> int:
    value = get_value(args, name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_bool_param(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on", "within"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", "intersect", "intersects"}:
        return False
    return default


def parse_string(args, name: str) -> str | None:
    value = (get_value(args, name, None) or "").strip()
    return value or None


def selection_summary(values: list[str] | None) -> str | None:
    values = values or []
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return f"{len(values)} selected"
