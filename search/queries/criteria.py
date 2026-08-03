from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from search.queries.filters.base import (
    DEFAULT_SEARCH_PER_PAGE,
    MAIN_CONTEXT,
    get_int,
    get_value,
)


@dataclass
class SearchCriteria:
    query: str = ""
    per_page: int = DEFAULT_SEARCH_PER_PAGE
    sort_by: str = "relevance"
    after: str | None = None
    results_hint: int = 0
    from_hint: str | None = None
    include_aggregations: bool = False
    keyword_size: int = 100
    org_size: int = 100
    publisher_size: int = 100
    filters: dict[str, Any] = field(default_factory=dict)
    resolved_filters: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_request_args(
        cls,
        args,
        *,
        route_context: str = MAIN_CONTEXT,
        per_page_name: str = "results",
        default_per_page: int = DEFAULT_SEARCH_PER_PAGE,
        max_per_page: int | None = None,
        include_aggregations: bool = False,
        keyword_size: int = 100,
        org_size: int = 100,
        publisher_size: int = 100,
        strip_query: bool = False,
    ) -> "SearchCriteria":
        from search.queries.registry import FILTERS

        query = get_value(args, "q", "") or ""
        if strip_query:
            query = query.strip()

        per_page = get_int(args, per_page_name, default_per_page)
        if max_per_page is not None:
            per_page = min(per_page, max_per_page)

        filters = {}
        for definition in FILTERS:
            if route_context not in definition.parse_contexts:
                continue
            value = definition.parse(args)
            if definition.is_active(value):
                filters[definition.name] = value

        return cls(
            query=query,
            per_page=per_page,
            sort_by=get_value(args, "sort", "relevance") or "relevance",
            after=get_value(args, "after"),
            results_hint=get_int(args, "results", 0),
            from_hint=get_value(args, "from_hint"),
            include_aggregations=include_aggregations,
            keyword_size=keyword_size,
            org_size=org_size,
            publisher_size=publisher_size,
            filters=filters,
        )

    @classmethod
    def from_values(
        cls,
        *,
        query: str = "",
        per_page: int = DEFAULT_SEARCH_PER_PAGE,
        after: str | None = None,
        sort_by: str = "relevance",
        include_aggregations: bool = False,
        keyword_size: int = 100,
        org_size: int = 100,
        publisher_size: int = 100,
        filters: dict[str, Any] | None = None,
    ) -> "SearchCriteria":
        return cls(
            query=query,
            per_page=per_page,
            sort_by=sort_by,
            after=after,
            include_aggregations=include_aggregations,
            keyword_size=keyword_size,
            org_size=org_size,
            publisher_size=publisher_size,
            filters=filters or {},
        )

    def get_filter(self, name: str, default: Any = None) -> Any:
        return self.filters.get(name, default)

    def get_resolved_filter(self, name: str, default: Any = None) -> Any:
        return self.resolved_filters.get(name, self.filters.get(name, default))

    def set_resolved_filter(self, name: str, value: Any) -> None:
        if value is None or value == "" or value == []:
            self.resolved_filters.pop(name, None)
            return
        self.resolved_filters[name] = value

    def get_geography(self) -> dict[str, Any]:
        geography = self.get_filter("geography")
        return geography if isinstance(geography, dict) else {}

    def get_spatial_geometry(self) -> dict | None:
        return self.get_geography().get("geometry")

    def has_active_filters(self, *, include_query: bool = False) -> bool:
        from search.queries.registry import FILTERS

        active_filter = any(
            definition.is_active(self.get_filter(definition.name))
            for definition in FILTERS
        )
        return active_filter or bool(include_query and self.query)

    def to_query_pairs(
        self,
        *,
        include_query: bool = True,
        include_sort: bool = True,
        include_after: bool = False,
        include_results: bool = False,
        exclude: set[str] | None = None,
    ) -> list[tuple[str, str]]:
        from search.queries.registry import FILTERS

        exclude = exclude or set()
        pairs: list[tuple[str, str]] = []
        if include_query and "q" not in exclude:
            pairs.append(("q", self.query))
        if include_sort and "sort" not in exclude:
            pairs.append(("sort", self.sort_by))
        if include_after and self.after and "after" not in exclude:
            pairs.append(("after", self.after))
        if include_results and self.results_hint and "results" not in exclude:
            pairs.append(("results", str(self.results_hint)))

        for definition in FILTERS:
            value = self.get_filter(definition.name)
            if not definition.is_active(value):
                continue
            for name, raw in definition.to_query_pairs(value):
                if name not in exclude:
                    pairs.append((name, raw))
        return pairs

    def to_query_dict(self, **kwargs) -> dict[str, Any]:
        grouped: dict[str, Any] = {}
        for name, value in self.to_query_pairs(**kwargs):
            if name in grouped:
                existing = grouped[name]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    grouped[name] = [existing, value]
            else:
                grouped[name] = value
        return grouped
