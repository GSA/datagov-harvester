import base64
import json
import logging
from dataclasses import dataclass

from opensearchpy.helpers import scan

from search.client import OpenSearchClient
from search.config import INDEX_NAME
from search.queries import (
    SearchCriteria,
    build_aggregation_specs,
    build_base_query,
    build_document_by_slug_query,
    build_filter_clauses,
    build_ispartof_query,
    build_last_harvested_stats_query,
    build_organization_counts_query,
    build_publisher_counts_query,
    build_search_body_query,
    build_search_filter_body_query,
    build_unique_keywords_query,
    parse_filter_aggregations,
)
from search.spatial import calc_distance_km, calc_geometry_centroid

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    total: int
    results: list[dict]
    search_after: list
    aggregations: dict | None = None

    def __len__(self):
        """Length of this is the length of results."""
        return len(self.results)

    @classmethod
    def empty(cls):
        """Return an empty search result instance."""
        return cls(total=0, results=[], search_after=None, aggregations=None)

    @classmethod
    def from_opensearch_result(cls, result_dict: dict, per_page_hint=0):
        """Make a results object from the result of an OpenSearch query.

        To know if we should give a `search_after` in the result, we need
        a hint for how many results "should" have been on the page and if
        there is more than that in this result, then we should give a
        value for search_after.

        In the `search` method we asked for one more than the per_page size
        to determine if there will be any more results left for another call.

        When the search body included aggregation "clauses", the parsed
        `aggregations` dict will be populated on the returned instance with
        `keywords`, `organizations`, and `publishers` lists.
        """

        total = result_dict["hits"]["total"]["value"]
        hits = result_dict["hits"]["hits"]
        results = [
            {
                **each["_source"],
                "_score": each.get("_score"),
                "_sort": each.get("sort"),
            }
            for each in hits
        ]
        if per_page_hint:
            if len(results) > per_page_hint:
                # more results than we need to return, there will be results if we
                # use search_after from the last result we return
                search_after = hits[per_page_hint - 1]["sort"]
                results = results[:per_page_hint]
            else:
                # no extra results, so no further search results
                # return everything and None for search_after
                search_after = None
        else:
            # no page size hint
            if hits:
                # return everything we have and the search_after from the last
                # result
                search_after = hits[-1]["sort"]
            else:
                # no results in the list
                search_after = None

        aggregations = parse_filter_aggregations(result_dict.get("aggregations"))

        return cls(
            total=total,
            results=results,
            search_after=search_after,
            aggregations=aggregations,
        )

    def search_after_obscured(self):
        """An encoded string representation of self.search_after.

        If self.search_after is None, don't encode it, just return None.
        """
        if self.search_after is None:
            return None
        return base64.urlsafe_b64encode(
            json.dumps(self.search_after, separators=(",", ":")).encode("utf-8")
        ).decode("utf-8")

    @staticmethod
    def decode_search_after(encoded_after):
        """Decode the encoded representation of self.search_after."""
        return json.loads(base64.urlsafe_b64decode(encoded_after).decode("utf-8"))


class OpenSearchReader:
    INDEX_NAME = INDEX_NAME

    def __init__(self, opensearchclient: OpenSearchClient):
        self.wrapper_client = opensearchclient
        self.client = self.wrapper_client.client

    def search(
        self,
        criteria: SearchCriteria,
        search_after: list = None,
    ) -> SearchResult:
        """Search our index for a query string.

        We use OpenSearch's multi-match to match our single query string
        against many fields. We use the "boost" numbers to score some fields
        higher than others.

        Supports:
        - Phrase search with quotes: "poor food" matches exact phrase
        - OR operator: food OR health
        - Combined: "poor food" OR health or "poor food" OR "electric vehicle"

        Search filters are supplied through `criteria`.

        We pass the `after` argument through to OpenSearch. It should be the
        value of the last `_sort` field from a previous search result with the
        same query.

        When include_aggregations is True, keyword and organization
        aggregations are embedded in the same request and returned via
        `SearchResult.aggregations`.
        """
        query = criteria.query
        per_page = criteria.per_page
        sort_by = criteria.sort_by
        include_aggregations = criteria.include_aggregations
        spatial_geometry = criteria.get_spatial_geometry()

        # Parse query for phrases and OR operators
        base_query = build_base_query(query)

        # compute centroid only if spatial_geometry provided (used for distance calc)
        distance_point = (
            calc_geometry_centroid(spatial_geometry)
            if spatial_geometry is not None
            else None
        )
        # normalize requested sort and pick sort_point only when appropriate
        normalized_sort = (sort_by or "relevance").lower()
        sort_point = (
            distance_point
            if (normalized_sort == "distance" and distance_point is not None)
            else None
        )
        # if user requested distance sorting but we don't have a point,
        # fall back to relevance
        if normalized_sort == "distance" and sort_point is None:
            sort_by = "relevance"

        search_body = build_search_body_query(base_query, sort_by, sort_point, per_page)

        filters = build_filter_clauses(criteria)

        # Apply filters if any exist
        if filters:
            search_body["query"] = build_search_filter_body_query(base_query, filters)

        if search_after is not None:
            search_body["search_after"] = search_after

        # `keyword`, `organization`, and `publisher` aggregations for the chips
        if include_aggregations:
            search_body["aggs"] = build_aggregation_specs(criteria)

        # print("QUERY:", search_body)
        result_dict = self.client.search(index=self.INDEX_NAME, body=search_body)
        result = SearchResult.from_opensearch_result(
            result_dict, per_page_hint=per_page
        )
        if distance_point:
            for item in result.results:
                spatial_centroid = item.get("spatial_centroid")
                distance_km = calc_distance_km(distance_point, spatial_centroid)
                if distance_km is not None:
                    item["_distance_km"] = distance_km
        return result

    def get_document_by_slug(self, slug_or_id: str) -> list[dict]:
        """
        get document by slug name or dataset id. only gets the first
        matching document and omits scoring.
        """

        query = build_document_by_slug_query(slug_or_id)

        result_dict = self.client.search(index=self.INDEX_NAME, body=query)

        return SearchResult.from_opensearch_result(result_dict, per_page_hint=1)

    def get_unique_keywords(self, size=100, min_doc_count=1, search=None) -> list[dict]:
        """
        Get unique keywords from all datasets with their document counts.

        Keywords are aggregated on the normalized (lowercased) sub-field so
        that case variants such as "Environment" and "environment" are counted
        as a single bucket. The returned ``keyword`` value is the lowercased
        canonical form.
        """
        query = build_unique_keywords_query(size, min_doc_count, search)

        result = self.client.search(index=self.INDEX_NAME, body=query)
        buckets = (
            result.get("aggregations", {}).get("unique_keywords", {}).get("buckets", [])
        )

        return [
            {"keyword": bucket["key"], "count": bucket["doc_count"]}
            for bucket in buckets
        ]

    def get_organization_counts(
        self, size=100, min_doc_count=1, as_dict=False
    ) -> list[dict]:
        """Aggregate datasets by organization slug to get counts."""
        query = build_organization_counts_query(size, min_doc_count)

        result = self.client.search(index=self.INDEX_NAME, body=query)
        buckets = (
            result.get("aggregations", {})
            .get("organizations", {})
            .get("by_slug", {})
            .get("buckets", [])
        )

        if as_dict:
            output = {}
            for bucket in buckets:
                output[bucket["key"]] = bucket["doc_count"]
            return output

        return [
            {"slug": bucket["key"], "count": bucket["doc_count"]} for bucket in buckets
        ]

    def get_publisher_counts(
        self, size=100, min_doc_count=1, as_dict=False
    ) -> list[dict] | dict[str, int]:
        """Aggregate datasets by publisher name to get counts."""
        query = build_publisher_counts_query(size, min_doc_count)

        result = self.client.search(index=self.INDEX_NAME, body=query)
        buckets = (
            result.get("aggregations", {})
            .get("unique_publishers", {})
            .get("buckets", [])
        )

        if as_dict:
            output = {}
            for bucket in buckets:
                output[bucket["key"]] = bucket["doc_count"]
            return output

        return [
            {"name": bucket["key"], "count": bucket["doc_count"]} for bucket in buckets
        ]

    def get_last_harvested_stats(self):
        query = build_last_harvested_stats_query()

        result = self.client.search(index=self.INDEX_NAME, body=query)
        age_bins = result.get("aggregations", {}).get("age_bins", {}).get("buckets", {})

        return {
            "age_bins": {
                "older": age_bins.get("older", {}).get("doc_count", 0),
                "last_year": age_bins.get("last_year", {}).get("doc_count", 0),
                "last_month": age_bins.get("last_month", {}).get("doc_count", 0),
                "last_week": age_bins.get("last_week", {}).get("doc_count", 0),
            },
        }

    def count_all_datasets(self) -> int:
        """
        Get the total count of all datasets in the index.
        """
        try:
            result = self.client.count(index=self.INDEX_NAME)
            return result.get("count", 0)
        except Exception as e:
            logger.error(f"Error counting datasets in OpenSearch: {e}")
            return 0

    def count_datasets_with_ispartof(self) -> int:
        """
        Get the total count of datasets whose DCAT payload includes isPartOf.
        """

        query = build_ispartof_query()

        try:
            result = self.client.count(
                index=self.INDEX_NAME,
                body=query,
            )
            return result.get("count", 0)
        except Exception as e:
            logger.error(f"Error counting datasets with isPartOf in OpenSearch: {e}")
            return 0

    def scan_index(
        self,
        index_name: str,
        size=200,
        source=False,
        stored_fields=[],
        docvalue_fields=[],
    ):
        return scan(
            self.client,
            index=index_name,
            size=size,
            _source=source,
            stored_fields=stored_fields,
            docvalue_fields=docvalue_fields,
        )
