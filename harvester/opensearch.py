import logging
import os
import time
from datetime import date, datetime
from typing import Any, Callable, TypeVar

from botocore.credentials import Credentials
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection, helpers
from opensearchpy.exceptions import ConnectionTimeout

logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT_RETRIES = 3
DEFAULT_TIMEOUT_BACKOFF_BASE = 2.0
DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_CLIENT_MAX_RETRIES = 3


T = TypeVar("T")


class OpenSearchInterface:
    INDEX_NAME = "datasets"
    TEXT_ANALYZER = "datagov_text"
    STOP_FILTER = "datagov_stop"

    SETTINGS = {
        "analysis": {
            "filter": {
                STOP_FILTER: {
                    "type": "stop",
                    "stopwords": "_english_",
                }
            },
            "analyzer": {
                TEXT_ANALYZER: {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", STOP_FILTER],
                }
            },
        }
    }

    MAPPINGS = {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": TEXT_ANALYZER,
                "search_analyzer": TEXT_ANALYZER,
            },
            "slug": {"type": "keyword"},
            "last_harvested_date": {"type": "date"},
            "dcat": {
                "type": "nested",
                "properties": {
                    "modified": {"type": "keyword"},
                    "issued": {"type": "keyword"},
                },
            },
            "description": {
                "type": "text",
                "analyzer": TEXT_ANALYZER,
                "search_analyzer": TEXT_ANALYZER,
            },
            "publisher": {
                "type": "text",
                "analyzer": TEXT_ANALYZER,
                "search_analyzer": TEXT_ANALYZER,
            },
            "keyword": {
                "type": "text",
                "analyzer": TEXT_ANALYZER,
                "search_analyzer": TEXT_ANALYZER,
                "fields": {"raw": {"type": "keyword"}},
            },
            "theme": {
                "type": "text",
                "analyzer": TEXT_ANALYZER,
                "search_analyzer": TEXT_ANALYZER,
            },
            "identifier": {
                "type": "text",
                "analyzer": TEXT_ANALYZER,
                "search_analyzer": TEXT_ANALYZER,
            },
            "has_spatial": {"type": "boolean"},
            "popularity": {"type": "integer"},
            "organization": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": TEXT_ANALYZER,
                        "search_analyzer": TEXT_ANALYZER,
                    },
                    "description": {
                        "type": "text",
                        "analyzer": TEXT_ANALYZER,
                        "search_analyzer": TEXT_ANALYZER,
                    },
                    "slug": {"type": "keyword"},
                    "organization_type": {"type": "keyword"},
                },
            },
            "spatial_shape": {"type": "geo_shape"},
            "spatial_centroid": {"type": "geo_point"},
        }
    }

    @staticmethod
    def _create_test_opensearch_client(host):
        """Get an OpenSearch client instance configured for our test cluster."""
        return OpenSearch(
            hosts=[{"host": host, "port": 9200}],
            http_compress=True,
            http_auth=("admin", "admin"),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=10,
            max_retries=DEFAULT_CLIENT_MAX_RETRIES,
            retry_on_timeout=True,
        )

    @staticmethod
    def _create_aws_opensearch_client(host):
        """Get an OpenSearch client instance configured for an AWS cluster."""
        access_key = os.getenv("OPENSEARCH_ACCESS_KEY")
        secret_key = os.getenv("OPENSEARCH_SECRET_KEY")
        auth = AWSV4SignerAuth(
            Credentials(access_key=access_key, secret_key=secret_key),
            "us-gov-west-1",
            "es",
        )
        return OpenSearch(
            hosts=[{"host": host, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
            timeout=60,
            max_retries=DEFAULT_CLIENT_MAX_RETRIES,
            retry_on_timeout=True,
        )

    def _ensure_index(self):
        """Ensure that the named index exists."""
        if not self.client.indices.exists(index=self.INDEX_NAME):
            body = {"mappings": self.MAPPINGS}
            if self.SETTINGS:
                body["settings"] = self.SETTINGS
            self.client.indices.create(index=self.INDEX_NAME, body=body)

    @classmethod
    def from_environment(cls):
        """Factory method to return a best-guess instance from environment variables."""
        opensearch_host = os.getenv("OPENSEARCH_HOST")
        if not opensearch_host:
            raise ValueError("OPENSEARCH_HOST is not set")
        if opensearch_host.endswith("es.amazonaws.com"):
            return cls(aws_host=opensearch_host)
        return cls(test_host=opensearch_host)

    def __init__(self, test_host=None, aws_host=None):
        """Interface for our OpenSearch cluster."""
        if aws_host is not None:
            if test_host is not None:
                raise ValueError("Cannot specify both test_host and aws_host")
            self.client = self._create_aws_opensearch_client(aws_host)
        else:
            if test_host is not None:
                self.client = self._create_test_opensearch_client(test_host)
            else:
                raise ValueError("Must specify either test_host or aws_host")

        self._ensure_index()

    @staticmethod
    def _normalize_dcat_dates(dcat: dict) -> dict:
        """Normalize date fields in DCAT to ensure they're always strings."""
        normalized_dcat = dcat.copy()
        date_fields = ["modified", "issued", "temporal"]
        for field in date_fields:
            if field in normalized_dcat:
                value = normalized_dcat[field]
                if isinstance(value, (datetime, date)):
                    normalized_dcat[field] = value.isoformat()
                elif value is not None and not isinstance(value, str):
                    normalized_dcat[field] = str(value)
        return normalized_dcat

    @staticmethod
    def _geometry_centroid(geometry: Any) -> dict | None:
        """Calculate the centroid of a geometry."""
        if not isinstance(geometry, dict):
            return None

        coords = geometry.get("coordinates")
        if coords is None:
            return None

        points: list[tuple[float, float]] = []

        def _add_point(value: Any) -> bool:
            if not isinstance(value, (list, tuple)):
                return False
            if len(value) < 2:
                return False
            lon, lat = value[0], value[1]
            if not isinstance(lon, (int, float)) or not isinstance(lat, (int, float)):
                return False
            points.append((float(lon), float(lat)))
            return True

        def _walk(value: Any) -> None:
            if _add_point(value):
                return
            if isinstance(value, (list, tuple)):
                for item in value:
                    _walk(item)

        _walk(coords)

        if not points:
            return None

        lon_total = sum(point[0] for point in points)
        lat_total = sum(point[1] for point in points)
        count = len(points)
        return {"lat": lat_total / count, "lon": lon_total / count}

    def dataset_to_document(self, dataset):
        """Map a dataset into a document for indexing."""
        spatial_value = dataset.dcat.get("spatial")
        has_spatial = bool(spatial_value and str(spatial_value).strip()) or (
            dataset.translated_spatial is not None
        )
        normalized_dcat = self._normalize_dcat_dates(dataset.dcat)
        spatial_centroid = self._geometry_centroid(dataset.translated_spatial)
        last_harvested = (
            dataset.last_harvested_date.isoformat()
            if dataset.last_harvested_date
            else None
        )
        organization = dataset.organization.to_dict() if dataset.organization else {}

        return {
            "_index": self.INDEX_NAME,
            "_id": dataset.id,
            "title": dataset.dcat.get("title", ""),
            "slug": dataset.slug,
            "last_harvested_date": last_harvested,
            "description": dataset.dcat.get("description", ""),
            "publisher": dataset.dcat.get("publisher", {}).get("name", ""),
            "dcat": normalized_dcat,
            "keyword": dataset.dcat.get("keyword", []),
            "theme": dataset.dcat.get("theme", []),
            "identifier": dataset.dcat.get("identifier", ""),
            "has_spatial": has_spatial,
            "organization": organization,
            "popularity": dataset.popularity if dataset.popularity is not None else None,
            "spatial_shape": dataset.translated_spatial,
            "spatial_centroid": spatial_centroid,
            "harvest_record": self._create_harvest_record_url(dataset),
        }

    @staticmethod
    def _create_harvest_record_url(dataset) -> str | None:
        if not getattr(dataset, "harvest_record_id", None):
            return None
        return f"/harvest_record/{dataset.harvest_record_id}"

    def _run_with_timeout_retry(
        self,
        action: Callable[[], T],
        *,
        action_name: str,
        timeout_retries: int,
        timeout_backoff_base: float,
    ) -> T:
        attempt = 0

        while True:
            try:
                return action()
            except ConnectionTimeout as exc:
                attempt += 1
                if attempt > timeout_retries:
                    logger.error(
                        "%s timed out after %s retries; giving up.",
                        action_name,
                        timeout_retries,
                        exc_info=exc,
                    )
                    raise

                wait_seconds = min(timeout_backoff_base**attempt, 60)
                logger.warning(
                    "%s timed out (attempt %s/%s); retrying in %.1f seconds.",
                    action_name,
                    attempt,
                    timeout_retries,
                    wait_seconds,
                    exc_info=exc,
                )
                time.sleep(wait_seconds)

    def _refresh(
        self,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
        request_timeout: int = DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS,
    ):
        def _do_refresh():
            return self.client.indices.refresh(
                index=self.INDEX_NAME, request_timeout=request_timeout
            )

        self._run_with_timeout_retry(
            _do_refresh,
            action_name="OpenSearch refresh",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )

    def index_datasets(
        self,
        dataset_iter,
        refresh_after=True,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
    ):
        """Index an iterator of dataset objects into OpenSearch."""
        datasets = getattr(dataset_iter, "items", dataset_iter)
        documents = [self.dataset_to_document(dataset) for dataset in datasets]

        def _stream_bulk():
            succeeded_local = 0
            failed_local = 0
            errors = []
            for success, item in helpers.streaming_bulk(
                self.client,
                documents,
                raise_on_error=False,
                max_retries=8,
            ):
                index_info = item.get("index")
                index_error = index_info.get("error")
                if success:
                    succeeded_local += 1
                    if item["index"]["result"].lower() not in ["created", "updated"]:
                        if index_info:
                            errors.append(
                                {
                                    "dataset_id": index_info.get("_id"),
                                    "status_code": index_info["_shards"].get("status"),
                                    "error_type": "Silent Error",
                                    "error_reason": "Unknown",
                                    "caused_by": index_info,
                                }
                            )
                else:
                    failed_local += 1
                    if index_info and index_error:
                        errors.append(
                            {
                                "dataset_id": index_info.get("_id"),
                                "status_code": index_info.get("status"),
                                "error_type": index_error.get("type"),
                                "error_reason": index_error.get("reason"),
                                "caused_by": index_error.get("caused_by"),
                            }
                        )
                    errors.append(item)
            return succeeded_local, failed_local, errors

        succeeded, failed, errors = self._run_with_timeout_retry(
            _stream_bulk,
            action_name="OpenSearch bulk index",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )

        if refresh_after:
            self._refresh()

        return (succeeded, failed, errors)

    def delete_dataset_by_id(
        self,
        dataset_id: str,
        refresh_after: bool = True,
        timeout_retries: int = DEFAULT_TIMEOUT_RETRIES,
        timeout_backoff_base: float = DEFAULT_TIMEOUT_BACKOFF_BASE,
    ) -> bool:
        if not dataset_id:
            return False

        def _do_delete():
            return self.client.delete(
                index=self.INDEX_NAME,
                id=dataset_id,
                ignore=[404],
                request_timeout=DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS,
            )

        self._run_with_timeout_retry(
            _do_delete,
            action_name="OpenSearch delete",
            timeout_retries=timeout_retries,
            timeout_backoff_base=timeout_backoff_base,
        )

        if refresh_after:
            self._refresh()

        return True
