import os
from urllib.parse import urlparse

from botocore.credentials import Credentials
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

from search.config import (
    DEFAULT_CATALOG_BASE_URL,
    DEFAULT_CLIENT_MAX_RETRIES,
    INDEX_NAME,
    KEYWORD_NORMALIZER,
    SETTINGS,
    STOP_FILTER,
    TEXT_ANALYZER,
)
from search.mappings import MAPPINGS


class OpenSearchClient:
    INDEX_NAME = INDEX_NAME
    MAPPINGS = MAPPINGS
    SETTINGS = SETTINGS
    KEYWORD_NORMALIZER = KEYWORD_NORMALIZER
    TEXT_ANALYZER = TEXT_ANALYZER
    STOP_FILTER = STOP_FILTER
    DEFAULT_CATALOG_BASE_URL = DEFAULT_CATALOG_BASE_URL

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

    @staticmethod
    def _extract_hostname(host_or_url: str) -> str | None:
        """
        Extract a hostname from a value that may be a bare host or a full URL.

        This helps ensure that host-based checks are performed on the actual
        hostname portion, not on an arbitrary string containing a host.
        """
        if not host_or_url:
            return None
        # If a scheme is present, parse as a URL to get the hostname.
        if "://" in host_or_url:
            parsed = urlparse(host_or_url)
            return parsed.hostname
        # Otherwise, treat the value as a bare hostname.
        return host_or_url

    @classmethod
    def from_environment(cls, ensure_index=True):
        """Build a client from the environment.

        Set ``ensure_index=False`` when the caller will create the index itself.
        """
        opensearch_host = os.getenv("OPENSEARCH_HOST")
        if not opensearch_host:
            raise ValueError("OPENSEARCH_HOST is not set")
        parsed_host = cls._extract_hostname(opensearch_host)
        if parsed_host and (
            parsed_host == "es.amazonaws.com"
            or parsed_host.endswith(".es.amazonaws.com")
        ):
            return cls(aws_host=opensearch_host, ensure_index=ensure_index)
        return cls(test_host=opensearch_host, ensure_index=ensure_index)

    def _ensure_index(self):
        """Ensure that the named index exists."""
        if not self.client.indices.exists(index=INDEX_NAME):
            body = {"mappings": MAPPINGS}
            if SETTINGS:
                body["settings"] = SETTINGS
            self.client.indices.create(index=INDEX_NAME, body=body)

    def __init__(self, test_host=None, aws_host=None, ensure_index=True):
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

        if ensure_index:
            self._ensure_index()
