from opensearchpy.helpers import scan

from harvester.opensearch.client import OpenSearchClient
from harvester.opensearch.config import INDEX_NAME


class OpenSearchReader:
    """Read side of the datasets index.

    Only the scan/iterate path lives here. The faceted search, aggregation, and
    count queries that used to sit alongside it are catalog concerns; the
    harvester only ever scans the index (see app/commands/search.py).
    """

    INDEX_NAME = INDEX_NAME

    def __init__(self, opensearchclient: OpenSearchClient):
        self.wrapper_client = opensearchclient
        self.client = self.wrapper_client.client

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
