from harvester.opensearch import OpenSearchReader


def test_scan_index_yields_indexed_document_ids(opensearch_writer):
    """Cover the one reader method the harvester uses (app/commands/search.py)."""
    reader = OpenSearchReader(opensearch_writer.wrapper_client)
    document_ids = {"scan-doc-1", "scan-doc-2"}

    try:
        for document_id in document_ids:
            opensearch_writer.client.index(
                index=opensearch_writer.INDEX_NAME,
                id=document_id,
                body={"publisher": "Scan Test"},
                refresh=True,
            )

        scanned = {hit["_id"] for hit in reader.scan_index(reader.INDEX_NAME, size=10)}

        assert document_ids <= scanned
    finally:
        for document_id in document_ids:
            opensearch_writer.client.delete(
                index=opensearch_writer.INDEX_NAME,
                id=document_id,
                ignore=[404],
                refresh=True,
            )
