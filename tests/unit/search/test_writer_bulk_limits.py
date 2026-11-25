from unittest.mock import MagicMock, patch

from search.writer import MAX_BULK_REQUEST_BYTES, OpenSearchWriter


@patch("search.writer.helpers.streaming_bulk", return_value=iter(()))
def test_bulk_requests_are_capped_below_the_aws_body_limit(streaming_bulk):
    writer = OpenSearchWriter(MagicMock())

    writer.index_documents([{"_id": "1"}], refresh_after=False)

    assert streaming_bulk.call_args.kwargs["max_chunk_bytes"] == MAX_BULK_REQUEST_BYTES
    assert MAX_BULK_REQUEST_BYTES < 10 * 1024 * 1024
