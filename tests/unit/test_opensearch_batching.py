from unittest import TestCase
from unittest.mock import Mock, patch

from harvester.harvest import HarvestSource
from harvester.lib.harvest_timings import HarvestTimings


class TestOpenSearchBatching(TestCase):
    def setUp(self):
        self.source = object.__new__(HarvestSource)
        self.source._opensearch_documents = []
        self.source._opensearch_needs_refresh = False
        self.source._opensearch = Mock()
        self.source._opensearch.index_documents.return_value = (1, 0, [])
        self.source._opensearch_initialized = True
        self.source.timings = HarvestTimings()

    @patch("harvester.harvest.OPENSEARCH_BATCH_SIZE", 3)
    def test_full_batch_is_sent_without_refresh(self):
        for n in range(3):
            self.source.queue_opensearch_document({"id": n})

        self.source.opensearch.index_documents.assert_called_once_with(
            [{"id": 0}, {"id": 1}, {"id": 2}], refresh_after=False
        )
        self.source.opensearch.refresh.assert_not_called()
        self.assertEqual(self.source._opensearch_documents, [])

    def test_finish_sends_partial_batch_and_refreshes_once(self):
        self.source.queue_opensearch_document({"id": 1})
        self.source.opensearch.index_documents.assert_not_called()

        self.source.finish_opensearch_writes()
        self.source.finish_opensearch_writes()

        self.source.opensearch.index_documents.assert_called_once_with(
            [{"id": 1}], refresh_after=False
        )
        self.source.opensearch.refresh.assert_called_once()

    def test_finish_without_changes_skips_refresh(self):
        self.source.finish_opensearch_writes()

        self.source.opensearch.index_documents.assert_not_called()
        self.source.opensearch.refresh.assert_not_called()

    def test_delete_alone_still_refreshes(self):
        self.source.note_opensearch_change()

        self.source.finish_opensearch_writes()

        self.source.opensearch.refresh.assert_called_once()

    def test_bulk_error_is_logged_and_queue_cleared(self):
        self.source.opensearch.index_documents.side_effect = RuntimeError("down")
        self.source.queue_opensearch_document({"id": 1})

        with self.assertLogs("harvest_runner", level="ERROR"):
            self.source.flush_opensearch_documents()

        self.assertEqual(self.source._opensearch_documents, [])
