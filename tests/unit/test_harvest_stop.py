import threading
import time
from unittest import TestCase
from unittest.mock import Mock, patch

import harvester.harvest as harvest_module
from harvester.harvest import (
    HarvestSource,
    HarvestStopped,
    _records_until_stopped,
    _stop_on_sigterm,
)
from harvester.lib.harvest_reporter import HarvestReporter


def bare_source():
    source = object.__new__(HarvestSource)
    source._job_id = "job-id"
    source._job_type = "harvest"
    source._reporter = HarvestReporter()
    source._db_interface = Mock()
    source._pending_count = 0
    source._pending_count_since = None
    source._counts_lock = threading.Lock()
    source._opensearch_lock = threading.Lock()
    source._opensearch_documents = []
    source._opensearch_queued_since = None
    source._opensearch_needs_refresh = False
    source._opensearch = Mock()
    source._opensearch.index_documents.return_value = (1, 0, [])
    source._opensearch_initialized = True
    return source


class StopFlagTestCase(TestCase):
    def tearDown(self):
        harvest_module._stop_requested.clear()


class TestStopOnSigterm(StopFlagTestCase):
    def test_handler_only_sets_the_flag(self):
        _stop_on_sigterm(15, None)

        self.assertTrue(harvest_module._stop_requested.is_set())

    def test_no_record_is_pulled_after_a_stop(self):
        pulled = []

        def records():
            for n in range(5):
                pulled.append(n)
                yield n

        it = _records_until_stopped(records())
        self.assertEqual(next(it), 0)
        harvest_module._stop_requested.set()

        with self.assertRaises(HarvestStopped):
            next(it)
        self.assertEqual(pulled, [0])

    def test_single_worker_stops_between_records(self):
        source = bare_source()
        done = []

        def record(n):
            r = Mock()

            def harvest():
                done.append(n)
                if n == 1:
                    harvest_module._stop_requested.set()

            r.harvest.side_effect = harvest
            return r

        with patch.dict("os.environ", {"HARVEST_RECORD_WORKERS": "1"}):
            with self.assertRaises(HarvestStopped):
                source.harvest_records(record(n) for n in range(5))

        self.assertEqual(done, [0, 1])


class TestFinalWrites(StopFlagTestCase):
    def run_harvest(self, source, *records):
        source.external_records = []
        source.external_records_by_type = {}
        source.deletions = set()
        source.source_type = "document"
        source.acquire_data_sources = Mock()
        source.filter_datasets_with_no_identifier = Mock()
        source.determine_internal_deletions = Mock()
        source.iter_internal_records_to_be_deleted = Mock(return_value=iter(()))
        source.filter_duplicate_identifiers = Mock()
        source.external_records_to_process = Mock(return_value=iter(records))
        source.run_full_harvest()

    def test_search_documents_are_sent_when_count_write_fails(self):
        source = bare_source()
        source.db_interface.update_harvest_job.side_effect = RuntimeError("db gone")
        record = Mock()

        def harvest():
            source.update_job_record_count_by_action("create")
            source.queue_opensearch_document({"id": 1})

        record.harvest.side_effect = harvest

        with self.assertLogs("harvest_runner", level="ERROR"):
            self.run_harvest(source, record)

        source.opensearch.index_documents.assert_called_once_with(
            [{"id": 1}], refresh_after=False
        )
        source.opensearch.refresh.assert_called_once()

    def test_stop_skips_later_records_and_still_sends_counts_and_documents(self):
        source = bare_source()
        first, second = Mock(), Mock()

        def harvest():
            source.update_job_record_count_by_action("create")
            source.queue_opensearch_document({"id": 1})
            harvest_module._stop_requested.set()

        first.harvest.side_effect = harvest

        with self.assertRaises(HarvestStopped):
            self.run_harvest(source, first, second)

        second.harvest.assert_not_called()
        self.assertEqual(
            source.db_interface.update_harvest_job.call_args.args[1]["records_added"], 1
        )
        source.opensearch.index_documents.assert_called_once_with(
            [{"id": 1}], refresh_after=False
        )


class TestOpenSearchFlusher(TestCase):
    @patch("harvester.harvest.OPENSEARCH_FLUSH_SECONDS", 0.3)
    def test_due_documents_are_sent_without_a_record_finishing(self):
        source = bare_source()
        stop = source._start_opensearch_flusher()
        try:
            source.queue_opensearch_document({"id": 1})
            # not due yet, so queueing alone doesn't send
            source.opensearch.index_documents.assert_not_called()
            deadline = time.monotonic() + 3
            while (
                not source.opensearch.index_documents.called
                and time.monotonic() < deadline
            ):
                time.sleep(0.05)
        finally:
            stop()

        source.opensearch.index_documents.assert_called_once_with(
            [{"id": 1}], refresh_after=False
        )
