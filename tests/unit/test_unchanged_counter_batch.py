from unittest import TestCase
from unittest.mock import Mock, patch

from harvester.harvest import HarvestSource
from harvester.lib.harvest_reporter import HarvestReporter


class TestUnchangedCounterBatch(TestCase):
    def setUp(self):
        self.source = object.__new__(HarvestSource)
        self.source._job_id = "job-id"
        self.source._reporter = HarvestReporter()
        self.source._db_interface = Mock()
        self.source._pending_unchanged_count = 0
        self.source._pending_unchanged_since = None

    def test_unchanged_records_write_once_per_batch(self):
        with patch("harvester.harvest.time.monotonic", return_value=0):
            for _ in range(99):
                self.source.update_job_record_count_by_action(None)
            self.source.db_interface.update_harvest_job.assert_not_called()

            self.source.update_job_record_count_by_action(None)

        self.source.db_interface.update_harvest_job.assert_called_once()
        self.assertEqual(
            self.source.db_interface.update_harvest_job.call_args.args[1][
                "records_ignored"
            ],
            100,
        )

    def test_elapsed_time_flushes_partial_batch(self):
        with patch("harvester.harvest.time.monotonic", return_value=0) as clock:
            self.source.update_job_record_count_by_action(None)
            clock.return_value = 2
            self.source.update_job_record_count_by_action(None)

        self.source.db_interface.update_harvest_job.assert_called_once()
        self.assertEqual(self.source.reporter.ignored, 2)

    def test_other_action_persists_pending_unchanged_count(self):
        with patch("harvester.harvest.time.monotonic", return_value=0):
            self.source.update_job_record_count_by_action(None)
            self.source.update_job_record_count_by_action("create")

        self.source.db_interface.update_harvest_job.assert_called_once()
        report = self.source.db_interface.update_harvest_job.call_args.args[1]
        self.assertEqual(report["records_ignored"], 1)
        self.assertEqual(report["records_added"], 1)
        self.assertEqual(self.source._pending_unchanged_count, 0)

    def test_final_flush_persists_partial_batch(self):
        with patch("harvester.harvest.time.monotonic", return_value=0):
            self.source.update_job_record_count_by_action(None)

        self.source.flush_pending_unchanged_counts()
        self.source.flush_pending_unchanged_counts()

        self.source.db_interface.update_harvest_job.assert_called_once()
        self.assertEqual(self.source._pending_unchanged_count, 0)

    def test_harvest_loop_flushes_last_partial_batch(self):
        source = self.source
        source._job_type = "harvest"
        source.external_records = []
        source.external_records_by_type = {}
        source.deletions = set()
        source.source_type = "document"
        source.acquire_data_sources = Mock()
        source.filter_datasets_with_no_identifier = Mock()
        source.determine_internal_deletions = Mock()
        source.iter_internal_records_to_be_deleted = Mock(return_value=iter(()))
        source.filter_duplicate_identifiers = Mock()
        record = Mock()
        record.harvest.side_effect = lambda: source.update_job_record_count_by_action(
            None
        )
        source.external_records_to_process = Mock(return_value=iter((record,)))

        with patch("harvester.harvest.time.monotonic", return_value=0):
            source.run_full_harvest()

        source.db_interface.update_harvest_job.assert_called_once()
        self.assertEqual(source.reporter.ignored, 1)
