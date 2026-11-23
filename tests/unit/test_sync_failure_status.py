from unittest import TestCase
from unittest.mock import MagicMock, patch

from harvester.harvest import Record


class TestSyncFailureStatus(TestCase):
    def setUp(self):
        self.source = MagicMock()
        self.source.job_type = "harvest"
        self.record = Record(self.source, "identifier", "{}")
        self.record.id = "record-id"
        self.record.action = "create"
        self.record.dataset_slug = "a-slug"
        self.db = self.source.db_interface

    def written_statuses(self):
        return [
            c.args[1]["status"] for c in self.db.update_harvest_record.call_args_list
        ]

    def test_failed_dataset_write_leaves_record_pending(self):
        with (
            patch.object(Record, "_metadata_for_dataset", return_value={"title": "t"}),
            patch.object(Record, "_dataset_payload", return_value={}),
            patch.object(
                Record, "_insert_dataset_with_unique_slug", side_effect=RuntimeError
            ),
        ):
            self.assertFalse(self.record.sync())

        self.assertEqual(self.written_statuses(), ["dataset_pending"])
        written = self.db.update_harvest_record.call_args.args[1]
        self.assertIsNotNone(written["date_finished"])
        self.assertEqual(self.record.status, "error")

    def test_failure_before_the_dataset_step_writes_nothing(self):
        with patch.object(
            Record, "_metadata_for_dataset", side_effect=RuntimeError("bad json")
        ):
            self.assertFalse(self.record.sync())

        self.assertEqual(self.written_statuses(), [])
