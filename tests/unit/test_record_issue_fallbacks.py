from unittest import TestCase
from unittest.mock import Mock

from database.interface import HarvesterDBInterface
from harvester.harvest import Record


def _rows(*messages):
    return [
        {
            "message": message,
            "type": "TestException",
            "harvest_job_id": "job",
            "harvest_record_id": "record",
            "severity": "warning",
        }
        for message in messages
    ]


class TestAddHarvestRecordErrorsFallback(TestCase):
    def setUp(self):
        self.db = Mock()
        self.interface = HarvesterDBInterface(session=self.db)
        self.added = []
        self.db.add_all.side_effect = lambda errors: self.added.extend(errors)
        self.db.add.side_effect = self.added.append

    def fail_commits_with(self, bad_message):
        def commit():
            pending, self.added[:] = list(self.added), []
            if any(e.message == bad_message for e in pending):
                raise ValueError("bad row")

        self.db.commit.side_effect = commit

    def test_failed_batch_retries_each_row(self):
        self.fail_commits_with("bad")

        stored = self.interface.add_harvest_record_errors(
            _rows("first", "bad", "third"), mirror=False
        )

        self.assertEqual([row["message"] for row in stored], ["first", "third"])
        self.assertEqual(self.db.rollback.call_count, 2)

    def test_only_stored_rows_are_mirrored(self):
        self.fail_commits_with("bad")
        self.interface.mirror_current_record_issues = Mock()

        stored = self.interface.add_harvest_record_errors(_rows("ok", "bad"))

        self.interface.mirror_current_record_issues.assert_called_once_with(stored)
        self.assertEqual([row["message"] for row in stored], ["ok"])


class TestSyncCurrentIssuesFallback(TestCase):
    def setUp(self):
        self.source = Mock()
        self.record = Record(self.source, "identifier")
        self.record.id = "record"
        self.record._unmirrored_issues = _rows("a warning")
        self.db = self.source.db_interface

    def test_failed_resync_mirrors_the_new_issues(self):
        self.db.sync_current_record_issues.return_value = False

        self.record.sync_current_issues()

        self.db.mirror_current_record_issues.assert_called_once_with(_rows("a warning"))
        self.assertEqual(self.record._unmirrored_issues, [])

    def test_successful_resync_skips_the_mirror(self):
        self.db.sync_current_record_issues.return_value = True

        self.record.sync_current_issues()

        self.db.mirror_current_record_issues.assert_not_called()
