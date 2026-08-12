from unittest.mock import MagicMock, patch

from harvester.delete_source import delete_source
from harvester.lib.source_delete import (
    DELETE_IN_PROGRESS_MESSAGE,
    enqueue_harvest_source_delete,
)


class TestEnqueueHarvestSourceDelete:
    def test_schedules_task_when_deletable(self):
        db = MagicMock()
        db.can_delete_harvest_source.return_value = (True, None, 200)
        handler = MagicMock()
        source_id = "2f2652de-91df-4c63-8b53-bfced20b276b"

        with patch(
            "harvester.lib.source_delete.create_task_handler", return_value=handler
        ):
            message, status = enqueue_harvest_source_delete(source_id, db)

        assert status == 202
        assert message == DELETE_IN_PROGRESS_MESSAGE
        db.delete_harvest_source.assert_not_called()
        handler.start_task.assert_called_once_with(
            command=f"python harvester/delete_source.py {source_id}",
            task_id=f"delete-harvest-source-{source_id}",
        )

    def test_returns_409_without_scheduling(self):
        db = MagicMock()
        db.can_delete_harvest_source.return_value = (
            False,
            "Failed: 2 records in the Harvest source, please clear it first.",
            409,
        )
        handler = MagicMock()

        with patch(
            "harvester.lib.source_delete.create_task_handler", return_value=handler
        ):
            message, status = enqueue_harvest_source_delete("abc", db)

        assert status == 409
        assert "clear it first" in message
        handler.start_task.assert_not_called()

    def test_returns_404_without_scheduling(self):
        db = MagicMock()
        db.can_delete_harvest_source.return_value = (
            False,
            "Harvest source not found",
            404,
        )
        handler = MagicMock()

        with patch(
            "harvester.lib.source_delete.create_task_handler", return_value=handler
        ):
            message, status = enqueue_harvest_source_delete("missing", db)

        assert (message, status) == ("Harvest source not found", 404)
        handler.start_task.assert_not_called()

    def test_returns_500_when_task_start_fails(self):
        db = MagicMock()
        db.can_delete_harvest_source.return_value = (True, None, 200)
        handler = MagicMock()
        handler.start_task.side_effect = RuntimeError("cf boom")

        with patch(
            "harvester.lib.source_delete.create_task_handler", return_value=handler
        ):
            message, status = enqueue_harvest_source_delete("abc", db)

        assert status == 500
        assert "Failed to schedule harvest source delete" in message


class TestDeleteSourceEntrypoint:
    def test_exits_zero_on_success(self):
        db = MagicMock()
        db.delete_harvest_source.return_value = ("Deleted", 200)

        with patch("harvester.delete_source.db_interface", db):
            assert delete_source("abc") == 0

        db.close.assert_called_once()

    def test_exits_nonzero_on_precheck_failure(self):
        db = MagicMock()
        db.delete_harvest_source.return_value = ("Harvest source not found", 404)

        with patch("harvester.delete_source.db_interface", db):
            assert delete_source("missing") == 1

    def test_exits_nonzero_on_exception(self):
        db = MagicMock()
        db.delete_harvest_source.side_effect = RuntimeError("boom")

        with patch("harvester.delete_source.db_interface", db):
            assert delete_source("abc") == 1

        db.close.assert_called_once()
