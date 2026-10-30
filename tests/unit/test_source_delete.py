from unittest.mock import MagicMock, patch

from harvester.archive_source import archive_source_command
from harvester.lib.source_delete import (
    ARCHIVE_IN_PROGRESS_MESSAGE,
    enqueue_harvest_source_archive,
)


class TestEnqueueHarvestSourceArchive:
    def test_schedules_task_when_archivable(self):
        db = MagicMock()
        db.can_archive_harvest_source.return_value = (True, None, 200)
        handler = MagicMock()
        source_id = "2f2652de-91df-4c63-8b53-bfced20b276b"
        user_email = "test@example.com"

        with patch(
            "harvester.lib.source_delete._task_handler_for_archive",
            return_value=handler,
        ):
            message, status = enqueue_harvest_source_archive(source_id, user_email, db)

        assert status == 202
        assert message == ARCHIVE_IN_PROGRESS_MESSAGE
        db.archive_harvest_source.assert_not_called()
        handler.start_task.assert_called_once_with(
            command=f"python -m harvester.archive_source {source_id} {user_email}",
            task_id=f"archive-harvest-source-{source_id}",
        )

    def test_returns_409_without_scheduling(self):
        db = MagicMock()
        db.can_archive_harvest_source.return_value = (
            False,
            "Failed: 2 records in the Harvest source, please clear it first.",
            409,
        )
        handler = MagicMock()

        with patch(
            "harvester.lib.source_delete._task_handler_for_archive",
            return_value=handler,
        ):
            message, status = enqueue_harvest_source_archive(
                "abc", "test@example.com", db
            )

        assert status == 409
        assert "clear it first" in message
        handler.start_task.assert_not_called()

    def test_returns_404_without_scheduling(self):
        db = MagicMock()
        db.can_archive_harvest_source.return_value = (
            False,
            "Harvest source not found",
            404,
        )
        handler = MagicMock()

        with patch(
            "harvester.lib.source_delete._task_handler_for_archive",
            return_value=handler,
        ):
            message, status = enqueue_harvest_source_archive(
                "missing", "test@example.com", db
            )

        assert (message, status) == ("Harvest source not found", 404)
        handler.start_task.assert_not_called()

    def test_returns_500_when_task_start_fails(self):
        db = MagicMock()
        db.can_archive_harvest_source.return_value = (True, None, 200)
        handler = MagicMock()
        handler.start_task.side_effect = RuntimeError("cf boom")

        with patch(
            "harvester.lib.source_delete._task_handler_for_archive",
            return_value=handler,
        ):
            message, status = enqueue_harvest_source_archive(
                "abc", "test@example.com", db
            )

        assert status == 500
        assert "Failed to schedule harvest source archive" in message

    def test_uses_local_handler_off_cloud_foundry(self):
        db = MagicMock()
        db.can_archive_harvest_source.return_value = (True, None, 200)
        local_handler = MagicMock()

        with (
            patch(
                "harvester.lib.source_delete.is_running_on_cloud_foundry",
                return_value=False,
            ),
            patch(
                "harvester.lib.source_delete.LocalTaskHandler",
                return_value=local_handler,
            ) as local_cls,
            patch(
                "harvester.lib.source_delete.create_task_handler"
            ) as create_handler,
        ):
            message, status = enqueue_harvest_source_archive(
                "abc", "test@example.com", db
            )

        assert status == 202
        assert message == ARCHIVE_IN_PROGRESS_MESSAGE
        local_cls.assert_called_once_with()
        create_handler.assert_not_called()
        local_handler.start_task.assert_called_once()

    def test_uses_create_task_handler_on_cloud_foundry(self):
        db = MagicMock()
        db.can_archive_harvest_source.return_value = (True, None, 200)
        cf_handler = MagicMock()

        with (
            patch(
                "harvester.lib.source_delete.is_running_on_cloud_foundry",
                return_value=True,
            ),
            patch(
                "harvester.lib.source_delete.create_task_handler",
                return_value=cf_handler,
            ) as create_handler,
            patch("harvester.lib.source_delete.LocalTaskHandler") as local_cls,
        ):
            message, status = enqueue_harvest_source_archive(
                "abc", "test@example.com", db
            )

        assert status == 202
        create_handler.assert_called_once_with()
        local_cls.assert_not_called()
        cf_handler.start_task.assert_called_once()


class TestArchiveSourceEntrypoint:
    def test_exits_zero_on_success(self):
        db = MagicMock()
        db.archive_harvest_source.return_value = ("Archived", 200)

        with patch("harvester.archive_source.db_interface", db):
            archive_source_command("abc", "test@example.com")

    def test_exits_nonzero_on_precheck_failure(self):
        db = MagicMock()
        db.archive_harvest_source.return_value = ("Harvest source not found", 404)

        with patch("harvester.archive_source.db_interface", db):
            try:
                archive_source_command("missing", "test@example.com")
                assert False, "Should have exited"
            except SystemExit as e:
                assert e.code == 1

    def test_exits_nonzero_on_exception(self):
        db = MagicMock()
        db.archive_harvest_source.side_effect = RuntimeError("boom")

        with patch("harvester.archive_source.db_interface", db):
            try:
                archive_source_command("abc", "test@example.com")
                assert False, "Should have exited"
            except SystemExit as e:
                assert e.code == 1
