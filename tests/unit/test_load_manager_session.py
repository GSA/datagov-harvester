from unittest.mock import MagicMock, call, patch

import pytest

import harvester
from harvester.lib import load_manager
from harvester.lib.load_manager import LoadManager


@pytest.fixture
def mock_interface():
    with patch("harvester.lib.load_manager.interface") as interface:
        yield interface


@pytest.fixture
def manager():
    manager = LoadManager()
    manager.handler = MagicMock()
    return manager


@pytest.fixture
def finished_task(manager):
    manager.handler.get_all_app_tasks.return_value = [
        {"guid": "guid", "state": "SUCCEEDED", "name": "harvest-job-job-id-harvest"}
    ]


class TestLoadManagerSessionOwnership:
    def test_load_manager_does_not_share_the_harvester_session(self):
        assert load_manager.session is not harvester.session

    def test_load_manager_session_reuses_the_harvester_engine(self):
        assert load_manager.session.bind is harvester.engine


class TestLoadManagerSessionLifecycle:
    def test_schedule_next_job_closes_session_for_manual_source(
        self, manager, mock_interface
    ):
        mock_interface.get_harvest_source.return_value = MagicMock(frequency="manual")

        manager.schedule_next_job("source-id")

        mock_interface.close.assert_called()

    def test_reschedule_next_run_closes_session(self, manager, mock_interface):
        mock_interface.get_queued_harvest_jobs_for_source.return_value = []
        mock_interface.get_harvest_source.return_value = MagicMock(frequency="daily")

        manager.reschedule_next_run("source-id")

        mock_interface.close.assert_called()

    def test_stop_job_closes_session_when_task_already_finished(
        self, manager, mock_interface, finished_task
    ):
        mock_interface.get_harvest_job.return_value = MagicMock(status="complete")

        manager.stop_job("job-id")

        mock_interface.close.assert_called()

    def test_trigger_manual_job_closes_session_when_job_already_active(
        self, manager, mock_interface
    ):
        mock_interface.enqueue_harvest_job_if_idle.return_value = (
            None,
            MagicMock(id="active-id", status="in_progress"),
        )

        manager.trigger_manual_job("source-id")

        mock_interface.close.assert_called()

    def test_start_job_closes_session_when_job_not_startable(
        self, manager, mock_interface
    ):
        mock_interface.start_harvest_job_if_unlocked.return_value = None

        manager.start_job("job-id")

        mock_interface.close.assert_called()
        manager.handler.start_task.assert_not_called()

    def test_start_job_closes_session_after_revert_when_task_fails_to_start(
        self, manager, mock_interface
    ):
        mock_interface.pget_harvest_jobs.return_value = []
        manager.handler.start_task.side_effect = RuntimeError("cf down")

        _, category = manager.start_job("job-id")

        assert category == "error"
        assert mock_interface.mock_calls[-2] == call.update_harvest_job(
            "job-id", {"status": "new", "date_started": None}
        )
        assert mock_interface.mock_calls[-1] == call.close()

    def test_session_closes_when_operation_raises(
        self, manager, mock_interface, finished_task
    ):
        mock_interface.get_harvest_job.side_effect = RuntimeError("db down")

        with pytest.raises(RuntimeError):
            manager.stop_job("job-id")

        mock_interface.close.assert_called()

    def test_start_closes_session_when_scheduler_raises(self, manager, mock_interface):
        manager.handler.get_running_app_tasks.side_effect = RuntimeError("cf down")

        with pytest.raises(RuntimeError):
            manager.start()

        mock_interface.close.assert_called()

    def test_failed_job_email_closes_session_before_sending(
        self, manager, mock_interface
    ):
        session_closed_at_send = []
        config = {"base_url": "https://harvest.test", "recipient": "a@b.gov"}

        with (
            patch.dict("harvester.lib.load_manager.SMTP_CONFIG", config),
            patch(
                "harvester.lib.load_manager.send_email_to_recipients",
                side_effect=lambda *a, **k: session_closed_at_send.append(
                    mock_interface.close.called
                ),
            ),
        ):
            manager._email_recipients_of_failed_jobs([("job-id", "name", "src-id")])

        assert session_closed_at_send == [True]
