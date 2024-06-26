from copy import copy
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from app.scripts.load_manager import (
    load_manager,
    schedule_first_job,
    trigger_manual_job,
)


@pytest.fixture
def mock_good_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "0")


@pytest.fixture
def mock_bad_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "1")


@freeze_time("Jan 14th, 2012")
class TestLoadManager:
    @patch("app.scripts.load_manager.interface")
    @patch("app.scripts.load_manager.CFHandler")
    def test_load_manager_invokes_tasks(
        self,
        CFHandlerMock,
        interface_mock,
        job_orm_dcatus,
        harvest_source_orm_dcatus,
        mock_good_cf_index,
    ):
        job_orm_dcatus_in_progress = copy(job_orm_dcatus)
        job_orm_dcatus_in_progress.status = "in_progress"
        CFHandlerMock.return_value.get_all_running_app_tasks.return_value = 2
        CFHandlerMock.return_value.start_task.return_value == "ok"
        interface_mock.get_new_harvest_jobs_in_past.return_value = [job_orm_dcatus]
        interface_mock.update_harvest_job.return_value = job_orm_dcatus_in_progress
        interface_mock.get_harvest_source.return_value = harvest_source_orm_dcatus

        load_manager()

        # assert get_all_running_app_tasks called once
        assert CFHandlerMock.return_value.get_all_running_app_tasks.call_count == 1

        # assert 1 slot available so one job created & scheduled
        assert CFHandlerMock.return_value.start_task.call_count == 1
        assert (
            CFHandlerMock.return_value.start_task.call_args.kwargs["command"]
            == f"python harvester/harvest.py {job_orm_dcatus.id}"
        )
        assert interface_mock.update_harvest_job.call_count == 1
        assert interface_mock.update_harvest_job.call_args[0][0] == job_orm_dcatus.id

        assert interface_mock.add_harvest_job.call_count == 1
        assert (
            interface_mock.add_harvest_job.call_args.args[0]["harvest_source_id"]
            == harvest_source_orm_dcatus.id
        )
        assert interface_mock.add_harvest_job.call_args.args[0][
            "date_created"
        ] == datetime.now() + timedelta(days=1)

    @patch("app.scripts.load_manager.interface")
    @patch("app.scripts.load_manager.CFHandler")
    def test_load_manager_hits_task_limit(
        self,
        CFHandlerMock,
        interface_mock,
        mock_good_cf_index,
    ):
        CFHandlerMock.return_value.get_all_running_app_tasks.return_value = 3
        interface_mock.get_new_harvest_jobs_in_past.return_value = []

        load_manager()

        # assert get_new_harvest_jobs_in_past called once
        assert interface_mock.get_new_harvest_jobs_in_past.call_count == 1
        # assert get_all_running_app_tasks called once
        assert CFHandlerMock.return_value.get_all_running_app_tasks.call_count == 1

        # assert create_task mocks were not called
        assert not CFHandlerMock.return_value.start_task.called
        assert not interface_mock.update_harvest_job.called

        # assert schedule_next_job mocks were not called
        assert not interface_mock.get_harvest_source.called
        assert not interface_mock.add_harvest_job.called

    @patch("app.scripts.load_manager.logger")
    def test_load_manager_bails_on_incorrect_index(
        self,
        logger_mock,
        mock_bad_cf_index,
    ):
        load_manager()

        # assert logger called with correct args
        assert logger_mock.info.call_count == 1
        assert (
            logger_mock.info.call_args[0][0]
            == "CF_INSTANCE_INDEX is not set or not equal to zero"
        )

    @patch("app.scripts.load_manager.CFHandler")
    def test_load_manager_schedules_first_job(
        self,
        CFHandlerMock,
        interface_with_multiple_jobs,
        source_data_dcatus,
        mock_good_cf_index,
    ):
        CFHandlerMock.return_value.get_all_running_app_tasks.return_value = 2
        CFHandlerMock.return_value.start_task.return_value == "ok"
        jobs = interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )
        assert len(jobs) == 3
        schedule_first_job(source_data_dcatus["id"])
        new_jobs = (
            interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
                source_data_dcatus["id"]
            )
        )
        assert len(new_jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert new_jobs[0].date_created == datetime.now() + timedelta(days=1)

    @patch("app.scripts.load_manager.CFHandler")
    def test_manual_job_doesnt_affect_scheduled_jobs(
        self, CFHandlerMock, source_data_dcatus, interface_no_jobs
    ):
        jobs = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )
        assert len(jobs) == 0
        schedule_first_job(source_data_dcatus["id"])
        jobs = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )

        assert len(jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)

        trigger_manual_job(source_data_dcatus["id"])

        jobs = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )

        assert len(jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)

        jobs = interface_no_jobs.get_harvest_job_by_source(source_data_dcatus["id"])

        assert len(jobs) == 2
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)
        assert jobs[0].status == "new"

        assert jobs[1].date_created == datetime.now()
        assert jobs[1].status == "in_progress"

    @patch("app.scripts.load_manager.CFHandler")
    def test_dont_create_new_job_if_job_already_in_progress(
        self, CFHandlerMock, source_data_dcatus, interface_no_jobs
    ):
        schedule_first_job(source_data_dcatus["id"])
        message = trigger_manual_job(source_data_dcatus["id"])
        new_job = interface_no_jobs.get_harvest_jobs_by_filter(
            {"harvest_source_id": source_data_dcatus["id"], "status": "in_progress"}
        )
        assert message == f"Updated job {new_job[0].id} to in_progress"
        message = trigger_manual_job(source_data_dcatus["id"])
        assert (
            message
            == f"Can't trigger harvest. Job {new_job[0].id} already in progress."
        )

        jobs = interface_no_jobs.get_harvest_job_by_source(source_data_dcatus["id"])

        assert len(jobs) == 2
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)
        assert jobs[0].status == "new"

        assert jobs[1].date_created == datetime.now()
        assert jobs[1].status == "in_progress"
