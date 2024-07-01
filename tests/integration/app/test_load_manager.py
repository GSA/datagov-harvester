from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from app.scripts.load_manager import (
    load_manager,
    schedule_first_job,
    trigger_manual_job,
)
from harvester.utils.general_utils import create_future_date


@pytest.fixture
def mock_good_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "0")


@pytest.fixture
def mock_bad_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "1")


@freeze_time("Jan 14th, 2012")
class TestLoadManager:
    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.cf_handler.TaskManager")
    def test_load_manager_invokes_tasks(
        self, TMMock, CFCMock, interface_no_jobs, source_orm_dcatus, mock_good_cf_index
    ):
        intervals = [-1, -2]
        jobs = [
            {
                "status": "new",
                "harvest_source_id": source_orm_dcatus.id,
                "date_created": datetime.now() + timedelta(days=interval),
            }
            for interval in intervals
        ]
        for job in jobs:
            interface_no_jobs.add_harvest_job(job)

        CFCMock.return_value.v3.apps.__getitem__.return_value.tasks.return_value = [
            {"state": "RUNNING"},
            {"state": "RUNNING"},
            {"state": "DONE"},
        ]

        jobs = interface_no_jobs.get_new_harvest_jobs_in_past()
        assert len(jobs) == 2
        job = jobs[0]
        assert job.status == "new"

        load_manager()

        # assert create_task ops
        start_task_mock = TMMock.return_value.create
        assert start_task_mock.call_count == 1
        ## assert command
        assert (
            start_task_mock.call_args[0][1] == f"python harvester/harvest.py {job.id}"
        )
        ## assert task_id
        assert start_task_mock.call_args[0][2] == f"harvest-job-{job.id}"
        assert job.status == "in_progress"

        # assert schedule_next_job ops
        # ruff: noqa: E501
        future_job = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            job.harvest_source_id
        )[0]

        harvest_source = interface_no_jobs.get_harvest_source(job.harvest_source_id)

        assert future_job.harvest_source_id == job.harvest_source_id
        assert future_job.date_created == create_future_date(harvest_source.frequency)

    @patch("app.scripts.load_manager.logger")
    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.cf_handler.TaskManager")
    def test_load_manager_hits_task_limit(
        self,
        TMMock,
        CFCMock,
        logger_mock,
        interface,
        mock_good_cf_index,
    ):
        CFCMock.return_value.v3.apps.__getitem__.return_value.tasks.return_value = [
            {"state": "RUNNING"},
            {"state": "RUNNING"},
            {"state": "RUNNING"},
        ]

        load_manager()

        # assert logger called with correct args
        assert logger_mock.info.call_count == 1
        assert (
            logger_mock.info.call_args[0][0]
            == "3 running_tasks >= max tasks count (3)."
        )

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

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.cf_handler.TaskManager")
    def test_load_manager_schedules_first_job(
        self,
        TMMock,
        CFCMock,
        interface_with_multiple_jobs,
        source_data_dcatus,
        mock_good_cf_index,
    ):
        CFCMock.return_value.v3.apps.__getitem__.return_value.tasks.return_value = [
            {"state": "RUNNING"},
            {"state": "RUNNING"},
        ]
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

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.cf_handler.TaskManager")
    def test_manual_job_doesnt_affect_scheduled_jobs(
        self, TMMock, CFCMock, interface_no_jobs, source_data_dcatus
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

        jobs = interface_no_jobs.get_all_harvest_jobs_by_filter(
            {"harvest_source_id": source_data_dcatus["id"]}
        )

        assert len(jobs) == 2
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)
        assert jobs[0].status == "new"

        assert jobs[1].date_created == datetime.now()
        assert jobs[1].status == "in_progress"

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.cf_handler.TaskManager")
    def test_dont_create_new_job_if_job_already_in_progress(
        self,
        TMMock,
        CFCMock,
        interface_no_jobs,
        source_data_dcatus,
    ):
        schedule_first_job(source_data_dcatus["id"])
        message = trigger_manual_job(source_data_dcatus["id"])
        new_job = interface_no_jobs.get_all_harvest_jobs_by_filter(
            {"harvest_source_id": source_data_dcatus["id"], "status": "in_progress"}
        )
        assert message == f"Updated job {new_job[0].id} to in_progress"
        message = trigger_manual_job(source_data_dcatus["id"])
        assert (
            message
            == f"Can't trigger harvest. Job {new_job[0].id} already in progress."
        )

        jobs = interface_no_jobs.get_all_harvest_jobs_by_filter(
            {"harvest_source_id": source_data_dcatus["id"]}
        )

        assert len(jobs) == 2
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)
        assert jobs[0].status == "new"

        assert jobs[1].date_created == datetime.now()
        assert jobs[1].status == "in_progress"
