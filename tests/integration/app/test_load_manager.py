import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from database.models import HarvestJobError
from harvester.lib.load_manager import LoadManager
from harvester.utils.general_utils import create_future_date


@pytest.fixture
def mock_good_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "0")


@pytest.fixture
def mock_bad_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "1")


@pytest.fixture
def all_tasks_json_fixture():
    file = Path(__file__).parents[0] / "fixtures/task_manager_all_tasks.json"
    with open(file, "r") as file:
        return json.load(file)


@freeze_time("Jan 14th, 2012")
class TestLoadManager:
    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.load_manager.MAX_TASKS_COUNT", 3)
    def test_load_manager_invokes_tasks(
        self,
        CFCMock,
        interface_no_jobs,
        source_data_dcatus_orm,
        mock_good_cf_index,
    ):
        intervals = [-1, -2]
        jobs = [
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_orm.id,
                "date_created": datetime.now() + timedelta(days=interval),
            }
            for interval in intervals
        ]
        for job in jobs:
            interface_no_jobs.add_harvest_job(job)

        CFCMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING"},
            {"state": "RUNNING"},
            {"state": "DONE"},
        ]

        jobs = interface_no_jobs.get_new_harvest_jobs_in_past()
        assert len(jobs) == 2
        job = jobs[0]
        assert job.status == "new"

        load_manager = LoadManager()
        load_manager.start()

        # assert create_task ops
        start_task_mock = CFCMock.return_value.v3.tasks.create
        assert start_task_mock.call_count == 1
        ## assert command
        assert (
            start_task_mock.call_args.kwargs["command"]
            == f"python harvester/harvest.py {job.id} harvest"  # using default job type
        )
        ## assert task_id
        assert (
            start_task_mock.call_args.kwargs["name"] == f"harvest-job-{job.id}-harvest"
        )
        assert job.status == "in_progress"

        # assert schedule_next_job ops
        # ruff: noqa: E501
        future_job = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            job.harvest_source_id
        )[0]

        harvest_source = interface_no_jobs.get_harvest_source(job.harvest_source_id)

        assert future_job.harvest_source_id == job.harvest_source_id
        assert future_job.date_created == create_future_date(harvest_source.frequency)

    @patch("harvester.lib.load_manager.logger")
    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    @patch("harvester.lib.load_manager.MAX_TASKS_COUNT", 3)
    def test_load_manager_hits_task_limit(
        self,
        CFCMock,
        logger_mock,
        interface,
        mock_good_cf_index,
    ):
        CFCMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING", "name": "harvest-job-"},
            {"state": "RUNNING", "name": "harvest-job-"},
            {"state": "RUNNING", "name": "harvest-job-"},
        ]

        load_manager = LoadManager()
        load_manager.start()

        # assert logger called with correct args
        assert logger_mock.info.call_count == 1
        assert (
            logger_mock.info.call_args[0][0]
            == "3 running_tasks >= max tasks count (3)."
        )

    @patch("harvester.lib.load_manager.logger")
    def test_load_manager_bails_on_incorrect_index(
        self,
        logger_mock,
        mock_bad_cf_index,
    ):
        load_manager = LoadManager()
        load_manager.start()

        # assert logger called with correct args
        assert logger_mock.debug.call_count == 1
        assert (
            logger_mock.debug.call_args[0][0]
            == "CF_INSTANCE_INDEX is not set or not equal to zero"
        )

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_load_manager_schedules_first_job(
        self,
        CFCMock,
        interface_with_multiple_jobs,
        source_data_dcatus,
        mock_good_cf_index,
    ):
        CFCMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING"},
            {"state": "RUNNING"},
        ]
        jobs = interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )
        assert len(jobs) == 3

        load_manager = LoadManager()
        load_manager.schedule_first_job(source_data_dcatus["id"])
        new_jobs = (
            interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
                source_data_dcatus["id"]
            )
        )
        assert len(new_jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert new_jobs[0].date_created == datetime.now() + timedelta(days=1)

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_manual_job_doesnt_affect_scheduled_jobs(
        self,
        CFCMock,
        mock_good_cf_index,
        interface_no_jobs,
        source_data_dcatus,
    ):
        jobs = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )
        assert len(jobs) == 0
        load_manager = LoadManager()
        load_manager.schedule_first_job(source_data_dcatus["id"])
        jobs = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )

        assert len(jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)

        load_manager = LoadManager()
        load_manager.trigger_manual_job(source_data_dcatus["id"])

        jobs = interface_no_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )
        assert len(jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)

        source_id = source_data_dcatus["id"]
        jobs = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}'",
            order_by="desc",
        )
        assert len(jobs) == 2
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)
        assert jobs[0].status == "new"

        assert jobs[1].date_created == datetime.now()
        assert jobs[1].status == "in_progress"

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_dont_create_new_job_if_job_already_in_progress(
        self,
        CFCMock,
        mock_good_cf_index,
        interface_no_jobs,
        source_data_dcatus,
    ):
        load_manager = LoadManager()
        load_manager.schedule_first_job(source_data_dcatus["id"])
        message = load_manager.trigger_manual_job(source_data_dcatus["id"])
        source_id = source_data_dcatus["id"]
        new_job = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}', status = 'in_progress'"
        )
        assert message == f"Updated job {new_job[0].id} to in_progress"
        message = load_manager.trigger_manual_job(source_data_dcatus["id"])
        assert (
            message
            == f"Can't trigger harvest. Job {new_job[0].id} already in progress."
        )

        jobs = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}'",
            order_by="desc",
        )

        assert len(jobs) == 2
        assert jobs[0].date_created == datetime.now() + timedelta(days=1)
        assert jobs[0].status == "new"

        assert jobs[1].date_created == datetime.now()
        assert jobs[1].status == "in_progress"

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_dont_start_new_job_if_job_already_in_progress(
        self,
        CFCMock,
        mock_good_cf_index,
        interface_no_jobs,
        source_data_dcatus,
    ):
        load_manager = LoadManager()
        load_manager.schedule_first_job(source_data_dcatus["id"])
        message = load_manager.trigger_manual_job(source_data_dcatus["id"])
        source_id = source_data_dcatus["id"]
        new_job = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}', status = 'new'"
        )[0]
        current_job = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}', status = 'in_progress'"
        )[0]
        assert message == f"Updated job {current_job.id} to in_progress"

        failing_start_job_msg = load_manager.start_job(new_job.id, job_type="harvest")
        assert f"Job {current_job.id} already in progress" in failing_start_job_msg

        jobs = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}'",
            order_by="desc",
        )

        assert len(jobs) == 2
        for job in jobs:
            if job.status == "new":
                assert job.date_created == datetime.now() + timedelta(days=1)
            elif job.status == "in_progress":
                assert job.date_created == datetime.now()

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_dont_create_new_job_if_another_job_already_scheduled(
        self,
        CFCMock,
        interface_with_multiple_jobs,
        source_data_dcatus,
        mock_good_cf_index,
    ):
        CFCMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING"},
            {"state": "RUNNING"},
        ]
        jobs = interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
            source_data_dcatus["id"]
        )
        assert len(jobs) == 3

        load_manager = LoadManager()
        load_manager.schedule_first_job(source_data_dcatus["id"])
        load_manager.schedule_next_job(source_data_dcatus["id"])
        # assert that no new job is created
        # when there is already a job scheduled in the future
        new_jobs = (
            interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
                source_data_dcatus["id"]
            )
        )
        assert len(new_jobs) == 1
        assert source_data_dcatus["frequency"] == "daily"
        assert new_jobs[0].date_created == datetime.now() + timedelta(days=1)

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_assert_env_var_changes_task_size(
        self,
        CFCMock,
        mock_good_cf_index,
        interface_no_jobs,
        source_data_dcatus,
        monkeypatch,
    ):
        load_manager = LoadManager()
        load_manager.trigger_manual_job(source_data_dcatus["id"])
        start_task_mock = CFCMock.return_value.v3.tasks.create
        assert start_task_mock.call_args.kwargs["memory_in_mb"] == "2056"
        assert start_task_mock.call_args.kwargs["disk_in_mb"] == "4096"

        # clear out in progress jobs
        source_id = source_data_dcatus["id"]
        jobs = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}'"
        )
        interface_no_jobs.delete_harvest_job(jobs[0].id)

        # set custom env vars
        monkeypatch.setenv("HARVEST_RUNNER_TASK_MEM", "1234")
        monkeypatch.setenv("HARVEST_RUNNER_TASK_DISK", "1234")

        load_manager.trigger_manual_job(source_data_dcatus["id"])
        start_task_mock = CFCMock.return_value.v3.tasks.create
        assert start_task_mock.call_args.kwargs["memory_in_mb"] == "1234"
        assert start_task_mock.call_args.kwargs["disk_in_mb"] == "1234"

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_trigger_cancel_job(
        self,
        CFCMock,
        all_tasks_json_fixture,
        mock_good_cf_index,
        interface_no_jobs,
        source_data_dcatus,
    ):
        CFCMock.return_value.v3.apps._pagination.return_value = all_tasks_json_fixture

        load_manager = LoadManager()
        load_manager.trigger_manual_job(source_data_dcatus["id"])

        source_id = source_data_dcatus["id"]
        jobs = interface_no_jobs.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}'"
        )

        task_guid_val = "3a24b55a02b0-eb7b-4eeb-9f45-645cedd3d93b"

        # modify tasks fixture in place
        all_tasks_json_fixture.append(
            {
                "guid": task_guid_val,
                "sequence_id": 197,
                "name": f"harvest-job-{jobs[0].id}-harvest",
                "command": "python harvester/harvest.py 47442c62-716d-4678-947c-61990106685f harvest",
                "state": "RUNNING",
                "memory_in_mb": 1536,
                "disk_in_mb": 4096,
            }
        )

        load_manager.stop_job(jobs[0].id)

        # assert cancel_task ops
        cancel_task_mock = CFCMock.return_value.v3.tasks.cancel
        assert cancel_task_mock.call_count == 1
        assert cancel_task_mock.call_args[0][0] == task_guid_val

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_clean_old_jobs_failed(self, CFCMock, interface_with_multiple_jobs):
        """Cleans up failed in_progress jobs in the database."""
        assert len(interface_with_multiple_jobs.get_in_progress_jobs()) == 3

        # CF reports no running jobs
        CFCMock.return_value.v3.apps._pagination.return_value = [
            {"state": "SUCCEEDED", "name": "harvest-job-"},
        ]

        load_manager = LoadManager()
        load_manager._clean_old_jobs()

        # no in progress jobs
        assert len(interface_with_multiple_jobs.get_in_progress_jobs()) == 0
        # and three new job errors
        assert interface_with_multiple_jobs.db.query(HarvestJobError).count() == 3

    @patch("harvester.lib.cf_handler.CloudFoundryClient")
    def test_clean_old_jobs_still_running(self, CFCMock, interface_with_multiple_jobs):
        """Doesn't clean up running in_progress jobs."""
        in_progress_jobs = interface_with_multiple_jobs.get_in_progress_jobs()
        assert len(in_progress_jobs) == 3

        # CF reports all running jobs
        CFCMock.return_value.v3.apps._pagination.return_value = [
            {"state": "RUNNING", "name": f"harvest-job-{job.id}-harvest"}
            for job in in_progress_jobs
        ]

        load_manager = LoadManager()
        load_manager._clean_old_jobs()

        # still 3 in progress jobs
        assert len(interface_with_multiple_jobs.get_in_progress_jobs()) == 3
        # and no new job errors
        assert interface_with_multiple_jobs.db.query(HarvestJobError).count() == 0
