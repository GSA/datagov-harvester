from unittest.mock import patch

import pytest

from app.scripts.load_manager import (
    CFHandler,
    HarvesterDBInterface,
    load_manager,
    sort_jobs,
)


@pytest.fixture
def mock_good_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "0")


@pytest.fixture
def mock_bad_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "1")


class TestLoadManager:
    @patch.object(HarvesterDBInterface, "update_harvest_job")
    @patch.object(CFHandler, "setup")
    @patch.object(CFHandler, "start_task")
    @patch.object(CFHandler, "get_all_running_tasks")
    @patch.object(CFHandler, "get_all_app_tasks")
    @patch.object(HarvesterDBInterface, "get_harvest_jobs_by_faceted_filter")
    def test_load_manager_invokes_tasks(
        self,
        db_get_harvest_jobs_by_faceted_filter_mock,
        cf_get_all_app_tasks_mock,
        cf_get_all_running_tasks_mock,
        cf_start_task_mock,
        cf_setup_mock,
        db_update_harvest_job_mock,
        job_data_dcatus,
        job_data_waf,
        dhl_cf_task_data,
        mock_good_cf_index,
    ):
        db_get_harvest_jobs_by_faceted_filter_mock.return_value = [
            job_data_dcatus,
            job_data_waf,
        ]
        cf_get_all_app_tasks_mock.return_value = []
        cf_get_all_running_tasks_mock.return_value = 2
        cf_start_task_mock.return_value = "ok"
        db_update_harvest_job_mock.return_value = "ok"
        load_manager()

        # assert all mocks are called when env vars in place
        assert db_get_harvest_jobs_by_faceted_filter_mock.called
        assert cf_get_all_app_tasks_mock.called
        assert cf_get_all_running_tasks_mock.called
        assert cf_start_task_mock.called
        assert db_update_harvest_job_mock.called

        # assert derived slot val is same as calls to cf_start_task_mock
        assert (
            3 - cf_get_all_running_tasks_mock.return_value
        ) == cf_start_task_mock.call_count

    @patch.object(HarvesterDBInterface, "update_harvest_job")
    @patch.object(CFHandler, "setup")
    @patch.object(CFHandler, "start_task")
    @patch.object(CFHandler, "get_all_running_tasks")
    @patch.object(CFHandler, "get_all_app_tasks")
    @patch.object(HarvesterDBInterface, "get_harvest_jobs_by_faceted_filter")
    def test_load_manager_hits_task_limit(
        self,
        db_get_harvest_jobs_by_faceted_filter_mock,
        cf_get_all_app_tasks_mock,
        cf_get_all_running_tasks_mock,
        cf_start_task_mock,
        cf_setup_mock,
        db_update_harvest_job_mock,
        job_data_dcatus,
        job_data_waf,
        dhl_cf_task_data,
        mock_good_cf_index,
    ):
        db_get_harvest_jobs_by_faceted_filter_mock.return_value = [
            job_data_dcatus,
            job_data_waf,
        ]
        cf_get_all_app_tasks_mock.return_value = []
        cf_get_all_running_tasks_mock.return_value = 3
        cf_start_task_mock.return_value = "ok"
        db_update_harvest_job_mock.return_value = "ok"
        load_manager()

        # assert certain mocks are called
        assert cf_setup_mock.called
        assert db_get_harvest_jobs_by_faceted_filter_mock.called
        assert cf_get_all_app_tasks_mock.called
        assert cf_get_all_running_tasks_mock.called
        # assert certain mocks are not called when
        # cf_get_all_running_tasks_mock is at limit
        assert not cf_start_task_mock.called
        assert not db_update_harvest_job_mock.called

        assert (
            3 - cf_get_all_running_tasks_mock.return_value
        ) == cf_start_task_mock.call_count

    @patch.object(HarvesterDBInterface, "update_harvest_job")
    @patch.object(CFHandler, "setup")
    @patch.object(CFHandler, "start_task")
    @patch.object(CFHandler, "get_all_running_tasks")
    @patch.object(CFHandler, "get_all_app_tasks")
    @patch.object(HarvesterDBInterface, "get_harvest_jobs_by_faceted_filter")
    def test_load_manager_bails_on_incorrect_index(
        self,
        db_get_harvest_jobs_by_faceted_filter_mock,
        cf_get_all_app_tasks_mock,
        cf_get_all_running_tasks_mock,
        cf_start_task_mock,
        cf_setup_mock,
        db_update_harvest_job_mock,
        job_data_dcatus,
        job_data_waf,
        dhl_cf_task_data,
        mock_bad_cf_index,
    ):
        db_get_harvest_jobs_by_faceted_filter_mock.return_value = [
            job_data_dcatus,
            job_data_dcatus,
        ]
        cf_get_all_app_tasks_mock.return_value = []
        cf_get_all_running_tasks_mock.return_value = 2
        cf_start_task_mock.return_value = "ok"
        db_update_harvest_job_mock.return_value = "ok"
        load_manager()

        # assert all mocks are NOT called when env vars NOT in place
        assert not db_get_harvest_jobs_by_faceted_filter_mock.called
        assert not cf_get_all_app_tasks_mock.called
        assert not cf_get_all_app_tasks_mock.called
        assert not cf_get_all_running_tasks_mock.called

    def test_sort_jobs(self):
        jobs = [
            {"status": "new"},
            {"status": "new"},
            {"status": "manual"},
        ]

        sorted_jobs = sort_jobs(jobs)

        assert sorted_jobs == [
            {"status": "manual"},
            {"status": "new"},
            {"status": "new"},
        ]
