import pytest

from unittest.mock import patch
from app.scripts.load_manager import load_manager, sort_jobs
from app.scripts.load_manager import CFHandler
from app.scripts.load_manager import HarvesterDBInterface


@pytest.fixture
def mock_good_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "0")


@pytest.fixture
def mock_bad_cf_index(monkeypatch):
    monkeypatch.setenv("CF_INSTANCE_INDEX", "1")


@pytest.fixture(autouse=True)
def mock_lm_config(monkeypatch):
    monkeypatch.setenv("LM_RUNNER_APP_GUID", "f4ab7f86-bee0-44fd-8806-1dca7f8e215a")


class TestLoadManager:
    @patch.object(HarvesterDBInterface, "update_harvest_job")
    @patch.object(CFHandler, "start_task")
    @patch.object(CFHandler, "get_all_running_tasks")
    @patch.object(CFHandler, "get_all_app_tasks")
    @patch.object(HarvesterDBInterface, "get_harvest_jobs_by_filter_multival")
    def test_load_manager_invokes_tasks(
        self,
        db_get_harvest_jobs_by_filter_multival_mock,
        cf_get_all_app_tasks_mock,
        cf_get_all_running_tasks_mock,
        cf_start_task_mock,
        db_update_harvest_job_mock,
        job_data_dcatus,
        job_data_waf,
        dhl_cf_task_data,
        mock_good_cf_index,
    ):
        db_get_harvest_jobs_by_filter_multival_mock.return_value = [
            job_data_dcatus,
            job_data_waf,
        ]
        cf_get_all_app_tasks_mock.return_value = []
        cf_get_all_running_tasks_mock.return_value = 2
        cf_start_task_mock.return_value = "ok"
        db_update_harvest_job_mock.return_value = "ok"
        load_manager()

        # assert all mocks are called when env vars in place
        assert db_get_harvest_jobs_by_filter_multival_mock.called
        assert cf_get_all_app_tasks_mock.called
        assert cf_get_all_running_tasks_mock.called
        assert cf_start_task_mock.called
        assert db_update_harvest_job_mock.called

        # assert derived slot val is same as calls to cf_start_task_mock
        assert (
            3 - cf_get_all_running_tasks_mock.return_value
        ) == cf_start_task_mock.call_count

    @patch.object(HarvesterDBInterface, "update_harvest_job")
    @patch.object(CFHandler, "start_task")
    @patch.object(CFHandler, "get_all_running_tasks")
    @patch.object(CFHandler, "get_all_app_tasks")
    @patch.object(HarvesterDBInterface, "get_harvest_jobs_by_filter_multival")
    def test_load_manager_hits_task_limit(
        self,
        db_get_harvest_jobs_by_filter_multival_mock,
        cf_get_all_app_tasks_mock,
        cf_get_all_running_tasks_mock,
        cf_start_task_mock,
        db_update_harvest_job_mock,
        job_data_dcatus,
        job_data_waf,
        dhl_cf_task_data,
        mock_good_cf_index,
    ):
        db_get_harvest_jobs_by_filter_multival_mock.return_value = [
            job_data_dcatus,
            job_data_waf,
        ]
        cf_get_all_app_tasks_mock.return_value = []
        cf_get_all_running_tasks_mock.return_value = 3
        cf_start_task_mock.return_value = "ok"
        db_update_harvest_job_mock.return_value = "ok"
        load_manager()

        # assert certain mocks are called
        assert db_get_harvest_jobs_by_filter_multival_mock.called
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
    @patch.object(CFHandler, "start_task")
    @patch.object(CFHandler, "get_all_running_tasks")
    @patch.object(CFHandler, "get_all_app_tasks")
    @patch.object(HarvesterDBInterface, "get_harvest_jobs_by_filter_multival")
    def test_load_manager_bails_on_incorrect_index(
        self,
        db_get_harvest_jobs_by_filter_multival_mock,
        cf_get_all_app_tasks_mock,
        cf_get_all_running_tasks_mock,
        cf_start_task_mock,
        db_update_harvest_job_mock,
        job_data_dcatus,
        job_data_waf,
        dhl_cf_task_data,
        mock_bad_cf_index,
    ):
        db_get_harvest_jobs_by_filter_multival_mock.return_value = [
            job_data_dcatus,
            job_data_dcatus,
        ]
        cf_get_all_app_tasks_mock.return_value = []
        cf_get_all_running_tasks_mock.return_value = 2
        cf_start_task_mock.return_value = "ok"
        db_update_harvest_job_mock.return_value = "ok"
        load_manager()

        # assert all mocks are NOT called when env vars NOT in place
        assert not db_get_harvest_jobs_by_filter_multival_mock.called
        assert not cf_get_all_app_tasks_mock.called
        assert not cf_get_all_app_tasks_mock.called
        assert not cf_get_all_running_tasks_mock.called

    def test_sort_jobs(self):
        jobs = [
            {"status": "pending"},
            {"status": "pending"},
            {"status": "pending_manual"},
        ]

        sorted_jobs = sort_jobs(jobs)

        assert sorted_jobs == [
            {"status": "pending_manual"},
            {"status": "pending"},
            {"status": "pending"},
        ]
