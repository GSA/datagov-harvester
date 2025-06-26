from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from scripts.orphan_job_clean_up import delete_jobs


class TestDeleteOrphanJobsScript:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def mock_interface(self):
        with patch("scripts.orphan_job_clean_up.HarvesterDBInterface") as mock_cls:
            instance = MagicMock()
            mock_cls.return_value = instance
            yield instance

    def test_dry_run_single_job(self, runner, mock_interface):
        """
        Test dry run for deleting a single job by ID.
        the dry run flag is not set and so it should default to True,
        and gives us the dry run message.
        """
        mock_job = MagicMock()
        mock_job.id = "abc123"
        mock_interface.get_harvest_job.return_value = mock_job

        result = runner.invoke(delete_jobs, ["--job-id", "abc123"])
        assert result.exit_code == 0
        assert "Dry run" in result.output
        assert "abc123" in result.output
        mock_interface.get_harvest_job.assert_called_once_with(job_id="abc123")
        mock_interface.delete_harvest_job.assert_not_called()

    def test_delete_single_job(self, runner, mock_interface):
        """
        Test deleting a single job by ID without dry run.
        We expect the deleted job message to be used.
        """
        mock_job = MagicMock()
        mock_job.id = "abc123"
        mock_interface.get_harvest_job.return_value = mock_job

        result = runner.invoke(delete_jobs, ["--job-id", "abc123", "--dry-run", False])
        assert result.exit_code == 0
        assert "Deleted job" in result.output
        mock_interface.delete_harvest_job.assert_called_once_with(job_id="abc123")

    def test_dry_run_orphaned_jobs(self, runner, mock_interface):
        """ "
        Test dry run for deleting all orphaned jobs.
        We expect the dry run message to be used, which should show
        multiple ids.
        """
        mock_job1 = MagicMock()
        mock_job1.id = "job1"
        mock_job2 = MagicMock()
        mock_job2.id = "job2"
        mock_interface.get_orphaned_harvest_jobs.return_value = [mock_job1, mock_job2]

        result = runner.invoke(delete_jobs, [])

        assert result.exit_code == 0
        assert "Dry run" in result.output
        assert "job1" in result.output and "job2" in result.output
        mock_interface.get_orphaned_harvest_jobs.assert_called_once()
        mock_interface.delete_harvest_job.assert_not_called()

    def test_no_job_found(self, runner, mock_interface):
        """ "
        Test that no job is found when trying to delete by ID.
        We should always expect the "No jobs found" message despite the dry run flag.
        """
        mock_interface.get_harvest_job.return_value = None

        result = runner.invoke(delete_jobs, ["--job-id", "notfound"])

        assert result.exit_code == 0
        assert "No jobs found" in result.output
