from unittest.mock import MagicMock

from scripts.orphan_job_clean_up import delete_jobs


class TestDeleteOrphanJobsScript:
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
        mock_interface.get_harvest_job.assert_called_once()
        mock_interface.update_harvest_job.assert_not_called()

    def test_delete_single_job(self, runner, mock_interface):
        """
        Test deleting a single job by ID without dry run.
        We expect the deleted job message to be used.
        """
        mock_job = MagicMock()
        mock_job.id = "abc123"
        mock_interface.get_harvest_job.return_value = mock_job

        result = runner.invoke(delete_jobs, ["--job-id", "abc123", "--no-dry-run"])
        assert result.exit_code == 0
        assert "Deleted job" in result.output
        mock_interface.update_harvest_job.assert_called_once()

    def test_delete_multiple_job(self, runner, mock_interface):
        """
        Test deleting multiple jobs by id, with the dry run flag set to False.
        We expect the deleted job message to be used for each job.
        """
        mock_job1 = MagicMock()
        mock_job1.id = "abc123"
        mock_job2 = MagicMock()
        mock_job2.id = "cba321"
        mock_interface.get_harvest_job.side_effect = [mock_job1, mock_job2]

        result = runner.invoke(
            delete_jobs, ["--job-id", "abc123", "--job-id", "cba321", "--no-dry-run"]
        )
        assert result.exit_code == 0
        assert "Deleted job" in result.output
        mock_interface.get_harvest_job.call_count == 2
        mock_interface.update_harvest_job.call_count == 2

    def test_dry_run_multiple_job(self, runner, mock_interface):
        """
        Test dry run for deleting a single job by ID.
        the dry run flag is not set and so it should default to True,
        and gives us the dry run message.
        """
        mock_job1 = MagicMock()
        mock_job1.id = "abc123"
        mock_job2 = MagicMock()
        mock_job2.id = "cba321"
        mock_interface.get_harvest_job.side_effect = [mock_job1, mock_job2]

        result = runner.invoke(
            delete_jobs, ["--job-id", "abc123", "--job-id", "cba321", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "Dry run" in result.output
        assert "cba321" in result.output
        mock_interface.get_harvest_job.call_count == 2
        mock_interface.update_harvest_job.assert_not_called()

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
        mock_interface.update_harvest_job.assert_not_called()

    def test_delete_orphaned_jobs(self, runner, mock_interface):
        """ "
        Test for deleting all orphaned jobs.
        We expect the dry run message to be used, which should show
        multiple ids.
        """
        mock_job1 = MagicMock()
        mock_job1.id = "job1"
        mock_job2 = MagicMock()
        mock_job2.id = "job2"
        mock_interface.get_orphaned_harvest_jobs.return_value = [mock_job1, mock_job2]

        result = runner.invoke(delete_jobs, ["--no-dry-run"])

        assert result.exit_code == 0
        assert "job1" in result.output and "job2" in result.output
        mock_interface.get_orphaned_harvest_jobs.assert_called_once()
        # should be called 2x as there should be 2 jobs to delete
        mock_interface.update_harvest_job.call_count == 2

    def test_no_job_found(self, runner, mock_interface):
        """ "
        Test that no job is found when trying to delete by ID.
        We should always expect the "No jobs found" message despite the dry run flag.
        """
        mock_interface.get_harvest_job.return_value = None

        result = runner.invoke(delete_jobs, ["--job-id", "notfound"])

        assert result.exit_code == 0
        assert "No jobs found" in result.output
