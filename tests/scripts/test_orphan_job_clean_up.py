from unittest.mock import MagicMock

from scripts.orphan_job_clean_up import delete_jobs


class TestDeleteOrphanJobsScript:

    def test_dry_run_single_job_id_not_found(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test to confirm that job id is provided but it's an invalid id.
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "abc123"
        mock_interface.get_harvest_job.return_value = mock_job

        result = runner.invoke(delete_jobs, ["--job-id", "abc123"])
        assert result.exit_code == 0
        assert "No tasks to be stopped\n" in result.output

    def test_dry_run_single_job_id_found(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test dry run for deleting a single job by ID.
        the dry run flag is not set and so it should default to True,
        and gives us the dry run message.
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"
        mock_interface.get_harvest_job.return_value = mock_job

        result = runner.invoke(
            delete_jobs, ["--job-id", "7cd474ba-5437-4d50-b7ec-6114a877510e"]
        )
        assert result.exit_code == 0
        assert mock_job.id in result.output
        assert "Dry run" in result.output

    def test_run_single_job_id_found(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test for deleting a single job by ID.
        the dry run flag is set to false and so we should
        see the stopped job message
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"
        mock_interface.get_harvest_job.return_value = mock_job

        result = runner.invoke(
            delete_jobs,
            ["--job-id", "7cd474ba-5437-4d50-b7ec-6114a877510e", "--no-dry-run"],
        )
        assert result.exit_code == 0
        assert mock_job.id in result.output
        assert "Stopped" in result.output
        assert "Dry run" not in result.output
        mock_interface.update_harvest_job.assert_called_once()

    def test_dry_run_multiple_job_id(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test dry run for deleting a multiple jobs by ID.
        the dry run flag is not set and so it should default to True,
        and gives us the dry run message.
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"
        mock_job2 = MagicMock()
        mock_job2.id = "abcdef12-3456-7890-abcd-ef1234567890"
        mock_interface.get_harvest_job.side_effect = [mock_job, mock_job2]

        result = runner.invoke(
            delete_jobs,
            [
                "--job-id",
                "7cd474ba-5437-4d50-b7ec-6114a877510e",
                "--job-id",
                "abcdef12-3456-7890-abcd-ef1234567890",
            ],
        )
        assert result.exit_code == 0
        assert mock_job.id in result.output
        assert "Dry run: would stop" in result.output
        assert "Dry run" in result.output
        mock_interface.update_harvest_job.call_count == 0

    def test_run_multiple_job_id(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test run for deleting a multiple jobs by ID.
        dry run is set to False, and gives us the stopped message.
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"
        mock_job2 = MagicMock()
        mock_job2.id = "abcdef12-3456-7890-abcd-ef1234567890"
        mock_interface.get_harvest_job.side_effect = [mock_job, mock_job2]

        result = runner.invoke(
            delete_jobs,
            [
                "--job-id",
                "7cd474ba-5437-4d50-b7ec-6114a877510e",
                "--job-id",
                "abcdef12-3456-7890-abcd-ef1234567890",
                "--no-dry-run",
            ],
        )
        assert result.exit_code == 0
        assert mock_job.id in result.output
        assert "Stopped 2 harvest job(s)" in result.output
        assert "Dry run" not in result.output
        mock_interface.update_harvest_job.call_count == 2

    def test_dry_run_multiple_no_job_id(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test dry run for deleting all stale jobs. Should produce dry run message
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"
        mock_job2 = MagicMock()
        mock_job2.id = "abcdef12-3456-7890-abcd-ef1234567890"
        mock_interface.get_harvest_job.side_effect = [mock_job, mock_job2]

        result = runner.invoke(
            delete_jobs,
            [],
        )
        assert result.exit_code == 0
        assert mock_job.id in result.output
        assert "Dry run: would stop 2 harvest job(s)" in result.output
        mock_interface.update_harvest_job.call_count == 0

    def test_run_multiple_no_job_id(
        self, mock_cf_handler, runner, sample_running_tasks, mock_interface
    ):
        """
        Test run for deleting all stale jobs. Should produce the
         stopped message
        """
        mock_cf_handler.get_running_app_tasks.return_value = sample_running_tasks
        mock_job = MagicMock()
        mock_job.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"
        mock_job2 = MagicMock()
        mock_job2.id = "abcdef12-3456-7890-abcd-ef1234567890"
        mock_interface.get_harvest_job.side_effect = [mock_job, mock_job2]

        result = runner.invoke(
            delete_jobs,
            ["--no-dry-run"],
        )
        assert result.exit_code == 0
        assert mock_job.id in result.output
        assert "Stopped" in result.output
        mock_interface.update_harvest_job.call_count == 2
