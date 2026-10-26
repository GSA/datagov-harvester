from unittest.mock import MagicMock, patch


def make_source(source_id, name):
    source = MagicMock()
    source.id = source_id
    source.name = name
    return source


def make_record(record_id, identifier, record_type="dataset"):
    record = MagicMock()
    record.id = record_id
    record.identifier = identifier
    record.record_type = record_type
    return record


def test_dry_run_reports_without_writing(app):
    with patch("app.commands.database.interface") as mock_interface:
        mock_interface.get_all_harvest_sources.return_value = [
            make_source("1", "Source With Gap"),
        ]
        mock_interface.get_current_record_issues_by_source.return_value = 0
        mock_interface.get_latest_harvest_records_by_source_orm.return_value = [
            make_record("r1", "dataset-1"),
            make_record("r2", "dataset-2"),
        ]

        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues"]
        )

    assert result.exit_code == 0
    assert "1 (Source With Gap): resyncing 2 identifier(s)" in result.output
    assert "Would resync 2 identifier(s) across 1 harvest source(s)." in result.output
    assert "Use --no-dry-run to write the changes." in result.output
    mock_interface.sync_current_record_issues.assert_not_called()


def test_skips_sources_with_existing_current_issues(app):
    with patch("app.commands.database.interface") as mock_interface:
        mock_interface.get_all_harvest_sources.return_value = [
            make_source("1", "Already Synced Source"),
        ]
        mock_interface.get_current_record_issues_by_source.return_value = 3

        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues", "--no-dry-run"]
        )

    assert result.exit_code == 0
    assert "Already Synced Source" not in result.output
    assert "Resynced 0 identifier(s) across 0 harvest source(s)." in result.output
    mock_interface.get_latest_harvest_records_by_source_orm.assert_not_called()
    mock_interface.sync_current_record_issues.assert_not_called()


def test_skips_sources_with_no_successful_records(app):
    with patch("app.commands.database.interface") as mock_interface:
        mock_interface.get_all_harvest_sources.return_value = [
            make_source("1", "Empty Source"),
        ]
        mock_interface.get_current_record_issues_by_source.return_value = 0
        mock_interface.get_latest_harvest_records_by_source_orm.return_value = []

        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues", "--no-dry-run"]
        )

    assert result.exit_code == 0
    assert "Resynced 0 identifier(s) across 0 harvest source(s)." in result.output
    mock_interface.sync_current_record_issues.assert_not_called()


def test_no_dry_run_resyncs_each_identifier(app):
    with patch("app.commands.database.interface") as mock_interface:
        mock_interface.get_all_harvest_sources.return_value = [
            make_source("1", "Source With Gap"),
        ]
        mock_interface.get_current_record_issues_by_source.return_value = 0
        mock_interface.get_latest_harvest_records_by_source_orm.return_value = [
            make_record("r1", "dataset-1"),
            make_record("r2", "dataset-2", record_type="data_service"),
        ]

        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues", "--no-dry-run"]
        )

    assert result.exit_code == 0
    assert "Resynced 2 identifier(s) across 1 harvest source(s)." in result.output
    assert mock_interface.sync_current_record_issues.call_count == 2
    mock_interface.sync_current_record_issues.assert_any_call(
        "1", "dataset", "dataset-1", "r1"
    )
    mock_interface.sync_current_record_issues.assert_any_call(
        "1", "data_service", "dataset-2", "r2"
    )
