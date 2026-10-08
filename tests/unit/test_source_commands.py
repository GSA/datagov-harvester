from unittest.mock import MagicMock, patch


def make_source(source_id, name, schema_type, organization_id=None):
    source = MagicMock()
    source.id = source_id
    source.name = name
    source.schema_type = schema_type
    source.organization_id = organization_id
    return source


def make_org(org_id, slug):
    org = MagicMock()
    org.id = org_id
    org.slug = slug
    return org


def test_dry_run_lists_only_matching_prefix(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("1", "DCAT-US Source", "dcatus1.1: federal"),
            make_source("2", "ISO Source", "iso19115_2"),
        ]

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--schema-type-prefix",
                "dcatus",
            ]
        )

    assert result.exit_code == 0
    assert "Found 1 harvest source(s) matching schema_type prefix in (dcatus)." in (
        result.output
    )
    assert "1  DCAT-US Source" in result.output
    assert "ISO Source" not in result.output
    assert "Dry run: no jobs queued." in result.output
    mock_db.add_harvest_job.assert_not_called()


def test_dry_run_no_filters_matches_everything(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("1", "DCAT-US Source", "dcatus1.1: federal"),
            make_source("2", "ISO Source", "iso19115_2"),
        ]

        result = app.test_cli_runner().invoke(
            args=["harvest_source", "force_reharvest_sources"]
        )

    assert result.exit_code == 0
    assert "Found 2 harvest source(s) matching any source." in result.output
    assert "DCAT-US Source" in result.output
    assert "ISO Source" in result.output
    mock_db.add_harvest_job.assert_not_called()


def test_dry_run_filters_by_organization(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("1", "Census Source", "dcatus1.1: federal", "org-census"),
            make_source("2", "Other Source", "dcatus1.1: federal", "org-other"),
        ]
        mock_db.get_organization.return_value = None
        mock_db.get_organization_by_slug.return_value = make_org(
            "org-census", "census-bureau"
        )

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--organization",
                "census-bureau",
            ]
        )

    assert result.exit_code == 0
    assert "Found 1 harvest source(s)" in result.output
    assert "Census Source" in result.output
    assert "Other Source" not in result.output
    mock_db.add_harvest_job.assert_not_called()


def test_dry_run_unknown_organization_fails(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_organization.return_value = None
        mock_db.get_organization_by_slug.return_value = None

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--organization",
                "does-not-exist",
            ]
        )

    assert result.exit_code == 1
    assert "No organization found matching 'does-not-exist'." in result.output
    mock_db.add_harvest_job.assert_not_called()


def test_dry_run_multiple_prefixes(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("1", "DCAT-US Source", "dcatus1.1: federal"),
            make_source("2", "ISO Source", "iso19115_2"),
            make_source("3", "Other Source", "iso19115_1"),
        ]

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--schema-type-prefix",
                "dcatus",
                "--schema-type-prefix",
                "iso19115",
            ]
        )

    assert result.exit_code == 0
    assert "Found 3 harvest source(s)" in result.output


def test_no_dry_run_queues_job_for_each_source(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("1", "DCAT-US Source", "dcatus1.1: federal"),
            make_source("2", "DCAT-US Source 2", "dcatus3.0"),
        ]
        mock_db.get_active_harvest_job_for_source.return_value = None
        queued_job = MagicMock()
        queued_job.id = "job-1"
        mock_db.add_harvest_job.return_value = queued_job

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--schema-type-prefix",
                "dcatus",
                "--no-dry-run",
            ]
        )

    assert result.exit_code == 0
    assert mock_db.add_harvest_job.call_count == 2
    for call in mock_db.add_harvest_job.call_args_list:
        job_data = call.args[0]
        assert job_data["status"] == "new"
        assert job_data["job_type"] == "force_harvest"
    assert "queued job job-1" in result.output


def test_no_dry_run_skips_source_with_active_job(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("1", "DCAT-US Source", "dcatus1.1: federal"),
        ]
        active_job = MagicMock()
        active_job.id = "active-job-1"
        mock_db.get_active_harvest_job_for_source.return_value = active_job

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--schema-type-prefix",
                "dcatus",
                "--no-dry-run",
            ]
        )

    assert result.exit_code == 0
    mock_db.add_harvest_job.assert_not_called()
    assert "skipped, job active-job-1 already new/in_progress" in result.output


def test_no_dry_run_no_matching_sources(app):
    with patch("app.commands.source.db") as mock_db:
        mock_db.get_all_harvest_sources.return_value = [
            make_source("2", "ISO Source", "iso19115_2"),
        ]

        result = app.test_cli_runner().invoke(
            args=[
                "harvest_source",
                "force_reharvest_sources",
                "--schema-type-prefix",
                "dcatus",
                "--no-dry-run",
            ]
        )

    assert result.exit_code == 0
    assert "Found 0 harvest source(s)" in result.output
    mock_db.add_harvest_job.assert_not_called()
