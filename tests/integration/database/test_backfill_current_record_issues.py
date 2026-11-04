from unittest.mock import patch

from database.models import HarvestRecordCurrentIssue, HarvestRecordError


def test_backfill_fills_pre_existing_unmirrored_warning(
    app,
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """A warning written before harvest_record_current_issue (#6440) existed
    never got mirrored and sits on a record that's since gone unchanged, so
    it's invisible to Outstanding Warnings. The backfill command should
    find and mirror it without a new harvest run."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[0])  # status="success"

    # Inserted directly, bypassing add_harvest_record_error's mirror hook,
    # to simulate an error that predates the mirror table.
    error = HarvestRecordError(
        message="a pre-existing warning",
        type="duplicate_keyword",
        severity="warning",
        harvest_job_id=job_data_dcatus["id"],
        harvest_record_id=record.id,
    )
    interface.db.add(error)
    interface.db.commit()

    source_id = source_data_dcatus["id"]

    def current_issues():
        return (
            interface.db.query(HarvestRecordCurrentIssue)
            .filter_by(harvest_source_id=source_id)
            .all()
        )

    assert current_issues() == []

    with patch("app.commands.database.interface", interface):
        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues", "--no-dry-run"]
        )

    assert result.exit_code == 0
    issues = current_issues()
    assert len(issues) == 1
    assert issues[0].id == error.id
    assert issues[0].identifier == record.identifier

    # Rerunning is a no-op: still exactly one row, same id, no duplicates.
    # Each real invocation gets a fresh session/identity map; expire here to
    # match that instead of reusing this test's cached objects.
    interface.db.expire_all()
    with patch("app.commands.database.interface", interface):
        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues", "--no-dry-run"]
        )

    assert result.exit_code == 0
    issues = current_issues()
    assert len(issues) == 1
    assert issues[0].id == error.id


def test_backfill_includes_record_whose_latest_attempt_errored(
    app,
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """A record's latest attempt can be status='error' (a validation
    failure) while still carrying a real warning alongside that error --
    the two aren't mutually exclusive (see Record.validate: errors and
    warnings are both reported, independent of the resulting status). The
    backfill must not skip this identifier just because its latest record
    isn't status='success'."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])  # status="error"
    assert record.status == "error"

    error = HarvestRecordError(
        message="schema validation failed",
        type="ValidationException",
        severity="error",
        harvest_job_id=job_data_dcatus["id"],
        harvest_record_id=record.id,
    )
    warning = HarvestRecordError(
        message="a pre-existing warning",
        type="duplicate_keyword",
        severity="warning",
        harvest_job_id=job_data_dcatus["id"],
        harvest_record_id=record.id,
    )
    interface.db.add_all([error, warning])
    interface.db.commit()

    source_id = source_data_dcatus["id"]

    with patch("app.commands.database.interface", interface):
        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues", "--no-dry-run"]
        )

    assert result.exit_code == 0
    issues = (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(harvest_source_id=source_id)
        .all()
    )
    assert {issue.id for issue in issues} == {error.id, warning.id}


def test_backfill_dry_run_does_not_write(
    app,
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[0])

    error = HarvestRecordError(
        message="a pre-existing warning",
        type="duplicate_keyword",
        severity="warning",
        harvest_job_id=job_data_dcatus["id"],
        harvest_record_id=record.id,
    )
    interface.db.add(error)
    interface.db.commit()

    with patch("app.commands.database.interface", interface):
        result = app.test_cli_runner().invoke(
            args=["database", "backfill-current-record-issues"]
        )

    assert result.exit_code == 0
    assert (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(harvest_source_id=source_data_dcatus["id"])
        .count()
        == 0
    )
