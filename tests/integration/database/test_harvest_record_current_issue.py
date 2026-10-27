import uuid

from database.models import HarvestRecordCurrentIssue


def test_add_harvest_record_error_mirrors_current_issue(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Writing a harvest_record_error immediately mirrors into the current
    table too, without waiting for a full resync."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])

    error = interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )

    current_errors = (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(harvest_source_id=source_data_dcatus["id"])
        .all()
    )
    assert len(current_errors) == 1
    assert current_errors[0].identifier == record.identifier
    # Same id/message/severity as the harvest_record_error row, not a copy.
    assert current_errors[0].id == error.id
    assert current_errors[0].error.message == "a warning"
    assert current_errors[0].error.severity == "warning"


def test_sync_current_record_issues_replaces_prior_set(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """sync_current_record_issues replaces, not appends -- a record with no
    issues this run ends up with none in the current table."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])
    source_id = source_data_dcatus["id"]
    record_type = record.record_type
    identifier = record.identifier

    interface.add_harvest_record_error(
        {
            "message": "stale warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )
    assert interface.get_current_record_issues_by_source(source_id, count=True) == 1

    new_record = interface.add_harvest_record(
        {
            **record_data_dcatus[2],
            "id": str(uuid.uuid4()),
            "harvest_job_id": job_data_dcatus["id"],
        }
    )
    interface.sync_current_record_issues(
        source_id, record_type, identifier, new_record.id
    )

    assert interface.get_current_record_issues_by_source(source_id, count=True) == 0


def test_sync_current_record_issues_carries_forward_latest_set(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """A reprocessed record ends up with exactly its latest issue set --
    no duplicates, no stale leftovers from the previous run."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])
    source_id = source_data_dcatus["id"]
    record_type = record.record_type
    identifier = record.identifier

    interface.add_harvest_record_error(
        {
            "message": "first warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )

    new_record = interface.add_harvest_record(
        {
            **record_data_dcatus[2],
            "id": str(uuid.uuid4()),
            "harvest_job_id": job_data_dcatus["id"],
        }
    )
    interface.add_harvest_record_error(
        {
            "message": "second warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": new_record.id,
            "severity": "warning",
        }
    )
    interface.sync_current_record_issues(
        source_id, record_type, identifier, new_record.id
    )

    current_errors = (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(
            harvest_source_id=source_id, record_type=record_type, identifier=identifier
        )
        .all()
    )
    assert len(current_errors) == 1
    assert current_errors[0].error.message == "second warning"
    assert current_errors[0].error.harvest_record_id == new_record.id


def test_clear_current_record_issues(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """clear_current_record_issues drops an identifier's current issues,
    e.g. because the record was deleted from the source."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])
    source_id = source_data_dcatus["id"]

    interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )
    assert interface.get_current_record_issues_by_source(source_id, count=True) == 1

    interface.clear_current_record_issues(
        source_id, record.record_type, record.identifier
    )

    assert interface.get_current_record_issues_by_source(source_id, count=True) == 0


def test_current_record_issues_cascade_on_source_delete(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])
    source_id = source_data_dcatus["id"]

    interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )

    interface.delete_harvest_source(source_id)

    assert (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(harvest_source_id=source_id)
        .count()
        == 0
    )


def test_current_record_issue_cascades_on_job_delete(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """A current-issue row's id is its harvest_record_error row's id (FK),
    so deleting the job that produced that error also removes the
    now-stale current-issue pointer instead of leaving it dangling."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])
    source_id = source_data_dcatus["id"]

    interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )
    assert interface.get_current_record_issues_by_source(source_id, count=True) == 1

    interface.delete_harvest_job(job_data_dcatus["id"])

    assert interface.get_current_record_issues_by_source(source_id, count=True) == 0


def test_get_current_record_issues_by_source_for_view(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])
    source_id = source_data_dcatus["id"]

    error = interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )

    records = interface.get_current_record_issues_by_source(source_id, for_view=True)
    assert len(records) == 1
    record_view = records[0]
    assert record_view["harvest_record_id"] == record.id
    assert record_view["identifier"] == record.identifier
    assert record_view["date_created"] == error.date_created
    assert len(record_view["issues"]) == 1
    issue = record_view["issues"][0]
    assert issue["type"] == "TestException"
    assert issue["severity"] == "warning"
    assert issue["messages"] == ["a warning"]
