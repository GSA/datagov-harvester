from sqlalchemy import event, text

from database.models import Dataset, HarvestRecord
from tests.conftest import create_record_for_dataset, dataset_payload


def test_add_harvest_source(interface, organization_data, source_data_dcatus):
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    assert source is not None
    assert source.name == source_data_dcatus["name"]


def test_add_harvest_source_waf_collection_no_parent_url(
    interface, organization_data, source_data_dcatus, caplog
):
    """A waf-collection source must have a non-null collection_parent_url."""
    interface.add_organization(organization_data)
    source_data_dcatus["source_type"] = "waf-collection"
    source = interface.add_harvest_source(source_data_dcatus)
    assert source is None
    assert 'violates check constraint "wafcollectionparenturl"' in caplog.text


def test_add_harvest_source_waf_collection_with_parent_url(
    interface, organization_data, source_data_dcatus
):
    """Can add a waf-collection source with parent URL."""
    interface.add_organization(organization_data)
    source_data_dcatus["source_type"] = "waf-collection"
    source_data_dcatus["collection_parent_url"] = "fake-url"
    source = interface.add_harvest_source(source_data_dcatus)
    assert source is not None


def test_add_harvest_source_document_with_collection_parent_url(
    interface, organization_data, source_data_dcatus, caplog
):
    """document harvest source cannot have collection_parent_url."""
    interface.add_organization(organization_data)
    source_data_dcatus["collection_parent_url"] = "fake-url"
    source = interface.add_harvest_source(source_data_dcatus)
    assert source is None
    assert 'violates check constraint "wafcollectionparenturl"' in caplog.text


def test_get_all_harvest_sources(interface, organization_data, source_data_dcatus):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)

    sources = interface.get_all_harvest_sources()
    assert len(sources) > 0
    assert sources[0].name == source_data_dcatus["name"]


def test_pget_harvest_sources_order_by_name(
    interface, organization_data, source_data_dcatus
):
    """harvest_sources sort by the requested column and honor the `-` descending prefix."""
    interface.add_organization(organization_data)
    for i in range(3):
        source = source_data_dcatus.copy()
        del source["id"]
        source["name"] = f"source-{i}"
        source["url"] = f"http://localhost:80/dcatus/source-{i}.json"
        interface.add_harvest_source(source)

    ascending = interface.pget_harvest_sources(order_by="name")
    assert [s.name for s in ascending] == ["source-0", "source-1", "source-2"]

    descending = interface.pget_harvest_sources(order_by="-name")
    assert [s.name for s in descending] == ["source-2", "source-1", "source-0"]


def test_get_harvest_source(interface, organization_data, source_data_dcatus):
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    fetched_source = interface.get_harvest_source(source.id)
    assert fetched_source is not None
    assert fetched_source.name == source_data_dcatus["name"]


def test_update_harvest_source(interface, organization_data, source_data_dcatus):
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    updates = {
        "name": "Updated Test Source",
        "notification_emails": ["example@gmail.com", "another@yahoo.com"],
    }

    updated_source = interface.update_harvest_source(source.id, updates)
    assert updated_source is not None
    assert updated_source.name == updates["name"]
    assert updated_source.notification_emails == [
        "example@gmail.com",
        "another@yahoo.com",
    ]


def test_delete_harvest_source(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    # Add an organization
    interface.add_organization(organization_data)

    # Add a harvest source
    source = interface.add_harvest_source(source_data_dcatus)
    assert source is not None

    # Case 1: Harvest source has no records, so it can be deleted successfully
    response = interface.delete_harvest_source(source.id)
    # ruff: noqa: E501
    assert response == (
        "Deleted harvest source with ID:2f2652de-91df-4c63-8b53-bfced20b276b successfully",
        200,
    )

    # Refresh the session to avoid ObjectDeletedError
    interface.db.expire_all()

    deleted_source = interface.get_harvest_source(source.id)
    assert deleted_source is None

    # Case 2: Harvest source has records, so deletion should fail
    # Add the harvest source again
    source = interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    record_data_dcatus[0]["ckan_id"] = "1234"
    record = interface.add_harvest_record(record_data_dcatus[0])

    response = interface.delete_harvest_source(source.id)
    assert response == (
        "Failed: 1 records in the Harvest source, please clear it first.",
        409,
    )

    # Ensure the source still exists after failed deletion attempt
    source_still_exists = interface.get_harvest_source(source.id)
    assert source_still_exists is not None

    # Case 3: Harvest source was cleared successfully which means
    # all latest records are labelled as "delete" allowing harvest source
    # deletion to occur
    interface.update_harvest_record(record.id, {"action": "delete"})
    response = interface.delete_harvest_source(source.id)
    assert response == (
        "Deleted harvest source with ID:2f2652de-91df-4c63-8b53-bfced20b276b successfully",
        200,
    )

    deleted_source = interface.get_harvest_source(source.id)
    assert deleted_source is None


def test_delete_harvest_source_does_not_load_children(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
):
    """Deleting a source must not load or delete child rows one at a time.

    Regression test for an OOM: with passive_deletes=False SQLAlchemy pulled every
    harvest_record (including source_raw) into memory and emitted one DELETE per
    row, killing the web worker on large sources. Postgres now does the cascade.
    """
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)
    job_data_dcatus["harvest_source_id"] = source.id
    interface.add_harvest_job(job_data_dcatus)

    for i in range(50):
        interface.add_harvest_record(
            {
                "identifier": f"bulk-delete-{i}",
                "harvest_job_id": job_data_dcatus["id"],
                "harvest_source_id": source.id,
                "action": "delete",  # so the 409 guard allows the delete
                "status": "success",
                "source_raw": "x" * 1000,
            }
        )

    assert (
        interface.db.query(HarvestRecord).filter_by(harvest_source_id=source.id).count()
        == 50
    )

    statements = []

    def record_statement(conn, cursor, statement, params, context, executemany):
        statements.append(" ".join(statement.split()))

    engine = interface.db.get_bind()
    event.listen(engine, "before_cursor_execute", record_statement)
    try:
        message, status = interface.delete_harvest_source(source.id)
    finally:
        event.remove(engine, "before_cursor_execute", record_statement)

    assert status == 200

    # Match on the statement text, not the statement count. SQLAlchemy batches
    # its per-row deletes into a single executemany, so counting would pass even
    # in the broken configuration this test exists to catch.
    child_deletes = [
        s
        for s in statements
        if s.upper().startswith("DELETE FROM HARVEST_RECORD")
        or s.upper().startswith("DELETE FROM HARVEST_JOB")
    ]
    assert child_deletes == [], f"expected DB-side cascade, got: {child_deletes}"

    interface.db.expire_all()
    assert interface.get_harvest_source(source.id) is None
    assert (
        interface.db.query(HarvestRecord).filter_by(harvest_source_id=source.id).count()
        == 0
    )


def test_delete_harvest_source_with_datasets(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
):
    """A source that has dataset rows must delete cleanly.

    The dataset FKs are non-nullable with ON DELETE CASCADE, but without
    passive_deletes on the `datasets` backref SQLAlchemy tried to NULL them and
    raised IntegrityError instead of letting Postgres cascade.
    """
    record = create_record_for_dataset(
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        identifier="delete-source-with-dataset",
    )
    slug = "delete-source-with-dataset"
    interface.insert_dataset(
        dataset_payload(slug, record, organization_data, source_data_dcatus)
    )

    source_id = source_data_dcatus["id"]
    assert (
        interface.db.query(Dataset).filter_by(harvest_source_id=source_id).count() == 1
    )

    # latest record action is "create", so clear it first to pass the 409 guard
    interface.update_harvest_record(record.id, {"action": "delete"})

    message, status = interface.delete_harvest_source(source_id)
    assert status == 200

    interface.db.expire_all()
    assert interface.get_harvest_source(source_id) is None
    assert (
        interface.db.query(Dataset).filter_by(harvest_source_id=source_id).count() == 0
    )


def test_harvest_fk_delete_actions(interface):
    """The schema, not the ORM, is what has to carry the cascade.

    Integration tests build their schema from the models via create_all, so this
    pins the ondelete rules the OOM fix depends on. harvest_record_error's link to
    its record is deliberately SET NULL ("n") -- record errors outlive the record
    they describe -- while everything else cascades ("c").
    """
    expected = {
        "harvest_source_organization_id_fkey": "c",
        "harvest_job_harvest_source_id_fkey": "c",
        "harvest_record_harvest_job_id_fkey": "c",
        "harvest_record_harvest_source_id_fkey": "c",
        "harvest_job_error_harvest_job_id_fkey": "c",
        "harvest_record_error_harvest_job_id_fkey": "c",
        "harvest_record_error_harvest_record_id_fkey": "n",
    }

    rows = interface.db.execute(
        text("""
            SELECT conname, confdeltype
            FROM pg_constraint
            WHERE contype = 'f' AND conname = ANY(:names)
        """),
        {"names": list(expected)},
    ).all()

    assert dict(rows) == expected


def test_harvest_source_by_jobid(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)
    job_data_dcatus["harvest_source_id"] = source.id

    harvest_job = interface.add_harvest_job(job_data_dcatus)
    harvest_source = interface.get_harvest_source_by_jobid(harvest_job.id)

    assert source.id == harvest_source.id


def test_extract_unsupported_schema(
    interface,
    organization_data,
    source_data_waf_csdgm,
):
    """attempts to add a 'csdgm' harvest source which is unsupported"""
    interface.add_organization(organization_data)
    # we can't add the source because we use an enum for schema type
    assert interface.add_harvest_source(source_data_waf_csdgm) is None


def test_add_harvest_source_with_description(interface, organization_data):
    """Test adding harvest source with description via interface."""
    interface.add_organization(organization_data)

    source_data = {
        "organization_id": organization_data["id"],
        "name": "EPA Data",
        "url": "https://epa.gov/data.json",
        "frequency": "daily",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "on_error",
        "description": "Environmental Protection Agency open data catalog",
    }

    source = interface.add_harvest_source(source_data)
    assert source.description == "Environmental Protection Agency open data catalog"


def test_add_harvest_source_description_nullable(interface, organization_data):
    """Test that description field is nullable."""
    interface.add_organization(organization_data)

    source_data = {
        "organization_id": organization_data["id"],
        "name": "Test Source No Desc",
        "url": "https://example.com/data2.json",
        "frequency": "weekly",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "on_error",
        "description": None,
    }

    source = interface.add_harvest_source(source_data)
    assert source.description is None


def test_update_harvest_source_description(
    interface, organization_data, source_data_dcatus
):
    """Test updating harvest source description."""
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    updated = interface.update_harvest_source(
        source.id, {"description": "Updated description for testing"}
    )
    assert updated.description == "Updated description for testing"


def test_get_harvest_source_includes_description(
    interface, organization_data, source_data_dcatus
):
    """Test that retrieved harvest source includes description."""
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    # Add description via update
    interface.update_harvest_source(
        source.id, {"description": "Test description content"}
    )

    retrieved = interface.get_harvest_source(source.id)
    assert retrieved.description == "Test description content"


def test_harvest_source_has_archive_columns(interface):
    """Test HarvestSource model has archive-related columns."""
    from sqlalchemy import inspect
    from database.models import HarvestSource

    inspector = inspect(HarvestSource)
    column_names = [c.name for c in inspector.columns]

    assert "is_archived" in column_names
    assert "archived_at" in column_names
    assert "archived_by" in column_names


def test_harvest_source_archive_columns_default_values(
    interface, organization_data, source_data_dcatus
):
    """Test archive columns have correct default values for new sources."""
    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/archive-default-test"
    source = interface.add_harvest_source(source_data_dcatus)

    assert source.is_archived is False
    assert source.archived_at is None
    assert source.archived_by is None


def test_can_archive_harvest_source_success(
    interface, organization_data, source_data_dcatus
):
    """Test can_archive_harvest_source returns True when source is cleared."""
    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/can-archive-success"
    source = interface.add_harvest_source(source_data_dcatus)

    ok, message, status = interface.can_archive_harvest_source(source.id)

    assert ok is True
    assert message is None
    assert status == 200


def test_can_archive_harvest_source_not_found(interface):
    """Test can_archive_harvest_source returns False for non-existent source."""
    ok, message, status = interface.can_archive_harvest_source("nonexistent-id")

    assert ok is False
    assert message == "Harvest source not found"
    assert status == 404


def test_can_archive_harvest_source_has_records(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    """Test can_archive_harvest_source returns False when records exist."""
    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/has-records-test"
    source = interface.add_harvest_source(source_data_dcatus)

    job_data_dcatus["harvest_source_id"] = source.id
    del job_data_dcatus["id"]
    job = interface.add_harvest_job(job_data_dcatus)

    record = interface.add_harvest_record(
        {
            "harvest_job_id": job.id,
            "harvest_source_id": source.id,
            "identifier": "test-record-1",
            "action": "create",
            "status": "success",
        }
    )

    dataset_payload_data = dataset_payload(
        "test-dataset-archive", record, organization_data, source_data_dcatus
    )
    dataset_payload_data["harvest_source_id"] = source.id
    interface.insert_dataset(dataset_payload_data)

    ok, message, status = interface.can_archive_harvest_source(source.id)

    assert ok is False
    assert "records in the Harvest source" in message
    assert "please clear it first" in message
    assert status == 409


def test_archive_harvest_source_success(
    interface, organization_data, source_data_dcatus
):
    """Test archive_harvest_source sets archive flags and locks source."""
    from datetime import datetime, timezone

    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/archive-success"
    source = interface.add_harvest_source(source_data_dcatus)

    user_email = "test@example.com"
    before_archive = datetime.now(timezone.utc)

    message, status = interface.archive_harvest_source(source.id, user_email)

    after_archive = datetime.now(timezone.utc)

    assert status == 200
    assert "Archived harvest source" in message

    interface.db.refresh(source)

    assert source.is_archived is True
    assert source.archived_by == user_email
    assert source.is_locked is True
    assert source.archived_at is not None
    assert before_archive <= source.archived_at <= after_archive


def test_archive_harvest_source_not_found(interface):
    """Test archive_harvest_source returns 404 for non-existent source."""
    message, status = interface.archive_harvest_source(
        "nonexistent-id", "test@example.com"
    )

    assert status == 404
    assert "Harvest source not found" in message


def test_archive_harvest_source_has_records(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    """Test archive_harvest_source returns 409 when records exist."""
    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/archive-has-records"
    source = interface.add_harvest_source(source_data_dcatus)

    job_data_dcatus["harvest_source_id"] = source.id
    del job_data_dcatus["id"]
    job = interface.add_harvest_job(job_data_dcatus)

    record = interface.add_harvest_record(
        {
            "harvest_job_id": job.id,
            "harvest_source_id": source.id,
            "identifier": "test-record-archive-fail",
            "action": "create",
            "status": "success",
        }
    )

    dataset_payload_data = dataset_payload(
        "test-dataset-archive-fail", record, organization_data, source_data_dcatus
    )
    dataset_payload_data["harvest_source_id"] = source.id
    interface.insert_dataset(dataset_payload_data)

    message, status = interface.archive_harvest_source(source.id, "test@example.com")

    assert status == 409
    assert "records in the Harvest source" in message


def test_archive_harvest_source_preserves_jobs(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    """Test archiving preserves all associated harvest jobs."""
    from database.models import HarvestJob

    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/archive-preserves-jobs"
    source = interface.add_harvest_source(source_data_dcatus)

    job_data_dcatus["harvest_source_id"] = source.id
    del job_data_dcatus["id"]
    job = interface.add_harvest_job(job_data_dcatus)

    job_id = job.id

    message, status = interface.archive_harvest_source(source.id, "test@example.com")

    assert status == 200

    job_after = interface.db.get(HarvestJob, job_id)
    assert job_after is not None
    assert job_after.harvest_source_id == source.id


def test_archive_harvest_source_preserves_errors(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    """Test archiving preserves all associated errors."""
    from database.models import HarvestJobError
    import uuid

    interface.add_organization(organization_data)
    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/archive-preserves-errors"
    source = interface.add_harvest_source(source_data_dcatus)

    job_data_dcatus["harvest_source_id"] = source.id
    del job_data_dcatus["id"]
    job = interface.add_harvest_job(job_data_dcatus)

    error = HarvestJobError(
        id=str(uuid.uuid4()),
        harvest_job_id=job.id,
        type="connection_error",
        message="Test error",
    )
    interface.db.add(error)
    interface.db.commit()
    error_id = error.id

    message, status = interface.archive_harvest_source(source.id, "test@example.com")

    assert status == 200

    error_after = interface.db.get(HarvestJobError, error_id)
    assert error_after is not None


def test_get_all_harvest_sources_excludes_archived(
    interface, organization_data, source_data_dcatus
):
    """Test get_all_harvest_sources excludes archived sources by default."""
    interface.add_organization(organization_data)

    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/active-source-exclude"
    active_source = interface.add_harvest_source(source_data_dcatus)

    source_data_dcatus["name"] = "Archived Source"
    source_data_dcatus["url"] = "https://example.com/archived-source-exclude"
    archived_source = interface.add_harvest_source(source_data_dcatus)

    interface.archive_harvest_source(archived_source.id, "test@example.com")

    sources = interface.get_all_harvest_sources(include_archived=False)
    source_ids = [s.id for s in sources]

    assert active_source.id in source_ids
    assert archived_source.id not in source_ids


def test_get_all_harvest_sources_includes_archived_when_requested(
    interface, organization_data, source_data_dcatus
):
    """Test get_all_harvest_sources includes archived sources when requested."""
    interface.add_organization(organization_data)

    del source_data_dcatus["id"]
    source_data_dcatus["url"] = "https://example.com/active-source-include"
    active_source = interface.add_harvest_source(source_data_dcatus)

    source_data_dcatus["name"] = "Archived Source"
    source_data_dcatus["url"] = "https://example.com/archived-source-include"
    archived_source = interface.add_harvest_source(source_data_dcatus)

    interface.archive_harvest_source(archived_source.id, "test@example.com")

    sources = interface.get_all_harvest_sources(include_archived=True)
    source_ids = [s.id for s in sources]

    assert active_source.id in source_ids
    assert archived_source.id in source_ids


def test_get_all_harvest_sources_archived_sorted_last(
    interface, organization_data, source_data_dcatus
):
    """Test archived sources appear at end of list."""
    interface.add_organization(organization_data)

    del source_data_dcatus["id"]
    source_data_dcatus["name"] = "Active Source 1"
    source_data_dcatus["url"] = "https://example.com/active-1-sort"
    interface.add_harvest_source(source_data_dcatus)

    source_data_dcatus["name"] = "Archived Source"
    source_data_dcatus["url"] = "https://example.com/archived-mid-sort"
    archived_source = interface.add_harvest_source(source_data_dcatus)

    source_data_dcatus["name"] = "Active Source 2"
    source_data_dcatus["url"] = "https://example.com/active-2-sort"
    interface.add_harvest_source(source_data_dcatus)

    interface.archive_harvest_source(archived_source.id, "test@example.com")

    sources = interface.get_all_harvest_sources(include_archived=True)

    archived_sources = [s for s in sources if s.is_archived]
    active_sources = [s for s in sources if not s.is_archived]

    assert len(active_sources) >= 2
    assert len(archived_sources) >= 1

    first_archived_index = sources.index(archived_sources[0])
    last_active_index = sources.index(active_sources[-1])

    assert first_archived_index > last_active_index
