from datetime import datetime, timezone

from database.interface import PAGINATE_ENTRIES_PER_PAGE


def test_add_harvest_record(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)
    harvest_job = interface.add_harvest_job(job_data_dcatus)

    record = interface.add_harvest_record(record_data_dcatus[0])

    assert record.harvest_source_id == source.id
    assert record.harvest_job_id == harvest_job.id


def test_endpoint_pagnation(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    id_lookup_table = {}
    for i in range(100):
        record = record_data_dcatus[0].copy()
        del record["id"]
        record["identifier"] = f"test-identifier-{i}"
        db_record = interface.add_harvest_record(record)
        id_lookup_table[db_record.identifier] = db_record.id

    # get first page
    db_records = interface.pget_harvest_records(page=0)
    assert len(db_records) == PAGINATE_ENTRIES_PER_PAGE
    assert db_records[0].identifier == "test-identifier-0"
    assert id_lookup_table[db_records[0].identifier] == db_records[0].id

    # get second page
    db_records = interface.pget_harvest_records(page=1)
    assert len(db_records) == PAGINATE_ENTRIES_PER_PAGE
    assert db_records[0].identifier == "test-identifier-10"
    assert id_lookup_table[db_records[0].identifier] == db_records[0].id

    # get first page again
    db_records = interface.pget_harvest_records(page=0)
    assert len(db_records) == PAGINATE_ENTRIES_PER_PAGE
    assert db_records[0].identifier == "test-identifier-0"
    assert id_lookup_table[db_records[0].identifier] == db_records[0].id

    # don't paginate via feature flag
    db_records = interface.pget_harvest_records(paginate=False)
    assert len(db_records) == 100
    assert id_lookup_table[db_records[50].identifier] == db_records[50].id

    # get page 6 (r. 100 - 119), which is out of bounds / empty
    db_records = interface.pget_harvest_records(page=11)
    assert len(db_records) == 0

    db_records = interface.pget_harvest_records(
        facets=f"id eq {id_lookup_table['test-identifier-0']}"
    )
    assert len(db_records) == 1
    assert db_records[0].harvest_job_id == job_data_dcatus["id"]


def test_endpoint_order_by(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    id_lookup_table = {}
    for i in range(100):
        record = record_data_dcatus[0].copy()
        del record["id"]
        record["identifier"] = f"test-identifier-{i}"
        db_record = interface.add_harvest_record(record)
        id_lookup_table[db_record.identifier] = db_record.id

    # get results in ascending order, and confirm it is default
    db_records = interface.pget_harvest_records()
    assert db_records[0].identifier == "test-identifier-0"
    assert id_lookup_table[db_records[0].identifier] == db_records[0].id

    # get results in descending order
    db_records = interface.pget_harvest_records(order_by="desc")
    assert db_records[0].identifier == "test-identifier-99"
    assert id_lookup_table[db_records[0].identifier] == db_records[0].id


def test_endpoint_count_for_non_paginated_methods(
    interface_with_fixture_json, source_data_dcatus, record_data_dcatus
):
    interface = interface_with_fixture_json
    count = interface.get_latest_harvest_records_by_source_orm(
        source_data_dcatus["id"],
        count=True,
    )
    assert (
        count
        == len(
            [record for record in record_data_dcatus if record["status"] == "success"]
        )
        == 2
    )


def test_sync_count_for_non_paginated_methods(
    interface_with_fixture_json, source_data_dcatus, record_data_dcatus
):
    interface = interface_with_fixture_json

    # test sync count by adding a valid record without a ckan_id
    interface.add_harvest_record(
        {
            "identifier": "test_identifier-11",
            "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
            "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
            "action": "create",
            "status": "success",
        }
    )

    count = interface.get_latest_harvest_records_by_source_orm(
        source_data_dcatus["id"],
        count=True,
    )

    assert (count - 1) == 2


def test_get_latest_harvest_records(
    interface,
    organization_data,
    source_data_dcatus,
    source_data_dcatus_2,
    job_data_dcatus,
    job_data_dcatus_2,
    latest_records,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    # another source for querying against. see last record in
    # `latest_records` fixture
    interface.add_harvest_source(source_data_dcatus_2)
    interface.add_harvest_job(job_data_dcatus)
    interface.add_harvest_job(job_data_dcatus_2)

    for record in latest_records:
        interface.add_harvest_record(record)

    latest_records = interface.get_latest_harvest_records_by_source(
        source_data_dcatus["id"]
    )

    # remove volatile fields so compare works
    for record in latest_records:
        del record["id"]
        record.pop("dataset_slug", None)

    expected_records = [
        {
            "identifier": "a",
            "source_hash": None,
            "date_created": datetime(2024, 3, 1, 0, 0, 0, 1000),
            "date_finished": None,
            "ckan_id": None,
            "action": "update",
        },
        {
            "identifier": "b",
            "source_hash": None,
            "date_created": datetime(2024, 3, 1, 0, 0, 0, 1000),
            "date_finished": None,
            "ckan_id": None,
            "action": "create",
        },
        {
            "identifier": "c",
            "source_hash": None,
            "date_created": datetime(2024, 5, 1, 0, 0, 0, 1000),
            "date_finished": None,
            "ckan_id": None,
            "action": "create",
        },
        {
            "identifier": "e",
            "source_hash": None,
            "date_created": datetime(2024, 4, 3, 0, 0, 0, 1000),
            "date_finished": None,
            "ckan_id": None,
            "action": "create",
        },
    ]

    assert len(latest_records) == 4
    # make sure there aren't records that are different
    assert not any(x != y for x, y in zip(latest_records, expected_records))


def test_delete_outdated_records(
    interface,
    organization_data,
    source_data_dcatus,
    source_data_dcatus_2,
    job_data_dcatus,
    job_data_dcatus_2,
    latest_records,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    # another source for querying against. see last records in
    # `latest_records` fixture
    interface.add_harvest_source(source_data_dcatus_2)
    interface.add_harvest_job(job_data_dcatus)
    interface.add_harvest_job(job_data_dcatus_2)
    records = [interface.add_harvest_record(record) for record in latest_records]

    # only adding 1 record error for simplicity
    # we have access to all record errors via relationship in HarvestRecord
    error_data = {
        "message": "record is invalid",
        "type": "ValidationException",
        "date_created": datetime.now(timezone.utc),
        "harvest_record_id": records[-2].id,
        "harvest_job_id": records[-2].harvest_job_id,
    }
    interface.add_harvest_record_error(error_data)

    latest_records_from_db1 = interface.get_latest_harvest_records_by_source(
        source_data_dcatus["id"]
    )

    latest_records_from_db2 = interface.get_latest_harvest_records_by_source(
        source_data_dcatus_2["id"]
    )

    outdated_records = interface.get_all_outdated_records(90)
    assert len(outdated_records) == 7

    all_records = (
        len(latest_records_from_db1)
        + len(latest_records_from_db2)
        + len(outdated_records)
    )
    # latest records for all harvest sources (2) and all outdated records
    # should be equal to the original fixture count
    assert all_records == len(latest_records)

    # we want outdated records for ALL harvest sources. this is harvest source 2
    hs2_outdated = next(r for r in outdated_records if r.identifier == "f")
    assert len(hs2_outdated.errors) == 1
    for record in outdated_records:
        interface.delete_harvest_record(record_id=record.id)

    # make sure only the outdated records and associated errors were deleted
    db_records = interface.pget_harvest_records(count=True)
    assert db_records == len(latest_records_from_db1) + len(latest_records_from_db2)

    db_record_errors = interface.pget_harvest_record_errors(count=True)

    # It should be expected the 1 error is still present
    assert db_record_errors == 1


def test_add_harvest_record_with_catalog_record_type(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test creating a HarvestRecord with record_type='catalog_record'."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    record_data = record_data_dcatus[0].copy()
    record_data["record_type"] = "catalog_record"

    record = interface.add_harvest_record(record_data)

    assert record.record_type == "catalog_record"
    assert record.harvest_source_id == source_data_dcatus["id"]
    assert record.harvest_job_id == job_data_dcatus["id"]


def test_harvest_record_default_record_type_is_dataset(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test that HarvestRecord defaults to record_type='dataset' for backward compatibility."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    # Create record without specifying record_type
    record_data = record_data_dcatus[0].copy()
    record = interface.add_harvest_record(record_data)

    assert record.record_type == "dataset"


def test_query_harvest_records_by_record_type(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test filtering HarvestRecords by record_type."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    # Create dataset records
    for i in range(3):
        record_data = record_data_dcatus[0].copy()
        del record_data["id"]
        record_data["identifier"] = f"dataset-{i}"
        record_data["record_type"] = "dataset"
        interface.add_harvest_record(record_data)

    # Create catalog_record records
    for i in range(2):
        record_data = record_data_dcatus[0].copy()
        del record_data["id"]
        record_data["identifier"] = f"catalog-record-{i}"
        record_data["record_type"] = "catalog_record"
        interface.add_harvest_record(record_data)

    # Query for only catalog_records
    catalog_records = interface.pget_harvest_records(
        facets="record_type eq catalog_record"
    )
    assert len(catalog_records) == 2
    for record in catalog_records:
        assert record.record_type == "catalog_record"

    # Query for only datasets
    dataset_records = interface.pget_harvest_records(facets="record_type eq dataset")
    assert len(dataset_records) == 3
    for record in dataset_records:
        assert record.record_type == "dataset"


def test_harvest_record_with_all_record_types(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test creating HarvestRecords with all supported record types."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    record_types = [
        "dataset",
        "catalog_record",
        "data_service",
        "dataset_series",
        "catalog",
    ]

    for record_type in record_types:
        record_data = record_data_dcatus[0].copy()
        del record_data["id"]
        record_data["identifier"] = f"test-{record_type}"
        record_data["record_type"] = record_type
        record = interface.add_harvest_record(record_data)
        assert record.record_type == record_type
