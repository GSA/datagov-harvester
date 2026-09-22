import json

from database.interface import PAGINATE_ENTRIES_PER_PAGE


def test_faceted_builder_queries(
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

    # source id, no facets
    db_records = interface.get_harvest_records_by_source(source_data_dcatus["id"])
    assert len(db_records) == PAGINATE_ENTRIES_PER_PAGE
    assert db_records[0].identifier == "test-identifier-0"
    assert id_lookup_table[db_records[0].identifier] == db_records[0].id

    # source id, plus page kwarg
    db_records = interface.get_harvest_records_by_source(
        source_data_dcatus["id"],
        page=1,
    )
    assert len(db_records) == PAGINATE_ENTRIES_PER_PAGE
    assert db_records[0].identifier == "test-identifier-10"

    # source id, plus pagination flag
    db_records = interface.get_harvest_records_by_source(
        source_data_dcatus["id"], paginate=False
    )
    assert len(db_records) == 100

    # source id, plus kwargs to return only count
    db_records = interface.get_harvest_records_by_source(
        source_data_dcatus["id"],
        count=True,
    )
    assert db_records == 100

    # source id, plus extra filter_text facet
    db_records = interface.get_harvest_records_by_source(
        source_data_dcatus["id"],
        facets=f"id eq {id_lookup_table['test-identifier-0']}",
    )
    assert len(db_records) == 1

    # source id, plus extra filter_text facet, plus kwargs to return only count
    db_records = interface.get_harvest_records_by_source(
        source_data_dcatus["id"],
        facets=f"id eq {id_lookup_table['test-identifier-0']}",
        count=True,
    )
    assert db_records == 1

    # source id, plus two facets
    db_records = interface.get_harvest_records_by_source(
        source_data_dcatus["id"],
        facets=f"id eq {id_lookup_table['test-identifier-4']},identifier eq test-identifier-4",  # noqa E501
        count=True,
    )
    assert db_records == 1


def test_get_model_fields_by_filter_returns_all_when_none(interface):
    """Test that helper returns all fields when filter is None."""
    from database.models import HarvestRecord

    fields = interface.get_model_fields_by_filter(HarvestRecord, fields_filter=None)
    field_names = [field.name for field in fields]

    assert "id" in field_names
    assert "harvest_job_id" in field_names
    assert "harvest_source_id" in field_names
    assert "identifier" in field_names
    assert "source_raw" in field_names
    assert "source_hash" in field_names
    assert "action" in field_names
    assert "status" in field_names


def test_get_model_fields_by_filter_returns_filtered_fields(interface):
    """Test that helper returns only requested fields."""
    from database.models import HarvestRecord

    fields = interface.get_model_fields_by_filter(
        HarvestRecord, fields_filter=["id", "harvest_source_id"]
    )
    field_names = [field.name for field in fields]

    assert len(field_names) == 2
    assert "id" in field_names
    assert "harvest_source_id" in field_names
    assert "source_raw" not in field_names
    assert "identifier" not in field_names


def test_get_model_fields_by_filter_handles_invalid_fields(interface):
    """Test that helper ignores non-existent field names."""
    from database.models import HarvestRecord

    fields = interface.get_model_fields_by_filter(
        HarvestRecord, fields_filter=["id", "nonexistent_field", "harvest_source_id"]
    )
    field_names = [field.name for field in fields]

    assert len(field_names) == 2
    assert "id" in field_names
    assert "harvest_source_id" in field_names
    assert "nonexistent_field" not in field_names


def test_pget_db_query_with_fields_filter_loads_only_specified_fields(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test that pget_db_query with fields_filter loads only requested columns."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    for i in range(2):
        record = record_data_dcatus[0].copy()
        del record["id"]
        record["identifier"] = f"test-record-{i}"
        record["source_raw"] = "large data content"
        interface.add_harvest_record(record)

    query = interface.pget_db_query(
        model="harvest_records", fields_filter=["id", "harvest_source_id", "identifier"]
    )
    results = query.all()

    assert len(results) == 2
    assert results[0].id is not None
    assert results[0].identifier in ["test-record-0", "test-record-1"]


def test_pget_db_query_without_fields_filter_loads_all_fields(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test that pget_db_query without fields_filter maintains existing behavior."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    record = record_data_dcatus[0].copy()
    del record["id"]
    record["identifier"] = "test-record-full"
    record["source_hash"] = "hash-full"
    record["source_raw"] = "test data value"
    record["action"] = "create"
    record["status"] = "success"
    interface.add_harvest_record(record)

    query = interface.pget_db_query(model="harvest_records")
    results = query.all()

    assert len(results) == 1
    assert results[0].id is not None
    assert results[0].identifier == "test-record-full"
    assert results[0].source_hash == "hash-full"
    assert results[0].source_raw == "test data value"
    assert results[0].action == "create"


def test_pget_db_query_fields_filter_with_facets(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    """Test that fields_filter works with facet filtering."""
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)

    for status in ["success", "error"]:
        record = record_data_dcatus[0].copy()
        del record["id"]
        record["identifier"] = f"{status}-record"
        record["status"] = status
        interface.add_harvest_record(record)

    query = interface.pget_db_query(
        model="harvest_records",
        facets="status eq success",
        fields_filter=["id", "identifier", "status"],
    )
    results = query.all()

    assert len(results) == 1
    assert results[0].identifier == "success-record"
    assert results[0].status == "success"
