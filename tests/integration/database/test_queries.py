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
