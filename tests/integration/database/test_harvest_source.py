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
