from database.models import HarvestRecordError


def test_add_harvest_record_error(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
    record_error_data,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    interface.add_harvest_record(record_data_dcatus[2])

    harvest_record_error = interface.add_harvest_record_error(record_error_data[0])
    assert isinstance(harvest_record_error, HarvestRecordError)
    assert harvest_record_error.message == record_error_data[0]["message"]

    harvest_record_error_from_db = interface.get_harvest_error(harvest_record_error.id)
    assert harvest_record_error.id == harvest_record_error_from_db.id
    assert (
        harvest_record_error.harvest_record_id
        == harvest_record_error_from_db.harvest_record_id
    )


def test_deleting_job_deletes_errors(
    interface_with_fixture_json,
    job_data_dcatus,
    record_error_data,
):
    """
    Test that confirms that HarvestRecordErrors are deleted on
    associated HarvestJob deletion.
    """
    interface = interface_with_fixture_json
    job_id = job_data_dcatus["id"]
    # Confirm that errors are created
    count = interface.get_harvest_record_errors_by_job(
        job_id,
        count=True,
    )
    assert count == len(record_error_data)

    # Delete harvest job with errors
    interface.delete_harvest_job(job_id)
    count = interface.get_harvest_record_errors_by_job(
        job_id,
        count=True,
    )
    # Confirm that HarvestRecordErrors are deleted
    # with the HarvestJob
    assert count == 0


def test_harvest_record_error_remains(
    interface_with_fixture_json,
    job_data_dcatus,
    record_data_dcatus,
    record_error_data,
):
    """
    Test to confirm that HarvestRecordErrors are not deleted when
    associated HarvestRecord is deleted.
    """
    interface = interface_with_fixture_json
    job_id = job_data_dcatus["id"]
    # Confirm that errors are created
    error_count = interface.pget_harvest_record_errors(
        count=True,
    )
    assert error_count == len(record_error_data)
    harvest_job_records = interface.get_harvest_records_by_job(job_id, paginate=False)

    # Confirm we have existing records and that they equal
    # the number from our baseline
    assert len(harvest_job_records) == len(record_data_dcatus)
    # Delete HarvestRecords
    for record in harvest_job_records:
        interface.delete_harvest_record(record_id=record.id)

    # Confirm no more HarvestRecords exist but we do have HarvestRecordErrors
    harvest_job_records = interface.get_harvest_records_by_job(job_id, paginate=False)
    error_count = interface.pget_harvest_record_errors(
        count=True,
    )
    assert len(harvest_job_records) != len(record_data_dcatus)
    assert error_count == len(record_error_data)


def test_endpoint_count(
    interface_with_fixture_json, job_data_dcatus, record_data_dcatus
):
    interface = interface_with_fixture_json
    job_id = job_data_dcatus["id"]
    count = interface.get_harvest_record_errors_by_job(
        job_id,
        count=True,
    )
    assert (
        count
        # two errors for each record
        == 2
        * len([record for record in record_data_dcatus if record["status"] == "error"])
        == 16
    )


def test_record_errors_by_severity(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    job = interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])

    interface.add_harvest_record_error(
        {
            "message": "an error",
            "type": "TestException",
            "harvest_job_id": job.id,
            "harvest_record_id": record.id,
            "severity": "error",
        }
    )
    interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job.id,
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )

    # by_job: default returns only errors, None returns all
    errors = interface.get_harvest_record_errors_by_job(job.id, per_page=999)
    assert {e[0].severity for e in errors} == {"error"}
    all_issues = interface.get_harvest_record_errors_by_job(
        job.id, severity=None, per_page=999
    )
    assert {e[0].severity for e in all_issues} == {"error", "warning"}
    warnings = interface.get_harvest_record_errors_by_job(
        job.id, severity="warning", per_page=999
    )
    assert {e[0].severity for e in warnings} == {"warning"}

    # by_record: same defaulting behavior
    assert {
        e.severity for e in interface.get_harvest_record_errors_by_record(record.id)
    } == {"error"}
    assert {
        e.severity
        for e in interface.get_harvest_record_errors_by_record(record.id, severity=None)
    } == {"error", "warning"}

    # pget: default returns only errors, None returns all
    assert {
        e.severity for e in interface.pget_harvest_record_errors(paginate=False)
    } == {"error"}
    assert {
        e.severity
        for e in interface.pget_harvest_record_errors(severity=None, paginate=False)
    } == {"error", "warning"}


def test_get_harvest_record_issues(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    record_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    job = interface.add_harvest_job(job_data_dcatus)
    record = interface.add_harvest_record(record_data_dcatus[2])

    interface.add_harvest_record_error(
        {
            "message": "an error",
            "type": "TestException",
            "harvest_job_id": job.id,
            "harvest_record_id": record.id,
            "severity": "error",
        }
    )
    interface.add_harvest_record_error(
        {
            "message": "a warning",
            "type": "TestException",
            "harvest_job_id": job.id,
            "harvest_record_id": record.id,
            "severity": "warning",
        }
    )

    # returns both errors and warnings
    issues = interface.get_harvest_record_issues(job.id, per_page=999)
    assert {i[0].severity for i in issues} == {"error", "warning"}
    # honors the count kwarg
    assert interface.get_harvest_record_issues(job.id, count=True) == 2
