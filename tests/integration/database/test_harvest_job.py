from datetime import datetime, timezone

from freezegun import freeze_time
from sqlalchemy import text

from database.models import HarvestJobError


def test_add_harvest_job_error(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
    job_error_data,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_dcatus)
    harvest_job_error = interface.add_harvest_job_error(job_error_data)

    assert isinstance(harvest_job_error, HarvestJobError)
    assert harvest_job_error.message == job_error_data["message"]

    db_harvest_job_error = interface.pget_harvest_job_errors(
        filter=text(f"harvest_job_id = '{job_data_dcatus['id']}'")
    )
    assert db_harvest_job_error[0].type == job_error_data["type"]
    assert db_harvest_job_error[0].id == harvest_job_error.id


def test_errors_by_job(
    interface_with_multiple_sources,
    job_data_dcatus,
    job_data_dcatus_2,
    record_error_data,
    record_error_data_2,
):
    interface = interface_with_multiple_sources
    job_id = job_data_dcatus["id"]
    count = interface.get_harvest_record_errors_by_job(
        job_id,
        count=True,
    )
    all_errors_count = interface.pget_harvest_record_errors(
        count=True,
    )
    assert count == len(record_error_data)
    assert all_errors_count == len(record_error_data) + len(record_error_data_2)


def test_record_errors_summary_by_job(
    interface_with_multiple_sources,
    job_data_dcatus,
    record_error_data,
):
    interface = interface_with_multiple_sources
    job_id = job_data_dcatus["id"]
    summary = interface.get_record_errors_summary_by_job(
        job_id,
    )
    assert sum(item["count"] for item in summary) == len(
        interface.get_harvest_record_errors_by_job(job_id, per_page=999)
    )
    assert all(set(item) == {"severity", "type", "count"} for item in summary)


def test_add_harvest_job_with_id(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    job = interface.add_harvest_job(job_data_dcatus)
    assert job.id == job_data_dcatus["id"]
    assert job.status == job_data_dcatus["status"]
    assert job.harvest_source_id == job_data_dcatus["harvest_source_id"]


def test_add_harvest_job_without_id(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)

    job_data_dcatus_id = job_data_dcatus["id"]
    del job_data_dcatus["id"]
    job = interface.add_harvest_job(job_data_dcatus)
    assert job.id
    assert job.id != job_data_dcatus_id
    assert job.status == job_data_dcatus["status"]
    assert job.harvest_source_id == job_data_dcatus["harvest_source_id"]


def test_add_harvest_job_records_warned(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    job_data_dcatus["records_warned"] = 3

    job = interface.add_harvest_job(job_data_dcatus)
    assert job.records_warned == 3


def test_update_harvest_job_records_warned(interface_no_jobs, job_data_dcatus):
    interface_no_jobs.add_harvest_job(job_data_dcatus)
    updated = interface_no_jobs.update_harvest_job(
        job_data_dcatus["id"], {"records_warned": 42}
    )
    assert updated.records_warned == 42


def test_add_harvest_job_dcatus_catalog(
    interface, organization_data, source_data_dcatus, job_data_dcatus
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    catalog = {"@type": "Catalog", "title": "Test Catalog"}
    job_data_dcatus["dcatus_catalog"] = catalog

    job = interface.add_harvest_job(job_data_dcatus)
    assert job.dcatus_catalog == catalog


def test_update_harvest_job_dcatus_catalog(interface_no_jobs, job_data_dcatus):
    interface_no_jobs.add_harvest_job(job_data_dcatus)
    catalog = {"@type": "Catalog", "title": "Updated Catalog"}
    updated = interface_no_jobs.update_harvest_job(
        job_data_dcatus["id"], {"dcatus_catalog": catalog}
    )
    assert updated.dcatus_catalog == catalog


def test_get_all_harvest_jobs_by_facet(
    source_data_dcatus, interface_with_multiple_jobs
):
    source_id = source_data_dcatus["id"]
    filtered_list = interface_with_multiple_jobs.pget_harvest_jobs(
        facets=f"status eq new, harvest_source_id eq {source_id}"
    )
    assert len(filtered_list) == 3
    assert filtered_list[0].status == "new"
    assert filtered_list[0].harvest_source_id == source_data_dcatus["id"]


def get_new_harvest_jobs_in_past(interface_with_multiple_jobs):
    filtered_job_list = interface_with_multiple_jobs.get_new_harvest_jobs_in_past()
    all_jobs_list = interface_with_multiple_jobs.get_all_harvest_jobs()
    assert len(all_jobs_list) == 24
    assert len(filtered_job_list) == 2
    assert (
        len(
            [
                x
                for x in all_jobs_list
                if x["status"] == "new"
                and x["date_created"].replace(
                    tzinfo=timezone.utc
                )  # TODO should we be pushing to UTC in db?
                < datetime.now(timezone.utc)
            ]
        )
        == 2
    )


@freeze_time("Jan 14th, 2012")
def test_get_new_harvest_jobs_by_source_in_future(interface_with_multiple_jobs):
    all_jobs_list = interface_with_multiple_jobs.pget_harvest_jobs(paginate=False)
    source_id = all_jobs_list[0].harvest_source_id
    filtered_job_list = (
        interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(source_id)
    )
    assert len(all_jobs_list) == 12
    assert len(filtered_job_list) == 3
    assert (
        len(
            [
                x
                for x in all_jobs_list
                if x.status == "new"
                and x.date_created.replace(
                    tzinfo=timezone.utc
                )  # TODO should we be pushing to UTC in db?
                > datetime.now(timezone.utc)
                and x.harvest_source_id == source_id
            ]
        )
        == 3
    )


def test_filter_jobs_by_faceted_filter(
    source_data_dcatus, interface_with_multiple_jobs
):
    faceted_list = interface_with_multiple_jobs.pget_harvest_jobs(
        facets="status eq new"
    )

    assert len(faceted_list) == 3
    assert len([x for x in faceted_list if x.status == "new"]) == 3
    assert (
        len(
            [x for x in faceted_list if x.harvest_source_id == source_data_dcatus["id"]]
        )
        == 3
    )


def test_delete_harvest_job(
    interface_no_jobs,
    job_data_dcatus,
):
    interface_no_jobs.add_harvest_job(job_data_dcatus)
    res = interface_no_jobs.delete_harvest_job(job_data_dcatus["id"])
    assert isinstance(res, tuple)
    assert res[0] == "Harvest job deleted successfully"
    assert res[1] == 200
