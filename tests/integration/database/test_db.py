from datetime import datetime, timezone

from sqlalchemy import text

from database.models import HarvestJobError, HarvestRecordError


class TestDatabase:
    def test_add_organization(self, interface, organization_data):
        org = interface.add_organization(organization_data)

        assert org is not None
        assert org.name == "Test Org"

    def test_get_all_organizations(self, interface, organization_data):
        interface.add_organization(organization_data)

        orgs = interface.get_all_organizations()
        assert len(orgs) > 0
        assert orgs[0].name == "Test Org"

    def test_update_organization(self, interface, organization_data):
        org = interface.add_organization(organization_data)

        updates = {"name": "Updated Org"}
        updated_org = interface.update_organization(org.id, updates)
        assert updated_org.name == "Updated Org"

    def test_delete_organization(self, interface, organization_data):
        org = interface.add_organization(organization_data)

        result = interface.delete_organization(org.id)
        assert result == "Organization deleted successfully"

    def test_add_harvest_source(self, interface, organization_data, source_data_dcatus):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)

        assert source is not None
        assert source.name == source_data_dcatus["name"]

    def test_get_all_harvest_sources(
        self, interface, organization_data, source_data_dcatus
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)

        sources = interface.get_all_harvest_sources()
        assert len(sources) > 0
        assert sources[0].name == source_data_dcatus["name"]

    def test_get_harvest_source(self, interface, organization_data, source_data_dcatus):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)

        fetched_source = interface.get_harvest_source(source.id)
        assert fetched_source is not None
        assert fetched_source.name == source_data_dcatus["name"]

    def test_update_harvest_source(
        self, interface, organization_data, source_data_dcatus
    ):
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
        self,
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
        assert response == "Harvest source deleted successfully"

        # Refresh the session to avoid ObjectDeletedError
        interface.db.expire_all()

        deleted_source = interface.get_harvest_source(source.id)
        assert deleted_source is None

        # Case 2: Harvest source has records, so deletion should fail
        # Add the harvest source again
        source = interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)
        interface.add_harvest_record(record_data_dcatus[0])

        response = interface.delete_harvest_source(source.id)
        assert response == (
            "Failed: 1 records in the Harvest source, please Clear it first."
        )

        # Ensure the source still exists after failed deletion attempt
        source_still_exists = interface.get_harvest_source(source.id)
        assert source_still_exists is not None

    def test_harvest_source_by_jobid(
        self, interface, organization_data, source_data_dcatus, job_data_dcatus
    ):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)
        job_data_dcatus["harvest_source_id"] = source.id

        harvest_job = interface.add_harvest_job(job_data_dcatus)
        harvest_source = interface.get_harvest_source_by_jobid(harvest_job.id)

        assert source.id == harvest_source.id

    def test_add_harvest_job_error(
        self,
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

    def test_add_harvest_record_error(
        self,
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
        interface.add_harvest_record(record_data_dcatus[0])

        harvest_record_error = interface.add_harvest_record_error(record_error_data[0])
        assert isinstance(harvest_record_error, HarvestRecordError)
        assert harvest_record_error.message == record_error_data[0]["message"]

        harvest_record_error_from_db = interface.get_harvest_error(
            harvest_record_error.id
        )
        assert harvest_record_error.id == harvest_record_error_from_db.id
        assert (
            harvest_record_error.harvest_record_id
            == harvest_record_error_from_db.harvest_record_id
        )

    def test_add_harvest_record(
        self,
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

    def test_add_harvest_records(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        for record in record_data_dcatus:
            del record["id"]

        id_lookup_table = interface.add_harvest_records(record_data_dcatus)
        db_records = interface.pget_harvest_records()
        assert len(id_lookup_table) == 10
        assert len(db_records) == 10
        assert id_lookup_table[db_records[0].identifier] == db_records[0].id

    def test_endpoint_pagnation(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        records = []
        for i in range(100):
            new_record = record_data_dcatus[0].copy()
            del new_record["id"]
            new_record["identifier"] = f"test-identifier-{i}"
            records.append(new_record)

        id_lookup_table = interface.add_harvest_records(records)

        # get first page
        db_records = interface.pget_harvest_records(page=0)
        assert len(db_records) == 20
        assert db_records[0].identifier == "test-identifier-0"
        assert id_lookup_table[db_records[0].identifier] == db_records[0].id

        # get second page
        db_records = interface.pget_harvest_records(page=1)
        assert len(db_records) == 20
        assert db_records[0].identifier == "test-identifier-20"
        assert id_lookup_table[db_records[0].identifier] == db_records[0].id

        # get first page again
        db_records = interface.pget_harvest_records(page=0)
        assert len(db_records) == 20
        assert db_records[0].identifier == "test-identifier-0"
        assert id_lookup_table[db_records[0].identifier] == db_records[0].id

        # don't paginate via feature flag
        db_records = interface.pget_harvest_records(paginate=False)
        assert len(db_records) == 100
        assert id_lookup_table[db_records[50].identifier] == db_records[50].id

        # get page 6 (r. 100 - 119), which is out of bounds / empty
        db_records = interface.pget_harvest_records(page=6)
        assert len(db_records) == 0

        db_records = interface.pget_harvest_records(
            filter=text(f"id = '{id_lookup_table['test-identifier-0']}'")
        )
        assert len(db_records) == 1
        assert db_records[0].harvest_job_id == job_data_dcatus["id"]

    def test_endpoint_count(
        self, interface_with_fixture_json, job_data_dcatus, record_data_dcatus
    ):
        interface = interface_with_fixture_json
        job_id = job_data_dcatus["id"]
        count = interface.get_harvest_record_errors_by_job(
            job_id, count=True, skip_pagination=True
        )
        assert count == len(record_data_dcatus)

    def test_errors_by_job(
        self,
        interface_with_multiple_sources,
        job_data_dcatus,
        job_data_dcatus_2,
        record_error_data,
        record_error_data_2,
    ):
        interface = interface_with_multiple_sources
        job_id = job_data_dcatus["id"]
        count = interface.get_harvest_record_errors_by_job(
            job_id, count=True, skip_pagination=True
        )
        all_errors_count = interface.pget_harvest_record_errors(
            count=True,
            skip_pagination=True,
        )
        assert count == len(record_error_data)
        assert all_errors_count == len(record_error_data) + len(record_error_data_2)

    def test_add_harvest_job_with_id(
        self, interface, organization_data, source_data_dcatus, job_data_dcatus
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        job = interface.add_harvest_job(job_data_dcatus)
        assert job.id == job_data_dcatus["id"]
        assert job.status == job_data_dcatus["status"]
        assert job.harvest_source_id == job_data_dcatus["harvest_source_id"]

    def test_add_harvest_job_without_id(
        self, interface, organization_data, source_data_dcatus, job_data_dcatus
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

    def test_get_all_harvest_jobs_by_filter(
        self, source_data_dcatus, interface_with_multiple_jobs
    ):
        filters = {
            "status": "new",
            "harvest_source_id": f"{source_data_dcatus['id']}",
        }
        filtered_list = interface_with_multiple_jobs.get_all_harvest_jobs_by_filter(
            filters
        )
        assert len(filtered_list) == 3
        assert filtered_list[0].status == "new"
        assert filtered_list[0].harvest_source_id == source_data_dcatus["id"]

    def get_new_harvest_jobs_in_past(self, interface_with_multiple_jobs):
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

    def test_get_new_harvest_jobs_by_source_in_future(
        self, interface_with_multiple_jobs
    ):
        all_jobs_list = interface_with_multiple_jobs.pget_harvest_jobs(paginate=False)
        source_id = all_jobs_list[0].harvest_source_id
        filtered_job_list = (
            interface_with_multiple_jobs.get_new_harvest_jobs_by_source_in_future(
                source_id
            )
        )
        assert len(all_jobs_list) == 12
        assert len(filtered_job_list) == 2
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
            == 2
        )

    def test_filter_jobs_by_faceted_filter(
        self, source_data_dcatus, interface_with_multiple_jobs
    ):
        faceted_list = interface_with_multiple_jobs.get_harvest_jobs_by_faceted_filter(
            "status", ["new", "in_progress"]
        )
        assert len(faceted_list) == 6
        assert len([x for x in faceted_list if x.status == "new"]) == 3
        assert (
            len(
                [
                    x
                    for x in faceted_list
                    if x.harvest_source_id == source_data_dcatus["id"]
                ]
            )
            == 6
        )

    def test_delete_harvest_job(
        self,
        interface_no_jobs,
        job_data_dcatus,
    ):
        interface_no_jobs.add_harvest_job(job_data_dcatus)
        res = interface_no_jobs.delete_harvest_job(job_data_dcatus["id"])
        assert isinstance(res, str)
        assert res == "Harvest job deleted successfully"

    def test_get_latest_harvest_records(
        self,
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
        interface.add_harvest_records(latest_records)

        latest_records = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )

        # remove so compare works
        for record in latest_records:
            del record["id"]

        expected_records = [
            {
                "identifier": "a",
                "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
                "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
                "source_hash": None,
                "source_raw": "data_1",
                "date_created": datetime(2024, 3, 1, 0, 0, 0, 1000),
                "date_finished": None,
                "ckan_id": None,
                "ckan_name": None,
                "action": "update",
                "status": "success",
            },
            {
                "identifier": "b",
                "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
                "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
                "source_hash": None,
                "source_raw": "data_10",
                "date_created": datetime(2024, 3, 1, 0, 0, 0, 1000),
                "date_finished": None,
                "ckan_id": None,
                "ckan_name": None,
                "action": "create",
                "status": "success",
            },
            {
                "identifier": "c",
                "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
                "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
                "source_hash": None,
                "source_raw": "data_12",
                "date_created": datetime(2024, 5, 1, 0, 0, 0, 1000),
                "date_finished": None,
                "ckan_id": None,
                "ckan_name": None,
                "action": "create",
                "status": "success",
            },
            {
                "identifier": "e",
                "harvest_job_id": "6bce761c-7a39-41c1-ac73-94234c139c76",
                "harvest_source_id": "2f2652de-91df-4c63-8b53-bfced20b276b",
                "source_hash": None,
                "source_raw": "data_123",
                "date_created": datetime(2024, 4, 3, 0, 0, 0, 1000),
                "date_finished": None,
                "ckan_id": None,
                "ckan_name": None,
                "action": "create",
                "status": "success",
            },
        ]

        assert len(latest_records) == 4
        # make sure there aren't records that are different
        assert not any(x != y for x, y in zip(latest_records, expected_records))

    def test_faceted_builder_queries(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        records = []
        for i in range(100):
            new_record = record_data_dcatus[0].copy()
            del new_record["id"]
            new_record["identifier"] = f"test-identifier-{i}"
            records.append(new_record)

        id_lookup_table = interface.add_harvest_records(records)

        # # source id, no facets
        db_records = interface.get_harvest_records_by_source(source_data_dcatus["id"])
        assert len(db_records) == 20

        # source id, plus page kwarg
        db_records = interface.get_harvest_records_by_source(
            source_data_dcatus["id"], page=1
        )
        assert len(db_records) == 20
        assert db_records[0].identifier == "test-identifier-20"

        # source id, plus pagination flag
        db_records = interface.get_harvest_records_by_source(
            source_data_dcatus["id"], paginate=False
        )
        assert len(db_records) == 100

        # source id, plus kwargs to return only count
        db_records = interface.get_harvest_records_by_source(
            source_data_dcatus["id"], skip_pagination=True, count=True
        )
        assert db_records == 100

        # source id, plus extra filter_text facet
        db_records = interface.get_harvest_records_by_source(
            source_data_dcatus["id"],
            facets=[f"id = '{id_lookup_table['test-identifier-0']}'"],
        )
        assert len(db_records) == 1

        # source id, plus extra filter_text facet, plus kwargs to return only count
        db_records = interface.get_harvest_records_by_source(
            source_data_dcatus["id"],
            facets=[f"id = '{id_lookup_table['test-identifier-0']}'"],
            skip_pagination=True,
            count=True,
        )
        assert db_records == 1
