from datetime import datetime, timezone

from database.models import HarvestJobError, HarvestRecordError
from harvester.harvest import HarvestSource
from harvester.utils.general_utils import dataset_to_hash, sort_dataset


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
        self, interface, organization_data, source_data_dcatus
    ):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)

        assert source is not None

        response = interface.delete_harvest_source(source.id)
        assert response == "Harvest source deleted successfully"

        deleted_source = interface.get_harvest_source(source.id)
        assert deleted_source is None

    def test_clear_harvest_source(
        self, interface, organization_data, source_data_dcatus
    ):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)

        assert source is not None

        response = interface.clear_harvest_source(source.id)
        assert response == "Harvest source cleared successfully"

        deleted_records = interface.get_harvest_record_by_source(source.id)
        assert deleted_records is None

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

        record = interface.add_harvest_record(record_data_dcatus)

        assert record.harvest_source_id == source.id
        assert record.harvest_job_id == harvest_job.id

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
        interface.add_harvest_record(record_data_dcatus)

        harvest_record_error = interface.add_harvest_record_error(record_error_data)
        assert isinstance(harvest_record_error, HarvestRecordError)
        assert harvest_record_error.message == record_error_data["message"]

        harvest_record_error_from_db = interface.get_harvest_error(
            harvest_record_error.id
        )
        assert harvest_record_error.id == harvest_record_error_from_db.id
        assert (
            harvest_record_error.harvest_record_id
            == harvest_record_error_from_db.harvest_record_id
        )

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

        records = []
        for i in range(10):
            new_record = record_data_dcatus.copy()
            del new_record["id"]
            new_record["identifier"] = f"test-identifier-{i}"
            records.append(new_record)

        id_lookup_table = interface.add_harvest_records(records)
        db_records = interface.get_all_harvest_records()
        assert len(id_lookup_table) == 10
        assert len(db_records) == 10
        assert id_lookup_table[db_records[0].identifier] == db_records[0].id

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
        all_jobs_list = interface_with_multiple_jobs.get_all_harvest_jobs()
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
                "action": "create",
                "status": "success",
            },
        ]

        assert len(latest_records) == 4
        # make sure there aren't records that are different
        assert not any(x != y for x, y in zip(latest_records, expected_records))

    def test_write_compare_to_db(
        self,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        interface,
        internal_compare_data,
    ):
        # add the necessary records to satisfy FKs
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        # prefill with records
        records = []
        for record in internal_compare_data["records"]:
            records.append(
                {
                    "identifier": record["identifier"],
                    "harvest_job_id": job_data_dcatus["id"],
                    "harvest_source_id": job_data_dcatus["harvest_source_id"],
                    "source_hash": dataset_to_hash(sort_dataset(record)),
                    "source_raw": str(record),
                }
            )

        interface.add_harvest_records(records)

        harvest_source = HarvestSource(job_data_dcatus["id"])
        harvest_source.get_record_changes()

        harvest_source.write_compare_to_db()

        expected = sorted(
            [
                "cftc-dc3",
                "cftc-dc7",
                "cftc-dc5",
                "cftc-dc4",
                "cftc-dc6",
                "cftc-dc1",
                "cftc-dc2",
            ]
        )
        assert len(harvest_source.internal_records_lookup_table) == len(expected)
        assert sorted(list(harvest_source.internal_records_lookup_table)) == expected
