class TestDatabase:
    def test_add_organization(self, interface, organization_data):
        org = interface.add_organization(organization_data)

        assert org is not None
        assert org.name == "Test Org"

    def test_get_all_organizations(self, interface, organization_data):
        interface.add_organization(organization_data)

        orgs = interface.get_all_organizations()
        assert len(orgs) > 0
        assert orgs[0]["name"] == "Test Org"

    def test_update_organization(self, interface, organization_data):
        org = interface.add_organization(organization_data)

        updates = {"name": "Updated Org"}
        updated_org = interface.update_organization(org.id, updates)
        assert updated_org["name"] == "Updated Org"

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
        assert sources[0]["name"] == source_data_dcatus["name"]

    def test_get_harvest_source(self, interface, organization_data, source_data_dcatus):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)

        fetched_source = interface.get_harvest_source(source.id)
        assert fetched_source is not None
        assert fetched_source["name"] == source_data_dcatus["name"]

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
        assert updated_source["name"] == updates["name"]
        assert updated_source["notification_emails"] == [
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

    def test_harvest_source_by_jobid(
        self, interface, organization_data, source_data_dcatus, job_data_dcatus
    ):
        interface.add_organization(organization_data)
        source = interface.add_harvest_source(source_data_dcatus)
        job_data_dcatus["harvest_source_id"] = source.id

        harvest_job = interface.add_harvest_job(job_data_dcatus)
        harvest_source = interface.get_source_by_jobid(harvest_job.id)

        assert source.id == harvest_source["id"]

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
            new_record["identifier"] = f"test-identifier-{i}"
            records.append(new_record)

        id_lookup_table = interface.add_harvest_records(records)
        db_records = interface.get_all_harvest_records()
        assert len(id_lookup_table) == 10
        assert len(db_records) == 10
        assert id_lookup_table[db_records[0]["identifier"]] == db_records[0]["id"]

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

    def test_get_harvest_jobs_by_filter(
        self, source_data_dcatus, interface_with_multiple_jobs
    ):
        filters = {
            "status": "pending",
            "harvest_source_id": f"{source_data_dcatus['id']}",
        }
        filtered_list = interface_with_multiple_jobs.get_harvest_jobs_by_filter(filters)
        assert len(filtered_list) == 1
        assert filtered_list[0]["status"] == "pending"
        assert filtered_list[0]["harvest_source_id"] == source_data_dcatus["id"]

    def test_filter_jobs_by_faceted_filter(
        self, source_data_dcatus, interface_with_multiple_jobs
    ):
        faceted_list = interface_with_multiple_jobs.get_harvest_jobs_by_faceted_filter(
            "status", ["pending", "pending_manual"]
        )
        assert len(faceted_list) == 4
        assert len([x for x in faceted_list if x["status"] == "pending"]) == 2
        assert (
            len(
                [
                    x
                    for x in faceted_list
                    if x["harvest_source_id"] == source_data_dcatus["id"]
                ]
            )
            == 2
        )

    def test_get_latest_harvest_records(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        source_data_dcatus_2,
        job_data_dcatus,
        latest_records,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        # another source for querying against. see last record in
        # `latest_records` fixture
        interface.add_harvest_source(source_data_dcatus_2)
        interface.add_harvest_job(job_data_dcatus)
        interface.add_harvest_records(latest_records)

        latest_records = interface.get_latest_records_by_source(
            source_data_dcatus["id"]
        )

        assert len(latest_records) == 4
