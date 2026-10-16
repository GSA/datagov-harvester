import json

from harvester.harvest import HarvestSource, Record
from harvester.utils.general_utils import dataset_to_hash, sort_dataset


class TestCompare:
    def test_compare(
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
        for record in internal_compare_data["records"]:
            data = {
                "identifier": record["identifier"],
                "harvest_job_id": internal_compare_data["job_id"],
                "harvest_source_id": internal_compare_data["harvest_source_id"],
                "source_hash": dataset_to_hash(sort_dataset(record)),
                "status": "success",
                "action": "create",
            }
            interface.add_harvest_record(data)

        harvest_source = HarvestSource(internal_compare_data["job_id"])
        harvest_source.run_full_harvest()
        harvest_source.report()

        assert harvest_source.reporter.added == 6
        assert harvest_source.reporter.updated == 1
        assert harvest_source.reporter.deleted == 1

        written_compare_records = interface.get_harvest_records_by_job(
            internal_compare_data["job_id"]
        )

        # 6 create + 1 update + 1 delete + 2 seeded records at beginning
        assert len(written_compare_records) == 10

    def test_current_errors_persist_when_record_unchanged(
        self,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        interface,
        internal_compare_data,
    ):
        """An unchanged record keeps its prior warning: compare() leaves
        action=None, so harvest() returns before ever touching
        sync_current_issues (#6440)."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        first_record_data = internal_compare_data["records"][0]
        record_hash = dataset_to_hash(sort_dataset(first_record_data))
        source_id = internal_compare_data["harvest_source_id"]
        db_record = interface.add_harvest_record(
            {
                "identifier": first_record_data["identifier"],
                "harvest_job_id": internal_compare_data["job_id"],
                "harvest_source_id": source_id,
                "source_hash": record_hash,
                "status": "success",
                "action": "create",
            }
        )
        interface.add_harvest_record_error(
            {
                "message": "a carried-forward warning",
                "type": "TestException",
                "harvest_job_id": internal_compare_data["job_id"],
                "harvest_record_id": db_record.id,
                "severity": "warning",
            }
        )
        assert interface.get_current_record_issues_by_source(source_id, count=True) == 1

        # a later job runs over the exact same (unchanged) content
        second_job = interface.add_harvest_job(
            {"status": "new", "harvest_source_id": source_id}
        )
        harvest_source = HarvestSource(second_job.id)
        harvest_source.acquire_minimum_internal_data()

        record = Record(
            harvest_source,
            first_record_data["identifier"],
            json.dumps(first_record_data),
            record_hash,
        )
        record.compare()

        assert record.action is None
        assert interface.get_current_record_issues_by_source(source_id, count=True) == 1

    def test_record_with_non_dcatus_fields_is_prepared_not_errored(
        self,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        interface,
    ):
        """
        regression test for https://github.com/GSA/data.gov/issues/5450

        the production symptom was that records carrying vendor-specific
        (non-DCAT-US) fields errored out of the job entirely, because
        external_records_to_process sorts each record before hashing it and
        the sort raised TypeError. this exercises that real code path
        rather than sort_dataset in isolation.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        # shaped after the ArcGIS record in the issue: a vendor "metadata"
        # field holding sibling dicts that share a first key whose values
        # are themselves dicts, which is what could not be ordered.
        record = {
            "identifier": "arcgis-non-dcatus-fields",
            "title": "Record with non-dcatus fields",
            "metadata": {
                "distInfo": {
                    "distributor": [
                        {"distorCont": {"rpOrgName": "FRA"}, "role": "originator"},
                        {"distorCont": {"rpOrgName": "DOT"}, "role": "distributor"},
                    ]
                }
            },
        }

        harvest_source = HarvestSource(job_data_dcatus["id"])
        harvest_source.external_records = [record]

        records = list(harvest_source.external_records_to_process())

        assert harvest_source.reporter.errored == 0
        assert len(records) == 1
        assert records[0].identifier == "arcgis-non-dcatus-fields"

        # the vendor fields survive into source_raw (canonically reordered,
        # but not dropped), which is re-parsed downstream for transformation
        distributors = json.loads(records[0].source_raw)["metadata"]["distInfo"][
            "distributor"
        ]
        assert {d["distorCont"]["rpOrgName"] for d in distributors} == {"FRA", "DOT"}

    def test_parent_identifier_change_forces_update(
        self,
        organization_data,
        source_data_dcatus3_0_service_serves_dataset,
        job_data_dcatus3_0_service_serves_dataset,
        interface,
    ):
        """
        a DataService/DatasetSeries identifier change must still force an
        update of its already-synced child dataset even though the child's
        own content (and thus its metadata_hash) hasn't changed, otherwise
        the child's isPartOf link in catalog goes stale.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_service_serves_dataset)
        first_job = interface.add_harvest_job(job_data_dcatus3_0_service_serves_dataset)

        first_source = HarvestSource(first_job.id)
        first_source.acquire_minimum_external_data()
        child = next(
            r
            for r in first_source.external_records_to_process()
            if r.identifier == "https://example.gov/datasets/served-by-service-one"
        )
        assert child.parent_identifier == "https://example.gov/services/one"

        # seed a prior successful harvest of the same child, but tagged with
        # a different (now-outdated) parent identifier, as if the
        # DataService's identifier had changed between harvests
        interface.add_harvest_record(
            {
                "identifier": child.identifier,
                "harvest_job_id": first_job.id,
                "harvest_source_id": source_data_dcatus3_0_service_serves_dataset["id"],
                "source_hash": child.metadata_hash,
                "parent_identifier": "https://example.gov/services/outdated",
                "status": "success",
                "action": "create",
                "record_type": "dataset",
                "ckan_id": "existing-ckan-id",
            }
        )

        second_job_data = dict(job_data_dcatus3_0_service_serves_dataset)
        second_job_data["id"] = "a0b1c2d3-e4f5-4a6b-8c9d-0e1f2a3b4c5d"
        second_job = interface.add_harvest_job(second_job_data)

        second_source = HarvestSource(second_job.id)
        second_source.acquire_data_sources()
        second_child = next(
            r
            for r in second_source.external_records_to_process()
            if r.identifier == "https://example.gov/datasets/served-by-service-one"
        )
        second_child.compare()

        assert second_child.action == "update"
        assert second_child.ckan_id == "existing-ckan-id"
