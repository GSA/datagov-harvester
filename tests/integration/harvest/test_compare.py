from harvester.harvest import HarvestSource
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

    # TODO: add sort test
