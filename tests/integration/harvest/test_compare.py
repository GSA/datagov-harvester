import json

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
        harvest_source.get_record_changes()

        assert len(harvest_source.compare_data["create"]) == 6
        assert len(harvest_source.compare_data["update"]) == 1
        assert len(harvest_source.compare_data["delete"]) == 1

    # TODO: add sort test

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
                    "source_raw": json.dumps(record),
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
