import json
from itertools import groupby

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
        harvest_source.extract()

        harvest_job_records = interface.get_harvest_records_by_job(
            job_data_dcatus["id"], paginate=False
        )
        records = {}
        for key, group in groupby(
            harvest_job_records, lambda x: x.action if x.status != "error" else None
        ):
            records[key] = sum(1 for _ in group)
        assert records["create"] == 6
        assert records["update"] == 1
        assert records["delete"] == 1

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
        harvest_source.extract()

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
        record_identifiers = []
        for record in harvest_source.records:
            record_identifiers.append(record.identifier)

        assert len(record_identifiers) == len(expected)
        assert sorted(list(record_identifiers)) == expected
