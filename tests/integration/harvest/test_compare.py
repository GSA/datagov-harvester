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
