import os

from harvester.harvest import harvest_job_starter

HARVEST_SOURCE_URL = os.getenv("HARVEST_SOURCE_URL")


class TestHarvestJobValidate:
    def test_validate_single_valid_record(
        self,
        interface,
        organization_data,
        source_data_dcatus_single_record,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_single_record["id"],
                "job_type": "validate",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 0
        assert harvest_job.records_validated == 1

    def test_validate_single_invalid_record(
        self,
        interface,
        organization_data,
        source_data_dcatus_invalid,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_invalid)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_invalid["id"],
                "job_type": "validate",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        harvest_job_starter(job_id, job_type)
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_errored == 1
        assert harvest_job.records_validated == 0

    def skip_test_valiate_new_against_existing_source(self):
        # TODO: seed a source with harvest data and validate
        # confirms we are only validating a single record.
        pass

    def skip_test_validate_non_dcat_record(self):
        # TODO: validate a transformed record
        pass
