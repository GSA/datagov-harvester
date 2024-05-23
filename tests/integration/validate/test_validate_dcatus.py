from harvester.harvest import HarvestSource


class TestValidateDCATUS:
    def test_validate_dcatus(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        test_record = harvest_source.external_records["cftc-dc1"]
        test_record.validate()

        assert test_record.valid is True
